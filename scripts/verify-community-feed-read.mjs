/**
 * Asserts what GET /feed returns: every post the reader may see, exactly once, in time
 * order across pages — whichever half of the feed each post lives in.
 *
 * THE FIXTURE IS A TIMELINE, AND EACH SPACE IS THERE TO SHOW ONE FAILURE:
 *   - SMALL is fanned out. The materialised half.
 *   - LARGE is past the threshold and read at query time. The other half.
 *   - SHRINK is large when its post is made and small afterwards. Its post was never
 *     fanned out, so a read path that judged "large" by the space's CURRENT size would
 *     stop querying it and the post would be in neither half. It must still appear.
 *   - GROW is small at its first post and large at its second. The first is in
 *     FeedEntry; a read path that ignored the fan-out flag would return it from both
 *     halves. Each must appear once.
 *   - BANNED holds three posts in the MIDDLE of the timeline, then bans the reader.
 *     They must not appear — and because they sit mid-timeline, a read path that
 *     filtered after cutting the page would let them eat a page's window, and the
 *     oldest real post would be skipped. The exactly-once walk is what catches that.
 *
 * AND THE SCORE MOVES MID-WALK. Between pages one and two the oldest post gets a
 * comment, which lifts its rankScore by ln(3) - ln(2), about five hours' worth of the
 * time term. A cursor over rankScore would skip or repeat it; one over (createdAt, id)
 * must not notice. That is the property the 2026-09-21 decision was made for.
 *
 * Scoped to two tenants created for this run.
 *
 * Run (with Postgres, Redis, Kafka and the Schema Registry up, topics created and
 * schemas registered, Community and the relay built):
 *   node scripts/verify-community-feed-read.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const RUN = randomUUID().replace(/-/g, '').slice(0, 8);
const TENANT = `tenant_fr_${RUN}`;
const OTHER_TENANT = `tenant_fr_other_${RUN}`;
const PORT = 3820;
/** Three: SMALL/GROW/BANNED start at two members, LARGE/SHRINK at four. */
const FANOUT_MAX = 3;
const GROUP = `community-feed-read-${RUN}`;
/** Small pages, so eight posts need five of them and every boundary gets crossed. */
const PAGE = 2;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  FEED_FANOUT_MAX_MEMBERS: String(FANOUT_MAX),
  FEED_CONSUMER_GROUP: GROUP,
  // `latest`, after asserting the consumer holds partitions: the topic has a week of
  // other runs' messages this run has no business applying.
  FEED_START_FROM: 'latest',
  RATE_LIMIT_SPACE_PER_MINUTE: '50',
  RATE_LIMIT_JOIN_PER_MINUTE: '50',
  RATE_LIMIT_POST_PER_MINUTE: '50',
  RATE_LIMIT_COMMENT_PER_MINUTE: '50',
  AUTH_PUBLIC_KEY_B64: Buffer.from(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem'), 'utf8')).toString('base64'),
});
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['COMMUNITY_RELAY_DATABASE_URL'] ??
    'postgresql://eventhub_community_relay:eventhub_community_relay@127.0.0.1:5432/eventhub_community',
  RELAY_FORBIDDEN_TABLE: 'Post',
  RELAY_METRICS_PORT: '3821',
});

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const excerpt = (log) => (log.length <= 2000 ? log : `${log.slice(0, 1500)}\n  [...]\n${log.slice(-500)}`);

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

const b64url = (v) => Buffer.from(v).toString('base64url');
const signingKey = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
function token(tenantId, userId) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({ sub: userId, tenantId, role: 'ATTENDEE', iss: communityEnv.JWT_ISSUER, aud: communityEnv.JWT_AUDIENCE, iat: now, exp: now + 900 }),
  );
  return `${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), signingKey))}`;
}

async function api(path, { method = 'GET', bearer, body } = {}) {
  const res = await fetch(`http://127.0.0.1:${PORT}${path}`, {
    method,
    headers: { ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = { raw: text };
  }
  return { status: res.status, body: parsed };
}

function start(dir, args, env) {
  const child = spawn(process.execPath, args, { cwd: join(ROOT, 'services', dir), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  return { child, log: () => log };
}

function relayOnce() {
  return new Promise((done) => {
    const relay = start('relay', ['dist/index.js', '--once'], relayEnv);
    const timer = setTimeout(() => relay.child.kill(), 60_000);
    relay.child.on('exit', (code) => {
      clearTimeout(timer);
      done({ code, log: relay.log() });
    });
  });
}

async function inTenant(client, tenant, sql, params = []) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** (createdAt, id) as one comparable string. createdAt arrives in a fixed ISO format. */
const keyOf = (p) => `${p.createdAt}|${p.id}`;

async function main() {
  const taken = await fetch(`http://127.0.0.1:${PORT}/health`).then(
    () => true,
    () => false,
  );
  if (taken) {
    console.error(`FAIL: something already answers on :${PORT}. Stop it before running this.`);
    process.exitCode = 1;
    return;
  }

  const owner = new pg.Client({ connectionString: communityEnv.DATABASE_URL });
  await owner.connect();
  const kafka = new Kafka({ clientId: `verify-feed-read-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  const service = start('community', ['dist/index.js'], communityEnv);
  const metrics = () => api('/metrics').then((r) => r.body?.feed ?? null);
  /** Every POST_CREATED ends as exactly one of these three, so their sum counts posts handled. */
  const handled = async () => {
    const m = await metrics();
    return m ? m.postsFannedOutTotal + m.spacesTooLargeToFanOutTotal + m.missingSpaceTotal : -1;
  };

  try {
    console.log('');
    console.log('  the consumer is holding partitions before anything is produced');
    const ready = await waitFor(async () => (await api('/ready')).status === 200, 60_000);
    check(Boolean(ready), `CONTROL: Community is ready on :${PORT}`);
    const joined = ready && (await waitFor(async () => ((await metrics())?.assignedPartitions ?? 0) > 0, 60_000));
    check(Boolean(joined), `CONTROL: its feed consumer holds ${(await metrics())?.assignedPartitions ?? 0} partition(s)`);
    if (!ready || !joined) {
      console.error(`  Community's output:\n${excerpt(service.log())}`);
      return;
    }

    const A = `u_author_${RUN}`;
    const R = `u_reader_${RUN}`;
    const asA = token(TENANT, A);
    const asR = token(TENANT, R);
    const crowd = [0, 1].map((i) => ({ id: `u_crowd_${i}_${RUN}`, t: token(TENANT, `u_crowd_${i}_${RUN}`) }));

    const space = async (slug) => (await api('/spaces', { method: 'POST', bearer: asA, body: { slug: `${slug}-${RUN}`, name: slug } })).body?.id;
    const join = (spaceId, t) => api(`/spaces/${spaceId}/join`, { method: 'POST', bearer: t });
    const leave = (spaceId, t) => api(`/spaces/${spaceId}/members/me`, { method: 'DELETE', bearer: t });
    const post = async (spaceId, body) => {
      const r = await api(`/spaces/${spaceId}/posts`, { method: 'POST', bearer: asA, body: { body } });
      if (r.status !== 201) throw new Error(`posting "${body}" answered ${r.status}`);
      await sleep(15); // distinct milliseconds, so the timeline below is the order
      return r.body.id;
    };
    /**
     * Publish, then wait until the consumer has handled this many more posts.
     *
     * The relay pass is RETRIED inside the wait, and what each pass did is kept. With
     * the compose stack up, relay-community drains this same outbox, and a row it holds
     * locked is one this pass skips; one run here got 4 of 8 in 45 s, and nothing on
     * the page could say why. If this control fails it now reports what every pass
     * claimed and published, and the outbox rows still waiting, so the next time is a
     * diagnosis rather than a mystery. CI runs no compose relay, so there it is one pass.
     */
    /**
     * `before` is a baseline taken BEFORE the posts were created, passed in — not read
     * here. It used to be read here, and that was a race: with the compose stack up,
     * relay-community publishes a post within milliseconds of it being written, so the
     * consumer had often handled most of the batch before this function started
     * counting. One run reported "3 of 8" while all eight had been correctly handled —
     * the flags and the walk below both passed. The measurement that settled it: all
     * eight published within 0.4 s of being created, five by the compose relay.
     */
    const settle = async (before, n, label) => {
      const passes = [];
      const done = await waitFor(async () => {
        const pass = await relayOnce();
        const line = pass.log.split('\n').find((l) => l.includes('"drained'));
        passes.push(`exit ${pass.code}${line ? ` ${line.match(/"claimed":\d+,"published":\d+/)?.[0] ?? ''}` : ''}`);
        return (await handled()) >= before + n;
      }, 90_000, 2_000);
      const waiting = await inTenant(owner, TENANT, `SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "publishedAt" IS NULL AND "tenantId" = $1`, [TENANT]);
      check(
        Boolean(done),
        `CONTROL: the consumer handled ${label} (${(await handled()) - before} of ${n})` +
          (done ? '' : ` — relay passes: ${passes.join('; ')}; outbox rows still unpublished: ${waiting.rows[0].n}`),
      );
    };

    console.log('');
    console.log('  the fixture: five spaces, eleven posts, three of them the reader may not see');
    const S = { small: await space('small'), large: await space('large'), shrink: await space('shrink'), grow: await space('grow'), banned: await space('banned') };
    for (const id of Object.values(S)) await join(id, asR);
    for (const c of crowd) {
      await join(S.large, c.t);
      await join(S.shrink, c.t);
    }

    const P = {};
    const firstMark = await handled();
    P.s1 = await post(S.small, 's1');
    P.l1 = await post(S.large, 'l1');
    P.b1 = await post(S.banned, 'b1');
    P.b2 = await post(S.banned, 'b2');
    P.b3 = await post(S.banned, 'b3');
    P.s2 = await post(S.small, 's2');
    P.k1 = await post(S.shrink, 'k1');
    P.g1 = await post(S.grow, 'g1');
    await settle(firstMark, 8, 'the first eight posts');

    // Now the spaces change size, AFTER the consumer judged the posts above.
    for (const c of crowd) {
      await leave(S.shrink, c.t);
      await join(S.grow, c.t);
    }
    const ban = await api(`/spaces/${S.banned}/members/${R}`, { method: 'PATCH', bearer: asA, body: { status: 'BANNED' } });
    check(ban.status === 200, `the reader is banned from BANNED after its posts were fanned out to them (${ban.status})`);

    const secondMark = await handled();
    P.g2 = await post(S.grow, 'g2');
    P.l2 = await post(S.large, 'l2');
    P.s3 = await post(S.small, 's3');
    await settle(secondMark, 3, 'the last three posts');

    const flags = await inTenant(owner, TENANT, `SELECT id, "fannedOut" FROM "Post" WHERE "tenantId" = $1`, [TENANT]);
    const fanned = new Map(flags.rows.map((r) => [r.id, r.fannedOut]));
    const want = { s1: true, s2: true, s3: true, g1: true, b1: true, b2: true, b3: true, l1: false, l2: false, k1: false, g2: false };
    const wrongFlags = Object.entries(want).filter(([k, v]) => fanned.get(P[k]) !== v).map(([k]) => k);
    check(
      wrongFlags.length === 0,
      `CONTROL: each post is in the half the fixture intends — fanned out: s1 s2 s3 g1 b1-3; read at query time: l1 l2 k1 g2${wrongFlags.length ? ` (wrong: ${wrongFlags.join(', ')})` : ''}`,
    );
    const sizes = await inTenant(owner, TENANT, `SELECT id, "memberCount" FROM "CommunitySpace" WHERE id = ANY($1)`, [[S.shrink, S.grow]]);
    const size = new Map(sizes.rows.map((r) => [r.id, Number(r.memberCount)]));
    check(
      size.get(S.shrink) <= FANOUT_MAX && size.get(S.grow) > FANOUT_MAX,
      `CONTROL: SHRINK is now small (${size.get(S.shrink)}) and GROW now large (${size.get(S.grow)}), so both crossings are real`,
    );

    const EXPECT = ['s1', 'l1', 's2', 'k1', 'g1', 'g2', 'l2', 's3'];
    const byId = new Map(Object.entries(P).map(([k, v]) => [v, k]));

    console.log('');
    console.log('  the whole feed, walked a page at a time, while a score moves');
    const pages = [];
    const scoreBefore = Number((await inTenant(owner, TENANT, `SELECT "rankScore" FROM "Post" WHERE id = $1`, [P.s1])).rows[0].rankScore);
    let cursor = null;
    for (let i = 0; ; i++) {
      if (i > 20) throw new Error('the feed did not end within twenty pages');
      const r = await api(`/feed?limit=${PAGE}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`, { bearer: asR });
      if (r.status !== 200) throw new Error(`GET /feed answered ${r.status}: ${JSON.stringify(r.body)}`);
      pages.push(r.body.posts);
      cursor = r.body.nextCursor;
      if (i === 0) {
        // The oldest post gets a comment between pages one and two.
        await api(`/posts/${P.s1}/comments`, { method: 'POST', bearer: asA, body: { body: 'moves the score' } });
      }
      if (!cursor) break;
    }
    const scoreAfter = Number((await inTenant(owner, TENANT, `SELECT "rankScore" FROM "Post" WHERE id = $1`, [P.s1])).rows[0].rankScore);
    check(
      scoreAfter - scoreBefore > 0.4,
      `CONTROL: s1's score rose mid-walk by ${(scoreAfter - scoreBefore).toFixed(3)}, past every post made in the ~5 hours before it`,
    );

    const seen = pages.flat().map((p) => byId.get(p.id) ?? `foreign:${p.id}`);
    const counts = new Map();
    for (const k of seen) counts.set(k, (counts.get(k) ?? 0) + 1);
    const missing = EXPECT.filter((k) => !counts.has(k));
    const twice = [...counts].filter(([, n]) => n > 1).map(([k]) => k);
    const extra = [...counts.keys()].filter((k) => !EXPECT.includes(k));
    check(
      missing.length === 0 && twice.length === 0 && extra.length === 0,
      `every post exactly once, across ${pages.length} pages (${seen.length} returned${missing.length ? `; MISSING ${missing.join(' ')}` : ''}${twice.length ? `; TWICE ${twice.join(' ')}` : ''}${extra.length ? `; NOT THEIRS ${extra.join(' ')}` : ''})`,
    );
    check(seen.includes('k1'), `k1 is there although SHRINK is small now and k1 was never fanned out${seen.includes('k1') ? '' : ' — MISSING'}`);
    check(
      (counts.get('g1') ?? 0) === 1,
      `g1 is there once although GROW is large now and g1 sits in FeedEntry (${counts.get('g1') ?? 0})`,
    );
    const banned = seen.filter((k) => /^b\d$/.test(k));
    check(banned.length === 0, `nothing from the space the reader was banned from (${banned.join(' ') || 'none'})`);

    // Across pages: everything on a later page is strictly older than everything on an
    // earlier one. Within a page the order is by rank, so this compares page bounds.
    const bounds = pages.filter((p) => p.length).map((p) => {
      const keys = p.map(keyOf).sort();
      return { newest: keys[keys.length - 1], oldest: keys[0] };
    });
    const overlaps = bounds.slice(1).filter((b, i) => !(b.newest < bounds[i].oldest)).length;
    check(bounds.length >= 4 && overlaps === 0, `each page is strictly older than the one before it (${bounds.length} non-empty pages, ${overlaps} overlap)`);

    // Within the last page: s1 now outranks l1, which is newer. Ranked, it comes first.
    const lastPage = pages.filter((p) => p.length).at(-1).map((p) => byId.get(p.id));
    check(
      lastPage.indexOf('s1') !== -1 && lastPage.indexOf('l1') !== -1 && lastPage.indexOf('s1') < lastPage.indexOf('l1'),
      `within a page the order is by rank: s1, older but commented, comes before l1 (${lastPage.join(', ')})`,
    );

    console.log('');
    console.log('  a cursor is input, and is refused like input');
    const garbage = await api('/feed?cursor=not-a-cursor', { bearer: asR });
    const impossible = await api(`/feed?cursor=${Buffer.from(JSON.stringify({ at: '2026-02-30T10:00:00.000', id: 'x' })).toString('base64url')}`, { bearer: asR });
    const tooMany = await api('/feed?limit=500', { bearer: asR });
    check(
      garbage.status === 400 && impossible.status === 400 && tooMany.status === 400,
      `garbage, 30 February and limit=500 are each a 400, not a 500 (${garbage.status}, ${impossible.status}, ${tooMany.status})`,
    );

    console.log('');
    console.log("  another tenant's reader sees none of it");
    const asOther = token(OTHER_TENANT, R); // the same user id, in another tenant
    const other = await api('/feed', { bearer: asOther });
    check(other.status === 200 && Array.isArray(other.body?.posts) && other.body.posts.length === 0, `an empty feed, even under the same user id (${other.status}, ${other.body?.posts?.length})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Community's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    const tables = ['FeedEntry', 'Reaction', 'Comment', 'Post', 'Membership', 'CommunitySpace', 'OutboxMessage'];
    let deleted = 0;
    let left = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        deleted += (await inTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      }
      for (const table of tables) {
        left += (await inTenant(owner, tenant, `SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rows: [{ n: -1 }] }))).rows[0].n;
      }
    }
    const groupsLeft = await deleteGroupsWhenEmpty(admin, [GROUP]).catch(() => [GROUP]);
    check(deleted > 0 && left === 0, `cleanup: this run's rows are gone (${deleted} deleted, ${left} left); consumer group ${groupsLeft.length ? 'left' : 'deleted'}`);
    await Promise.allSettled([owner.end(), admin.disconnect()]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every post the reader may see comes back once, in time order, from whichever half holds it, while its score moves');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
