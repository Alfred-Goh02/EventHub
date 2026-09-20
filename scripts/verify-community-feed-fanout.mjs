/**
 * Asserts what the fan-out consumer does with community.feed, and what it refuses to do.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - A post in a SMALL space reaches every ACTIVE member's feed, and nobody else's. A
 *     banned member getting a row would be invisible until the read path filtered it
 *     out for ever afterwards.
 *   - A post in a LARGE space is NOT fanned out. The control is a small space in the
 *     same run that IS, so "no rows" cannot pass by nothing having worked at all.
 *   - A REDELIVERED message writes nothing twice. Delivery is at-least-once, so this
 *     republishes the exact bytes and requires the row count to be unchanged. A
 *     consumer without the unique key looks identical until the day a partition
 *     replays.
 *   - The score is COPIED from the post, never adjusted by a delta. The same reaction
 *     message is applied twice and the copy must still equal the post: `rankScore + x`
 *     would be wrong by exactly x, and only ever on a redelivery.
 *   - A message this consumer can never apply goes to community.feed.dlq and the
 *     partition MOVES ON. A consumer that rethrew poison would stall for ever, and a
 *     consumer that dead-lettered an outage would lose good events.
 *   - Another tenant's post never lands in this tenant's feed.
 *
 * Scoped to two tenants created for this run.
 *
 * Run (with Postgres, Redis, Kafka and the Schema Registry up, the topics created and
 * schemas registered, Community and the relay built):
 *   node scripts/verify-community-feed-fanout.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const RUN = randomUUID().replace(/-/g, '').slice(0, 8);
const TENANT = `tenant_ff_${RUN}`;
const OTHER_TENANT = `tenant_ff_other_${RUN}`;
const PORT = 3818;
const TOPIC = 'community.feed';
const DLQ = 'community.feed.dlq';
/**
 * Small, so "too large" is one member away rather than five thousand.
 *
 * Three, not two, and the reason is a fixture bug worth keeping written down: the
 * small space holds an author, a member and a banned member, and BANNING DOES NOT
 * DECREMENT memberCount — the row is still there, the person simply cannot read. At a
 * threshold of two that space was itself "too large", nothing was fanned out, and two
 * assertions below passed on the empty result.
 */
const FANOUT_MAX = 3;
/** Its own group, so this run never inherits committed offsets from another. */
const GROUP = `community-feed-verify-${RUN}`;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  FEED_FANOUT_MAX_MEMBERS: String(FANOUT_MAX),
  FEED_CONSUMER_GROUP: GROUP,
  /**
   * latest, NOT earliest. The topic holds a week of other runs' messages; reading them
   * all would be slow and would count their outcomes into this run's metrics. The
   * order below is deliberate — the consumer is up and holding partitions BEFORE
   * anything is produced, which is asserted rather than assumed.
   */
  FEED_START_FROM: 'latest',
  RATE_LIMIT_SPACE_PER_MINUTE: '50',
  RATE_LIMIT_JOIN_PER_MINUTE: '50',
  RATE_LIMIT_POST_PER_MINUTE: '50',
  AUTH_PUBLIC_KEY_B64: Buffer.from(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem'), 'utf8')).toString('base64'),
});
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['COMMUNITY_RELAY_DATABASE_URL'] ??
    'postgresql://eventhub_community_relay:eventhub_community_relay@127.0.0.1:5432/eventhub_community',
  RELAY_FORBIDDEN_TABLE: 'Post',
  RELAY_METRICS_PORT: '3819',
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

/** One relay pass over Community's outbox, then exit. */
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

const feedRows = (owner, tenant, postId) =>
  inTenant(owner, tenant, `SELECT "userId", "rankScore" FROM "FeedEntry" WHERE "postId" = $1 ORDER BY "userId"`, [postId]).then((r) => r.rows);

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
  const kafka = new Kafka({ clientId: `verify-feed-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const producer = kafka.producer();
  await producer.connect();
  const registry = new SchemaRegistry({ host: relayEnv.SCHEMA_REGISTRY_URL });

  const service = start('community', ['dist/index.js'], communityEnv);
  const metrics = () => api('/metrics').then((r) => r.body?.feed ?? null);

  try {
    console.log('');
    console.log('  the consumer is up and holding partitions before anything is produced');
    const ready = await waitFor(async () => (await api('/ready')).status === 200, 60_000);
    check(Boolean(ready), `CONTROL: Community is ready on :${PORT}`);
    if (!ready) {
      console.error(`  Community's output:\n${excerpt(service.log())}`);
      return;
    }
    /**
     * THE ORDER MATTERS AND IS ASSERTED, NOT ASSUMED.
     *
     * The consumer starts from `latest`, so a message produced before it holds
     * partitions is one it will never see — and every assertion below would then be
     * measuring a consumer that simply was not listening. This waits for the broker to
     * have assigned it partitions first.
     */
    const joined = await waitFor(async () => ((await metrics())?.assignedPartitions ?? 0) > 0, 60_000);
    check(Boolean(joined), `CONTROL: it holds ${(await metrics())?.assignedPartitions ?? 0} partition(s) of ${TOPIC}`);
    if (!joined) {
      console.error(`  Community's output:\n${excerpt(service.log())}`);
      return;
    }

    console.log('');
    console.log('  a post in a small space reaches every active member');
    const AUTHOR = `u_author_${RUN}`;
    const MEMBER = `u_member_${RUN}`;
    const BANNED = `u_banned_${RUN}`;
    const asAuthor = token(TENANT, AUTHOR);
    const asMember = token(TENANT, MEMBER);
    const asBanned = token(TENANT, BANNED);

    const small = await api('/spaces', { method: 'POST', bearer: asAuthor, body: { slug: `small-${RUN}`, name: 'Small' } });
    const smallId = small.body?.id;
    await api(`/spaces/${smallId}/join`, { method: 'POST', bearer: asMember });
    await api(`/spaces/${smallId}/join`, { method: 'POST', bearer: asBanned });
    // Banned AFTER joining, which is the only way a banned membership exists.
    const ban = await api(`/spaces/${smallId}/members/${BANNED}`, { method: 'PATCH', bearer: asAuthor, body: { status: 'BANNED' } });
    check(small.status === 201 && ban.status === 200, `a space with an author, a member and a banned member (${small.status}, ${ban.status})`);

    const post = await api(`/spaces/${smallId}/posts`, { method: 'POST', bearer: asAuthor, body: { body: `hello ${RUN}` } });
    const postId = post.body?.id;
    check(post.status === 201, `the post is written (${post.status})`);

    const drained = await relayOnce();
    /**
     * Exit 0 is true of a relay that published nothing, and compose may be running
     * relay-community against the same outbox anyway — so this is NOT the control for
     * anything. It is here to make a relay that crashed obvious. The real evidence that
     * the message reached the topic is the fan-out below: no publish, no rows.
     */
    check(drained.code === 0, `a relay pass did not error (${drained.code})${drained.code === 0 ? '' : `:\n${excerpt(drained.log)}`}`);

    const fanned = await waitFor(async () => {
      const rows = await feedRows(owner, TENANT, postId);
      return rows.length >= 2 ? rows : null;
    }, 45_000);
    const users = (fanned ?? []).map((r) => r.userId).sort();
    check(
      users.length === 2 && users.includes(AUTHOR) && users.includes(MEMBER),
      `the author and the member each have a feed row (${users.length}: ${users.join(', ') || 'none'})`,
    );
    // `!users.includes(BANNED)` alone is TRUE OF AN EMPTY LIST, and the first run of
    // this file proved it: the fixture fanned nothing out and this still passed. The
    // fan-out has to have happened for the exclusion to mean anything.
    check(
      users.length > 0 && !users.includes(BANNED),
      `and the banned member has none — they cannot read the space${users.includes(BANNED) ? ' but got a row anyway' : users.length === 0 ? ' (VACUOUS: nothing was fanned out)' : ''}`,
    );

    console.log('');
    console.log('  the same message again writes nothing twice');
    /**
     * The exact bytes off the topic, republished. Not a second post — a REDELIVERY,
     * which is what at-least-once actually produces and what the unique key exists for.
     */
    const consumerScan = kafka.consumer({ groupId: `${GROUP}-scan` });
    await consumerScan.connect();
    await consumerScan.subscribe({ topic: TOPIC, fromBeginning: true });
    let original = null;
    await consumerScan.run({
      eachMessage: async ({ message }) => {
        const decoded = await registry.decode(message.value).catch(() => null);
        if (decoded?.postId === postId && decoded?.kind === 'POST_CREATED') {
          original = { key: message.key, value: message.value, headers: message.headers };
        }
      },
    });
    await waitFor(async () => original !== null, 45_000);
    await consumerScan.disconnect();
    check(Boolean(original), `CONTROL: the original message was found on ${TOPIC}${original ? '' : ' — nothing to redeliver, so the check below would prove nothing'}`);

    if (original) {
      const before = (await feedRows(owner, TENANT, postId)).length;
      await producer.send({ topic: TOPIC, messages: [original] });
      // Long enough for the consumer to have applied it; the assertion is that the
      // count did NOT move, so a wait that is too short would pass for the wrong
      // reason. The metric below is what proves it was actually processed.
      const seenAgain = await waitFor(async () => ((await metrics())?.postsFannedOutTotal ?? 0) >= 2, 45_000);
      const after = (await feedRows(owner, TENANT, postId)).length;
      check(Boolean(seenAgain), `CONTROL: the consumer processed the redelivery (postsFannedOutTotal ${(await metrics())?.postsFannedOutTotal})`);
      check(before === 2 && after === 2, `and wrote nothing twice (${before} rows before, ${after} after)`);
    }

    console.log('');
    console.log('  activity moves the copy to whatever the post now says');
    /**
     * A COMMENT, NOT A REACTION, AND THE REASON MATTERS.
     *
     * The score is ln(greatest(reactions + 2*comments, 1) + 1) + t. Going from zero
     * reactions to one moves NOTHING: greatest(0, 1) and greatest(1, 1) are both 1.
     * The first version of this section used a single reaction, so the post's score
     * never changed, the resync matched no rows, and both assertions below compared
     * two values that were already equal — they stayed green with the copy replaced by
     * `rankScore + 0.5`, which is the exact bug they exist to catch.
     *
     * One comment is worth 2, so greatest(2, 1) = 2 and the score really moves. The
     * control below asserts that it did, because if it ever stops moving this section
     * goes back to proving nothing.
     */
    const scoreAtFanOut = Number((await feedRows(owner, TENANT, postId))[0]?.rankScore);
    const comment = await api(`/posts/${postId}/comments`, { method: 'POST', bearer: asMember, body: { body: `moves the score ${RUN}` } });
    check(comment.status === 201, `a comment is added, which is worth 2 to the formula (${comment.status})`);
    await relayOnce();

    const postNow = await waitFor(async () => {
      const r = await inTenant(owner, TENANT, `SELECT "rankScore", "commentCount", "reactionCount" FROM "Post" WHERE id = $1`, [postId]);
      const row = r.rows[0];
      return row && Number(row.rankScore) !== scoreAtFanOut ? row : null;
    }, 30_000);
    const finalPost = await inTenant(owner, TENANT, `SELECT "rankScore", "commentCount" FROM "Post" WHERE id = $1`, [postId]);
    check(
      postNow !== null,
      `CONTROL: the post's own score moved (${scoreAtFanOut} -> ${finalPost.rows[0]?.rankScore}, commentCount ${finalPost.rows[0]?.commentCount})${postNow === null ? ' — UNCHANGED, so nothing below can fail' : ''}`,
    );

    const matched = await waitFor(async () => {
      const rows = await inTenant(
        owner,
        TENANT,
        `SELECT f."rankScore" AS feed, p."rankScore" AS post
           FROM "FeedEntry" f JOIN "Post" p ON p.id = f."postId"
          WHERE f."postId" = $1 AND f."rankScore" = p."rankScore"`,
        [postId],
      );
      return rows.rows.length === 2 ? rows.rows : null;
    }, 45_000);
    check(Boolean(matched), `every copy of the score equals the post's (${matched ? matched[0].feed : 'never matched'})`);

    /**
     * THE SAME MESSAGE, AGAIN.
     *
     * This is what separates a copy from a delta. `SET rankScore = rankScore + x`
     * survives every test above and is wrong by exactly x the first time a partition
     * replays — so the redelivery is the test, and the assertion is that nothing moved.
     */
    const commentMsg = await (async () => {
      const c = kafka.consumer({ groupId: `${GROUP}-scan2` });
      await c.connect();
      await c.subscribe({ topic: TOPIC, fromBeginning: true });
      let found = null;
      await c.run({
        eachMessage: async ({ message }) => {
          const d = await registry.decode(message.value).catch(() => null);
          if (d?.postId === postId && d?.kind === 'COMMENT_CREATED') found = { key: message.key, value: message.value, headers: message.headers };
        },
      });
      await waitFor(async () => found !== null, 45_000);
      await c.disconnect();
      return found;
    })();
    check(Boolean(commentMsg), 'CONTROL: the comment message was found, so there is something to redeliver');

    if (commentMsg) {
      const scoreBefore = (await feedRows(owner, TENANT, postId)).map((r) => Number(r.rankScore));
      const resyncsBefore = (await metrics())?.scoreResyncsTotal ?? 0;
      await producer.send({ topic: TOPIC, messages: [commentMsg] });
      await waitFor(async () => ((await metrics())?.scoreResyncsTotal ?? 0) >= resyncsBefore, 20_000);
      await sleep(2_000);
      const scoreAfter = (await feedRows(owner, TENANT, postId)).map((r) => Number(r.rankScore));
      // Two empty arrays stringify the same, so the length is part of the assertion.
      // Without it this passed on a run where nothing had been fanned out at all.
      check(
        scoreBefore.length === 2 && JSON.stringify(scoreBefore) === JSON.stringify(scoreAfter),
        `applying it twice changes nothing — the score is copied, not added to (${scoreBefore.join(', ') || 'VACUOUS: no rows'} then ${scoreAfter.join(', ') || 'none'})`,
      );
    }

    console.log('');
    console.log('  a space past the threshold is not fanned out');
    const bigId = (await api('/spaces', { method: 'POST', bearer: asAuthor, body: { slug: `big-${RUN}`, name: 'Big' } })).body?.id;
    for (let i = 0; i < FANOUT_MAX; i++) {
      await api(`/spaces/${bigId}/join`, { method: 'POST', bearer: token(TENANT, `u_crowd_${i}_${RUN}`) });
    }
    const members = await inTenant(owner, TENANT, `SELECT "memberCount" FROM "CommunitySpace" WHERE id = $1`, [bigId]);
    check(Number(members.rows[0].memberCount) === FANOUT_MAX + 1, `CONTROL: the big space has ${members.rows[0].memberCount} members, past the threshold of ${FANOUT_MAX}`);

    const bigPost = await api(`/spaces/${bigId}/posts`, { method: 'POST', bearer: asAuthor, body: { body: `big ${RUN}` } });
    const tooLargeBefore = (await metrics())?.spacesTooLargeToFanOutTotal ?? 0;
    await relayOnce();
    const refused = await waitFor(async () => ((await metrics())?.spacesTooLargeToFanOutTotal ?? 0) > tooLargeBefore, 45_000);
    const bigRows = await feedRows(owner, TENANT, bigPost.body?.id);
    check(Boolean(refused), 'CONTROL: the consumer saw it and declined to fan it out');
    check(bigRows.length === 0, `no feed rows were written for it (${bigRows.length})`);

    console.log('');
    console.log('  a message it can never apply is parked, and the partition moves on');
    const before = (await metrics())?.deadLetteredTotal ?? 0;
    await producer.send({ topic: TOPIC, messages: [{ key: smallId, value: Buffer.from('not avro at all') }] });
    const parked = await waitFor(async () => ((await metrics())?.deadLetteredTotal ?? 0) > before, 45_000);
    check(Boolean(parked), `the undecodable message is dead-lettered (${(await metrics())?.deadLetteredTotal})`);

    // And the partition is not stuck: a good message after it is still applied.
    const afterPoison = await api(`/spaces/${smallId}/posts`, { method: 'POST', bearer: asAuthor, body: { body: `after poison ${RUN}` } });
    await relayOnce();
    const laterRows = await waitFor(async () => {
      const rows = await feedRows(owner, TENANT, afterPoison.body?.id);
      return rows.length === 2 ? rows : null;
    }, 45_000);
    check(Boolean(laterRows), `and the post after it is still fanned out, so the partition moved on (${laterRows?.length ?? 0} rows)`);

    const dlqCount = await (async () => {
      const c = kafka.consumer({ groupId: `${GROUP}-dlq` });
      await c.connect();
      await c.subscribe({ topic: DLQ, fromBeginning: true });
      let n = 0;
      await c.run({
        eachMessage: async ({ message }) => {
          if (message.headers?.['dlq-consumer-group']?.toString() === GROUP) n += 1;
        },
      });
      await waitFor(async () => n > 0, 30_000);
      await c.disconnect();
      return n;
    })();
    check(dlqCount >= 1, `and it is on ${DLQ}, tagged with this consumer group (${dlqCount})`);

    console.log('');
    console.log("  another tenant's post is not in this tenant's feed");
    const asOther = token(OTHER_TENANT, `u_other_${RUN}`);
    const otherSpace = await api('/spaces', { method: 'POST', bearer: asOther, body: { slug: `other-${RUN}`, name: 'Other' } });
    const otherPost = await api(`/spaces/${otherSpace.body?.id}/posts`, { method: 'POST', bearer: asOther, body: { body: 'theirs' } });
    await relayOnce();
    const otherFanned = await waitFor(async () => {
      const rows = await feedRows(owner, OTHER_TENANT, otherPost.body?.id);
      return rows.length === 1 ? rows : null;
    }, 45_000);
    check(Boolean(otherFanned), `CONTROL: the other tenant's own post was fanned out to them (${otherFanned?.length ?? 0})`);
    const leaked = await inTenant(owner, TENANT, `SELECT count(*)::int AS n FROM "FeedEntry" WHERE "postId" = $1`, [otherPost.body?.id]);
    check(leaked.rows[0].n === 0, `and none of it is visible in ${TENANT} (${leaked.rows[0].n})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Community's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    const tables = ['FeedEntry', 'Reaction', 'Comment', 'Post', 'Membership', 'CommunitySpace', 'OutboxMessage'];
    let deleted = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        const gone = await inTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }));
        deleted += gone.rowCount ?? 0;
      }
    }
    let left = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        const n = await inTenant(owner, tenant, `SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rows: [{ n: -1 }] }));
        left += n.rows[0].n;
      }
    }
    const groupsLeft = await deleteGroupsWhenEmpty(admin, [GROUP, `${GROUP}-scan`, `${GROUP}-scan2`, `${GROUP}-dlq`]).catch(() => []);
    // `deleted > 0` is the control: "0 deleted, 0 left" is what a cleanup filtered by
    // the policy it forgot to set looks like, which is how the foundation run abandoned
    // rows for several chapters.
    check(deleted > 0 && left === 0, `cleanup: this run's rows are gone (${deleted} deleted, ${left} left); consumer groups ${groupsLeft.length === 0 ? 'deleted' : `left: ${groupsLeft.join(', ')}`}`);
    await Promise.allSettled([owner.end(), producer.disconnect(), admin.disconnect()]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a post reaches the feeds of the people who can read it, exactly once, and a space too large for that is left to the read path');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
