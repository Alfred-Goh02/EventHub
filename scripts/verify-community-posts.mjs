/**
 * Asserts what a post, a comment and a reaction do — in the database, and on the way out.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - A post and its event are ONE COMMIT. The outbox row is read back beside the post,
 *     addressed to community.feed and keyed by the space, and the relay then publishes
 *     it: this run drains the outbox itself and reads the message off Kafka, decoded
 *     against the registered schema. A handler that called Kafka inline would look the
 *     same until the broker hiccuped.
 *   - A thread is a materialised path. Paths and depths are read back as a, a.b, a.b.c,
 *     the API returns them nested, and the ORDER the database returns them in IS render
 *     order.
 *   - A parent on another post is refused BY THE DATABASE: the path expression goes
 *     NULL and the NOT NULL constraint stops it, rather than a separate SELECT that
 *     leaves a window.
 *   - Nine levels is refused by the CHECK constraint.
 *   - Reacting twice counts once, and the rank score always equals the formula
 *     recomputed here from the counters. A counter and its derived score drifting apart
 *     is invisible until the feed order is wrong, so it is asserted every time.
 *   - A reaction kind the server did not name is refused. kind is part of the unique
 *     key, so without a closed set one account can invent a kind per request and run a
 *     post's rank up as far as the limiter allows.
 *   - Another tenant's token finds none of it.
 *
 * Scoped to two tenants created for this run.
 *
 * Run (with Postgres, Redis, Kafka and the Schema Registry up, the topic created and
 * schemas registered, Community and the relay built):
 *   node scripts/verify-community-posts.mjs
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
const TENANT = `tenant_cp_${RUN}`;
const OTHER_TENANT = `tenant_cp_other_${RUN}`;
const PORT = 3816;
const TOPIC = 'community.feed';
/**
 * The limiter is exercised on POSTS here, with a small budget, and comments are given
 * a large one: the depth-cap check needs eight comments in a row, and the first version
 * of this run hit the comment limit at five and reported the limiter's 429 as though
 * the depth cap had refused it.
 */
const POSTS_PER_MINUTE = 3;
const COMMENTS_PER_MINUTE = 40;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  RATE_LIMIT_COMMENT_PER_MINUTE: String(COMMENTS_PER_MINUTE),
  RATE_LIMIT_POST_PER_MINUTE: String(POSTS_PER_MINUTE),
  AUTH_PUBLIC_KEY_B64: Buffer.from(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem'), 'utf8')).toString('base64'),
});
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['COMMUNITY_RELAY_DATABASE_URL'] ??
    'postgresql://eventhub_community_relay:eventhub_community_relay@127.0.0.1:5432/eventhub_community',
  RELAY_FORBIDDEN_TABLE: 'Post',
  RELAY_METRICS_PORT: '3817',
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
  const kafka = new Kafka({ clientId: `verify-community-posts-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const registry = new SchemaRegistry({ host: relayEnv.SCHEMA_REGISTRY_URL });
  const scanGroups = [];

  /** Every community.feed message carrying one of this run's outbox ids. */
  async function readFeed(messageIds, timeoutMs = 25_000) {
    const groupId = `verify-community-posts-${RUN}-${scanGroups.length}`;
    scanGroups.push(groupId);
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
    const found = new Map();
    // Every message this consumer saw, this run's or not. Without it, "nothing arrived"
    // cannot be told from "the consumer never fetched at all" — which is the difference
    // between a broken relay and a consumer that had not finished joining its group.
    let seen = 0;
    await consumer.run({
      eachMessage: async ({ message }) => {
        seen += 1;
        const id = message.headers?.['message-id']?.toString();
        if (!id || !messageIds.includes(id)) return;
        found.set(id, {
          key: message.key?.toString() ?? null,
          eventType: message.headers?.['event-type']?.toString() ?? null,
          tenant: message.headers?.['tenant-id']?.toString() ?? null,
          value: await registry.decode(message.value),
        });
      },
    });
    await waitFor(async () => found.size >= messageIds.length, timeoutMs, 250);
    await sleep(300);
    await consumer.disconnect();
    found.seen = seen;
    return found;
  }

  const service = start('community', ['dist/index.js'], communityEnv);
  const AUTHOR = `u_author_${RUN}`;
  const READER = `u_reader_${RUN}`;
  const asAuthor = token(TENANT, AUTHOR);
  const asReader = token(TENANT, READER);
  const asOther = token(OTHER_TENANT, AUTHOR);

  try {
    console.log('  the fixture');
    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Community is ready on :${PORT}` : `it never became ready:\n${excerpt(service.log())}`);
    if (failures > 0) return;
    const space = await api('/spaces', { method: 'POST', bearer: asAuthor, body: { slug: `posts-${RUN}`, name: 'Posting' } });
    const spaceId = space.body?.id;
    await api(`/spaces/${spaceId}/join`, { method: 'POST', bearer: asReader });
    check(space.status === 201 && Boolean(spaceId), `a space with an author and a reader in it (${space.status})`);

    console.log('');
    console.log('  a post and its event are one commit');
    const posted = await api(`/spaces/${spaceId}/posts`, { method: 'POST', bearer: asAuthor, body: { body: `hello ${RUN}` } });
    const postId = posted.body?.id;
    check(posted.status === 201 && Boolean(postId), `the post is written (${posted.status})`);
    const outbox = await inTenant(
      owner,
      TENANT,
      `SELECT id, topic, "eventType", "aggregateId", payload FROM "OutboxMessage" WHERE payload->>'postId' = $1 ORDER BY seq`,
      [postId],
    );
    const row = outbox.rows[0];
    check(outbox.rows.length === 1 && row.topic === TOPIC && row.aggregateId === spaceId, `and one outbox row for ${TOPIC}, keyed by the space (${outbox.rows.length}, ${row?.topic}, key ${row?.aggregateId === spaceId ? 'matches' : row?.aggregateId})`);
    check(row?.payload?.messageId === row?.id, 'whose payload carries the row id, so a redelivery is recognisable');
    check(row?.payload?.kind === 'POST_CREATED' && row?.payload?.body === `hello ${RUN}`, `and the post itself (${row?.payload?.kind})`);
    // Without a row there is nothing to publish and nothing below can mean anything, so
    // this ends here rather than throwing halfway through the next section.
    if (!row) return;

    const scored = await inTenant(
      owner,
      TENANT,
      `SELECT "rankScore", ln(greatest("reactionCount" + 2 * "commentCount", 1) + 1) + extract(epoch from "createdAt") / 45000.0 AS expected
         FROM "Post" WHERE id = $1`,
      [postId],
    );
    check(
      Math.abs(Number(scored.rows[0].rankScore) - Number(scored.rows[0].expected)) < 1e-9,
      `its rank score is the formula, not a second copy of it (${scored.rows[0].rankScore})`,
    );

    console.log('');
    console.log('  the relay publishes what the transaction left behind');
    const drained = await relayOnce();
    check(drained.code === 0, `a relay pass over Community's outbox did not error (${drained.code})${drained.code === 0 ? '' : `:\n${excerpt(drained.log)}`}`);
    /**
     * AN EXIT CODE OF 0 IS TRUE OF A RELAY THAT PUBLISHED NOTHING — AND SOMEONE ELSE
     * MAY HAVE DONE THE PUBLISHING.
     *
     * This line was labelled CONTROL and carried the whole section on `code === 0`.
     * Two things were wrong with that. It is true of a pass that found nothing to do;
     * and compose runs relay-community against this same outbox, so with the stack up
     * the row is published whether this pass runs or not. Deleting the call to
     * relayOnce() left every assertion below green, which is what a control must never
     * do.
     *
     * publishedAt is the relay's own record of having handed THIS row to Kafka,
     * whichever relay did it. Asserting it makes the section fail when nothing was
     * published, and separates a relay that never published from a consumer that never
     * read.
     */
    const published = await inTenant(owner, TENANT, `SELECT "publishedAt", "lastError" FROM "OutboxMessage" WHERE id = $1`, [row.id]);
    // `rows[0]?.publishedAt !== null` was the first version of this, and it is true of
    // NO ROW as well as of a published one — the assertion would have held most
    // strongly in the case it exists to rule out. The row has to be there and the
    // column has to be set.
    const publishedAt = published.rows.length === 1 ? published.rows[0].publishedAt : undefined;
    check(
      publishedAt instanceof Date,
      `CONTROL: and marked the row published, so a missing message below is the read's fault${
        publishedAt instanceof Date ? ` (${publishedAt.toISOString()})` : ` — it did not (${published.rows.length} row(s), publishedAt ${String(publishedAt)}, lastError ${published.rows[0]?.lastError ?? 'none'})\n${excerpt(drained.log)}`
      }`,
    );
    const feed = await readFeed([row.id]);
    const message = feed.get(row.id);
    check(Boolean(message), message ? 'the message is on community.feed' : `nothing arrived on ${TOPIC} for that row (the consumer read ${feed.seen} message(s) in all)`);
    check(message?.key === spaceId, `keyed by the space, so one space stays ordered (${message?.key === spaceId ? 'yes' : message?.key})`);
    check(message?.value?.kind === 'POST_CREATED' && message?.value?.postId === postId, `and decodes against the registered schema (${message?.value?.kind})`);
    check(message?.tenant === TENANT, `carrying its tenant in a header for consumers that scope by it (${message?.tenant === TENANT ? 'yes' : message?.tenant})`);

    console.log('');
    console.log('  a thread is a path, and the database enforces its shape');
    const first = await api(`/posts/${postId}/comments`, { method: 'POST', bearer: asReader, body: { body: 'top' } });
    const reply = await api(`/posts/${postId}/comments`, { method: 'POST', bearer: asAuthor, body: { body: 'reply', parentId: first.body?.id } });
    const deeper = await api(`/posts/${postId}/comments`, { method: 'POST', bearer: asReader, body: { body: 'deeper', parentId: reply.body?.id } });
    check(first.status === 201 && reply.status === 201 && deeper.status === 201, `three comments, two of them replies (${first.status}, ${reply.status}, ${deeper.status})`);
    const paths = await inTenant(owner, TENANT, `SELECT id, path::text AS path, depth FROM "Comment" WHERE "postId" = $1 ORDER BY path`, [postId]);
    check(
      paths.rows.length === 3 && paths.rows[0].depth === 1 && paths.rows[1].depth === 2 && paths.rows[2].depth === 3,
      `their depths are 1, 2, 3 (${paths.rows.map((r) => r.depth).join(', ')})`,
    );
    check(
      paths.rows[1].path === `${paths.rows[0].path}.${paths.rows[1].id}` && paths.rows[2].path.startsWith(`${paths.rows[1].path}.`),
      'and each path extends its parent, which is what makes a subtree one query',
    );
    const thread = await api(`/posts/${postId}/comments`, { bearer: asReader });
    const top = thread.body?.comments?.[0];
    check(
      thread.body?.count === 3 && top?.replies?.length === 1 && top?.replies?.[0]?.replies?.length === 1,
      `the API nests them in one pass over path order (${thread.body?.count} rows, ${top?.replies?.length ?? 0} reply, ${top?.replies?.[0]?.replies?.length ?? 0} deeper)`,
    );

    const otherPost = await api(`/spaces/${spaceId}/posts`, { method: 'POST', bearer: asAuthor, body: { body: 'another' } });
    const foreignParent = await api(`/posts/${otherPost.body?.id}/comments`, { method: 'POST', bearer: asAuthor, body: { body: 'x', parentId: first.body?.id } });
    check(foreignParent.status === 404, `a parent from another post is refused by the database, not by a separate check (${foreignParent.status})`);

    // Its own user, with a full comment budget: eight replies in a row would otherwise
    // meet the rate limiter first, and a 429 would be read as the depth cap refusing.
    const asDigger = token(TENANT, `u_digger_${RUN}`);
    await api(`/spaces/${spaceId}/join`, { method: 'POST', bearer: asDigger });
    let parent = first.body?.id;
    let deepStatus = 201;
    for (let depth = 2; depth <= 9 && deepStatus === 201; depth++) {
      const made = await api(`/posts/${postId}/comments`, { method: 'POST', bearer: asDigger, body: { body: `d${depth}`, parentId: parent } });
      deepStatus = made.status;
      parent = made.body?.id ?? parent;
    }
    check(deepStatus === 422, `and nine levels is refused by the depth cap (${deepStatus})`);

    console.log('');
    console.log('  a reaction counts once, however many times it is sent');
    const reacted = await api(`/posts/${postId}/reactions`, { method: 'POST', bearer: asReader, body: { kind: 'LIKE' } });
    const again = await api(`/posts/${postId}/reactions`, { method: 'POST', bearer: asReader, body: { kind: 'LIKE' } });
    check(reacted.status === 201 && reacted.body?.added === true, `the first is added (${reacted.status})`);
    check(again.status === 200 && again.body?.added === false, `the second changes nothing (${again.status})`);
    const counted = await inTenant(
      owner,
      TENANT,
      `SELECT "reactionCount", "commentCount", "rankScore",
              ln(greatest("reactionCount" + 2 * "commentCount", 1) + 1) + extract(epoch from "createdAt") / 45000.0 AS expected
         FROM "Post" WHERE id = $1`,
      [postId],
    );
    check(counted.rows[0].reactionCount === 1, `the count is one (${counted.rows[0].reactionCount})`);
    check(
      Math.abs(Number(counted.rows[0].rankScore) - Number(counted.rows[0].expected)) < 1e-9,
      `and the score still equals the formula over the counters (${counted.rows[0].rankScore} vs ${counted.rows[0].expected})`,
    );
    /**
     * A KIND THE SERVER DID NOT NAME IS NOT A REACTION.
     *
     * kind is part of the unique key, so two different kinds from the same user are two
     * rows and two increments. With any upper-case string accepted, one account could
     * invent a kind per request and run a post's reactionCount — and therefore its rank
     * — as high as the limiter allows, forever. The count below is asserted to stay at
     * one: without the allow-list it reaches three.
     */
    const invented = await api(`/posts/${postId}/reactions`, { method: 'POST', bearer: asReader, body: { kind: 'AAAA' } });
    const invented2 = await api(`/posts/${postId}/reactions`, { method: 'POST', bearer: asReader, body: { kind: 'AAAB' } });
    const afterInvented = await inTenant(owner, TENANT, `SELECT "reactionCount" FROM "Post" WHERE id = $1`, [postId]);
    check(
      invented.status === 400 && invented2.status === 400 && afterInvented.rows[0].reactionCount === 1,
      `an invented kind is refused, so one account cannot inflate a rank (${invented.status}, ${invented2.status}, count ${afterInvented.rows[0].reactionCount})`,
    );

    const removed = await api(`/posts/${postId}/reactions/LIKE`, { method: 'DELETE', bearer: asReader });
    const removedAgain = await api(`/posts/${postId}/reactions/LIKE`, { method: 'DELETE', bearer: asReader });
    const afterRemoval = await inTenant(owner, TENANT, `SELECT "reactionCount" FROM "Post" WHERE id = $1`, [postId]);
    check(removed.status === 204 && removedAgain.status === 404 && afterRemoval.rows[0].reactionCount === 0, `removing it reverses both, once (${removed.status}, ${removedAgain.status}, count ${afterRemoval.rows[0].reactionCount})`);

    console.log('');
    console.log('  the limiter covers posting too');
    // A fresh user, so the budget is whole: everyone above has spent some of theirs.
    const asFlooder = token(TENANT, `u_flood_${RUN}`);
    await api(`/spaces/${spaceId}/join`, { method: 'POST', bearer: asFlooder });
    const burst = [];
    for (let i = 0; i < POSTS_PER_MINUTE + 2; i++) {
      burst.push(await api(`/spaces/${spaceId}/posts`, { method: 'POST', bearer: asFlooder, body: { body: `spam ${i}` } }));
    }
    const made = burst.filter((r) => r.status === 201).length;
    const refused = burst.filter((r) => r.status === 429).length;
    check(made === POSTS_PER_MINUTE && refused === 2, `past ${POSTS_PER_MINUTE} a minute the rest are refused (${made} written, ${refused} refused)`);

    console.log('');
    console.log("  another tenant's post is not there");
    const crossRead = await api(`/posts/${postId}/comments`, { bearer: asOther });
    const crossWrite = await api(`/posts/${postId}/reactions`, { method: 'POST', bearer: asOther, body: { kind: 'LIKE' } });
    check(crossRead.status === 404 && crossWrite.status === 404, `reading and reacting with another tenant's token are both 404 (${crossRead.status}, ${crossWrite.status})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Community's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of ['Reaction', 'Comment', 'Post', 'OutboxMessage', 'Membership', 'CommunitySpace']) {
        try {
          await owner.query('BEGIN');
          await owner.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
          await owner.query(`DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]);
          await owner.query('COMMIT');
        } catch (err) {
          await owner.query('ROLLBACK').catch(() => {});
          console.error(`  cleanup of ${table} failed: ${err.message}`);
        }
      }
    }
    let left = 0;
    for (const table of ['Reaction', 'Comment', 'Post', 'OutboxMessage', 'Membership', 'CommunitySpace']) {
      const rows = await owner
        .query(`SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = ANY($1)`, [[TENANT, OTHER_TENANT]])
        .catch(() => ({ rows: [{ n: -1 }] }));
      left += rows.rows[0].n;
    }
    const groupsLeft = await deleteGroupsWhenEmpty(admin, scanGroups);
    await admin.disconnect().catch(() => {});
    check(left === 0, `cleanup: this run's rows are gone (${left} left); consumer groups ${groupsLeft.length === 0 ? 'deleted' : `left to expire: ${groupsLeft.join(', ')}`}`);
    await owner.end().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a post and its event commit together and reach Kafka, a thread keeps its shape in the database, and a reaction counts once');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
