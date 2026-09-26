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
 *   - EVERY symbol in ActivityKind encodes and decodes against the registered schema,
 *     with the nullable fields its kind implies. Only POST_CREATED was read off the
 *     topic at first; the comment and reaction rows sat in the outbox, and CI starts
 *     infrastructure only, so nothing else would have drained them.
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
/** Low, so the truncation boundary is four comments away rather than five hundred. */
const THREAD_PAGE_LIMIT = 3;
/** This run makes one space, so the space limiter can be met in three requests. */
const SPACES_PER_MINUTE = 2;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  RATE_LIMIT_COMMENT_PER_MINUTE: String(COMMENTS_PER_MINUTE),
  RATE_LIMIT_POST_PER_MINUTE: String(POSTS_PER_MINUTE),
  THREAD_PAGE_LIMIT: String(THREAD_PAGE_LIMIT),
  RATE_LIMIT_SPACE_PER_MINUTE: String(SPACES_PER_MINUTE),
  /**
   * No consumer. This run proves the write path and the outbox; the fan-out is
   * verify-community-feed-fanout.mjs's job. Left on, the consumer spawned here joins
   * the shared group, commits offsets for other runs' messages, and writes FeedEntry
   * rows as a side effect — five of them survived one pass before this was set.
   */
  FEED_CONSUMER_ENABLED: 'false',
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
      /**
       * The timestamps are compared IN SQL, deliberately.
       *
       * OutboxMessage.createdAt is `timestamp without time zone` holding UTC, and node-
       * postgres parses such a column as LOCAL time. Read into JS on this machine the
       * row came back eight hours off — the local offset, to the millisecond — and
       * comparing it to the payload would have failed on a correct service and passed
       * on CI, which runs in UTC. Postgres comparing its own column to its own rendering
       * has no timezone to get wrong.
       */
      `SELECT id, topic, "eventType", "aggregateId", payload,
              to_char("createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "createdAtIso",
              payload->>'occurredAt' = to_char("createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "clockMatches"
         FROM "OutboxMessage" WHERE payload->>'postId' = $1 ORDER BY seq`,
      [postId],
    );
    const row = outbox.rows[0];
    check(outbox.rows.length === 1 && row.topic === TOPIC && row.aggregateId === spaceId, `and one outbox row for ${TOPIC}, keyed by the space (${outbox.rows.length}, ${row?.topic}, key ${row?.aggregateId === spaceId ? 'matches' : row?.aggregateId})`);
    check(row?.payload?.messageId === row?.id, 'whose payload carries the row id, so a redelivery is recognisable');
    /**
     * occurredAt IS THE ROW'S OWN TIMESTAMP, TO THE MILLISECOND.
     *
     * The schema says it is when the change was committed and consumers order on it.
     * Built from the app's `new Date()` it was really this process's clock, which
     * drifts from the database's — invisible on one machine, and exactly the kind of
     * skew that reorders a merged feed in production. Compared as instants, so a
     * rendering difference is not read as agreement.
     */
    check(
      row?.clockMatches === true,
      `and a timestamp from the database, not this process's clock (${row?.payload?.occurredAt} vs row ${row?.createdAtIso})`,
    );
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
      thread.body?.count === 3 && top?.replies?.length === 1 && top?.replies?.[0]?.replies?.length === 1 && thread.body?.truncated === false,
      `the API nests them in one pass over path order (${thread.body?.count} rows, ${top?.replies?.length ?? 0} reply, ${top?.replies?.[0]?.replies?.length ?? 0} deeper, truncated ${thread.body?.truncated})`,
    );

    /**
     * A COMMENT MUST MOVE THE SCORE, not merely leave it equal to the formula.
     *
     * Everything else here recomputes the formula from the stored columns and compares,
     * which agrees with itself whatever the formula is. `rankScore` pasted its
     * arguments in unparenthesised, so `2 * ${comments}` with `"commentCount" + 1`
     * became `2 * "commentCount" + 1`: a comment was worth 1 instead of 2, and the
     * first comment moved the score by exactly nothing, because greatest(1, 1) and
     * greatest(0, 1) are both 1. Three comments existed above and none of them proved
     * anything about it.
     */
    const scoredThread = await inTenant(
      owner,
      TENANT,
      `SELECT "rankScore", "commentCount",
              ln(greatest("reactionCount" + 2 * "commentCount", 1) + 1) + extract(epoch from "createdAt") / 45000.0 AS expected
         FROM "Post" WHERE id = $1`,
      [postId],
    );
    check(
      Number(scoredThread.rows[0].commentCount) > 0 &&
        Math.abs(Number(scoredThread.rows[0].rankScore) - Number(scoredThread.rows[0].expected)) < 1e-9 &&
        Number(scoredThread.rows[0].rankScore) !== Number(scored.rows[0].rankScore),
      `and commenting MOVED it, not just kept it equal to the formula (${scored.rows[0].rankScore} -> ${scoredThread.rows[0].rankScore} over ${scoredThread.rows[0].commentCount} comment(s))`,
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

    /**
     * A CAP THE CALLER CANNOT SEE IS A LIE ABOUT THE THREAD.
     *
     * The depth loop above left this post with more comments than the page limit. Read
     * back, the response must say it was cut: returning `count: <limit>` for a thread
     * of any size above the limit reads exactly like a complete thread of that size.
     * Asserted against the same field reading false a few lines up, so the flag has to
     * move rather than just be present.
     */
    const capped = await api(`/posts/${postId}/comments`, { bearer: asReader });
    check(
      capped.body?.truncated === true && capped.body?.count === THREAD_PAGE_LIMIT,
      `past the page limit the thread says it was cut (count ${capped.body?.count} of limit ${THREAD_PAGE_LIMIT}, truncated ${capped.body?.truncated})`,
    );

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
    console.log('  every kind this service emits encodes and decodes');
    /**
     * ALL FOUR SYMBOLS, NOT JUST THE ONE THE FIRST SECTION HAPPENED TO PUBLISH.
     *
     * Only POST_CREATED was ever read off the topic. The comment and reaction rows sat
     * in the outbox unread, so three of the four symbols in ActivityKind had never been
     * encoded against the registered schema by anything — and CI would not have caught
     * it, because `build and verify` starts infrastructure only. relay-community is not
     * running there, so nothing else drains what this leaves behind. A field this
     * service writes that the schema does not declare would have reached a consumer
     * before it reached a test.
     *
     * One row per event type, so this stays four messages however many the run made.
     */
    const kinds = await inTenant(
      owner,
      TENANT,
      `SELECT DISTINCT ON ("eventType") id, "eventType" FROM "OutboxMessage"
        WHERE "tenantId" = $1 ORDER BY "eventType", seq`,
      [TENANT],
    );
    const EXPECTED_KINDS = ['POST_CREATED', 'COMMENT_CREATED', 'REACTION_ADDED', 'REACTION_REMOVED'];
    check(
      kinds.rows.length === EXPECTED_KINDS.length,
      `CONTROL: the run emitted all ${EXPECTED_KINDS.length} event types (${kinds.rows.map((r) => r.eventType).join(', ')})`,
    );
    const secondPass = await relayOnce();
    check(secondPass.code === 0, `a second relay pass did not error (${secondPass.code})${secondPass.code === 0 ? '' : `:\n${excerpt(secondPass.log)}`}`);
    const everyKind = await readFeed(kinds.rows.map((r) => r.id));
    const decoded = [...everyKind.values()].map((m) => m.value?.kind).sort();
    check(
      decoded.length === EXPECTED_KINDS.length && EXPECTED_KINDS.every((k) => decoded.includes(k)),
      `each one decodes against the registered schema (${decoded.join(', ') || 'none'})`,
    );
    // The nullable fields are what a record-with-a-discriminator gets wrong: a reaction
    // has no body and no comment, a reply has both a comment and a parent.
    const byKind = Object.fromEntries([...everyKind.values()].map((m) => [m.value?.kind, m.value]));
    check(
      byKind['REACTION_ADDED']?.body === null &&
        byKind['REACTION_ADDED']?.commentId === null &&
        byKind['COMMENT_CREATED']?.commentId !== null &&
        byKind['COMMENT_CREATED']?.body !== null,
      `and carries the fields its kind implies, null where it has none (reaction body ${String(byKind['REACTION_ADDED']?.body)}, comment id ${byKind['COMMENT_CREATED']?.commentId ? 'set' : 'null'})`,
    );

    console.log('');
    console.log('  a comment names whom it answers, and only someone who can still read it');
    /**
     * Every "nobody" case below has a "somebody" case beside it: a service that never
     * set recipientId at all would pass every null check, so the ones that must be a
     * person are what make the nulls mean anything.
     */
    const comment = async (bearer, body, parentId, onPost = postId) =>
      (await api(`/posts/${onPost}/comments`, { method: 'POST', bearer, body: { body, ...(parentId ? { parentId } : {}) } })).body?.id;
    const B = `u_banned_${RUN}`;
    const L = `u_leaver_${RUN}`;
    for (const who of [B, L]) await api(`/spaces/${spaceId}/join`, { method: 'POST', bearer: token(TENANT, who) });
    const toPost = await comment(asReader, `answers the post ${RUN}`);
    const toReader = await comment(asAuthor, `answers the reader ${RUN}`, toPost);
    const toSelf = await comment(asReader, `answers themselves ${RUN}`, toPost);
    const byBanned = await comment(token(TENANT, B), `before the ban ${RUN}`);
    const byLeaver = await comment(token(TENANT, L), `before leaving ${RUN}`);
    const banned = await api(`/spaces/${spaceId}/members/${B}`, { method: 'PATCH', bearer: asAuthor, body: { status: 'BANNED' } });
    const left = await api(`/spaces/${spaceId}/members/me`, { method: 'DELETE', bearer: token(TENANT, L) });
    const toBanned = await comment(asAuthor, `answers the banned ${RUN}`, byBanned);
    const toLeaver = await comment(asAuthor, `answers the one who left a public space ${RUN}`, byLeaver);

    // A private space: its owner, and a member added the only way a private space has
    // one today — there is no invite yet — who comments, is answered, then leaves.
    const P_OWNER = `u_private_owner_${RUN}`;
    const P_MEMBER = `u_private_member_${RUN}`;
    const asPOwner = token(TENANT, P_OWNER);
    const privateSpace = (await api('/spaces', { method: 'POST', bearer: asPOwner, body: { slug: `private-${RUN}`, name: 'Private', visibility: 'PRIVATE' } })).body?.id;
    await inTenant(owner, TENANT, `INSERT INTO "Membership" (id, "tenantId", "spaceId", "userId", role, status) VALUES ($1, $2, $3, $4, 'MEMBER', 'ACTIVE')`, [`m_private_${RUN}`, TENANT, privateSpace, P_MEMBER]);
    const privatePost = (await api(`/spaces/${privateSpace}/posts`, { method: 'POST', bearer: asPOwner, body: { body: `private ${RUN}` } })).body?.id;
    const byMember = await comment(token(TENANT, P_MEMBER), `a member speaks ${RUN}`, null, privatePost);
    const toMember = await comment(asPOwner, `answers the member ${RUN}`, byMember, privatePost);
    const memberLeft = await api(`/spaces/${privateSpace}/members/me`, { method: 'DELETE', bearer: token(TENANT, P_MEMBER) });
    const toFormerMember = await comment(asPOwner, `answers them after they left ${RUN}`, byMember, privatePost);
    check(
      [toPost, toReader, toSelf, byBanned, byLeaver, toBanned, toLeaver, privateSpace, privatePost, byMember, toMember, toFormerMember].every(Boolean) &&
        banned.status === 200 && left.status === 204 && memberLeft.status === 204,
      `CONTROL: the fixture — comments written, one member banned, one left a public space, one left a private one (${banned.status}, ${left.status}, ${memberLeft.status})`,
    );

    const recipients = Object.fromEntries(
      (await inTenant(
        owner,
        TENANT,
        `SELECT payload->>'commentId' AS "commentId", payload->>'recipientId' AS "recipientId", id
           FROM "OutboxMessage" WHERE "tenantId" = $1 AND "eventType" = 'community.comment_created'`,
        [TENANT],
      )).rows.map((r) => [r.commentId, r]),
    );
    const expected = [
      ['a top-level comment answers the post\'s author', toPost, AUTHOR],
      ['a reply answers the comment\'s author', toReader, READER],
      ['answering yourself tells nobody', toSelf, null],
      ['a banned author is told nothing', toBanned, null],
      ['someone who left a PUBLIC space can still read it, so is told', toLeaver, L],
      ['a private space\'s member is told while a member', toMember, P_MEMBER],
      ['and not once they have left', toFormerMember, null],
    ];
    for (const [what, id, who] of expected) {
      const got = recipients[id]?.recipientId ?? null;
      check(Boolean(recipients[id]) && got === who, `${what} (${got === null ? 'nobody' : got === who ? 'yes' : got})`);
    }

    // Through Kafka, not just the outbox: a field the registered schema does not declare
    // is dropped by the encoder without an error, so the row is not the evidence.
    await relayOnce();
    const answeredIds = [recipients[toPost]?.id, recipients[toReader]?.id].filter(Boolean);
    const decodedAnswers = await readFeed(answeredIds);
    const onTopic = answeredIds.map((id) => decodedAnswers.get(id)?.value?.recipientId);
    check(
      onTopic.length === 2 && onTopic[0] === AUTHOR && onTopic[1] === READER,
      `and recipientId survives the registered schema onto the topic (${onTopic.map((r) => (r === undefined ? 'MISSING' : r)).join(', ')})`,
    );

    /**
     * A HIDE THAT COMMITS WHILE THE COMMENT IS BEING WRITTEN.
     *
     * The route checks the post and the parent before its transaction, and decides the
     * recipient later, after taking the post's row lock. Each case holds a hide open on
     * that row, sends the comment — which passes every earlier check, because the hide is
     * not committed, and then waits on the lock — and commits the hide while it waits.
     * The comment is still written; nobody may be told about it. `blocked` is the
     * control: a comment that never waited on the hide raced nothing.
     */
    async function hiddenWhileWriting(hide, write) {
      const locker = new pg.Client({ connectionString: communityEnv.DATABASE_URL });
      await locker.connect();
      try {
        await locker.query('BEGIN');
        await locker.query("SELECT set_config('app.tenant_id', $1, true)", [TENANT]);
        const { pid } = (await locker.query('SELECT pg_backend_pid() AS pid')).rows[0];
        for (const [sql, params] of hide) await locker.query(sql, params);
        const pending = write();
        const blocked = await waitFor(
          async () => (await owner.query('SELECT count(*)::int AS n FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))', [pid])).rows[0].n > 0,
          10_000,
          50,
        );
        await locker.query('COMMIT');
        return { id: await pending, blocked: Boolean(blocked) };
      } catch (err) {
        await locker.query('ROLLBACK').catch(() => {});
        throw err;
      } finally {
        await locker.end().catch(() => {});
      }
    }
    // Users of their own, so no earlier section's rate-limit budget is in play.
    const RACE_AUTHOR = `u_race_author_${RUN}`;
    const asRaceA = token(TENANT, `u_race_a_${RUN}`);
    const asRaceB = token(TENANT, `u_race_b_${RUN}`);
    for (const t of [token(TENANT, RACE_AUTHOR), asRaceA, asRaceB]) await api(`/spaces/${spaceId}/join`, { method: 'POST', bearer: t });
    const racePost = (await api(`/spaces/${spaceId}/posts`, { method: 'POST', bearer: token(TENANT, RACE_AUTHOR), body: { body: `raced ${RUN}` } })).body?.id;
    const toHide = await comment(asRaceB, `about to be hidden ${RUN}`, null, racePost);
    const grand = await comment(asRaceA, `a grandparent about to be hidden ${RUN}`, null, racePost);
    const underGrand = await comment(asRaceB, `under the grandparent ${RUN}`, grand, racePost);
    const touchPost = ['UPDATE "Post" SET "commentCount" = "commentCount" WHERE id = $1', [racePost]];
    const hideComment = (id) => ['UPDATE "Comment" SET status = \'HIDDEN\'::"ContentStatus" WHERE id = $1', [id]];
    const races = [
      ['the parent is hidden while the reply waits', await hiddenWhileWriting([hideComment(toHide), touchPost], () => comment(asRaceA, `answers a hidden parent ${RUN}`, toHide, racePost))],
      ['a comment above the parent is hidden while the reply waits', await hiddenWhileWriting([hideComment(grand), touchPost], () => comment(asRaceA, `answers under a hidden grandparent ${RUN}`, underGrand, racePost))],
      ['the post is hidden while the comment waits', await hiddenWhileWriting([['UPDATE "Post" SET status = \'HIDDEN\'::"ContentStatus" WHERE id = $1', [racePost]]], () => comment(asRaceA, `on a hidden post ${RUN}`, null, racePost))],
    ];
    const raced = Object.fromEntries(
      (await inTenant(
        owner,
        TENANT,
        `SELECT payload->>'commentId' AS "commentId", payload->>'recipientId' AS "recipientId"
           FROM "OutboxMessage" WHERE "tenantId" = $1 AND payload->>'commentId' = ANY($2)`,
        [TENANT, races.map(([, r]) => r.id).filter(Boolean)],
      )).rows.map((r) => [r.commentId, r.recipientId]),
    );
    check(
      Boolean(racePost && toHide && grand && underGrand),
      'CONTROL: the race fixture — a post, a comment to hide, a grandparent to hide and a comment under it',
    );
    for (const [what, r] of races) {
      const got = r.id && r.id in raced ? raced[r.id] : 'MISSING';
      check(r.blocked && got === null, `${what}: nobody is told (waited on the hide: ${r.blocked}; ${got === null ? 'nobody' : got})`);
    }

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

    /**
     * AND CREATING A SPACE, WHICH NOTHING METERED.
     *
     * It was the only write in this service without a limiter, in the service whose own
     * env comment calls it out for unbounded rows — and the heaviest, since a space is
     * a container other people join. Its own user, for a whole budget.
     */
    const asFounder = token(TENANT, `u_founder_${RUN}`);
    const founded = [];
    for (let i = 0; i < SPACES_PER_MINUTE + 1; i++) {
      founded.push(await api('/spaces', { method: 'POST', bearer: asFounder, body: { slug: `sp-${i}-${RUN}`, name: `Space ${i}` } }));
    }
    const built = founded.filter((r) => r.status === 201).length;
    const stopped = founded.filter((r) => r.status === 429).length;
    check(
      built === SPACES_PER_MINUTE && stopped === 1,
      `and creating spaces is metered too (${built} created, ${stopped} refused)`,
    );

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
    /**
     * FeedEntry is in this list because the service this run spawns carries the fan-out
     * consumer, so posting here writes feed rows as a side effect — measured, five rows
     * after one pass. ModerationAction because the recipient section bans someone, and a
     * ban is audited: without it every run left one row, eight of them before anyone
     * counted.
     *
     * Both the deletes and the count run INSIDE the tenant. The count used to run
     * outside it, where FORCE row security shows the owner nothing, so it said "0 left"
     * whatever was left. The deleted count is the control: "0 deleted, 0 left" is the bug.
     */
    const tables = ['FeedEntry', 'Reaction', 'ModerationAction', 'Comment', 'Post', 'OutboxMessage', 'Membership', 'CommunitySpace'];
    let deleted = 0;
    let left = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        deleted += await inTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).then(
          (r) => r.rowCount ?? 0,
          (err) => (console.error(`  cleanup of ${table} failed: ${err.message}`), 0),
        );
      }
      for (const table of tables) {
        left += (await inTenant(owner, tenant, `SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rows: [{ n: -1 }] }))).rows[0].n;
      }
    }
    const groupsLeft = await deleteGroupsWhenEmpty(admin, scanGroups);
    await admin.disconnect().catch(() => {});
    check(
      deleted > 0 && left === 0,
      `cleanup: this run's rows are gone (${deleted} deleted, ${left} left); consumer groups ${groupsLeft.length === 0 ? 'deleted' : `left to expire: ${groupsLeft.join(', ')}`}`,
    );
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
