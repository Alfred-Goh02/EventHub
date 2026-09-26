/**
 * Asserts that a reply in Community reaches the person it answers by email — once, with
 * a way out — and reaches nobody it should not.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - A reply named for someone with a contact card is emailed to the address in that
 *     card, once: the same message again sends nothing, and the duplicate counter
 *     moving is the proof it was read, since "still one email" is also what a consumer
 *     that never saw it would produce.
 *   - The email carries List-Unsubscribe and List-Unsubscribe-Post, and the token in it
 *     is signed for THIS tenant, user and category — checked here with the secret, not
 *     just found.
 *   - The reply's words are someone else's text: markup in them arrives escaped.
 *   - The unsubscribe link taken from the received email, POSTed, stops the next reply.
 *     The whole loop, not its parts.
 *   - Nothing is sent when Community named nobody, when the reply is to yourself, for a
 *     message that is not a reply, to someone who turned replies off, to someone with
 *     no card, or to a suppressed address. Each has its own counter as the control that
 *     the message was read — "no email" alone is also what a stalled consumer produces.
 *   - A reply retried from a retry tier is handled as a reply. The tiers carry every
 *     route's messages; without routing by the original topic it would be decoded as a
 *     password reset and dead-lettered.
 *
 * Community's messages are produced here, with every field the case needs, rather than
 * written through Community: which recipient Community names is verify-community-
 * posts.mjs's job. Contact cards are sealed here as Auth seals them and stored the way
 * the contact consumer stores them.
 *
 * Notification runs on groups of its own, starting at `latest` on community.feed, so a
 * canary on this run's key proves it is reading the partition every later message
 * lands on before anything real is sent.
 *
 * Scoped to a tenant created for this run; its rows, inbox messages and consumer groups
 * are removed at the end, and what was deleted is reported.
 *
 * Run (with Postgres, Kafka, the Schema Registry and Mailpit up; Notification built;
 * both keypairs generated): node scripts/verify-reply-emails.mjs
 */

import { spawn } from 'node:child_process';
import { createHash, createHmac, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { CompactEncrypt, SignJWT, calculateJwkThumbprint, exportJWK, importPKCS8, importSPKI } from 'jose';
import { serviceEnv } from './lib/service-env.mjs';
import { busyGroups, deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');

const TOPIC = 'community.feed';
const MAILPIT = process.env['MAILPIT_URL'] ?? 'http://127.0.0.1:8025';
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_replies_${RUN}`;
const SPACE = `space_replies_${RUN}`;
const GROUP = `verify-reply-emails-${RUN}`;
const PORT = 3985;

/** Everyone in the run, and the address their card holds. */
const who = (name) => ({ id: `user_${name}_${RUN}`, email: `reply-${name}-${RUN}@mailcheck.example` });
const ACTOR = who('actor');
const R = who('recipient');
const U = who('unsubscriber');
const O = who('optedout');
const N = who('nocard');
const S = who('suppressed');
const T = who('retried');

const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(PORT),
  KAFKA_CONSUMER_GROUP: GROUP,
  DISPATCH_START_FROM: 'latest',
  COMMUNITY_START_FROM: 'latest',
  CONTACTS_CONSUMER_GROUP: `${GROUP}-contacts`,
  // The link in the email points at THIS instance, so the loop below can follow it.
  NOTIFICATION_PUBLIC_URL: `http://127.0.0.1:${PORT}`,
  AUTH_PUBLIC_KEY_B64: Buffer.from(key('auth', 'public.pem')).toString('base64'),
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
const excerpt = (log) => (log.length <= 3000 ? log : `${log.slice(0, 2000)}\n  [...]\n${log.slice(-1000)}`);

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
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

async function inbox(address) {
  const res = await fetch(`${MAILPIT}/api/v1/search?query=${encodeURIComponent(`to:${address}`)}`);
  if (!res.ok) throw new Error(`Mailpit search answered ${res.status}`);
  return (await res.json()).messages ?? [];
}
const message = async (id) => {
  const res = await fetch(`${MAILPIT}/api/v1/message/${encodeURIComponent(id)}`);
  return res.ok ? res.json() : null;
};
const headersOf = async (id) => {
  const res = await fetch(`${MAILPIT}/api/v1/message/${encodeURIComponent(id)}/headers`);
  return res.ok ? res.json() : {};
};

/** The claims in an unsubscribe token, if the service's secret signed it. */
function verifyToken(token) {
  const [v, body, sig] = token.split('.');
  if (v !== 'v1' || !body || !sig) return null;
  const expected = createHmac('sha256', notificationEnv.UNSUBSCRIBE_SECRET).update(`v1.${body}`).digest();
  const given = Buffer.from(sig, 'base64url');
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;
  return JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
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

  // Nor may the real Notification be consuming: it shares the sent-log, so it would
  // claim this run's replies and send them itself. The group is the one Notification's
  // .env.example names, which is the one compose runs.
  const realGroup = serviceEnv(ROOT, 'notification').KAFKA_CONSUMER_GROUP;
  const kafka = new Kafka({ clientId: `verify-replies-${RUN}`, brokers: notificationEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const busy = await busyGroups(admin, [realGroup, `${realGroup}-retry`]);
  if (busy.length > 0) {
    console.error(`FAIL: ${busy.join(' and ')} has active members, so another Notification is sending. Stop it first (in compose: docker compose stop notification).`);
    process.exitCode = 1;
    await admin.disconnect();
    return;
  }

  const owner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  await owner.connect();
  const producer = kafka.producer();
  await producer.connect();
  const registry = new SchemaRegistry({ host: notificationEnv.SCHEMA_REGISTRY_URL });
  const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);

  const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
  const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
  const authSigning = await importPKCS8(key('auth', 'private.pem'), 'RS256');

  /** A card as Auth seals one and the contact consumer stores it: the sealed string, its message id and its instant. */
  async function storeCard(user) {
    const messageId = randomUUID();
    const occurredAt = '2026-09-26T00:00:00.000Z';
    const signed = await new SignJWT({ tenantId: TENANT, email: user.email, occurredAt })
      .setProtectedHeader({ alg: 'RS256', typ: 'auth-contact+jwt' })
      .setSubject(user.id)
      .setJti(messageId)
      .setIssuer(notificationEnv.JWT_ISSUER)
      .setAudience('eventhub-notification')
      .setIssuedAt()
      .sign(authSigning);
    const sealed = await new CompactEncrypt(new TextEncoder().encode(signed))
      .setProtectedHeader({ alg: 'RSA-OAEP-256', enc: 'A256GCM', cty: 'JWT', kid })
      .encrypt(notificationPublic);
    await inTenant(
      owner,
      TENANT,
      `INSERT INTO "ContactCard" ("tenantId", "userId", sealed, "messageId", "occurredAt") VALUES ($1, $2, $3, $4, ($5::timestamptz AT TIME ZONE 'UTC'))`,
      [TENANT, user.id, sealed, messageId, occurredAt],
    );
  }

  /** A community.feed message, keyed by the run's space so every one lands on one partition. */
  function activity(fields) {
    return {
      messageId: randomUUID(),
      occurredAt: new Date().toISOString(),
      tenantId: TENANT,
      spaceId: SPACE,
      kind: 'COMMENT_CREATED',
      actorId: ACTOR.id,
      postId: `post_${RUN}`,
      commentId: `c_${randomBytes(4).toString('hex')}`,
      parentCommentId: `p_${RUN}`,
      body: 'a reply',
      recipientId: R.id,
      ...fields,
    };
  }
  const produce = async (value, { topic = TOPIC, headers = {} } = {}) =>
    producer.send({ topic, messages: [{ key: SPACE, value: await registry.encode(schemaId, value), headers }] });

  const metrics = async () => (await (await fetch(`http://127.0.0.1:${PORT}/metrics`)).json());
  /** Produce, then wait until the counter named moves: the proof the message was read. */
  async function produceAndWait(value, counter, opts, timeoutMs = 45_000) {
    const read = (m) => (counter.startsWith('dispatch.') ? m.dispatch[counter.slice(9)] : m.replies[counter]);
    const before = read(await metrics());
    await produce(value, opts);
    return waitFor(async () => read(await metrics()) > before, timeoutMs);
  }

  const service = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', 'notification'), env: notificationEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  service.stdout.on('data', (d) => (log += d));
  service.stderr.on('data', (d) => (log += d));
  const everyone = [ACTOR, R, U, O, N, S, T];

  try {
    console.log('  the fixture');
    for (const user of [R, U, O, S, T]) await storeCard(user);
    await inTenant(owner, TENANT, `INSERT INTO "NotificationPreference" ("tenantId", "userId", category, channel, enabled) VALUES ($1, $2, 'COMMUNITY_REPLY', 'EMAIL', false)`, [TENANT, O.id]);
    await inTenant(
      owner,
      TENANT,
      `INSERT INTO "EmailSuppression" (id, "tenantId", "addressHash", reason, "expiresAt") VALUES ($1, $2, $3, 'HARD_BOUNCE', now() + interval '1 day')`,
      [randomUUID(), TENANT, createHash('sha256').update(S.email.trim().toLowerCase()).digest('hex')],
    );
    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${PORT}` : `it never became ready:\n${excerpt(log)}`);
    if (!ready) return;
    // Both groups: the retry tiers' group also starts at latest, and the tier check at
    // the end would otherwise pass or fail on whether it had joined in time.
    const joined = await waitFor(async () => {
      const { groups } = await admin.describeGroups([GROUP, `${GROUP}-retry`]);
      return groups.length === 2 && groups.every((g) => g.state === 'Stable' && g.members.length > 0);
    }, 60_000, 500);
    check(Boolean(joined), 'CONTROL: its dispatch group and its retry group have both joined');
    // A canary on this run's key until one is counted: the group started at latest, so
    // anything produced before it held the partition was never going to be read.
    const reading = await waitFor(async () => produceAndWait(activity({ kind: 'POST_CREATED', recipientId: null }), 'notAReplyTotal', undefined, 5_000), 60_000, 500);
    check(Boolean(reading), 'CONTROL: it read a canary on the partition every message below lands on');
    if (failures > 0) return;

    console.log('');
    console.log('  a reply reaches the person it answers, once');
    const hostile = `<script>alert(1)</script> & "quoted" ${'x'.repeat(400)}`;
    const reply = activity({ body: hostile });
    await produce(reply);
    const received = await waitFor(async () => {
      const m = await inbox(R.email);
      return m.length >= 1 ? m : null;
    }, 45_000);
    check(received?.length === 1, `one email in the recipient's inbox (${received?.length ?? 0})`);
    const mail = received?.[0] ? await message(received[0].ID) : null;
    const headers = received?.[0] ? await headersOf(received[0].ID) : {};
    check(mail?.Subject === 'Someone replied to your comment', `the subject says what happened (${mail?.Subject})`);
    check(
      Boolean(mail) && !mail.HTML.includes('<script>') && mail.HTML.includes('&lt;script&gt;') && mail.HTML.includes('&amp;'),
      'the reply\'s markup arrives escaped, not as markup',
    );
    const quoted = mail?.Text?.split('\n').find((l) => l.startsWith('  ')) ?? '';
    check([...quoted.trim()].length <= 281 && quoted.trim().endsWith('…'), `only an excerpt is quoted (${[...quoted.trim()].length} characters)`);
    const listUnsubscribe = headers['List-Unsubscribe']?.[0] ?? '';
    const unsubscribeUrl = listUnsubscribe.match(/^<(.+)>$/)?.[1] ?? '';
    const claims = unsubscribeUrl ? verifyToken(new URL(unsubscribeUrl).searchParams.get('token') ?? '') : null;
    check(
      headers['List-Unsubscribe-Post']?.[0] === 'List-Unsubscribe=One-Click' && Boolean(unsubscribeUrl),
      `it carries List-Unsubscribe and List-Unsubscribe-Post (${listUnsubscribe ? 'present' : 'missing'})`,
    );
    check(
      claims?.t === TENANT && claims?.u === R.id && claims?.c === 'COMMUNITY_REPLY',
      "and the token in it is signed with the service's secret, for this tenant, this user and replies only",
    );
    check(Boolean(mail) && mail.Text.includes(unsubscribeUrl) && mail.Text.includes(`/posts/${encodeURIComponent(reply.postId)}`), 'the body carries the same link, and a link to the post');

    const again = await produceAndWait(reply, 'dispatch.duplicatesSkippedTotal');
    await sleep(1_000);
    check(Boolean(again) && (await inbox(R.email)).length === 1, 'the same message again is read, and sends nothing');

    const onPost = activity({ parentCommentId: null, body: 'on the post' });
    await produce(onPost);
    const second = await waitFor(async () => ((await inbox(R.email)).length === 2 ? true : null), 45_000);
    const subjects = (await inbox(R.email)).map((m) => m.Subject).sort();
    check(Boolean(second) && subjects.includes('Someone commented on your post'), `a top-level comment says so (${subjects.join(' / ')})`);

    console.log('');
    console.log('  and nobody it should not');
    const cases = [
      ['Community named nobody', activity({ recipientId: null }), 'nobodyToTellTotal', null],
      ['a reply to yourself', activity({ actorId: R.id }), 'toSelfTotal', R],
      ['a reaction, which is not a reply', activity({ kind: 'REACTION_ADDED' }), 'notAReplyTotal', R],
      ['someone who turned replies off', activity({ recipientId: O.id }), 'optedOutTotal', O],
      ['someone with no contact card', activity({ recipientId: N.id }), 'noAddressTotal', N],
    ];
    for (const [what, value, counter, target] of cases) {
      const read = await produceAndWait(value, counter);
      await sleep(500);
      const mails = target ? (await inbox(target.email)).length : 0;
      const expected = target === R ? 2 : 0;
      check(Boolean(read) && mails === expected, `${what}: read, and nothing sent (${counter} moved: ${Boolean(read)}; inbox ${mails})`);
    }
    const suppressedRead = await produceAndWait(activity({ recipientId: S.id }), 'dispatch.suppressedTotal');
    const sStatus = (await inTenant(owner, TENANT, `SELECT status::text AS s FROM "NotificationDelivery" WHERE "tenantId" = $1 AND "recipientId" = $2`, [TENANT, S.id])).rows[0]?.s;
    check(Boolean(suppressedRead) && sStatus === 'SUPPRESSED' && (await inbox(S.email)).length === 0, `a suppressed address: recorded ${sStatus}, and nothing sent`);

    console.log('');
    console.log('  the unsubscribe link in the email stops the next one');
    await produce(activity({ recipientId: U.id, body: 'first' }));
    const first = await waitFor(async () => {
      const m = await inbox(U.email);
      return m.length === 1 ? m[0] : null;
    }, 45_000);
    const link = first ? ((await headersOf(first.ID))['List-Unsubscribe']?.[0] ?? '').match(/^<(.+)>$/)?.[1] : null;
    const posted = link ? await fetch(link, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: 'List-Unsubscribe=One-Click' }) : null;
    check(Boolean(first) && posted?.status === 200, `the link from the received email, POSTed as a mail client's one-click does, answers 200 (${posted?.status})`);
    const stopped = await produceAndWait(activity({ recipientId: U.id, body: 'second' }), 'optedOutTotal');
    await sleep(500);
    check(Boolean(stopped) && (await inbox(U.email)).length === 1, `and the next reply to them is read and not sent (inbox ${(await inbox(U.email)).length})`);

    console.log('');
    console.log('  a reply retried from a tier is handled as a reply');
    await produce(activity({ recipientId: T.id, body: 'retried' }), {
      topic: 'notification.retry.1',
      headers: { 'retry-original-topic': TOPIC, 'retry-attempt': '1', 'retry-not-before': String(Date.now()), 'retry-first-failed-at': new Date().toISOString() },
    });
    const retried = await waitFor(async () => ((await inbox(T.email)).length === 1 ? true : null), 60_000);
    check(Boolean(retried), 'it reaches the person it answers, rather than being decoded as a password reset and dead-lettered');
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Notification's output:\n${excerpt(log)}`);
  } finally {
    service.kill();
    let removed = 0;
    for (const table of ['NotificationDelivery', 'ContactCard', 'NotificationPreference', 'EmailSuppression']) {
      removed += (await inTenant(owner, TENANT, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [TENANT]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
    }
    const left = (await inTenant(owner, TENANT, `SELECT (SELECT count(*) FROM "NotificationDelivery" WHERE "tenantId" = $1) + (SELECT count(*) FROM "ContactCard" WHERE "tenantId" = $1)
       + (SELECT count(*) FROM "NotificationPreference" WHERE "tenantId" = $1) + (SELECT count(*) FROM "EmailSuppression" WHERE "tenantId" = $1) AS n`, [TENANT])).rows[0].n;
    const ids = [];
    for (const user of everyone) ids.push(...(await inbox(user.email).catch(() => [])).map((m) => m.ID));
    if (ids.length > 0) {
      await fetch(`${MAILPIT}/api/v1/messages`, { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ IDs: ids }) }).catch(() => {});
    }
    check(removed > 0 && Number(left) === 0, `cleanup: this run's rows are gone (${removed} deleted, ${left} left), and ${ids.length} email(s) removed from Mailpit`);
    const stuck = await deleteGroupsWhenEmpty(admin, [GROUP, `${GROUP}-retry`, `${GROUP}-contacts`]);
    if (stuck.length > 0) console.log(`    note  consumer group(s) left to expire: ${stuck.join(', ')}`);
    await Promise.allSettled([producer.disconnect(), admin.disconnect(), owner.end()]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a reply reaches the person it answers, once, with a way out that works — and nobody it should not');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
