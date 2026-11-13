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
 *   - The body links to the post on the RECIPIENT'S TENANT'S OWN SUBDOMAIN: the
 *     example's APP_URL_TEMPLATE filled with the slug Auth signed into their contact
 *     card. Two tenants, each with a slug only this run uses, and each recipient's link
 *     must open exactly their own tenant's host, so a link on one shared host -- what
 *     PUBLIC_APP_URL was -- or on the other tenant's fails. The unsubscribe link must
 *     NOT follow: it stays on NOTIFICATION_PUBLIC_URL, one host for both tenants.
 *   - A card with no slug, from before cards carried one, and cards whose signed slug
 *     the gateway would not route -- a reserved name, a capital and a dot, a slash --
 *     each get the email WITHOUT the post link, and the counter that says so moves. No
 *     link anywhere in such an email opens a host made from the slug.
 *   - The reply's words are someone else's text: markup in them arrives escaped, in
 *     both parts, and only an excerpt is quoted in either. The fixture puts an emoji
 *     across the excerpt's cut, so a cut by UTF-16 unit, which splits it, is visible.
 *   - The unsubscribe link taken from the received email, POSTed, stops the next reply.
 *     The whole loop, not its parts.
 *   - Nothing is sent when Community named nobody, when the reply is to yourself, for a
 *     message that is not a reply, to someone who turned replies off, to someone with
 *     no card or a card that does not open, or to a suppressed address. Each has its own
 *     counter as the control that the message was read — "no email" alone is also what
 *     a stalled consumer produces.
 *   - Past REPLY_MAX_AGE_MS a reply is not sent, and the age is Postgres's to judge. The
 *     service runs an hour fast (scripts/lib/skewed-clock.mjs) with a limit of 45
 *     minutes, a value nothing else uses: a reply 30 minutes old must go out, which a
 *     comparison on the process's clock would call 90 minutes old, and one 50 minutes old
 *     must not, which a limit wired to any hour-long setting would let through.
 *   - The event-type header is read before the value: a post whose bytes cannot be
 *     decoded is skipped, not dead-lettered. A header present with a null value is read
 *     as absent, rather than crashing the consumer on every redelivery.
 *   - A message that can never be a reply is dead-lettered WITHOUT its value, and the
 *     replay command, run exactly as the README documents it, says so rather than
 *     replaying nothing.
 *   - A reply carrying a pending password reset's message id does not touch the reset's
 *     delivery row.
 *   - A store outage in front of the send is retried, not dropped: the reply goes to the
 *     first tier stamped with community.feed, and arrives once the store is back. The
 *     outage is a revoked SELECT, on the card table and the preference table in turn.
 *   - A retry that declines — the recipient opted out after an attempt claimed the
 *     delivery — closes the claimed row, rather than leaving it PENDING for the sweeper
 *     to call abandoned.
 *   - A reply retried from a tier is handled as a reply. The tiers carry every route's
 *     messages; without routing by the original topic it would be decoded as a password
 *     reset and dead-lettered.
 *   - A reply stuck on its store does not hold up a password reset, on the main topics or
 *     on the retry tiers: with one reply stuck on each, a reset and a reset's retry both
 *     arrive while both replies are still waiting.
 *   - A reply already on the topic before the service first started is never sent: the
 *     replies group starts at `latest` whatever the reset path does, and the reset path
 *     runs `earliest` here, as compose runs it, so a route that took the reset path's
 *     setting would read it.
 *
 * Community's messages are produced here, with every field the case needs, rather than
 * written through Community: which recipient Community names is verify-community-
 * posts.mjs's job. Contact cards are sealed here as Auth seals them and stored the way
 * the contact consumer stores them.
 *
 * Notification runs on groups of its own. Its reset path starts `earliest`, as compose
 * runs it, with this run's groups committed at the end of their topics first, so it does
 * not work through every reset ever requested; its contact-card group is committed at the
 * end of auth.user-contact the same way (scripts/lib/notification-groups.mjs), since the
 * cards this run needs are written straight to the table. Its replies group starts where its
 * default says, and a canary on this run's key proves it is reading the partition every
 * later message lands on before anything real is sent.
 *
 * Scoped to two tenants created for this run; their rows, inbox messages and consumer
 * groups are removed at the end, and what was deleted is reported. Every SELECT this revokes is
 * granted back in the same run, and granted again at the start in case a run was killed
 * holding one.
 *
 * Run (with Postgres, Kafka, the Schema Registry and Mailpit up; Notification built;
 * both keypairs generated): node scripts/verify-reply-emails.mjs
 */

import { spawn } from 'node:child_process';
import { createHash, createHmac, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { CompactEncrypt, SignJWT, calculateJwkThumbprint, exportJWK, importPKCS8, importSPKI } from 'jose';
import { serviceEnv } from './lib/service-env.mjs';
import { busyGroups, commitAtEnd, deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';
import { contactsFromTheEnd, notificationGroups } from './lib/notification-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');

const TOPIC = 'community.feed';
const RESET_TOPIC = 'auth.email-requested';
const TIERS = ['notification.retry.1', 'notification.retry.2', 'notification.retry.3'];
const DLQ = 'notification.dlq';
const MAILPIT = process.env['MAILPIT_URL'] ?? 'http://127.0.0.1:8025';
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_replies_${RUN}`;
/** A second tenant, so a post link that does not follow the recipient's tenant shows. */
const TENANT2 = `tenant_replies_other_${RUN}`;
const TENANTS = [TENANT, TENANT2];
/** Each tenant's subdomain, as Auth signs it into a card: slugs only this run uses. */
const SLUG = `replies-${RUN}`;
const SLUG2 = `replies-other-${RUN}`;
/** The host each recipient's post link must open: the example's template, filled. */
const hostOf = (slug) => `${slug}.eventhub.localhost:5173`;
const SPACE = `space_replies_${RUN}`;
const GROUP = `verify-reply-emails-${RUN}`;
const GROUPS = notificationGroups(GROUP);
const PORT = 3985;
/** Short, so a retried reply comes back within the run. */
const DELAYS_MS = [3000, 3000, 3000];
/** Forty-five minutes: a limit no other setting shares, so a limit wired to another would show. */
const MAX_AGE_MS = 2_700_000;
const MINUTE = 60_000;

/** Everyone in the run, and the address their card holds. */
const who = (name) => ({ id: `user_${name}_${RUN}`, email: `reply-${name}-${RUN}@mailcheck.example` });
const ACTOR = who('actor');
const R = who('recipient');
const U = who('unsubscriber');
const O = who('optedout');
const N = who('nocard');
const S = who('suppressed');
const T = who('retried');
const E = who('early');
const K = who('resetkey');
const X = who('unreadable');
const C = who('cardoutage');
const V = who('prefoutage');
const P = who('declined');
const L = who('stuck');
const L2 = who('stuckretry');
const W = who('resetter');
const W2 = who('resetretry');
const Y = who('recent');
/** In the second tenant, with that tenant's slug on the card. */
const R2 = { ...who('othertenant'), tenant: TENANT2 };
/** Cards that name no subdomain a link can open: none at all, as before cards carried one, and three the gateway would not route. */
const NS = who('noslug');
const BAD = [
  [who('reservedslug'), 'www'],
  [who('shapedslug'), 'Evil.Example'],
  [who('pathslug'), 'evil.example/x'],
];

const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(PORT),
  // Both of its groups are this run's: see scripts/lib/notification-groups.mjs.
  ...GROUPS.env,
  DISPATCH_RETRY_DELAYS_MS: DELAYS_MS.join(','),
  REPLY_MAX_AGE_MS: String(MAX_AGE_MS),
  // An hour fast, so a comparison made on this process's clock is visible.
  NODE_OPTIONS: `--import=${pathToFileURL(join(ROOT, 'scripts', 'lib', 'skewed-clock.mjs')).href}`,
  // The link in the email points at THIS instance, so the loop below can follow it.
  NOTIFICATION_PUBLIC_URL: `http://127.0.0.1:${PORT}`,
  // APP_URL_TEMPLATE is the example's: the post links below must open its hosts.
  AUTH_PUBLIC_KEY_B64: Buffer.from(key('auth', 'public.pem')).toString('base64'),
});
// DISPATCH_START_FROM stays .env.example's `earliest`, and COMMUNITY_START_FROM is left
// to its default, which is how compose runs both.
delete notificationEnv.COMMUNITY_START_FROM;
/** The role the service runs as, from its own URL rather than a copy of its name. */
const APP_ROLE = decodeURIComponent(new URL(notificationEnv.APP_DATABASE_URL).username);
const OUTAGE_TABLES = ['ContactCard', 'NotificationPreference', 'EmailSuppression'];

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
const kafkaHeaders = (m) => Object.fromEntries(Object.entries(m.headers ?? {}).map(([k, v]) => [k, v?.toString()]));
/** The post link on its own line in an email's text, whole, or null. */
const postLinkIn = (text) => text?.match(/^(https?:\/\/\S*\/posts\/\S+?)\r?$/m)?.[1] ?? null;
/** Every URL in both parts of an email. */
const urlsIn = (mail) => [...`${mail?.Text ?? ''}\n${mail?.HTML ?? ''}`.matchAll(/https?:\/\/[^\s"<>]+/g)].map((m) => m[0]);

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

  // Nor may the real Notification be consuming: it shares the sent-log and Mailpit, so
  // it would claim this run's replies and send them itself, and a check here would pass
  // on its work. The groups are the ones Notification's .env.example names, which are
  // the ones compose runs.
  const realGroup = serviceEnv(ROOT, 'notification').KAFKA_CONSUMER_GROUP;
  const kafka = new Kafka({ clientId: `verify-replies-${RUN}`, brokers: notificationEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const busy = await busyGroups(admin, [realGroup, `${realGroup}-retry`, `${realGroup}-replies`, `${realGroup}-replies-retry`]);
  if (busy.length > 0) {
    console.error(`FAIL: ${busy.join(' and ')} has active members, so another Notification is sending. Stop it first (in compose: docker compose stop notification).`);
    process.exitCode = 1;
    await admin.disconnect();
    return;
  }

  const owner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  await owner.connect();
  const grantBack = (table) => owner.query(`GRANT SELECT ON "${table}" TO "${APP_ROLE}"`);
  const revoke = (table) => owner.query(`REVOKE SELECT ON "${table}" FROM "${APP_ROLE}"`);
  // A run killed while holding a revoke would leave the service unable to read a table.
  for (const table of OUTAGE_TABLES) await grantBack(table);

  const producer = kafka.producer();
  await producer.connect();
  const registry = new SchemaRegistry({ host: notificationEnv.SCHEMA_REGISTRY_URL });
  const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);
  const resetSchemaId = await registry.getLatestSchemaId(`${RESET_TOPIC}-value`);

  const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
  const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
  const authSigning = await importPKCS8(key('auth', 'private.pem'), 'RS256');
  const seal = async (jws) =>
    new CompactEncrypt(new TextEncoder().encode(jws)).setProtectedHeader({ alg: 'RSA-OAEP-256', enc: 'A256GCM', cty: 'JWT', kid }).encrypt(notificationPublic);

  /**
   * A card as Auth seals one and the contact consumer stores it: the sealed string, its
   * message id and its instant. The tenant's slug is signed in with the rest, as Auth
   * signs it, unless `slug` is null: a card from before cards carried one.
   */
  async function storeCard(user, sealedOverride, { slug = user.tenant === TENANT2 ? SLUG2 : SLUG } = {}) {
    const tenant = user.tenant ?? TENANT;
    const messageId = randomUUID();
    const occurredAt = '2026-09-26T00:00:00.000Z';
    const signed = await new SignJWT({ tenantId: tenant, email: user.email, occurredAt, ...(slug === null ? {} : { tenantSlug: slug }) })
      .setProtectedHeader({ alg: 'RS256', typ: 'auth-contact+jwt' })
      .setSubject(user.id)
      .setJti(messageId)
      .setIssuer(notificationEnv.JWT_ISSUER)
      .setAudience('eventhub-notification')
      .setIssuedAt()
      .sign(authSigning);
    const sealed = sealedOverride ?? (await seal(signed));
    await inTenant(
      owner,
      tenant,
      `INSERT INTO "ContactCard" ("tenantId", "userId", sealed, "messageId", "occurredAt") VALUES ($1, $2, $3, $4, ($5::timestamptz AT TIME ZONE 'UTC'))`,
      [tenant, user.id, sealed, messageId, occurredAt],
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
  /**
   * With the headers the relay stamps: message-id, and event-type from the kind.
   * `eventType: null` leaves the header off, so the value alone decides.
   */
  const produce = async (value, { topic = TOPIC, headers = {}, eventType } = {}) => {
    const type = eventType === undefined ? `community.${value.kind.toLowerCase()}` : eventType;
    return producer.send({
      topic,
      messages: [
        {
          key: SPACE,
          value: await registry.encode(schemaId, value),
          headers: { 'message-id': value.messageId, ...(type ? { 'event-type': type } : {}), ...headers },
        },
      ],
    });
  };

  /**
   * A password reset as Auth requests one: signed by Auth, sealed to Notification. Two
   * hours out, because the service's clock runs one hour fast. Onto auth.email-requested,
   * or straight onto a tier as a retry of one.
   */
  async function requestReset(user, { topic = RESET_TOPIC, headers = {} } = {}) {
    const messageId = randomUUID();
    const expiresAt = new Date(Date.now() + 120 * MINUTE);
    const jws = await new SignJWT({ tenantId: TENANT, email: user.email, link: `https://app.example/reset-password?token=${RUN}`, kind: 'PASSWORD_RESET' })
      .setProtectedHeader({ alg: 'RS256', typ: 'auth-email+jwt' })
      .setSubject(user.id)
      .setJti(messageId)
      .setIssuer(notificationEnv.JWT_ISSUER)
      .setAudience('eventhub-notification')
      .setIssuedAt()
      .setExpirationTime(expiresAt)
      .sign(authSigning);
    await producer.send({
      topic,
      messages: [
        {
          key: user.id,
          value: await registry.encode(resetSchemaId, {
            messageId,
            occurredAt: new Date().toISOString(),
            tenantId: TENANT,
            userId: user.id,
            kind: 'PASSWORD_RESET',
            expiresAt: expiresAt.toISOString(),
            sealed: await seal(jws),
          }),
          headers: { 'message-id': messageId, ...headers },
        },
      ],
    });
    return messageId;
  }
  /** The headers forward() puts on a first retry, due now. */
  const firstRetryOf = (originalTopic) => ({
    'retry-original-topic': originalTopic,
    'retry-attempt': '1',
    'retry-not-before': String(Date.now()),
    'retry-first-failed-at': new Date().toISOString(),
  });

  const scanGroups = [];
  /** Every message on `topic` carrying this message id, read from the beginning. */
  async function scan(topic, messageId, timeoutMs = 20_000) {
    const groupId = `${GROUP}-scan-${scanGroups.length}`;
    scanGroups.push(groupId);
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    const found = [];
    await consumer.run({
      eachMessage: async ({ message: m }) => {
        if (m.headers?.['message-id']?.toString() === messageId) found.push(m);
      },
    });
    await waitFor(async () => found.length > 0, timeoutMs, 250);
    await sleep(500);
    await consumer.disconnect();
    return found;
  }

  const metrics = async () => (await (await fetch(`http://127.0.0.1:${PORT}/metrics`)).json());
  const read = (m, counter) => (counter.startsWith('dispatch.') ? m.dispatch[counter.slice(9)] : m.replies[counter]);
  /** Produce, then wait until the counter named moves: the proof the message was read. */
  async function produceAndWait(value, counter, opts, timeoutMs = 45_000) {
    const before = read(await metrics(), counter);
    await produce(value, opts);
    return waitFor(async () => read(await metrics(), counter) > before, timeoutMs);
  }
  const deliveries = async (messageId) =>
    (await inTenant(
      owner,
      TENANT,
      `SELECT d.status::text AS status, d."templateKey", d."idempotencyKey", d."lastError" FROM "NotificationDelivery" d
        WHERE d."tenantId" = $1 AND d."sourceMessageId" = $2 ORDER BY d."templateKey"`,
      [TENANT, messageId],
    )).rows;

  const everyone = [ACTOR, R, U, O, N, S, T, E, K, X, C, V, P, L, L2, W, W2, Y, R2, NS, ...BAD.map(([u]) => u)];
  let service = null;
  let log = '';

  try {
    console.log('  the fixture, before the service starts');
    for (const user of [R, U, O, S, T, E, K, C, V, P, L, L2, Y, R2]) await storeCard(user);
    await storeCard(NS, undefined, { slug: null });
    for (const [user, slug] of BAD) await storeCard(user, undefined, { slug });
    // Sealed to nobody: a card that was stored and no longer opens.
    await storeCard(X, 'not-a-jwe');
    await inTenant(owner, TENANT, `INSERT INTO "NotificationPreference" ("tenantId", "userId", category, channel, enabled) VALUES ($1, $2, 'COMMUNITY_REPLY', 'EMAIL', false)`, [TENANT, O.id]);
    await inTenant(
      owner,
      TENANT,
      `INSERT INTO "EmailSuppression" (id, "tenantId", "addressHash", reason, "expiresAt") VALUES ($1, $2, $3, 'HARD_BOUNCE', now() + interval '1 day')`,
      [randomUUID(), TENANT, createHash('sha256').update(S.email.trim().toLowerCase()).digest('hex')],
    );
    // Already on the topic when the service first starts. Never to be sent.
    const early = activity({ recipientId: E.id, body: 'written before the service started' });
    await produce(early);
    // The reset path starts `earliest`, as compose runs it; committing this run's groups
    // at the end of their topics first keeps it from working through every reset ever
    // requested, and leaves the replies group the only one without a position. The
    // contacts group too: this run's cards are written straight to the table, and a
    // contacts group from the start would replay every card on auth.user-contact.
    await commitAtEnd(admin, GROUP, [RESET_TOPIC]);
    await commitAtEnd(admin, `${GROUP}-retry`, TIERS);
    const contacts = await contactsFromTheEnd(admin, ROOT, GROUPS);

    service = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', 'notification'), env: notificationEnv, stdio: ['ignore', 'pipe', 'pipe'] });
    service.stdout.on('data', (d) => (log += d));
    service.stderr.on('data', (d) => (log += d));

    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${PORT}` : `it never became ready:\n${excerpt(log)}`);
    if (!ready) return;
    check(log.includes('clock skewed by +3600s'), 'CONTROL: its clock runs an hour fast, so a check on it would show');
    // Every group: each retry group and the replies group matter below, and a check would
    // otherwise pass or fail on whether one had joined in time.
    const groups = [GROUP, `${GROUP}-retry`, `${GROUP}-replies`, `${GROUP}-replies-retry`];
    const joined = await waitFor(async () => {
      const { groups: described } = await admin.describeGroups(groups);
      return described.length === groups.length && described.every((g) => g.state === 'Stable' && g.members.length > 0);
    }, 60_000, 500);
    check(Boolean(joined), 'CONTROL: its reset group, its replies group and each one\'s retry group have all joined');
    const own = await contacts.joined();
    check(own.ok, own.message);
    // A canary on this run's key until one is counted: the replies group started at
    // latest, so anything produced before it held the partition was never going to be read.
    const reading = await waitFor(async () => produceAndWait(activity({ kind: 'POST_CREATED', recipientId: null }), 'notAReplyTotal', undefined, 5_000), 60_000, 500);
    check(Boolean(reading), 'CONTROL: it read a canary on the partition every message below lands on');
    if (failures > 0) return;

    console.log('');
    console.log('  a reply reaches the person it answers, once');
    // Markup to escape, and an emoji whose code point is the 280th: the excerpt keeps it
    // whole, and a cut by UTF-16 unit would leave half of it.
    const prefix = '<script>alert(1)</script> & "quoted" ';
    const hostile = `${prefix}${'x'.repeat(279 - [...prefix].length)}😀${'y'.repeat(60)}`;
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
    const quoted = (mail?.Text?.split('\n').find((l) => l.startsWith('  ')) ?? '').trim();
    check(
      [...quoted].length === 281 && quoted.endsWith('😀…') && !quoted.includes('�') && !quoted.includes('yyyy'),
      `only an excerpt is quoted, cut whole at 280 code points (${[...quoted].length} characters, ends ${JSON.stringify(quoted.slice(-3))})`,
    );
    check(
      Boolean(mail) && mail.HTML.includes('😀…') && !mail.HTML.includes('yyyy') && !mail.HTML.includes('�'),
      'and only the same excerpt in the HTML part, which is what most clients show',
    );
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
    const postUrl = `http://${hostOf(SLUG)}/posts/${encodeURIComponent(reply.postId)}`;
    check(
      Boolean(mail) && mail.Text.includes(unsubscribeUrl) && mail.HTML.includes(`href="${unsubscribeUrl}"`),
      'both parts carry the same unsubscribe link, where the reader can see it',
    );
    // The post link on its own line, whole, compared with ===: a link with something
    // appended still contains the real one.
    const postLink = postLinkIn(mail?.Text);
    check(
      postLink === postUrl && Boolean(mail) && mail.HTML.includes(`href="${postUrl}"`),
      `and link to the post on the recipient's tenant's own subdomain, in both parts (${postLink ? new URL(postLink).host : 'no post link'})`,
    );
    check(
      Boolean(unsubscribeUrl) && new URL(unsubscribeUrl).origin === notificationEnv.NOTIFICATION_PUBLIC_URL,
      `while the unsubscribe link stays on NOTIFICATION_PUBLIC_URL, the one host for every tenant (${unsubscribeUrl ? new URL(unsubscribeUrl).host : 'none'})`,
    );

    const again = await produceAndWait(reply, 'dispatch.duplicatesSkippedTotal');
    await sleep(1_000);
    check(Boolean(again) && (await inbox(R.email)).length === 1, 'the same message again is read, and sends nothing');

    const onPost = activity({ parentCommentId: null, body: 'on the post' });
    await produce(onPost);
    const second = await waitFor(async () => ((await inbox(R.email)).length === 2 ? true : null), 45_000);
    const subjects = (await inbox(R.email)).map((m) => m.Subject).sort();
    check(Boolean(second) && subjects.includes('Someone commented on your post'), `a top-level comment says so (${subjects.join(' / ')})`);

    console.log('');
    console.log("  in another tenant the post link follows the recipient's tenant, and the unsubscribe link does not");
    const elsewhere = activity({ tenantId: TENANT2, recipientId: R2.id, body: 'in the other tenant' });
    await produce(elsewhere);
    const otherMail = await waitFor(async () => {
      const m = await inbox(R2.email);
      return m.length >= 1 ? m : null;
    }, 45_000);
    const om = otherMail?.[0] ? await message(otherMail[0].ID) : null;
    const otherPost = postLinkIn(om?.Text);
    check(
      otherMail?.length === 1 && otherPost === `http://${hostOf(SLUG2)}/posts/${encodeURIComponent(elsewhere.postId)}`,
      `one email, whose post link opens ${otherPost ? new URL(otherPost).host : 'nothing'}, not ${hostOf(SLUG)}`,
    );
    const otherUnsubscribe = ((otherMail?.[0] ? await headersOf(otherMail[0].ID) : {})['List-Unsubscribe']?.[0] ?? '').match(/^<(.+)>$/)?.[1] ?? '';
    check(
      Boolean(otherUnsubscribe) &&
        new URL(otherUnsubscribe).origin === new URL(unsubscribeUrl).origin &&
        verifyToken(new URL(otherUnsubscribe).searchParams.get('token') ?? '')?.t === TENANT2,
      `its unsubscribe link is on the same host as the first tenant's, signed for this tenant (${otherUnsubscribe ? new URL(otherUnsubscribe).host : 'none'})`,
    );

    console.log('');
    console.log('  a card that names no subdomain the gateway routes: the email goes, without the post link');
    /**
     * A card from before cards carried the slug, and three whose signed slug Kong would
     * refuse. Each reply is still news, and its unsubscribe link still works; only the
     * post link needs the slug, so only the post link is left out. Put into the template
     * raw, each of these would make a host -- null.eventhub..., www.eventhub...,
     * evil.example.eventhub..., evil.example -- which is what the host check reads for.
     */
    const unsubscribeHost = new URL(notificationEnv.NOTIFICATION_PUBLIC_URL).host;
    for (const [user, slug] of [[NS, null], ...BAD]) {
      const before = (await metrics()).replies.withoutLinkTotal;
      await produce(activity({ recipientId: user.id, body: `to a card with ${slug ?? 'no slug'}` }));
      const got = await waitFor(async () => ((await inbox(user.email)).length === 1 ? (await inbox(user.email))[0] : null), 45_000);
      const m = got ? await message(got.ID) : null;
      const after = (await metrics()).replies.withoutLinkTotal;
      const hosts = [...new Set(urlsIn(m).map((u) => new URL(u).host))];
      check(
        Boolean(m) &&
          after === before + 1 &&
          !urlsIn(m).some((u) => u.includes('/posts/')) &&
          hosts.length === 1 && hosts[0] === unsubscribeHost &&
          /Sign in to EventHub to read the conversation/.test(m.Text) && m.HTML.includes('Sign in to EventHub to read the conversation'),
        `${slug === null ? 'no slug on the card' : `the signed slug ${JSON.stringify(slug)}`}: one email, no post link, the only host in it the unsubscribe one ` +
          `(${hosts.join(', ') || 'no link'}), and counted (withoutLinkTotal ${before} -> ${after})`,
      );
    }

    console.log('');
    console.log('  and nobody it should not');
    const minutesAgo = (m) => new Date(Date.now() - m * MINUTE).toISOString();
    const cases = [
      ['Community named nobody', activity({ recipientId: null }), 'nobodyToTellTotal', null],
      ['a reply to yourself', activity({ actorId: R.id }), 'toSelfTotal', R],
      // No event-type header, so the value's own kind is what is judged.
      ['a reaction, which is not a reply', activity({ kind: 'REACTION_ADDED' }), 'notAReplyTotal', R, { eventType: null }],
      [`a reply 50 minutes old, past a limit of ${MAX_AGE_MS / MINUTE}`, activity({ occurredAt: minutesAgo(50) }), 'tooOldTotal', R],
      ['someone who turned replies off', activity({ recipientId: O.id }), 'optedOutTotal', O],
      ['someone with no contact card', activity({ recipientId: N.id }), 'noAddressTotal', N],
    ];
    for (const [what, value, counter, target, opts] of cases) {
      const moved = await produceAndWait(value, counter, opts);
      await sleep(500);
      const mails = target ? (await inbox(target.email)).length : 0;
      const expected = target === R ? 2 : 0;
      check(Boolean(moved) && mails === expected, `${what}: read, and nothing sent (${counter} moved: ${Boolean(moved)}; inbox ${mails})`);
    }
    /**
     * A header present with a NULL value. kafkajs decodes one from the wire (length -1)
     * but will not produce one, so the service's own function is asked directly: it must
     * read as absent, not throw the TypeError kafkajs treats as unrecoverable, which
     * crashed the consumer on every redelivery of such a message.
     */
    const { header } = await import(pathToFileURL(join(ROOT, 'services', 'notification', 'dist', 'dispatch', 'retry.js')).href);
    const readNull = (headers, name) => {
      try {
        return header(headers, name);
      } catch (err) {
        return err;
      }
    };
    const nulls = [readNull({ 'event-type': null }, 'event-type'), readNull({ 'event-type': [null] }, 'event-type')];
    check(
      nulls.every((r) => r === undefined),
      `a header present with a null value reads as absent (${nulls.map((r) => (r instanceof Error ? r.message : String(r))).join('; ')})`,
    );

    // The other side of the age limit, and the one a clock mistake breaks: 30 minutes by
    // the database, 90 by this service's own clock.
    await produce(activity({ recipientId: Y.id, occurredAt: minutesAgo(30), body: 'thirty minutes old' }));
    const recent = await waitFor(async () => ((await inbox(Y.email)).length === 1 ? true : null), 45_000);
    check(Boolean(recent), `a reply 30 minutes old, inside the limit by the database's clock, is sent`);

    // A card that does not open is the keys' fault, not the reply's: nothing sent, and
    // nothing dead-lettered, since replaying the reply would not fix the card.
    const beforeUnreadable = await metrics();
    const unreadable = await produceAndWait(activity({ recipientId: X.id }), 'noAddressTotal');
    await sleep(1_500);
    const afterUnreadable = await metrics();
    check(
      Boolean(unreadable) && afterUnreadable.dispatch.deadLetteredTotal === beforeUnreadable.dispatch.deadLetteredTotal && (await inbox(X.email)).length === 0,
      `a card that no longer opens: read, nothing sent, nothing dead-lettered (dead-lettered ${beforeUnreadable.dispatch.deadLetteredTotal} -> ${afterUnreadable.dispatch.deadLetteredTotal})`,
    );

    // The header is read before the value: a post whose bytes are not even Avro is
    // skipped as not a reply. Decoded first, it would be poison and dead-lettered.
    const beforeGarbage = await metrics();
    await producer.send({
      topic: TOPIC,
      messages: [{ key: SPACE, value: Buffer.from(`not avro ${RUN}`), headers: { 'message-id': randomUUID(), 'event-type': 'community.post_created' } }],
    });
    const skippedByHeader = await waitFor(async () => (await metrics()).replies.notAReplyTotal > beforeGarbage.replies.notAReplyTotal, 30_000);
    await sleep(1_000);
    const afterGarbage = await metrics();
    check(
      Boolean(skippedByHeader) && afterGarbage.replies.poisonTotal === beforeGarbage.replies.poisonTotal,
      `a post that cannot be decoded is skipped by its header, not decoded (poison ${beforeGarbage.replies.poisonTotal} -> ${afterGarbage.replies.poisonTotal})`,
    );

    const suppressedRead = await produceAndWait(activity({ recipientId: S.id }), 'dispatch.suppressedTotal');
    const sStatus = (await inTenant(owner, TENANT, `SELECT status::text AS s FROM "NotificationDelivery" WHERE "tenantId" = $1 AND "recipientId" = $2`, [TENANT, S.id])).rows[0]?.s;
    check(Boolean(suppressedRead) && sStatus === 'SUPPRESSED' && (await inbox(S.email)).length === 0, `a suppressed address: recorded ${sStatus}, and nothing sent`);

    console.log('');
    console.log("  a reply that can never be sent is dead-lettered without its words");
    const poison = activity({ commentId: null, body: `words that must not be kept ${RUN}` });
    const poisoned = await produceAndWait(poison, 'dispatch.deadLetteredTotal');
    const [dead] = poisoned ? await scan(DLQ, poison.messageId) : [];
    const deadHeaders = dead ? kafkaHeaders(dead) : {};
    check(Boolean(dead), `CONTROL: its dead-letter copy is on ${DLQ} (${dead ? deadHeaders['dlq-reason'] : 'not found'})`);
    check(
      Boolean(dead) && dead.value === null && deadHeaders['dlq-value-withheld'] === 'true' && deadHeaders['dlq-original-topic'] === TOPIC,
      `with no value, marked withheld, and still saying where it came from (value ${dead?.value === null ? 'null' : `${dead?.value?.length} bytes`})`,
    );
    // Exactly as the README documents it, `--` included: pnpm hands the script that `--`
    // on every shell but PowerShell, and a script that did not strip it would look up a
    // message whose id is "--".
    const replay = await new Promise((done) => {
      const child = spawn(`pnpm --filter @eventhub/notification run dlq:replay -- ${poison.messageId}`, {
        cwd: ROOT,
        env: notificationEnv,
        shell: true,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let out = '';
      child.stdout.on('data', (d) => (out += d));
      child.stderr.on('data', (d) => (out += d));
      child.on('exit', (code) => done({ code, out }));
    });
    check(
      replay.code !== 0 && replay.out.includes('without its value'),
      `dlq:replay refuses it and says why, rather than replaying nothing (exit ${replay.code}: ${replay.out.trim().split(/\r?\n/).pop()?.slice(0, 120)})`,
    );

    console.log('');
    console.log("  a reply cannot take over a password reset's delivery");
    // A reset whose send failed and is waiting in a tier: its row is PENDING. A reply on
    // the unsigned topic carrying the same message id, for the same user, must be its
    // own delivery.
    const resetId = randomUUID();
    const resetKey = `${resetId}:email:${K.id}`;
    await inTenant(
      owner,
      TENANT,
      `INSERT INTO "NotificationDelivery" (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId")
       VALUES ($1, $2, $3, 'email', 'account', 'password-reset', $4, $5)`,
      [randomUUID(), TENANT, K.id, resetKey, resetId],
    );
    await produce(activity({ messageId: resetId, recipientId: K.id, body: 'same id as a reset' }));
    const kMail = await waitFor(async () => ((await inbox(K.email)).length === 1 ? true : null), 45_000);
    // The email can reach Mailpit a moment before SENT is committed after it, so the rows
    // are read once the reply's own row says SENT, or after waiting for it.
    await waitFor(async () => (await deliveries(resetId)).some((r) => r.templateKey === 'community-reply' && r.status === 'SENT'), 10_000);
    const kRows = await deliveries(resetId);
    const resetRow = kRows.find((r) => r.idempotencyKey === resetKey);
    const replyRow = kRows.find((r) => r.templateKey === 'community-reply');
    check(Boolean(kMail) && replyRow?.status === 'SENT', `CONTROL: the reply was sent, as a delivery of its own (${replyRow?.status ?? 'no row'})`);
    check(resetRow?.status === 'PENDING', `and the reset's row is untouched, still PENDING for its own retry (${resetRow?.status})`);

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
    console.log('  a store outage is retried, not dropped');
    /**
     * The outage is the app role losing SELECT on one table: a store failure in front of
     * the send that nothing about the message caused. Granted back as soon as the reply
     * has been moved to a tier, so its retry finds the store working.
     */
    for (const [table, user] of [['ContactCard', C], ['NotificationPreference', V]]) {
      const value = activity({ recipientId: user.id, body: `through an outage of ${table}` });
      const before = (await metrics()).dispatch.retriedTotal;
      await revoke(table);
      let moved;
      try {
        await produce(value);
        moved = await waitFor(async () => (await metrics()).dispatch.retriedTotal > before, 30_000, 100);
      } finally {
        await grantBack(table);
      }
      const arrived = await waitFor(async () => ((await inbox(user.email)).length === 1 ? true : null), 45_000);
      const [tierCopy] = moved ? await scan(TIERS[0], value.messageId) : [];
      const tierHeaders = tierCopy ? kafkaHeaders(tierCopy) : {};
      check(
        Boolean(moved) && tierHeaders['retry-original-topic'] === TOPIC && tierHeaders['retry-attempt'] === '1',
        `with ${table} unreadable, the reply went to the first tier stamped with where it came from (${tierHeaders['retry-original-topic'] ?? 'no tier copy'})`,
      );
      check(Boolean(arrived) && (await inbox(user.email)).length === 1, `and once ${table} could be read again, it arrived, once`);
    }

    console.log('');
    console.log('  a retry that declines closes the delivery an earlier attempt claimed');
    {
      const value = activity({ recipientId: P.id, body: 'claimed, then declined' });
      const before = await metrics();
      await revoke('EmailSuppression');
      let moved;
      let claimed;
      try {
        await produce(value);
        moved = await waitFor(async () => (await metrics()).dispatch.retriedTotal > before.dispatch.retriedTotal, 30_000, 100);
        claimed = (await deliveries(value.messageId))[0]?.status;
        // Opted out between the attempt and its retry, as a click on an earlier email would.
        await inTenant(owner, TENANT, `INSERT INTO "NotificationPreference" ("tenantId", "userId", category, channel, enabled) VALUES ($1, $2, 'COMMUNITY_REPLY', 'EMAIL', false)`, [TENANT, P.id]);
      } finally {
        await grantBack('EmailSuppression');
      }
      check(Boolean(moved) && claimed === 'PENDING', `CONTROL: the first attempt claimed the delivery and failed after the claim (${claimed})`);
      const closed = await waitFor(async () => {
        const [row] = await deliveries(value.messageId);
        return row?.status !== 'PENDING' ? row : null;
      }, 30_000);
      check(
        closed?.status === 'FAILED' && closed?.lastError === 'not sent: opted out' && (await inbox(P.email)).length === 0,
        `the retry found them opted out, sent nothing, and closed the row (${closed?.status ?? 'still PENDING'}: ${closed?.lastError ?? '-'})`,
      );
    }

    console.log('');
    console.log('  a reply retried from a tier is handled as a reply');
    await produce(activity({ recipientId: T.id, body: 'retried' }), {
      topic: TIERS[0],
      headers: { 'retry-original-topic': TOPIC, 'retry-attempt': '1', 'retry-not-before': String(Date.now()), 'retry-first-failed-at': new Date().toISOString() },
    });
    const retried = await waitFor(async () => ((await inbox(T.email)).length === 1 ? true : null), 60_000);
    check(Boolean(retried), 'it reaches the person it answers, rather than being decoded as a password reset and dead-lettered');

    console.log('');
    console.log('  a reply stuck on its store does not hold up a password reset');
    /**
     * The card table is locked, so a reply waits on it inside its attempt; a reset reads
     * no card. One reply is sent the ordinary way and one straight onto the first tier,
     * so a reply is stuck on each of the replies consumers; then a reset is requested,
     * and a reset's retry put on the same tier. On a consumer shared with replies, a reset
     * could only be read once the reply in front of it finished — by the lock going, or
     * by failing and moving to a tier. So both resets must arrive while both replies are
     * still stuck: two backends waiting on the lock, and nothing moved to a tier.
     */
    {
      const locker = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
      await locker.connect();
      const { pid } = (await locker.query('SELECT pg_backend_pid() AS pid')).rows[0];
      const waitingOnLock = async () =>
        (await owner.query('SELECT count(*)::int AS n FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))', [pid])).rows[0].n;
      let blocked = 0;
      let stillBlocked = 0;
      let arrived = false;
      let retriedMeanwhile = null;
      await locker.query('BEGIN');
      try {
        await locker.query('LOCK TABLE "ContactCard" IN ACCESS EXCLUSIVE MODE');
        const before = (await metrics()).dispatch.retriedTotal;
        await produce(activity({ recipientId: L.id, body: 'held up' }));
        await produce(activity({ recipientId: L2.id, body: 'held up on a tier' }), { topic: TIERS[0], headers: firstRetryOf(TOPIC) });
        blocked =
          (await waitFor(async () => {
            const n = await waitingOnLock();
            return n >= 2 ? n : null;
          }, 20_000, 50)) ?? (await waitingOnLock());
        await requestReset(W);
        await requestReset(W2, { topic: TIERS[0], headers: firstRetryOf(RESET_TOPIC) });
        arrived = Boolean(await waitFor(async () => (await inbox(W.email)).length === 1 && (await inbox(W2.email)).length === 1, 20_000, 100));
        stillBlocked = await waitingOnLock();
        retriedMeanwhile = (await metrics()).dispatch.retriedTotal - before;
      } finally {
        await locker.query('ROLLBACK').catch(() => {});
        await locker.end().catch(() => {});
      }
      check(blocked >= 2, `CONTROL: both replies were waiting on the locked card table (${blocked} waiting)`);
      check(
        arrived && stillBlocked >= 2 && retriedMeanwhile === 0,
        `the reset and the reset's retry arrived while both replies were still stuck (arrived: ${arrived}; still waiting: ${stillBlocked}; moved to a tier meanwhile: ${retriedMeanwhile})`,
      );
      const released = await waitFor(async () => ((await inbox(L.email)).length === 1 && (await inbox(L2.email)).length === 1 ? true : null), 45_000);
      check(Boolean(released), 'and once the table was free, both replies arrived too');
    }

    console.log('');
    console.log('  a reply already on the topic when the service first started is never sent');
    // Everything above was read after it, on the same partition, by a group that has
    // been reading since before the canary: had it started earlier, this would be there.
    check((await inbox(E.email)).length === 0, `it was never read, so never sent (inbox ${(await inbox(E.email)).length})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Notification's output:\n${excerpt(log)}`);
  } finally {
    service?.kill();
    for (const table of OUTAGE_TABLES) await grantBack(table).catch((err) => check(false, `could not grant SELECT on ${table} back: ${err.message}`));
    let removed = 0;
    let left = 0;
    for (const tenant of TENANTS) {
      for (const table of ['NotificationDelivery', 'ContactCard', 'NotificationPreference', 'EmailSuppression']) {
        removed += (await inTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      }
      left += Number((await inTenant(owner, tenant, `SELECT (SELECT count(*) FROM "NotificationDelivery" WHERE "tenantId" = $1) + (SELECT count(*) FROM "ContactCard" WHERE "tenantId" = $1)
         + (SELECT count(*) FROM "NotificationPreference" WHERE "tenantId" = $1) + (SELECT count(*) FROM "EmailSuppression" WHERE "tenantId" = $1) AS n`, [tenant])).rows[0].n);
    }
    const ids = [];
    for (const user of everyone) ids.push(...(await inbox(user.email).catch(() => [])).map((m) => m.ID));
    if (ids.length > 0) {
      await fetch(`${MAILPIT}/api/v1/messages`, { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ IDs: ids }) }).catch(() => {});
    }
    check(removed > 0 && Number(left) === 0, `cleanup: this run's rows are gone (${removed} deleted, ${left} left), and ${ids.length} email(s) removed from Mailpit`);
    const stuck = await deleteGroupsWhenEmpty(admin, [...GROUPS.all, ...scanGroups]);
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
