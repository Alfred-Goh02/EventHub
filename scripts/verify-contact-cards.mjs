/**
 * Asserts that Auth publishes each user's address as a sealed contact card, and that
 * Notification keeps a copy it can trust and nobody else can read.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - Registering queues a card in the same transaction, keyed tenantId:userId, with
 *     no address anywhere in the outbox row, and an occurredAt Postgres stamped. The
 *     key is read off the TOPIC, because compaction keeps the last record per key and
 *     a key of the user id alone would let tenants erase each other's cards.
 *   - Notification stores the card still SEALED: the row holds no address, and the
 *     verifier opens the stored ciphertext itself and checks Auth's signature, so a
 *     row full of something other than the genuine card fails.
 *   - The publish command queues exactly the users who never had a card, run in its
 *     DOCUMENTED form; a second run queues none.
 *   - The latest card wins by the signed occurredAt: an older genuine card is ignored,
 *     a newer one replaces.
 *   - Forgeries never reach the table and are dead-lettered: signed by the wrong key,
 *     an email request passed off as a card, an envelope whose occurredAt, tenant or
 *     key disagrees with what Auth signed. Each is counted, and the row read back
 *     unchanged — a consumer that stalled on the first would change nothing either,
 *     so a genuine card sent AFTER them must still land.
 *   - A tombstone withdraws the card.
 *
 * Notification runs on groups of its own. Its contact consumer reads the compacted
 * topic from the start, as the real one does, so every baseline is taken after it
 * has caught up.
 *
 * Scoped to tenants created for this run; they, their users, invites, outbox rows and
 * cards, and the run's consumer groups are removed at the end.
 *
 * Run (with Postgres, Kafka and the Schema Registry up, topics created and schemas
 * registered; Auth, the relay and Notification built; both keypairs generated):
 *   node scripts/verify-contact-cards.mjs
 */

import { execFile, spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import {
  CompactEncrypt,
  SignJWT,
  calculateJwkThumbprint,
  compactDecrypt,
  exportJWK,
  importPKCS8,
  importSPKI,
  jwtVerify,
} from 'jose';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');

const TOPIC = 'auth.user-contact';
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_contact_${RUN}`;
const OTHER = `tenant_contact_other_${RUN}`;
const EMAIL = `contact-${RUN}@mailcheck.example`;
const DIRECT_EMAIL = `contact-direct-${RUN}@mailcheck.example`;
const GROUP = `verify-contact-cards-${RUN}`;
const CANARY = `user_contact_canary_${RUN}`;

const NOTIFICATION_PORT = 3981;
const AUTH_PORT = 3982;

/**
 * Auth runs with its clock an HOUR FAST. The card's occurredAt must come from the
 * database; on a machine whose clocks agree, a stamp from `new Date()` lands on the same
 * millisecond and the check cannot tell them apart — measured, it stayed green with
 * exactly that bug. With the skew, a process-clock stamp is an hour out.
 */
const authEnv = serviceEnv(ROOT, 'auth', {
  PORT: String(AUTH_PORT),
  NODE_OPTIONS: `--import=${pathToFileURL(join(ROOT, 'scripts', 'lib', 'skewed-clock.mjs')).href}`,
});
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['AUTH_RELAY_DATABASE_URL'] ??
    'postgresql://eventhub_auth_relay:eventhub_auth_relay@localhost:5432/eventhub_auth',
  RELAY_FORBIDDEN_TABLE: 'User',
  RELAY_METRICS_PORT: '3983',
});
const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(NOTIFICATION_PORT),
  KAFKA_CONSUMER_GROUP: `${GROUP}-dispatch`,
  DISPATCH_START_FROM: 'latest',
  CONTACTS_CONSUMER_GROUP: `${GROUP}-contacts`,
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
const sha256 = (s) => createHash('sha256').update(s).digest('hex');

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
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

/** The publish command in its DOCUMENTED form, through pnpm, as an operator runs it. */
function publishCommand() {
  return new Promise((done) => {
    execFile(
      process.platform === 'win32' ? 'pnpm.cmd' : 'pnpm',
      ['--filter', '@eventhub/auth', 'run', 'contact-cards:publish'],
      { cwd: ROOT, env: authEnv, shell: process.platform === 'win32', maxBuffer: 16 * 1024 * 1024 },
      (err, stdout, stderr) => done({ code: err ? (err.code ?? 1) : 0, out: `${stdout}${stderr}` }),
    );
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
  for (const port of [NOTIFICATION_PORT, AUTH_PORT]) {
    const taken = await fetch(`http://127.0.0.1:${port}/health`).then(
      () => true,
      () => false,
    );
    if (taken) {
      console.error(`FAIL: something already answers on :${port}. Stop it before running this.`);
      process.exitCode = 1;
      return;
    }
  }

  const authApp = new pg.Client({ connectionString: authEnv.APP_DATABASE_URL });
  const relayDb = new pg.Client({ connectionString: relayEnv.RELAY_DATABASE_URL });
  const notifyApp = new pg.Client({ connectionString: notificationEnv.APP_DATABASE_URL });
  await Promise.all([authApp, relayDb, notifyApp].map((c) => c.connect()));

  const kafka = new Kafka({ clientId: `verify-contact-cards-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  const producer = kafka.producer();
  const registry = new SchemaRegistry({ host: relayEnv.SCHEMA_REGISTRY_URL });
  await Promise.all([admin.connect(), producer.connect()]);

  const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
  const notificationPrivate = await importPKCS8(key('notification', 'private.pem'), 'RSA-OAEP-256');
  const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
  const authSigning = await importPKCS8(key('auth', 'private.pem'), 'RS256');
  const authVerifying = await importSPKI(key('auth', 'public.pem'), 'RS256');
  // A key that is not Auth's. Anyone can encrypt to Notification; only Auth can sign.
  const impostorSigning = await importPKCS8(key('notification', 'private.pem'), 'RS256');

  /** A card made the way Auth makes one, with whatever each case needs to be wrong. */
  async function sealCard({ tenantId, userId, email, messageId, occurredAt, signingKey = authSigning, typ = 'auth-contact+jwt' }) {
    const signed = await new SignJWT({ tenantId, email, occurredAt })
      .setProtectedHeader({ alg: 'RS256', typ })
      .setSubject(userId)
      .setJti(messageId)
      .setIssuer(authEnv.JWT_ISSUER)
      .setAudience('eventhub-notification')
      .setIssuedAt()
      .sign(signingKey);
    return new CompactEncrypt(new TextEncoder().encode(signed))
      .setProtectedHeader({ alg: 'RSA-OAEP-256', enc: 'A256GCM', cty: 'JWT', kid })
      .encrypt(notificationPublic);
  }

  async function produce({ key: k, value }) {
    const encoded = value === null ? null : await registry.encode(await registry.getLatestSchemaId(`${TOPIC}-value`), value);
    await producer.send({ topic: TOPIC, messages: [{ key: k, value: encoded }] });
  }

  /** What Notification holds for a user, read as the app role in that user's tenant. */
  const card = (tenantId, userId) =>
    inTenant(notifyApp, tenantId, `SELECT sealed, "messageId" FROM "ContactCard" WHERE "tenantId" = $1 AND "userId" = $2`, [tenantId, userId]).then(
      (r) => r.rows[0] ?? null,
    );
  const metrics = async () => (await (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/metrics`)).json()).contacts;

  /** Opens a stored card the way Notification will, and returns the address Auth signed. */
  async function openStored(sealed) {
    const { plaintext } = await compactDecrypt(sealed, notificationPrivate, {
      keyManagementAlgorithms: ['RSA-OAEP-256'],
      contentEncryptionAlgorithms: ['A256GCM'],
    });
    const { payload } = await jwtVerify(new TextDecoder().decode(plaintext), authVerifying, {
      algorithms: ['RS256'],
      typ: 'auth-contact+jwt',
      audience: 'eventhub-notification',
    });
    return payload;
  }

  const notification = start('notification', ['dist/index.js'], notificationEnv);
  const auth = start('auth', ['dist/index.js'], authEnv);
  let userId = null;
  let directId = null;

  try {
    console.log('  the fixture');
    for (const [id, slug] of [[TENANT, `contact-${RUN}`], [OTHER, `contact-other-${RUN}`]]) {
      await authApp.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [id, `Contact ${RUN}`, slug]);
    }
    const [ready, authUp] = await Promise.all([
      waitFor(async () => (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/ready`)).ok, 60_000, 500),
      waitFor(async () => (await fetch(`http://127.0.0.1:${AUTH_PORT}/health`)).ok, 60_000, 500),
    ]);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${NOTIFICATION_PORT}` : `Notification never became ready:\n${excerpt(notification.log())}`);
    check(Boolean(authUp), authUp ? `CONTROL: a private Auth is listening on :${AUTH_PORT}` : `Auth never came up:\n${excerpt(auth.log())}`);
    check(auth.log().includes('clock skewed by +3600s'), "CONTROL: Auth's clock is an hour fast, so a stamp from it cannot pass for the database's");
    if (failures > 0) return;
    /**
     * A CANARY FIRST, then the lag. On a topic nothing has been written to, a new group
     * has no position anywhere and its lag is unknown rather than zero — the first run
     * of this file failed exactly there. A genuine card, stored, proves the consumer is
     * reading; then every count below is taken after it has caught up, or the backlog
     * of the compacted topic would move them.
     */
    const canaryId = randomUUID();
    const canaryAt = new Date().toISOString();
    await produce({
      key: `${TENANT}:${CANARY}`,
      value: { messageId: canaryId, occurredAt: canaryAt, tenantId: TENANT, userId: CANARY, sealed: await sealCard({ tenantId: TENANT, userId: CANARY, email: `canary-${EMAIL}`, messageId: canaryId, occurredAt: canaryAt }) },
    });
    const reading = await waitFor(async () => ((await card(TENANT, CANARY))?.messageId === canaryId ? true : null), 90_000, 500);
    check(Boolean(reading), 'CONTROL: its contact consumer stored a canary card, so it is reading the topic');
    if (failures > 0) return;
    const caughtUp = await waitFor(async () => {
      const m = await metrics();
      return m.running && m.lag?.total === 0 ? m : null;
    }, 90_000, 1_000);
    check(Boolean(caughtUp), `CONTROL: its contact consumer is running and has read the topic to the end (lag ${caughtUp?.lag?.total ?? 'unknown'})`);
    if (failures > 0) return;

    console.log('');
    console.log('  registering queues a sealed card, in the same transaction');
    const inviteToken = randomBytes(32).toString('base64url');
    await inTenant(
      authApp,
      TENANT,
      `INSERT INTO "Invite" (id, "tenantId", email, role, "tokenHash", "invitedBy", "expiresAt", "createdAt")
       VALUES ($1, $2, $3, 'ATTENDEE', $4, 'verify', now() + interval '1 day', now())`,
      [`inv_contact_${RUN}`, TENANT, EMAIL, sha256(inviteToken)],
    );
    const registered = await fetch(`http://127.0.0.1:${AUTH_PORT}/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ token: inviteToken, password: `password-${RUN}` }),
    }).then(async (r) => ({ status: r.status, body: await r.json().catch(() => ({})) }));
    userId = registered.body?.id ?? null;
    check(registered.status === 201 && Boolean(userId), `a user registers through an invite (${registered.status})`);

    const rows = (await relayDb.query(
      `SELECT o.id, o."aggregateId", o.payload::text AS payload, o.payload->>'occurredAt' AS "occurredAt"
         FROM "OutboxMessage" o WHERE o."tenantId" = $1 AND o.topic = $2 ORDER BY o.seq`,
      [TENANT, TOPIC],
    )).rows;
    const queued = rows[0];
    check(rows.length === 1 && queued.aggregateId === `${TENANT}:${userId}`, `one card is queued, keyed tenantId:userId (${rows.length} row(s), ${queued?.aggregateId})`);
    check(Boolean(queued) && !queued.payload.toLowerCase().includes(EMAIL.toLowerCase()), 'and the address is nowhere in the outbox row: only ciphertext is');
    /**
     * Two renderings Postgres made, compared as text: the card's occurredAt and the
     * user's contactPublishedAt are stamped by one statement, under the user's row
     * lock. Never Date.parse on either side — see CLAUDE.md on timestamps.
     */
    const marker = (await inTenant(
      authApp,
      TENANT,
      `SELECT to_char("contactPublishedAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS at FROM "User" WHERE id = $1`,
      [userId],
    )).rows[0]?.at;
    check(
      Boolean(marker) && queued?.occurredAt === marker,
      `the user is marked published, and the card's occurredAt is that same stamp, from Postgres's clock (${queued?.occurredAt} / ${marker})`,
    );

    console.log('');
    console.log('  the relay publishes it under that key, and Notification keeps it sealed');
    const storedBefore = (await metrics()).cardsStoredTotal;
    await relayOnce();
    const published = await waitFor(async () => (await relayDb.query(`SELECT "publishedAt" FROM "OutboxMessage" WHERE id = $1`, [queued.id])).rows[0]?.publishedAt, 30_000);
    check(Boolean(published), 'the outbox row is published');

    // The key on the TOPIC, which is what compaction keeps.
    let onTopic = null;
    const scan = kafka.consumer({ groupId: `${GROUP}-scan` });
    await scan.connect();
    await scan.subscribe({ topic: TOPIC, fromBeginning: true });
    await scan.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const decoded = await registry.decode(message.value).catch(() => null);
        if (decoded?.messageId === queued.id) onTopic = { key: message.key?.toString('utf8') };
      },
    });
    await waitFor(async () => onTopic !== null, 45_000);
    await scan.disconnect();
    check(onTopic?.key === `${TENANT}:${userId}`, `on the topic its key is tenantId:userId (${onTopic?.key ?? 'not found'})`);

    const stored = await waitFor(async () => {
      const c = await card(TENANT, userId);
      return c?.messageId === queued.id ? c : null;
    }, 45_000);
    check(Boolean(stored) && (await metrics()).cardsStoredTotal > storedBefore, "Notification's copy is this card");
    check(Boolean(stored) && !stored.sealed.toLowerCase().includes(EMAIL.toLowerCase()), 'and holds no address: the row is ciphertext');
    const opened = stored ? await openStored(stored.sealed).catch((err) => ({ error: err.message })) : null;
    check(opened?.email === EMAIL && opened?.tenantId === TENANT && opened?.sub === userId, `opened with Notification's key and verified with Auth's, it is this user's address (${opened?.email ?? opened?.error})`);

    console.log('');
    console.log('  the publish command covers users who never had a card, once');
    directId = `user_contact_direct_${RUN}`;
    await inTenant(authApp, TENANT, `INSERT INTO "User" (id, "tenantId", email, "passwordHash", "updatedAt") VALUES ($1, $2, $3, 'not-a-hash', now())`, [directId, TENANT, DIRECT_EMAIL]);
    const firstRun = await publishCommand();
    const firstQueued = Number(firstRun.out.match(/contact cards queued: (\d+)/)?.[1] ?? -1);
    const directRows = (await relayDb.query(`SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1 AND topic = $2 AND "aggregateId" = $3`, [TENANT, TOPIC, `${TENANT}:${directId}`])).rows[0].n;
    check(firstRun.code === 0 && firstQueued >= 1 && directRows === 1, `the documented command queued ${firstQueued} card(s), one of them for the user who had none (exit ${firstRun.code})`);
    const secondRun = await publishCommand();
    const secondQueued = Number(secondRun.out.match(/contact cards queued: (\d+)/)?.[1] ?? -1);
    const directAgain = (await relayDb.query(`SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1 AND topic = $2 AND "aggregateId" = $3`, [TENANT, TOPIC, `${TENANT}:${directId}`])).rows[0].n;
    check(secondRun.code === 0 && secondQueued === 0 && directAgain === 1, `run again, it queues nothing (${secondQueued} queued, still ${directAgain} row for that user)`);

    console.log('');
    console.log('  the latest card wins, by the occurredAt Auth signed');
    const current = await card(TENANT, userId);
    const older = randomUUID();
    const olderAt = '2020-01-01T00:00:00.000Z';
    const ignoredBefore = (await metrics()).olderCardsIgnoredTotal;
    await produce({ key: `${TENANT}:${userId}`, value: { messageId: older, occurredAt: olderAt, tenantId: TENANT, userId, sealed: await sealCard({ tenantId: TENANT, userId, email: `old-${EMAIL}`, messageId: older, occurredAt: olderAt }) } });
    const ignored = await waitFor(async () => (await metrics()).olderCardsIgnoredTotal > ignoredBefore, 30_000);
    check(Boolean(ignored) && (await card(TENANT, userId))?.messageId === current.messageId, 'an older genuine card is read and ignored: the stored card is unchanged');
    const newer = randomUUID();
    const newerAt = '2099-01-01T00:00:00.000Z';
    await produce({ key: `${TENANT}:${userId}`, value: { messageId: newer, occurredAt: newerAt, tenantId: TENANT, userId, sealed: await sealCard({ tenantId: TENANT, userId, email: `new-${EMAIL}`, messageId: newer, occurredAt: newerAt }) } });
    const replaced = await waitFor(async () => ((await card(TENANT, userId))?.messageId === newer ? true : null), 30_000);
    check(Boolean(replaced), 'a newer genuine card replaces it');

    console.log('');
    console.log('  forgeries never reach the table, and are dead-lettered');
    const before = await metrics();
    const target = `user_contact_forged_${RUN}`;
    const at = new Date().toISOString();
    const forged = [
      ['signed by a key that is not Auth\'s', { signingKey: impostorSigning }],
      ['an email request passed off as a card', { typ: 'auth-email+jwt' }],
      ['an envelope claiming a later time than Auth signed', { envelopeAt: '2100-01-01T00:00:00.000Z' }],
      ['an envelope naming another tenant than the seal', { envelopeTenant: OTHER }],
      ['a message filed under another user\'s key', { key: `${TENANT}:${userId}` }],
    ];
    for (const [, wrong] of forged) {
      const id = randomUUID();
      const sealed = await sealCard({ tenantId: TENANT, userId: target, email: `forged-${EMAIL}`, messageId: id, occurredAt: at, ...wrong });
      const envelopeTenant = wrong.envelopeTenant ?? TENANT;
      await produce({
        key: wrong.key ?? `${envelopeTenant}:${target}`,
        value: { messageId: id, occurredAt: wrong.envelopeAt ?? at, tenantId: envelopeTenant, userId: target, sealed },
      });
    }
    // A genuine card AFTER the forgeries, on the same key: a consumer stalled on the
    // first refusal would never store it.
    const honest = randomUUID();
    await produce({ key: `${TENANT}:${target}`, value: { messageId: honest, occurredAt: at, tenantId: TENANT, userId: target, sealed: await sealCard({ tenantId: TENANT, userId: target, email: `honest-${EMAIL}`, messageId: honest, occurredAt: at }) } });
    const honestStored = await waitFor(async () => ((await card(TENANT, target))?.messageId === honest ? true : null), 45_000);
    // Waited for, not read once: the forgery filed under the other tenant's key can sit
    // on another partition and be handled after the honest card lands.
    await waitFor(async () => ((await metrics()).deadLetteredTotal - before.deadLetteredTotal >= forged.length ? true : null), 30_000);
    const after = await metrics();
    check(Boolean(honestStored), 'a genuine card sent after them is stored: the consumer moved past every refusal');
    check(
      after.deadLetteredTotal - before.deadLetteredTotal === forged.length,
      `each of the ${forged.length} forgeries was dead-lettered (${after.deadLetteredTotal - before.deadLetteredTotal}): ${forged.map(([what]) => what).join('; ')}`,
    );
    check((await card(OTHER, target)) === null, 'nothing was stored under the other tenant');
    check((await card(TENANT, userId))?.messageId === newer, "and the user whose key was borrowed still has their own card");

    console.log('');
    console.log('  a tombstone withdraws the card');
    const withdrawnBefore = (await metrics()).cardsWithdrawnTotal;
    await produce({ key: `${TENANT}:${target}`, value: null });
    const withdrawn = await waitFor(async () => ((await metrics()).cardsWithdrawnTotal > withdrawnBefore && (await card(TENANT, target)) === null ? true : null), 30_000);
    check(Boolean(withdrawn), 'the copy is deleted');

    console.log('');
    console.log("  one tenant never sees another's cards");
    const seen = await inTenant(notifyApp, OTHER, `SELECT count(*)::int AS n FROM "ContactCard" WHERE "tenantId" = $1`, [TENANT]);
    const control = await inTenant(notifyApp, TENANT, `SELECT count(*)::int AS n FROM "ContactCard" WHERE "tenantId" = $1`, [TENANT]);
    check(seen.rows[0].n === 0 && control.rows[0].n >= 1, `under ${OTHER}, ${seen.rows[0].n} of ${TENANT}'s cards are visible (CONTROL: ${control.rows[0].n} under its own)`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Notification's output:\n${excerpt(notification.log())}`);
  } finally {
    notification.child.kill();
    auth.child.kill();
    /**
     * TOMBSTONES FOR EVERY KEY THIS RUN USED. The topic is compacted: without them the
     * run's cards stay on it for good, and every Notification that ever reads it from
     * the start copies cards for tenants that no longer exist.
     */
    const keys = [CANARY, userId, directId, `user_contact_forged_${RUN}`].filter(Boolean).map((u) => `${TENANT}:${u}`);
    keys.push(`${OTHER}:user_contact_forged_${RUN}`);
    const tombstoned = await producer
      .send({ topic: TOPIC, messages: keys.map((k) => ({ key: k, value: null })) })
      .then(() => keys.length, () => 0);
    check(tombstoned === keys.length, `cleanup: a tombstone for each of the run's ${keys.length} keys on the compacted topic (${tombstoned} sent)`);
    let deleted = 0;
    for (const tenant of [TENANT, OTHER]) {
      deleted += (await inTenant(notifyApp, tenant, `DELETE FROM "ContactCard" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      deleted += (await relayDb.query(`DELETE FROM "OutboxMessage" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      for (const table of ['RefreshToken', 'Invite', 'User']) {
        deleted += (await inTenant(authApp, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      }
      deleted += (await authApp.query(`DELETE FROM "Tenant" WHERE id = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
    }
    const left =
      (await authApp.query(`SELECT count(*)::int AS n FROM "Tenant" WHERE id IN ($1, $2)`, [TENANT, OTHER])).rows[0].n +
      (await relayDb.query(`SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" IN ($1, $2)`, [TENANT, OTHER])).rows[0].n;
    check(deleted > 0 && left === 0, `cleanup: this run's rows are gone (${deleted} deleted, ${left} left)`);
    const stuck = await deleteGroupsWhenEmpty(admin, [`${GROUP}-dispatch`, `${GROUP}-dispatch-retry`, `${GROUP}-dispatch-replies`, `${GROUP}-dispatch-replies-retry`, `${GROUP}-contacts`, `${GROUP}-scan`]);
    if (stuck.length > 0) console.log(`    note  consumer group(s) left to expire: ${stuck.join(', ')}`);
    await Promise.allSettled([producer.disconnect(), admin.disconnect(), authApp.end(), relayDb.end(), notifyApp.end()]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log("every user's address reaches Notification sealed, keyed by tenant and user, stored still sealed, newest first, and nothing forged gets in");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
