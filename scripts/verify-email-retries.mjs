/**
 * Asserts what Notification does when a send cannot be completed now: it waits in
 * delayed retry tiers without holding anyone else up, delivers once when it can,
 * and dead-letters what never will succeed.
 *
 * Notification runs against a fake SMTP server this run controls
 * (scripts/lib/fake-smtp.mjs), so an outage is the server being stopped, and a
 * troubled address is a 451 or 550 reply to its RCPT TO. The retry delays are
 * shortened to seconds; the tier topics are the real ones.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - An outage is ridden out and the email arrives once. The row is seen PENDING
 *     with a failed attempt first, and the main topic's lag is 0 while it waits, so
 *     the wait happened in a tier and not by blocking the partition.
 *   - A retry is not tried before its tier's time. The email's arrival is compared
 *     with the retry-not-before header on the tier message itself.
 *   - Retries run out into the dead-letter topic, with the attempts and the reason.
 *   - One troubled address does not hold up another user. THE TWO USERS ARE CHOSEN
 *     TO SHARE A PARTITION, by the partitioner the relay uses; on different
 *     partitions the second email would arrive promptly whether or not anything
 *     blocked.
 *   - Poison goes straight to the dead-letter topic without touching a tier.
 *   - A hard 550 is recorded FAILED and not retried.
 *
 * Scoped to a tenant, its users and consumer groups created for this run. Messages
 * it leaves in the retry and dead-letter topics are found again only by this run's
 * message ids.
 *
 * Run (with Postgres, Kafka and the Schema Registry up; Auth, the relay and
 * Notification built; both keypairs generated): node scripts/verify-email-retries.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, Partitioners, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { CompactEncrypt, SignJWT, calculateJwkThumbprint, exportJWK, importPKCS8, importSPKI } from 'jose';
import { serviceEnv } from './lib/service-env.mjs';
import { createFakeSmtp } from './lib/fake-smtp.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');

const TOPIC = 'auth.email-requested';
const TIER_TOPICS = ['notification.retry.1', 'notification.retry.2', 'notification.retry.3'];
const DLQ = 'notification.dlq';
const DELAYS_MS = [1500, 3000, 4500];

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_mailretry_${RUN}`;
/** Its subdomain, which is how Auth is told the tenant. */
const TENANT_SLUG = `mailretry-${RUN}`;
const GROUP = `verify-email-retries-${RUN}`;
const SMTP_PORT = 3190;
const NOTIFICATION_PORT = 3191;
const AUTH_PORT = 3193;

const authEnv = serviceEnv(ROOT, 'auth', { PORT: String(AUTH_PORT), NODE_ENV: 'development', AUTH_DEV_TOKEN_SINK: '1', LOG_LEVEL: 'debug' });
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['AUTH_RELAY_DATABASE_URL'] ?? 'postgresql://eventhub_auth_relay:eventhub_auth_relay@localhost:5432/eventhub_auth',
  RELAY_FORBIDDEN_TABLE: 'User',
  RELAY_METRICS_PORT: '3192',
});
const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(NOTIFICATION_PORT),
  KAFKA_CONSUMER_GROUP: GROUP,
  DISPATCH_START_FROM: 'latest',
  DISPATCH_RETRY_DELAYS_MS: DELAYS_MS.join(','),
  SMTP_HOST: '127.0.0.1',
  SMTP_PORT: String(SMTP_PORT),
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
      done(code);
    });
  });
}

async function inTenant(client, sql, params) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [TENANT]);
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

const post = (path, body, headers = {}) =>
  fetch(`http://127.0.0.1:${AUTH_PORT}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  }).then((r) => r.status);

const dispatch = async () => (await (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/metrics`)).json()).dispatch;

/** The dev sink's latest reset link for an address in this run's tenant. */
function sinkLink(log, email) {
  let found = null;
  for (const line of log.split(/\r?\n/)) {
    try {
      const entry = JSON.parse(line);
      if (entry.devOnlyResetUrl && entry.tenantId === TENANT && entry.email === email) found = entry.devOnlyResetUrl;
    } catch {
      // not a JSON log line
    }
  }
  return found;
}

const resetLinkIn = (text) => text?.match(/^(https?:\/\/\S*reset-password\?token=\S+?)\r?$/m)?.[1] ?? null;

/** The body of a message the fake server accepted, with quoted-printable soft breaks undone. */
const bodyOf = (accepted) => accepted.data.replace(/=\r\n/g, '').replace(/=3D/g, '=');

async function main() {
  for (const port of [NOTIFICATION_PORT, AUTH_PORT, SMTP_PORT]) {
    const taken = await fetch(`http://127.0.0.1:${port}/`).then(
      () => true,
      () => false,
    );
    if (taken) {
      console.error(`FAIL: something already answers on :${port}. Stop it before running this.`);
      process.exitCode = 1;
      return;
    }
  }

  const kafka = new Kafka({ clientId: `verify-email-retries-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  // Another Notification on the real group shares the sent-log and would claim this
  // run's deliveries first. The group name is the one Notification's .env.example sets.
  const realGroup = serviceEnv(ROOT, 'notification').KAFKA_CONSUMER_GROUP;
  // A consumer stopped a moment ago can stay a member until its session times out
  // (30 s): compose once had to kill Notification after its grace period. So the
  // groups get that long to empty before this concludes one is really delivering.
  let busy = [];
  const idle = await waitFor(async () => {
    const { groups } = await admin.describeGroups([realGroup, `${realGroup}-retry`]);
    busy = groups.filter((g) => g.members.length > 0).map((g) => g.groupId);
    return busy.length === 0;
  }, 40_000, 2_000);
  if (!idle) {
    console.error(`FAIL: ${busy.join(' and ')} ${busy.length === 1 ? 'has' : 'have'} active members, so another Notification is delivering. Stop it first (in compose: docker compose stop notification).`);
    process.exitCode = 1;
    await admin.disconnect();
    return;
  }

  const partitionIds = (await admin.fetchTopicMetadata({ topics: [TOPIC] })).topics[0].partitions.map((p) => p.partitionId).sort();
  const partitioner = Partitioners.DefaultPartitioner();
  const partitionOf = (k) => partitioner({ topic: TOPIC, partitionMetadata: partitionIds.map((partitionId) => ({ partitionId, leader: 0 })), message: { key: k } });

  const users = {
    outage: `user_outage_${RUN}`,
    exhausted: `user_exhausted_${RUN}`,
    troubled: `user_troubled_${RUN}`,
    rejected: `user_rejected_${RUN}`,
  };
  let n = 0;
  while (partitionOf(`user_neighbour_${RUN}_${n}`) !== partitionOf(users.troubled)) n += 1;
  users.neighbour = `user_neighbour_${RUN}_${n}`;
  const email = (user) => `${user.replace(/_/g, '-')}@mailcheck.example`;

  const authApp = new pg.Client({ connectionString: authEnv.APP_DATABASE_URL });
  const relayDb = new pg.Client({ connectionString: relayEnv.RELAY_DATABASE_URL });
  const notifyApp = new pg.Client({ connectionString: notificationEnv.APP_DATABASE_URL });
  const notifyOwner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  await Promise.all([authApp, relayDb, notifyApp, notifyOwner].map((c) => c.connect()));
  const producer = kafka.producer();
  await producer.connect();
  const registry = new SchemaRegistry({ host: relayEnv.SCHEMA_REGISTRY_URL });

  const smtp = createFakeSmtp();
  await smtp.start(SMTP_PORT);
  const scanGroups = [];

  /** Every message on a topic carrying this message-id header, read from the start by a throwaway group. */
  async function scan(topic, messageId, want = 1, timeoutMs = 20_000) {
    const groupId = `verify-email-retries-scan-${RUN}-${scanGroups.length}`;
    scanGroups.push(groupId);
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    const found = [];
    await consumer.run({
      eachMessage: async ({ message }) => {
        if (message.headers?.['message-id']?.toString() === messageId) found.push(message);
      },
    });
    await waitFor(async () => found.length >= want, timeoutMs, 250);
    await sleep(500);
    await consumer.disconnect();
    return found;
  }
  const headersOf = (message) => Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()]));

  const outboxIdFor = async (user) =>
    (await relayDb.query(`SELECT o.id FROM "OutboxMessage" o WHERE o."tenantId" = $1 AND o."aggregateId" = $2 ORDER BY o.seq DESC LIMIT 1`, [TENANT, user])).rows[0]?.id;
  const deliveryFor = async (messageId) =>
    (await inTenant(notifyApp, `SELECT d.status::text AS status, d.attempts, d."lastError" FROM "NotificationDelivery" d WHERE d."tenantId" = $1 AND d."sourceMessageId" = $2`, [TENANT, messageId])).rows;

  const notification = start('notification', ['dist/index.js'], notificationEnv);
  const auth = start('auth', ['dist/index.js'], authEnv);

  try {
    console.log('  the fixture');
    await authApp.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [TENANT, `Mail retries ${RUN}`, TENANT_SLUG]);
    for (const user of Object.values(users)) {
      await inTenant(authApp, `INSERT INTO "User" (id, "tenantId", email, "passwordHash", "updatedAt") VALUES ($1, $2, $3, 'not-a-hash', now())`, [user, TENANT, email(user)]);
    }
    const [ready, authUp] = await Promise.all([
      waitFor(async () => (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/ready`)).ok, 60_000, 500),
      waitFor(async () => (await fetch(`http://127.0.0.1:${AUTH_PORT}/health`)).ok, 60_000, 500),
    ]);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${NOTIFICATION_PORT}, sending to a fake SMTP server on :${SMTP_PORT}` : `Notification never became ready:\n${excerpt(notification.log())}`);
    check(Boolean(authUp), authUp ? `CONTROL: a private Auth with the dev sink on is listening on :${AUTH_PORT}` : `Auth never came up:\n${excerpt(auth.log())}`);
    if (failures > 0) return;

    const joined = await waitFor(async () => {
      const { groups } = await admin.describeGroups([GROUP, `${GROUP}-retry`]);
      return groups.every((g) => g.state === 'Stable' && g.members.length > 0);
    }, 60_000, 500);
    check(Boolean(joined), 'CONTROL: its dispatch and retry consumer groups have both joined');
    check(
      partitionOf(users.troubled) === partitionOf(users.neighbour),
      `CONTROL: the troubled address's user and its neighbour share partition ${partitionOf(users.troubled)}, so a blocked partition would hold the neighbour up`,
    );

    // Canaries: expired messages keyed by every user, on the main topic and on every
    // tier, so each consumer is known to be reading every partition used below.
    const base = await dispatch();
    const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);
    const canary = async (topic, user) =>
      producer.send({
        topic,
        messages: [{
          key: user,
          value: await registry.encode(schemaId, { messageId: randomUUID(), occurredAt: new Date().toISOString(), tenantId: TENANT, userId: user, kind: 'PASSWORD_RESET', expiresAt: new Date(Date.now() - 60_000).toISOString(), sealed: 'canary' }),
          headers: { 'message-id': `canary-${randomUUID()}` },
        }],
      });
    let canaries = 0;
    for (const topic of [TOPIC, ...TIER_TOPICS]) {
      for (const user of Object.values(users)) {
        await canary(topic, user);
        canaries += 1;
      }
    }
    const reading = await waitFor(async () => (await dispatch()).expiredSkippedTotal >= base.expiredSkippedTotal + canaries, 45_000, 500);
    check(Boolean(reading), `CONTROL: it counted all ${canaries} canaries, so both consumers read every partition this run writes to`);
    if (failures > 0) return;

    async function requestReset(user) {
      const status = await post('/forgot-password', { email: email(user) }, { 'X-Tenant-Slug': TENANT_SLUG });
      const link = await waitFor(async () => sinkLink(auth.log(), email(user)), 5_000, 100);
      await relayOnce();
      const messageId = await outboxIdFor(user);
      return { status, link, messageId };
    }

    console.log('');
    console.log('  an outage is ridden out in a retry tier, and the email arrives once');
    await smtp.stop();
    const before1 = await dispatch();
    const outage = await requestReset(users.outage);
    check(outage.status === 202 && Boolean(outage.link) && Boolean(outage.messageId), 'CONTROL: the reset was requested and published');
    const retried = await waitFor(async () => (await dispatch()).retriedTotal > before1.retriedTotal, 20_000);
    check(Boolean(retried), 'the failed send was moved to a retry tier');
    const [pending] = await deliveryFor(outage.messageId);
    check(pending?.status === 'PENDING' && pending.attempts >= 1 && Boolean(pending.lastError), `its delivery waits PENDING with a failed attempt recorded (${pending?.status}, ${pending?.attempts} attempt(s))`);
    const mainCaughtUp = await waitFor(async () => (await dispatch()).lag?.total === 0, 10_000, 500);
    check(Boolean(mainCaughtUp), "and the main topic's lag is 0 meanwhile, so the partition was not held while it waits");
    await smtp.start(SMTP_PORT);
    const delivered = await waitFor(async () => (smtp.to(email(users.outage)).length > 0 ? smtp.to(email(users.outage)) : null), 20_000);
    check(Boolean(delivered), 'once SMTP was back, the email arrived');
    await sleep(DELAYS_MS[DELAYS_MS.length - 1]);
    check(smtp.to(email(users.outage)).length === 1, `exactly once (${smtp.to(email(users.outage)).length})`);
    check(resetLinkIn(delivered ? bodyOf(delivered[0]) : '') === outage.link, 'with the link Auth issued');
    const [sent] = await deliveryFor(outage.messageId);
    check(sent?.status === 'SENT' && sent.attempts >= 2, `the sent-log records it SENT after ${sent?.attempts} attempts`);
    const tierCopies = await scan(TIER_TOPICS[0], outage.messageId);
    const notBefore = Number(tierCopies[0] ? headersOf(tierCopies[0])['retry-not-before'] : NaN);
    check(Boolean(tierCopies[0]) && headersOf(tierCopies[0])['retry-original-topic'] === TOPIC && headersOf(tierCopies[0])['retry-attempt'] === '1', 'the first tier holds it, marked attempt 1 from auth.email-requested');
    check(delivered?.[0] && delivered[0].at >= notBefore, `and it was not tried before that tier's retry-not-before (arrived ${delivered?.[0] ? delivered[0].at - notBefore : '?'} ms after it)`);

    console.log('');
    console.log('  retries run out into the dead-letter topic');
    await smtp.stop();
    const before2 = await dispatch();
    const exhausted = await requestReset(users.exhausted);
    const dead = await waitFor(async () => (await dispatch()).deadLetteredTotal > before2.deadLetteredTotal, 45_000, 500);
    check(Boolean(dead), 'after every tier failed, it was dead-lettered');
    const [dlqCopy] = await scan(DLQ, exhausted.messageId);
    const dlqHeaders = dlqCopy ? headersOf(dlqCopy) : {};
    check(dlqHeaders['dlq-attempts'] === String(DELAYS_MS.length + 1), `after ${dlqHeaders['dlq-attempts']} attempts: the first try and one per tier`);
    check(/^retries exhausted/.test(dlqHeaders['dlq-reason'] ?? '') && dlqHeaders['dlq-original-topic'] === TOPIC && dlqHeaders['dlq-source-topic'] === TIER_TOPICS[2], `with the reason, from ${dlqHeaders['dlq-source-topic']}, originally ${dlqHeaders['dlq-original-topic']}`);
    const [abandoned] = await deliveryFor(exhausted.messageId);
    check(abandoned?.status === 'PENDING' && abandoned.attempts === DELAYS_MS.length + 1, `its delivery row stays PENDING with ${abandoned?.attempts} attempts, and nothing was sent (${smtp.to(email(users.exhausted)).length})`);
    await smtp.start(SMTP_PORT);

    console.log('');
    console.log('  a dead-lettered delivery is replayed by hand, and a second replay sends nothing');
    const replay = () =>
      new Promise((done) => {
        const child = start('notification', ['dist/scripts/dlq-replay.js', exhausted.messageId], notificationEnv);
        child.child.on('exit', (code) => done({ code, log: child.log() }));
      });
    const first = await replay();
    check(first.code === 0 && first.log.includes(`to ${TOPIC}`), `dlq:replay put it back on ${TOPIC} (exit ${first.code}: ${first.log.trim().split(/\r?\n/).pop()})`);
    const replayed = await waitFor(async () => (smtp.to(email(users.exhausted)).length > 0 ? true : null), 20_000);
    check(Boolean(replayed), 'and the email arrived');
    const [recovered] = await deliveryFor(exhausted.messageId);
    check(recovered?.status === 'SENT', `its delivery row moved from PENDING to ${recovered?.status}`);
    const beforeSecondReplay = await dispatch();
    const second = await replay();
    const skipped = await waitFor(async () => {
      const m = await dispatch();
      return m.duplicatesSkippedTotal > beforeSecondReplay.duplicatesSkippedTotal ? m : null;
    }, 20_000);
    check(second.code === 0 && Boolean(skipped), 'a second replay was received and skipped as already delivered');
    await sleep(DELAYS_MS[0]);
    check(smtp.to(email(users.exhausted)).length === 1, `so the address still has exactly one email (${smtp.to(email(users.exhausted)).length})`);
    const missing = await new Promise((done) => {
      const child = start('notification', ['dist/scripts/dlq-replay.js', `not-dead-lettered-${RUN}`], notificationEnv);
      child.child.on('exit', (code) => done(code));
    });
    check(missing === 1, `replaying an id that was never dead-lettered fails rather than doing nothing quietly (exit ${missing})`);

    console.log('');
    console.log('  one troubled address does not hold up another user on its partition');
    smtp.rules.set(email(users.troubled), '451 4.2.1 mailbox busy, try again later');
    const before3 = await dispatch();
    const troubled = await requestReset(users.troubled);
    const neighbour = await requestReset(users.neighbour);
    const neighbourArrived = await waitFor(async () => smtp.to(email(users.neighbour)).length > 0, 15_000);
    check(Boolean(neighbourArrived), "the neighbour's email arrived while the troubled address was failing");
    check(smtp.to(email(users.troubled)).length === 0 && (await dispatch()).retriedTotal > before3.retriedTotal, 'and the troubled one was waiting in a tier, not sent');
    smtp.rules.delete(email(users.troubled));
    const troubledArrived = await waitFor(async () => smtp.to(email(users.troubled)).length > 0, 20_000);
    check(Boolean(troubledArrived), 'once the address accepted mail, its email arrived too');
    await sleep(DELAYS_MS[DELAYS_MS.length - 1]);
    check(smtp.to(email(users.troubled)).length === 1 && smtp.to(email(users.neighbour)).length === 1, 'each exactly once');
    void troubled;
    void neighbour;

    console.log('');
    console.log('  poison skips the tiers, and a hard refusal is recorded and not retried');
    const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
    const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
    const forgedId = randomUUID();
    const future = new Date(Date.now() + 10 * 60_000);
    const forgedJws = await new SignJWT({ tenantId: TENANT, email: email(users.outage), link: 'https://evil.example/reset-password?token=forged', kind: 'PASSWORD_RESET' })
      .setProtectedHeader({ alg: 'RS256', typ: 'auth-email+jwt' })
      .setSubject(users.outage)
      .setJti(forgedId)
      .setIssuer(authEnv.JWT_ISSUER)
      .setAudience('eventhub-notification')
      .setIssuedAt()
      .setExpirationTime(future)
      .sign(await importPKCS8(key('notification', 'private.pem'), 'RS256'));
    const sealed = await new CompactEncrypt(new TextEncoder().encode(forgedJws))
      .setProtectedHeader({ alg: 'RSA-OAEP-256', enc: 'A256GCM', cty: 'JWT', kid })
      .encrypt(notificationPublic);
    const before4 = await dispatch();
    await producer.send({
      topic: TOPIC,
      messages: [{
        key: users.outage,
        value: await registry.encode(schemaId, { messageId: forgedId, occurredAt: new Date().toISOString(), tenantId: TENANT, userId: users.outage, kind: 'PASSWORD_RESET', expiresAt: future.toISOString(), sealed }),
        headers: { 'message-id': forgedId },
      }],
    });
    const poisoned = await waitFor(async () => {
      const m = await dispatch();
      return m.deadLetteredTotal > before4.deadLetteredTotal ? m : null;
    }, 20_000);
    check(Boolean(poisoned) && poisoned.retriedTotal === before4.retriedTotal, 'a forged seal was dead-lettered without being retried');
    const [poisonCopy] = await scan(DLQ, forgedId);
    const poisonHeaders = poisonCopy ? headersOf(poisonCopy) : {};
    check(poisonHeaders['dlq-attempts'] === '1' && /signature/.test(poisonHeaders['dlq-reason'] ?? '') && poisonHeaders['dlq-source-topic'] === TOPIC, `on its first attempt, straight from ${poisonHeaders['dlq-source-topic']}: ${poisonHeaders['dlq-reason']}`);

    smtp.rules.set(email(users.rejected), '550 5.1.1 no such mailbox');
    const before5 = await dispatch();
    const rejected = await requestReset(users.rejected);
    const refused = await waitFor(async () => {
      const m = await dispatch();
      return m.rejectedTotal > before5.rejectedTotal ? m : null;
    }, 20_000);
    check(Boolean(refused), 'a 550 for the recipient was taken as permanent');
    const [failed] = await deliveryFor(rejected.messageId);
    check(failed?.status === 'FAILED' && /550/.test(failed.lastError ?? ''), `its delivery is recorded FAILED (${failed?.status}: ${failed?.lastError})`);
    await sleep(DELAYS_MS[0] * 2);
    const afterRejected = await dispatch();
    check(afterRejected.retriedTotal === before5.retriedTotal && afterRejected.deadLetteredTotal === before5.deadLetteredTotal, 'and it was neither retried nor dead-lettered');

    console.log('');
    console.log("  Notification's output holds nothing to steal");
    const output = notification.log();
    const secrets = [outage.link, exhausted.link, troubled.link, neighbour.link, rejected.link, ...Object.values(users).map(email)].filter(Boolean);
    check(output.includes('delivery failed; retrying later'), 'CONTROL: its output is captured (it logged a retry)');
    check(!secrets.some((s) => output.includes(s)), 'and holds no link or address');
  } catch (err) {
    // Reported, not thrown: a Notification that died mid-run once ended this with a
    // bare fetch error and no FAIL line saying what had stopped. Auth is named too, with
    // the socket error Node keeps under "fetch failed", since the delivery verifier once
    // lost its Auth mid-run with nothing more than those two words.
    const children = [['Notification', notification], ['Auth', auth]];
    const cause = err?.cause ? ` [${err.cause.code ?? err.cause.message ?? String(err.cause)}]` : '';
    const exited = children.filter(([, p]) => p.child.exitCode !== null || p.child.signalCode !== null).map(([name, p]) => ` (${name} had exited: ${p.child.exitCode ?? p.child.signalCode})`);
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}${cause}${exited.join('')}`);
    for (const [name, p] of children) console.error(`  ${name}'s output:\n${excerpt(p.log())}`);
  } finally {
    notification.child.kill();
    auth.child.kill();
    await smtp.stop().catch(() => {});
    await producer.disconnect().catch(() => {});
    await relayDb.query('DELETE FROM "OutboxMessage" WHERE "tenantId" = $1', [TENANT]).catch(() => {});
    await inTenant(notifyOwner, 'DELETE FROM "NotificationDelivery" WHERE "tenantId" = $1', [TENANT]).catch(() => {});
    // The 550 5.1.1 above suppresses its address, and that row is this run's too.
    await inTenant(notifyOwner, 'DELETE FROM "EmailSuppression" WHERE "tenantId" = $1', [TENANT]).catch(() => {});
    await inTenant(authApp, 'DELETE FROM "User" WHERE "tenantId" = $1', [TENANT]).catch(() => {});
    await authApp.query('DELETE FROM "Tenant" WHERE id = $1', [TENANT]).catch(() => {});
    const groupsLeft = await deleteGroupsWhenEmpty(admin, [GROUP, `${GROUP}-retry`, `${GROUP}-replies`, `${GROUP}-replies-retry`, ...scanGroups]);
    await admin.disconnect().catch(() => {});
    const left = {
      outbox: (await relayDb.query('SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1', [TENANT])).rows[0].n,
      deliveries: (await inTenant(notifyOwner, 'SELECT count(*)::int AS n FROM "NotificationDelivery" WHERE "tenantId" = $1', [TENANT])).rows[0].n,
    };
    check(left.outbox === 0 && left.deliveries === 0, `cleanup: this run's outbox rows and deliveries are gone (${JSON.stringify(left)}); consumer groups ${groupsLeft.length === 0 ? 'deleted' : `left to expire: ${groupsLeft.join(', ')}`}`);
    await Promise.all([authApp, relayDb, notifyApp, notifyOwner].map((c) => c.end().catch(() => {})));
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('failed sends wait in retry tiers without holding anyone up, arrive once, and run out into the dead-letter topic');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
