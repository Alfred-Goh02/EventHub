/**
 * Asserts that Notification stops mailing an address its mail server says does not
 * exist, and only such an address.
 *
 * Notification runs against the fake SMTP server (scripts/lib/fake-smtp.mjs), whose
 * refusals this run chooses, worded the way real servers word them: repeating the
 * recipient's address back.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - A 550 5.1.1 suppresses the address. The row is read back: its status code, the
 *     delivery it came from, and the service's own hash of the address, with no
 *     address anywhere in it.
 *   - A suppressed address is not tried again. THE SERVER'S REFUSAL IS LIFTED FIRST, so
 *     a second attempt would succeed and be seen; with the 550 still in place, "nothing
 *     arrived" is also what a second refusal looks like. The fake server counts every
 *     RCPT TO, refused or not, and that count must not move.
 *   - Suppression belongs to one tenant. A second tenant with the same address still
 *     gets its email while the first tenant's suppression stands.
 *   - A 550 5.7.1 and a bare 550 suppress nothing, and the next reset is tried again.
 *   - The second tenant can suppress the same address for ITSELF. A unique key on the
 *     hash alone would swallow its insert against a row RLS hides from it, and it
 *     would keep mailing a dead address with nothing to show for it.
 *   - The sender may add and extend a suppression and may not remove one: as the app
 *     role, DELETE, moving a row to another tenant, and inserting under another tenant
 *     are each refused with 42501, while extending one is allowed.
 *   - `suppression:remove` lifts ONE address, run the documented way through pnpm,
 *     which passes a `--` the script has to survive. A second suppression in the same
 *     tenant must still be there afterwards.
 *   - A suppression ENDS. An expired row does not block a send, and the bounce that
 *     follows extends the window and counts itself instead of being ignored.
 *   - Reply text repeating the address reaches no sent-log row and no retry or
 *     dead-letter header, INCLUDING an address with an apostrophe in it, which an
 *     address-shaped pattern can easily half-match. A 451 is used, because transient
 *     text travels furthest.
 *
 * Every reset must queue a NEW outbox row, checked as it is requested. Otherwise a
 * throttled or skipped request would hand back the previous message's id, and each
 * check below would pass or fail about the wrong message.
 *
 * Scoped to two tenants, their users and consumer groups created for this run; the
 * rows are removed at the end.
 *
 * Run (with Postgres, Kafka and the Schema Registry up; Auth, the relay and Notification
 * built, and Notification's migrations applied; both keypairs generated):
 *   node scripts/verify-email-suppression.mjs
 */

import { spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { serviceEnv } from './lib/service-env.mjs';
import { createFakeSmtp } from './lib/fake-smtp.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');
// The service's own hash, imported rather than rewritten here: a copy in this file
// would only ever agree with itself.
const { addressHash } = await import(pathToFileURL(join(ROOT, 'services', 'notification', 'dist', 'dispatch', 'suppression.js')).href);

const TOPIC = 'auth.email-requested';
const TIER_TOPICS = ['notification.retry.1', 'notification.retry.2', 'notification.retry.3'];
const DLQ = 'notification.dlq';
const DELAYS_MS = [1000, 1500, 2000];
/** Shorter than the default, so the window the rows carry is the configured one and not a constant. */
const TTL_DAYS = 7;

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_suppress_${RUN}`;
const OTHER_TENANT = `tenant_suppress_other_${RUN}`;
const GROUP = `verify-email-suppression-${RUN}`;
const SMTP_PORT = 3290;
const NOTIFICATION_PORT = 3291;
const AUTH_PORT = 3293;

const authEnv = serviceEnv(ROOT, 'auth', { PORT: String(AUTH_PORT), NODE_ENV: 'development', AUTH_DEV_TOKEN_SINK: '1', LOG_LEVEL: 'debug' });
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['AUTH_RELAY_DATABASE_URL'] ?? 'postgresql://eventhub_auth_relay:eventhub_auth_relay@localhost:5432/eventhub_auth',
  RELAY_FORBIDDEN_TABLE: 'User',
  RELAY_METRICS_PORT: '3292',
});
const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(NOTIFICATION_PORT),
  KAFKA_CONSUMER_GROUP: GROUP,
  DISPATCH_START_FROM: 'latest',
  DISPATCH_RETRY_DELAYS_MS: DELAYS_MS.join(','),
  SUPPRESSION_TTL_DAYS: String(TTL_DAYS),
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

async function inTenant(client, tenant, sql, params) {
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

const post = (path, body, headers = {}) =>
  fetch(`http://127.0.0.1:${AUTH_PORT}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  }).then((r) => r.status);

const dispatch = async () => (await (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/metrics`)).json()).dispatch;

/** Every reset link the dev sink printed for this run's tenants. */
function sinkLinks(log) {
  const links = [];
  for (const line of log.split(/\r?\n/)) {
    try {
      const entry = JSON.parse(line);
      if (entry.devOnlyResetUrl && (entry.tenantId === TENANT || entry.tenantId === OTHER_TENANT)) links.push(entry.devOnlyResetUrl);
    } catch {
      // not a JSON log line
    }
  }
  return links;
}

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

  const kafka = new Kafka({ clientId: `verify-email-suppression-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  // Another Notification on the real group shares the sent-log and the suppression
  // list, and would handle this run's deliveries first. See verify-email-retries.mjs
  // for why the groups get 40 s to empty.
  const realGroup = serviceEnv(ROOT, 'notification').KAFKA_CONSUMER_GROUP;
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

  const users = {
    gone: { id: `user_gone_${RUN}`, tenant: TENANT, email: `gone-${RUN}@mailcheck.example` },
    policy: { id: `user_policy_${RUN}`, tenant: TENANT, email: `policy-${RUN}@mailcheck.example` },
    bare: { id: `user_bare_${RUN}`, tenant: TENANT, email: `bare-${RUN}@mailcheck.example` },
    // A second address to suppress in the same tenant, so lifting one is seen to lift
    // only one; also the address whose window is run out below.
    second: { id: `user_second_${RUN}`, tenant: TENANT, email: `second-${RUN}@mailcheck.example` },
    // WITH AN APOSTROPHE, which Auth accepts and an address-shaped pattern can
    // half-match, leaving the name in a log or a Kafka header.
    echo: { id: `user_echo_${RUN}`, tenant: TENANT, email: `echo-o'brien-${RUN}@mailcheck.example` },
    // The same address as `gone`, in another tenant.
    twin: { id: `user_twin_${RUN}`, tenant: OTHER_TENANT, email: `gone-${RUN}@mailcheck.example` },
  };

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
    const groupId = `verify-email-suppression-scan-${RUN}-${scanGroups.length}`;
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

  const outboxIdFor = async (u) =>
    (await relayDb.query(`SELECT o.id FROM "OutboxMessage" o WHERE o."tenantId" = $1 AND o."aggregateId" = $2 ORDER BY o.seq DESC LIMIT 1`, [u.tenant, u.id])).rows[0]?.id;
  const deliveryFor = async (u, messageId) =>
    (await inTenant(notifyApp, u.tenant, `SELECT d.id, d.status::text AS status, d.attempts, d."lastError" FROM "NotificationDelivery" d WHERE d."tenantId" = $1 AND d."sourceMessageId" = $2`, [u.tenant, messageId])).rows;
  // Read as the app role, so this sees what the service sees.
  const suppressionsIn = async (tenant) =>
    (await inTenant(notifyApp, tenant, `SELECT s.* FROM "EmailSuppression" s WHERE s."tenantId" = $1`, [tenant])).rows;

  const notification = start('notification', ['dist/index.js'], notificationEnv);
  const auth = start('auth', ['dist/index.js'], authEnv);

  try {
    console.log('  the fixture');
    for (const tenant of [TENANT, OTHER_TENANT]) {
      await authApp.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [tenant, `Suppression ${tenant}`, tenant.replace(/_/g, '-')]);
    }
    for (const u of Object.values(users)) {
      await inTenant(authApp, u.tenant, `INSERT INTO "User" (id, "tenantId", email, "passwordHash", "updatedAt") VALUES ($1, $2, $3, 'not-a-hash', now())`, [u.id, u.tenant, u.email]);
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

    // Canaries: expired messages keyed by every user, on the main topic and on every
    // tier, so each consumer is known to be reading every partition used below.
    const base = await dispatch();
    const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);
    let canaries = 0;
    for (const topic of [TOPIC, ...TIER_TOPICS]) {
      for (const u of Object.values(users)) {
        await producer.send({
          topic,
          messages: [{
            key: u.id,
            value: await registry.encode(schemaId, { messageId: randomUUID(), occurredAt: new Date().toISOString(), tenantId: u.tenant, userId: u.id, kind: 'PASSWORD_RESET', expiresAt: new Date(Date.now() - 60_000).toISOString(), sealed: 'canary' }),
            headers: { 'message-id': `canary-${randomUUID()}` },
          }],
        });
        canaries += 1;
      }
    }
    const reading = await waitFor(async () => (await dispatch()).expiredSkippedTotal >= base.expiredSkippedTotal + canaries, 45_000, 500);
    check(Boolean(reading), `CONTROL: it counted all ${canaries} canaries, so both consumers read every partition this run writes to`);
    if (failures > 0) return;

    const lastIds = new Map();
    async function requestReset(u) {
      const status = await post('/forgot-password', { email: u.email }, { 'X-Tenant-ID': u.tenant });
      const messageId = await waitFor(async () => {
        const id = await outboxIdFor(u);
        return id && id !== lastIds.get(u.id) ? id : null;
      }, 5_000, 100);
      if (status !== 202 || !messageId) throw new Error(`a reset for ${u.id} was not queued as a new outbox row (status ${status})`);
      lastIds.set(u.id, messageId);
      await relayOnce();
      return { messageId };
    }

    console.log('');
    console.log('  a refusal that proves the address does not exist suppresses it');
    smtp.rules.set(users.gone.email, '550 5.1.1 <{address}>: Recipient address rejected: User unknown');
    const before1 = await dispatch();
    const gone1 = await requestReset(users.gone);
    const refused = await waitFor(async () => (await dispatch()).rejectedTotal > before1.rejectedTotal, 20_000);
    check(Boolean(refused), 'the 550 5.1.1 was taken as permanent');
    const [goneFailed] = await deliveryFor(users.gone, gone1.messageId);
    check(goneFailed?.status === 'FAILED' && /550 5\.1\.1/.test(goneFailed.lastError ?? ''), `its delivery is recorded FAILED (${goneFailed?.status}: ${goneFailed?.lastError})`);
    const after1 = await dispatch();
    check(after1.suppressionsAddedTotal === before1.suppressionsAddedTotal + 1, `and one address was suppressed (${after1.suppressionsAddedTotal - before1.suppressionsAddedTotal})`);
    const rows1 = await suppressionsIn(TENANT);
    const row = rows1[0];
    check(rows1.length === 1 && row.reason === 'HARD_BOUNCE' && row.smtpStatus === '5.1.1', `the suppression records HARD_BOUNCE and 5.1.1 (${rows1.length} row(s): ${row?.reason} ${row?.smtpStatus})`);
    check(row?.addressHash === addressHash(users.gone.email), "keyed by the service's hash of the address");
    // Computed here, not borrowed: the import above would agree with a reversible
    // encoding of the address just as happily as with a digest.
    check(row?.addressHash === createHash('sha256').update(users.gone.email).digest('hex'), 'which is a SHA-256 of it, and not some other encoding');
    const window = row ? new Date(row.expiresAt).getTime() - Date.now() : NaN;
    check(
      window > (TTL_DAYS - 1) * 86_400_000 && window < (TTL_DAYS + 1) * 86_400_000,
      `ending after the configured ${TTL_DAYS} days rather than never (${(window / 86_400_000).toFixed(1)} days)`,
    );
    check(row?.bounces === 1, `after one bounce (${row?.bounces})`);
    check(Boolean(row) && row.sourceDeliveryId === goneFailed?.id, 'and pointing at the delivery that was refused');
    const rowText = JSON.stringify(row ?? {}).toLowerCase();
    check(Boolean(row) && !rowText.includes(users.gone.email.toLowerCase()) && !rowText.includes(`gone-${RUN}`), 'with no address anywhere in the row');
    check(smtp.tried(users.gone.email).length === 1, `CONTROL: the server was asked about that address once (${smtp.tried(users.gone.email).length})`);

    console.log('');
    console.log('  a suppressed address is not tried again, even once its server would accept it');
    // Lifted first, so a second attempt would succeed and be seen, instead of meeting
    // the same 550 and looking exactly like suppression.
    smtp.rules.delete(users.gone.email);
    const before2 = await dispatch();
    const gone2 = await requestReset(users.gone);
    const counted = await waitFor(async () => (await dispatch()).suppressedTotal > before2.suppressedTotal, 20_000);
    check(Boolean(counted), 'the next reset for it was counted suppressed');
    const [goneSuppressed] = await deliveryFor(users.gone, gone2.messageId);
    check(goneSuppressed?.status === 'SUPPRESSED' && goneSuppressed.attempts === 0, `recorded SUPPRESSED with no attempt (${goneSuppressed?.status}, ${goneSuppressed?.attempts} attempt(s))`);
    await sleep(DELAYS_MS[DELAYS_MS.length - 1]);
    check(smtp.tried(users.gone.email).length === 1 && smtp.to(users.gone.email).length === 0, `and the server was not asked again (${smtp.tried(users.gone.email).length} RCPT TO, ${smtp.to(users.gone.email).length} accepted)`);

    console.log('');
    console.log('  a suppression belongs to one tenant');
    const twin = await requestReset(users.twin);
    const twinArrived = await waitFor(async () => smtp.to(users.twin.email).length > 0, 20_000);
    check(Boolean(twinArrived), `another tenant's reset to the same address was delivered while the first tenant's suppression stands (${(await suppressionsIn(TENANT)).length} there)`);
    // Waited for, not read once: SENT is recorded just after the server accepts the
    // message, so a single read right after it arrives can still see PENDING.
    const twinSent = await waitFor(async () => {
      const [row] = await deliveryFor(users.twin, twin.messageId);
      return row?.status === 'SENT' ? row : null;
    }, 20_000, 250);
    check(Boolean(twinSent), `and recorded SENT in its own tenant (${twinSent?.status ?? (await deliveryFor(users.twin, twin.messageId))[0]?.status})`);

    // The other half of the key. With a unique index on the hash alone, this tenant's
    // own bounce would conflict with the row RLS hides from it, insert nothing, and
    // leave it mailing a dead address for good.
    smtp.rules.set(users.twin.email, '550 5.1.1 <{address}>: Recipient address rejected: User unknown');
    const beforeTwinBounce = await dispatch();
    const twin2 = await requestReset(users.twin);
    const twinRefused = await waitFor(async () => (await dispatch()).rejectedTotal > beforeTwinBounce.rejectedTotal, 20_000);
    const [twinFailed] = await deliveryFor(users.twin, twin2.messageId);
    const twinRows = await suppressionsIn(OTHER_TENANT);
    check(Boolean(twinRefused) && twinFailed?.status === 'FAILED', `its own 550 5.1.1 for that address was recorded FAILED (${twinFailed?.status})`);
    check(twinRows.length === 1 && twinRows[0].addressHash === addressHash(users.twin.email), `and suppressed the address for that tenant too (${twinRows.length} row(s) there)`);
    check((await suppressionsIn(TENANT)).length === 1, 'while the first tenant still holds exactly its own');
    smtp.rules.delete(users.twin.email);

    console.log('');
    console.log('  refusals that do not prove the address is gone suppress nothing');
    smtp.rules.set(users.policy.email, '550 5.7.1 <{address}>: Message rejected by policy');
    smtp.rules.set(users.bare.email, '550 Requested action not taken: <{address}>');
    const before4 = await dispatch();
    const policy1 = await requestReset(users.policy);
    const bare1 = await requestReset(users.bare);
    const both = await waitFor(async () => (await dispatch()).rejectedTotal >= before4.rejectedTotal + 2, 20_000);
    const [policyFailed] = await deliveryFor(users.policy, policy1.messageId);
    const [bareFailed] = await deliveryFor(users.bare, bare1.messageId);
    check(Boolean(both) && policyFailed?.status === 'FAILED' && bareFailed?.status === 'FAILED', `a 550 5.7.1 and a 550 with no enhanced code were both recorded FAILED (${policyFailed?.status}, ${bareFailed?.status})`);
    const after4 = await dispatch();
    const stillOne = (await suppressionsIn(TENANT)).length;
    check(after4.suppressionsAddedTotal === before4.suppressionsAddedTotal && stillOne === 1, `and neither was suppressed (${stillOne} suppression in the tenant)`);
    await requestReset(users.policy);
    const triedAgain = await waitFor(async () => smtp.tried(users.policy.email).length >= 2, 20_000);
    check(Boolean(triedAgain), `the next reset for the policy-blocked address was tried again (${smtp.tried(users.policy.email).length} RCPT TO)`);

    console.log('');
    console.log('  the sender may add and extend a suppression, and may not remove one');
    const refusal = async (tenant, sql, params) => {
      try {
        await inTenant(notifyApp, tenant, sql, params);
        return null;
      } catch (err) {
        return err.code ?? String(err.message ?? err);
      }
    };
    const visible = await inTenant(notifyApp, OTHER_TENANT, 'SELECT s."tenantId" FROM "EmailSuppression" s', []);
    check(visible.rows.length === 1 && visible.rows[0].tenantId === OTHER_TENANT, `as the app role in the second tenant, only that tenant's own suppression is visible (${visible.rows.length} row(s))`);
    const crossInsert = await refusal(
      OTHER_TENANT,
      `INSERT INTO "EmailSuppression" (id, "tenantId", "addressHash", reason, "expiresAt") VALUES ($1, $2, $3, 'HARD_BOUNCE', now() + interval '1 day')`,
      [randomUUID(), TENANT, addressHash(`smuggled-${RUN}@mailcheck.example`)],
    );
    check(crossInsert === '42501', `it cannot suppress an address for another tenant (${crossInsert})`);
    const deleteRefused = await refusal(OTHER_TENANT, 'DELETE FROM "EmailSuppression"', []);
    check(deleteRefused === '42501', `it cannot delete a suppression (${deleteRefused})`);
    const retenant = await refusal(OTHER_TENANT, 'UPDATE "EmailSuppression" SET "tenantId" = $1', [TENANT]);
    check(retenant === '42501', `and it cannot move one to another tenant (${retenant})`);
    const extend = await refusal(OTHER_TENANT, `UPDATE "EmailSuppression" SET "expiresAt" = now() + interval '1 day'`, []);
    check(extend === null, `CONTROL: it can extend one, so the three refusals are the grants and the policy, not a broken statement (${extend ?? 'allowed'})`);

    console.log('');
    console.log('  an address is matched whatever its case or surrounding space');
    const hashed = addressHash(users.gone.email);
    check(addressHash(`  ${users.gone.email.toUpperCase()} `) === hashed, 'the address in capitals, padded with spaces, has the same hash');
    check(addressHash(users.policy.email) !== hashed, 'CONTROL: a different address does not');

    console.log('');
    console.log('  a second address is suppressed in the same tenant');
    smtp.rules.set(users.second.email, '550 5.1.2 <{address}>: Domain not found');
    const beforeSecond = await dispatch();
    const second1 = await requestReset(users.second);
    const secondRefused = await waitFor(async () => (await dispatch()).rejectedTotal > beforeSecond.rejectedTotal, 20_000);
    const [secondFailed] = await deliveryFor(users.second, second1.messageId);
    const twoRows = await suppressionsIn(TENANT);
    check(Boolean(secondRefused) && secondFailed?.status === 'FAILED' && twoRows.length === 2, `a 550 5.1.2 suppressed a second address there (${twoRows.length} row(s), delivery ${secondFailed?.status})`);
    smtp.rules.delete(users.second.email);

    console.log('');
    console.log('  one suppression is lifted by hand, and only that one');
    // Run the way the script's own usage line documents it, through pnpm, which hands
    // the script a `--` of its own on every shell but PowerShell.
    const removeViaPnpm = ([tenant, address]) =>
      new Promise((done) => {
        // One command line rather than an args array: with shell: true Node only
        // concatenates the array, which it warns about, and the shell is the point
        // here — the documented command is a shell command.
        const child = spawn(`pnpm --filter @eventhub/notification run suppression:remove -- "${tenant}" "${address}"`, {
          cwd: ROOT,
          env: { ...process.env, ...notificationEnv },
          stdio: ['ignore', 'pipe', 'pipe'],
          shell: true,
        });
        let log = '';
        child.stdout.on('data', (d) => (log += d));
        child.stderr.on('data', (d) => (log += d));
        child.on('exit', (code) => done({ code, log }));
      });
    const lifted = await removeViaPnpm([TENANT, users.gone.email.toUpperCase()]);
    const leftAfterLift = await suppressionsIn(TENANT);
    check(lifted.code === 0 && leftAfterLift.length === 1, `suppression:remove, run the documented way with the address in capitals, lifted one (exit ${lifted.code}; ${leftAfterLift.length} left)`);
    check(leftAfterLift[0]?.addressHash === addressHash(users.second.email), "and left the tenant's other suppression alone");
    const deliveredBefore = smtp.to(users.gone.email).length;
    const gone3 = await requestReset(users.gone);
    const mailedAgain = await waitFor(async () => smtp.to(users.gone.email).length > deliveredBefore, 20_000);
    const goneSent = await waitFor(async () => {
      const [row] = await deliveryFor(users.gone, gone3.messageId);
      return row?.status === 'SENT' ? row : null;
    }, 20_000, 250);
    check(Boolean(mailedAgain) && Boolean(goneSent), `the next reset was delivered (${goneSent?.status ?? (await deliveryFor(users.gone, gone3.messageId))[0]?.status})`);
    const again = await removeViaPnpm([TENANT, users.gone.email]);
    check(again.code === 1, `lifting a suppression that is not there fails rather than doing nothing quietly (exit ${again.code})`);

    console.log('');
    console.log('  a suppression ends, and the bounce that follows puts it back');
    // The window run out by hand rather than waited for. A relay that was wrong about
    // a live mailbox is only recoverable because this window exists.
    await inTenant(notifyOwner, TENANT, `UPDATE "EmailSuppression" SET "expiresAt" = now() - interval '1 day' WHERE "addressHash" = $1`, [addressHash(users.second.email)]);
    smtp.rules.set(users.second.email, '550 5.1.2 <{address}>: Domain not found');
    const beforeExpired = await dispatch();
    const triedBefore = smtp.tried(users.second.email).length;
    const second2 = await requestReset(users.second);
    const triedAgainAfterExpiry = await waitFor(async () => smtp.tried(users.second.email).length > triedBefore, 20_000);
    check(Boolean(triedAgainAfterExpiry), `an expired suppression no longer holds the address back (${smtp.tried(users.second.email).length} RCPT TO)`);
    await waitFor(async () => (await dispatch()).rejectedTotal > beforeExpired.rejectedTotal, 20_000);
    const [refreshed] = await suppressionsIn(TENANT);
    const newWindow = refreshed ? new Date(refreshed.expiresAt).getTime() - Date.now() : NaN;
    check(refreshed?.bounces === 2 && newWindow > (TTL_DAYS - 1) * 86_400_000, `and the bounce that followed extended the window and counted itself (bounces ${refreshed?.bounces}, ${(newWindow / 86_400_000).toFixed(1)} days)`);
    const [secondFailedAgain] = await deliveryFor(users.second, second2.messageId);
    check(secondFailedAgain?.status === 'FAILED', `with its delivery recorded FAILED again (${secondFailedAgain?.status})`);
    smtp.rules.delete(users.second.email);

    console.log('');
    console.log('  reply text that repeats the address stays out of rows and headers');
    smtp.rules.set(users.echo.email, '451 4.2.1 <{address}>: mailbox busy, try again later');
    const before7 = await dispatch();
    const echo = await requestReset(users.echo);
    const deadLettered = await waitFor(async () => (await dispatch()).deadLetteredTotal > before7.deadLetteredTotal, 30_000, 500);
    check(Boolean(deadLettered), 'CONTROL: the 451 went through every retry tier into the dead-letter topic');
    const echoAddress = users.echo.email.toLowerCase();
    const [echoRow] = await deliveryFor(users.echo, echo.messageId);
    const echoError = echoRow?.lastError ?? '';
    check(echoAddress.includes("'"), "CONTROL: the address under test carries an apostrophe, which Auth accepts in a local part");
    check(/451/.test(echoError) && echoError.includes('[address]'), `CONTROL: its lastError kept the reply, with the address replaced (${echoError})`);
    check(!echoError.toLowerCase().includes(echoAddress), 'and holds no address');
    check(!echoError.toLowerCase().includes("o'"), 'nor the part before the apostrophe, which a pattern that treats it as a delimiter leaves behind');
    const [tierCopy] = await scan(TIER_TOPICS[0], echo.messageId);
    const tierError = tierCopy ? (headersOf(tierCopy)['retry-last-error'] ?? '') : '';
    check(
      /451/.test(tierError) && !tierError.toLowerCase().includes(echoAddress) && !tierError.toLowerCase().includes("o'"),
      `the tier's retry-last-error holds the reply and no part of the address (${tierError})`,
    );
    const [dlqCopy] = await scan(DLQ, echo.messageId);
    const dlqReason = dlqCopy ? (headersOf(dlqCopy)['dlq-reason'] ?? '') : '';
    check(
      /451/.test(dlqReason) && !dlqReason.toLowerCase().includes(echoAddress) && !dlqReason.toLowerCase().includes("o'"),
      `the dead-letter reason holds the reply and no part of the address (${dlqReason})`,
    );

    console.log('');
    console.log("  Notification's output holds nothing to steal");
    const output = notification.log().toLowerCase();
    const links = sinkLinks(auth.log());
    const addresses = [...new Set(Object.values(users).map((u) => u.email.toLowerCase()))];
    check(output.includes('recorded suppressed'), 'CONTROL: its output is captured (it logged a suppression)');
    check(links.length > 0, `CONTROL: the dev sink gave ${links.length} link(s) to look for`);
    check(
      !addresses.some((a) => output.includes(a)) && !links.some((l) => output.includes(l.toLowerCase())) && !output.includes("o'brien"),
      'and holds no link, no address, and no part of one',
    );
  } catch (err) {
    // Reported, not thrown, and with both children named: see verify-email-retries.mjs.
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
    for (const tenant of [TENANT, OTHER_TENANT]) {
      await relayDb.query('DELETE FROM "OutboxMessage" WHERE "tenantId" = $1', [tenant]).catch(() => {});
      await inTenant(notifyOwner, tenant, 'DELETE FROM "NotificationDelivery" WHERE "tenantId" = $1', [tenant]).catch(() => {});
      await inTenant(notifyOwner, tenant, 'DELETE FROM "EmailSuppression" WHERE "tenantId" = $1', [tenant]).catch(() => {});
      await inTenant(authApp, tenant, 'DELETE FROM "User" WHERE "tenantId" = $1', [tenant]).catch(() => {});
      await authApp.query('DELETE FROM "Tenant" WHERE id = $1', [tenant]).catch(() => {});
    }
    const groupsLeft = await deleteGroupsWhenEmpty(admin, [GROUP, `${GROUP}-retry`, `${GROUP}-replies`, `${GROUP}-replies-retry`, ...scanGroups]);
    await admin.disconnect().catch(() => {});
    const left = { outbox: 0, deliveries: 0, suppressions: 0 };
    for (const tenant of [TENANT, OTHER_TENANT]) {
      left.outbox += (await relayDb.query('SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1', [tenant])).rows[0].n;
      left.deliveries += (await inTenant(notifyOwner, tenant, 'SELECT count(*)::int AS n FROM "NotificationDelivery" WHERE "tenantId" = $1', [tenant])).rows[0].n;
      left.suppressions += (await inTenant(notifyOwner, tenant, 'SELECT count(*)::int AS n FROM "EmailSuppression" WHERE "tenantId" = $1', [tenant])).rows[0].n;
    }
    check(left.outbox === 0 && left.deliveries === 0 && left.suppressions === 0, `cleanup: this run's outbox rows, deliveries and suppressions are gone (${JSON.stringify(left)}); consumer groups ${groupsLeft.length === 0 ? 'deleted' : `left to expire: ${groupsLeft.join(', ')}`}`);
    await Promise.all([authApp, relayDb, notifyApp, notifyOwner].map((c) => c.end().catch(() => {})));
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('an address its server says does not exist is suppressed for its tenant, is never tried again until lifted, and no address leaks on the way');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
