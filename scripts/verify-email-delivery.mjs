/**
 * Asserts that a password reset reaches the user's inbox once, with a link that
 * works, and that nothing Notification should refuse is ever sent.
 *
 * The path: Auth queues a sealed request, the relay publishes it, and Notification
 * opens the seal, verifies Auth's signature, claims the delivery in its sent-log
 * and sends over SMTP to Mailpit. scripts/verify-email-request.mjs covers the first
 * half; this reads the inbox through Mailpit's API.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - One email with the live link. The link read back from the inbox resets the
 *     password; a delivered but wrong link would look like success otherwise.
 *   - A redelivery sends nothing. The same outbox row is published again, and
 *     Notification's received counter must move. "Still one email" is also what a
 *     consumer that never saw the redelivery would produce.
 *   - A forged seal and an expired request are refused, and a real request behind
 *     them on the same partition is still delivered. A consumer stalled on the first
 *     refusal would also have sent nothing to the victim.
 *   - No token, link or address in Notification's own output.
 *
 * READING THE RIGHT PARTITION. Notification starts on a fresh group at 'latest', so
 * the run first sends an expired canary keyed by this run's user, and waits for
 * Notification to count it before anything real is sent. Every later message has
 * the same key, so the same partition, and the consumer is known to be reading it.
 *
 * Scoped to a tenant, a user and a consumer group created for this run; all of them,
 * the delivery rows and the inbox messages are removed at the end.
 *
 * Run (with Postgres, Kafka, the Schema Registry and Mailpit up; Auth, the relay and
 * Notification built; both keypairs generated): node scripts/verify-email-delivery.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { CompactEncrypt, SignJWT, calculateJwkThumbprint, exportJWK, importPKCS8, importSPKI } from 'jose';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');

const TOPIC = 'auth.email-requested';
const MAILPIT = process.env['MAILPIT_URL'] ?? 'http://127.0.0.1:8025';
const SUBJECT = 'Reset your EventHub password';

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_maildelivery_${RUN}`;
const USER_ID = `user_maildelivery_${RUN}`;
const EMAIL = `deliver-${RUN}@mailcheck.example`;
const VICTIM = `victim-${RUN}@mailcheck.example`;
const GROUP = `verify-email-delivery-${RUN}`;
const NEW_PASSWORD = `new-password-${RUN}`;

const NOTIFICATION_PORT = 3091;
const AUTH_PORT = 3093;

const authEnv = serviceEnv(ROOT, 'auth', {
  PORT: String(AUTH_PORT),
  NODE_ENV: 'development',
  AUTH_DEV_TOKEN_SINK: '1',
  LOG_LEVEL: 'debug',
});
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['AUTH_RELAY_DATABASE_URL'] ??
    'postgresql://eventhub_auth_relay:eventhub_auth_relay@localhost:5432/eventhub_auth',
  RELAY_FORBIDDEN_TABLE: 'User',
  RELAY_METRICS_PORT: '3092',
});
const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(NOTIFICATION_PORT),
  KAFKA_CONSUMER_GROUP: GROUP,
  DISPATCH_START_FROM: 'latest',
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

/**
 * The start and the end of a service's output, for a failure message. The end
 * alone was the tail of a Prisma stack trace, with the error that explained it cut
 * off the front.
 */
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
      done({ code, log: relay.log() });
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
  }).then(async (r) => ({ status: r.status, body: await r.text() }));

const dispatch = async () => (await (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/metrics`)).json()).dispatch;

/** The dev sink's reset links for this run's tenant, oldest first. */
function sinkLinks(log) {
  const links = [];
  for (const line of log.split(/\r?\n/)) {
    try {
      const entry = JSON.parse(line);
      if (entry.devOnlyResetUrl && entry.tenantId === TENANT) links.push(entry.devOnlyResetUrl);
    } catch {
      // not a JSON log line
    }
  }
  return links;
}

/**
 * The reset link on its own line in an email's text, whole. Compared with ===,
 * never includes(): a link with a character appended still contains the real one,
 * and includes() passed exactly that broken link.
 */
const resetLinkIn = (text) => text?.match(/^(https?:\/\/\S*reset-password\?token=\S+?)\r?$/m)?.[1] ?? null;

/** The first href in an email's HTML, unescaped, for the same comparison. */
function hrefIn(html) {
  const raw = html?.match(/href="([^"]*)"/)?.[1];
  if (raw === undefined) return null;
  return raw.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
}

/** One message from Mailpit, or null. Never .json() on a 404, which ended a run with a SyntaxError instead of a FAIL. */
async function messageById(id) {
  const res = await fetch(`${MAILPIT}/api/v1/message/${encodeURIComponent(id)}`);
  return res.ok ? res.json() : null;
}

async function inbox(address) {
  const res = await fetch(`${MAILPIT}/api/v1/search?query=${encodeURIComponent(`to:${address}`)}`);
  if (!res.ok) throw new Error(`Mailpit search answered ${res.status}`);
  return (await res.json()).messages ?? [];
}

async function main() {
  // Nothing may already answer on these ports. A process left from an earlier run
  // would pass the readiness checks below on behalf of code this run never started.
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

  // Nor may the real Notification be consuming. It shares the sent-log, so it would
  // claim this run's messages first: the inbox would still hold one email, sent by a
  // process this run never started, and the checks on this run's instance would
  // fail for reasons that have nothing to do with the code. The group's name is the
  // one Notification's own .env.example configures.
  const realGroup = serviceEnv(ROOT, 'notification').KAFKA_CONSUMER_GROUP;
  const probe = new Kafka({ clientId: `verify-email-delivery-probe-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING }).admin();
  await probe.connect();
  // A consumer stopped a moment ago can stay a member until its session times out
  // (30 s), so the groups get that long to empty before this refuses.
  let busy = [];
  const idle = await waitFor(async () => {
    const { groups } = await probe.describeGroups([realGroup, `${realGroup}-retry`]);
    busy = groups.filter((g) => g.members.length > 0).map((g) => g.groupId);
    return busy.length === 0;
  }, 40_000, 2_000);
  await probe.disconnect();
  if (!idle) {
    console.error(
      `FAIL: ${busy.join(' and ')} still ${busy.length === 1 ? 'has' : 'have'} active members, so another Notification is ` +
        'delivering from auth.email-requested. Stop it first (in compose: docker compose stop notification).',
    );
    process.exitCode = 1;
    return;
  }

  const authApp = new pg.Client({ connectionString: authEnv.APP_DATABASE_URL });
  const relayDb = new pg.Client({ connectionString: relayEnv.RELAY_DATABASE_URL });
  const notifyApp = new pg.Client({ connectionString: notificationEnv.APP_DATABASE_URL });
  const notifyOwner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  await Promise.all([authApp, relayDb, notifyApp, notifyOwner].map((c) => c.connect()));

  const kafka = new Kafka({ clientId: `verify-email-delivery-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  const producer = kafka.producer();
  const registry = new SchemaRegistry({ host: relayEnv.SCHEMA_REGISTRY_URL });
  await admin.connect();
  await producer.connect();

  const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
  const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
  const authSigning = await importPKCS8(key('auth', 'private.pem'), 'RS256');
  const impostorSigning = await importPKCS8(key('notification', 'private.pem'), 'RS256');

  /** A seal made the way Auth makes one, with whichever signing key the case needs. */
  async function seal({ messageId, email, link, signingKey, exp }) {
    const signed = await new SignJWT({ tenantId: TENANT, email, link, kind: 'PASSWORD_RESET' })
      .setProtectedHeader({ alg: 'RS256', typ: 'auth-email+jwt' })
      .setSubject(USER_ID)
      .setJti(messageId)
      .setIssuer(authEnv.JWT_ISSUER)
      .setAudience('eventhub-notification')
      .setIssuedAt()
      .setExpirationTime(exp)
      .sign(signingKey);
    return new CompactEncrypt(new TextEncoder().encode(signed))
      .setProtectedHeader({ alg: 'RSA-OAEP-256', enc: 'A256GCM', cty: 'JWT', kid })
      .encrypt(notificationPublic);
  }

  async function produce({ messageId, expiresAt, sealed }) {
    const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);
    const value = { messageId, occurredAt: new Date().toISOString(), tenantId: TENANT, userId: USER_ID, kind: 'PASSWORD_RESET', expiresAt, sealed };
    await producer.send({
      topic: TOPIC,
      messages: [{ key: USER_ID, value: await registry.encode(schemaId, value), headers: { 'message-id': messageId, 'tenant-id': TENANT } }],
    });
  }

  const outboxRows = () =>
    relayDb
      .query(`SELECT o.id, o."publishedAt" FROM "OutboxMessage" o WHERE o."tenantId" = $1 ORDER BY o.seq`, [TENANT])
      .then((r) => r.rows);
  const deliveries = () =>
    inTenant(
      notifyApp,
      TENANT,
      `SELECT d."sourceMessageId", d."idempotencyKey", d.status::text AS status, d."providerMessageId", d.attempts
         FROM "NotificationDelivery" d WHERE d."tenantId" = $1 ORDER BY d."createdAt"`,
      [TENANT],
    ).then((r) => r.rows);

  const notification = start('notification', ['dist/index.js'], notificationEnv);
  const auth = start('auth', ['dist/index.js'], authEnv);
  const mailIds = new Set();
  const secrets = [];

  try {
    console.log('  the fixture');
    await authApp.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [TENANT, `Mail delivery ${RUN}`, `maildelivery-${RUN}`]);
    await inTenant(authApp, TENANT, `INSERT INTO "User" (id, "tenantId", email, "passwordHash", "updatedAt") VALUES ($1, $2, $3, 'not-a-hash', now())`, [USER_ID, TENANT, EMAIL]);

    // Waited for side by side, each with a full minute: one run saw Auth miss a
    // 30-second window that began only after Notification was ready.
    const [ready, authUp] = await Promise.all([
      waitFor(async () => (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/ready`)).ok, 60_000, 500),
      waitFor(async () => (await fetch(`http://127.0.0.1:${AUTH_PORT}/health`)).ok, 60_000, 500),
    ]);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${NOTIFICATION_PORT} (database, consumer and SMTP)` : `Notification never became ready:\n${excerpt(notification.log())}`);
    check(Boolean(authUp), authUp ? `CONTROL: a private Auth with the dev sink on is listening on :${AUTH_PORT}` : `Auth never came up:\n${excerpt(auth.log())}`);
    if (failures > 0) return;

    const joined = await waitFor(async () => {
      const { groups } = await admin.describeGroups([GROUP]);
      return groups[0]?.state === 'Stable' && groups[0].members.length > 0;
    }, 60_000, 500);
    check(Boolean(joined), 'CONTROL: its consumer group has joined');
    const base = await dispatch();
    await produce({ messageId: randomUUID(), expiresAt: new Date(Date.now() - 60_000).toISOString(), sealed: 'canary' });
    const reading = await waitFor(async () => (await dispatch()).expiredSkippedTotal > base.expiredSkippedTotal, 30_000);
    check(Boolean(reading), 'CONTROL: it counted an expired canary keyed by this run\'s user, so it reads the partition every later message lands on');
    if (failures > 0) return;

    console.log('');
    console.log('  a reset is delivered once, with the link Auth issued');
    const first = await post('/forgot-password', { email: EMAIL }, { 'X-Tenant-ID': TENANT });
    check(first.status === 202, `/forgot-password answered 202 (${first.status})`);
    const link1 = await waitFor(async () => sinkLinks(auth.log())[0], 5_000, 100);
    const token1 = link1 ? new URL(link1).searchParams.get('token') : '';
    secrets.push(token1, link1);
    check(Boolean(token1 && token1.length >= 40), 'CONTROL: the dev sink gave this run the real link to compare against');
    const relay1 = await relayOnce();
    const [row1] = await outboxRows();
    check(Boolean(row1?.publishedAt), `CONTROL: the relay published the request (exited ${relay1.code})`);
    if (failures > 0) return;

    const arrived = await waitFor(async () => {
      const messages = await inbox(EMAIL);
      return messages.length >= 1 ? messages : null;
    }, 30_000, 500);
    // A second copy lands a moment after the first, so it gets the chance to. This
    // returned at the first email, and could have passed a consumer that sent two.
    const doubled = arrived ? await waitFor(async () => (await inbox(EMAIL)).length > 1, 3_000, 500) : null;
    check(arrived?.length === 1 && !doubled, `exactly one email reached ${EMAIL} (${doubled ? 'a second copy arrived' : (arrived?.length ?? 0)})`);
    if (!arrived) {
      console.error(excerpt(notification.log()));
      return;
    }
    arrived.forEach((m) => mailIds.add(m.ID));
    const mail = await messageById(arrived[0].ID);
    const mailHeaders = await (await fetch(`${MAILPIT}/api/v1/message/${encodeURIComponent(arrived[0].ID)}/headers`)).json().catch(() => ({}));
    check(mail?.Subject === SUBJECT, `its subject is "${mail?.Subject}"`);
    const textLink = resetLinkIn(mail?.Text);
    check(textLink === link1, `its text carries exactly the link Auth issued${textLink === link1 ? '' : textLink ? ' (it carries a different one)' : ' (it carries none)'}`);
    const htmlLink = hrefIn(mail?.HTML);
    check(htmlLink === link1, `and so does its HTML link${htmlLink === link1 ? '' : htmlLink ? ' (it points somewhere else)' : ' (there is none)'}`);
    check(mailHeaders['X-Eventhub-Message-Id']?.[0] === row1.id || mailHeaders['X-EventHub-Message-Id']?.[0] === row1.id, 'its X-EventHub-Message-Id header is the outbox message id');

    const [delivery] = await deliveries();
    check(
      delivery?.status === 'SENT' && delivery.idempotencyKey === `${row1.id}:email:${USER_ID}` && Boolean(delivery.providerMessageId),
      `the sent-log records it SENT under ${delivery?.idempotencyKey}, with the provider's id`,
    );

    console.log('');
    console.log('  the same message delivered again sends nothing');
    const beforeRedelivery = await dispatch();
    await relayDb.query(`UPDATE "OutboxMessage" SET "publishedAt" = NULL WHERE id = $1`, [row1.id]);
    const relay2 = await relayOnce();
    check((await outboxRows())[0]?.publishedAt, `CONTROL: the relay published the same row again (exited ${relay2.code})`);
    const received = await waitFor(async () => (await dispatch()).receivedTotal > beforeRedelivery.receivedTotal, 30_000);
    // Only "received" here. This used to wait for the skip as well, so a consumer
    // that resent the email failed the CONTROL, which then read as "never received".
    check(Boolean(received), 'CONTROL: Notification received the redelivery');
    const outcomes = (m) => m.sentTotal + m.duplicatesSkippedTotal + m.expiredSkippedTotal + m.poisonSkippedTotal + m.rejectedTotal + m.transientFailuresTotal;
    const settled = await waitFor(async () => {
      const m = await dispatch();
      return outcomes(m) > outcomes(beforeRedelivery) ? m : null;
    }, 30_000);
    check(
      settled?.duplicatesSkippedTotal === beforeRedelivery.duplicatesSkippedTotal + 1 && settled?.sentTotal === beforeRedelivery.sentTotal,
      'and skipped it as already delivered, sending nothing',
    );
    // A second email would reach the inbox a moment after the send is counted.
    const resent = await waitFor(async () => (await inbox(EMAIL)).length > 1, 3_000, 500);
    check(!resent, 'the inbox still holds one email');
    check((await deliveries()).length === 1, 'and the sent-log one row');

    console.log('');
    console.log('  a forged seal and an expired request send nothing, and do not stop the partition');
    const beforeRefusals = await dispatch();
    const forgedId = randomUUID();
    const future = new Date(Date.now() + 10 * 60_000);
    await produce({
      messageId: forgedId,
      expiresAt: future.toISOString(),
      sealed: await seal({ messageId: forgedId, email: VICTIM, link: 'https://evil.example/reset-password?token=forged', signingKey: impostorSigning, exp: future }),
    });
    const expiredId = randomUUID();
    await produce({
      messageId: expiredId,
      expiresAt: new Date(Date.now() - 60_000).toISOString(),
      sealed: await seal({ messageId: expiredId, email: EMAIL, link: `${link1}-expired`, signingKey: authSigning, exp: new Date(Date.now() - 60_000) }),
    });
    const lateId = randomUUID();
    await produce({
      messageId: lateId,
      // The envelope says it is still valid; the signed exp, which is what counts, says not.
      expiresAt: future.toISOString(),
      sealed: await seal({ messageId: lateId, email: EMAIL, link: `${link1}-late`, signingKey: authSigning, exp: new Date(Date.now() - 60_000) }),
    });
    const refused = await waitFor(async () => {
      const m = await dispatch();
      return m.poisonSkippedTotal >= beforeRefusals.poisonSkippedTotal + 1 && m.expiredSkippedTotal >= beforeRefusals.expiredSkippedTotal + 2 ? m : null;
    }, 30_000);
    check(refused?.poisonSkippedTotal === beforeRefusals.poisonSkippedTotal + 1, 'the seal signed by a key that is not Auth\'s was refused as poison');
    check(refused?.expiredSkippedTotal === beforeRefusals.expiredSkippedTotal + 2, 'an expired envelope, and a current envelope around an expired seal, were both skipped as expired');

    const inboxBefore = (await inbox(EMAIL)).length;
    const second = await post('/forgot-password', { email: EMAIL }, { 'X-Tenant-ID': TENANT });
    check(second.status === 202, `a second reset request answered 202 (${second.status})`);
    const link2 = await waitFor(async () => sinkLinks(auth.log())[1], 5_000, 100);
    const token2 = link2 ? new URL(link2).searchParams.get('token') : '';
    secrets.push(token2, link2);
    await relayOnce();
    const grown = await waitFor(async () => {
      const messages = await inbox(EMAIL);
      return messages.length > inboxBefore ? messages : null;
    }, 30_000, 500);
    (grown ?? []).forEach((m) => mailIds.add(m.ID));
    const extra = grown ? await waitFor(async () => (await inbox(EMAIL)).length > inboxBefore + 1, 3_000, 500) : null;
    check(
      grown?.length === inboxBefore + 1 && !extra,
      `the request behind the refused ones was still delivered, as one more email (${grown ? `${(extra ? inboxBefore + 2 : grown.length) - inboxBefore}${extra ? ' or more' : ''}` : 0})`,
    );
    // The newest by when Mailpit received it. "The one not seen before" found nothing
    // under a consumer that sent every email twice, and asked Mailpit for undefined.
    const latest = grown ? [...grown].sort((a, b) => Date.parse(b.Created) - Date.parse(a.Created))[0] : null;
    const newest = latest ? await messageById(latest.ID) : null;
    check(resetLinkIn(newest?.Text) === link2, 'carrying exactly the second link');
    const victimMail = await inbox(VICTIM);
    victimMail.forEach((m) => mailIds.add(m.ID));
    check(victimMail.length === 0, `nothing was sent to the forged seal's address (${victimMail.length})`);
    const rows = await deliveries();
    check(!rows.some((r) => [forgedId, expiredId, lateId].includes(r.sourceMessageId)), 'and no refused request reached the sent-log');

    console.log('');
    console.log('  the link from the inbox is the live credential');
    // Read out of the email itself. This used the dev sink's token, which would have
    // passed with a broken link in the email.
    const emailedLink = resetLinkIn(newest?.Text) ?? '';
    const emailedToken = emailedLink ? new URL(emailedLink).searchParams.get('token') ?? '' : '';
    const reset = await post('/reset-password', { token: emailedToken, password: NEW_PASSWORD });
    check(reset.status === 200, `the link read out of the email resets the password (${reset.status})`);
    const login = await post('/login', { email: EMAIL, password: NEW_PASSWORD }, { 'X-Tenant-ID': TENANT });
    check(login.status === 200, `and the new password signs in (${login.status})`);

    console.log('');
    console.log("  Notification's output holds nothing to steal");
    const output = notification.log();
    const leaked = [['a token', token1], ['a token', token2], ['a link', link1], ['a link', link2], ['the address', EMAIL], ["the forged seal's address", VICTIM], ['the forged link', 'evil.example']].filter(([, s]) => s && output.includes(s));
    check(output.includes('password reset email sent'), 'CONTROL: its output is captured (it logged the send)');
    check(leaked.length === 0, leaked.length === 0 ? 'and holds no token, link or address' : `and holds ${[...new Set(leaked.map(([n]) => n))].join(', ')}`);

    const caughtUp = await waitFor(async () => (await dispatch()).lag?.total === 0, 20_000, 1_000);
    check(Boolean(caughtUp), '/metrics shows lag 0 once every message is committed');
  } catch (err) {
    // Reported, not thrown, so a run that stops says what stopped it. Both children
    // are named, with the socket error Node keeps under "fetch failed": a POST to Auth
    // once ended this run with those two words and nothing to say why.
    const children = [['Notification', notification], ['Auth', auth]];
    const cause = err?.cause ? ` [${err.cause.code ?? err.cause.message ?? String(err.cause)}]` : '';
    const exited = children.filter(([, p]) => p.child.exitCode !== null || p.child.signalCode !== null).map(([name, p]) => ` (${name} had exited: ${p.child.exitCode ?? p.child.signalCode})`);
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}${cause}${exited.join('')}`);
    for (const [name, p] of children) console.error(`  ${name}'s output:\n${excerpt(p.log())}`);
  } finally {
    notification.child.kill();
    auth.child.kill();
    // Searched again rather than trusting the ids seen along the way: an email that
    // arrived after the last look was once left behind.
    const found = [...(await inbox(EMAIL).catch(() => [])), ...(await inbox(VICTIM).catch(() => []))].map((m) => m.ID);
    await fetch(`${MAILPIT}/api/v1/messages`, {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ IDs: [...new Set([...mailIds, ...found])] }),
    }).catch(() => {});
    await relayDb.query('DELETE FROM "OutboxMessage" WHERE "tenantId" = $1', [TENANT]).catch(() => {});
    await inTenant(notifyOwner, TENANT, 'DELETE FROM "NotificationDelivery" WHERE "tenantId" = $1', [TENANT]).catch(() => {});
    await inTenant(authApp, TENANT, 'DELETE FROM "User" WHERE id = $1', [USER_ID]).catch(() => {});
    await authApp.query('DELETE FROM "Tenant" WHERE id = $1', [TENANT]).catch(() => {});
    await producer.disconnect().catch(() => {});
    // The group empties once the killed consumer's session times out.
    const groupsLeft = await deleteGroupsWhenEmpty(admin, [GROUP, `${GROUP}-retry`, `${GROUP}-replies`, `${GROUP}-replies-retry`]);
    await admin.disconnect().catch(() => {});
    const left = {
      outbox: (await relayDb.query('SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1', [TENANT])).rows[0].n,
      deliveries: (await inTenant(notifyOwner, TENANT, 'SELECT count(*)::int AS n FROM "NotificationDelivery" WHERE "tenantId" = $1', [TENANT])).rows[0].n,
      inbox: (await inbox(EMAIL).catch(() => [])).length + (await inbox(VICTIM).catch(() => [])).length,
    };
    check(left.outbox === 0 && left.deliveries === 0 && left.inbox === 0, `cleanup: this run's outbox rows, deliveries and emails are gone (${JSON.stringify(left)}); consumer groups ${groupsLeft.length === 0 ? 'deleted' : `left to expire: ${groupsLeft.join(', ')}`}`);
    await Promise.all([authApp, relayDb, notifyApp, notifyOwner].map((c) => c.end().catch(() => {})));
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a reset email is delivered once, with a link that works, and nothing refused is ever sent');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
