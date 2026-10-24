/**
 * Asserts that an invitation reaches the person invited: once, sealed on the way, with
 * a link that registers them, and that nothing Notification should refuse is sent.
 *
 * THE BUG THIS GUARDS. POST /invites handed its link to the dev sink and nothing else,
 * a leftover from before Notification existed. The sink is off unless configured and
 * refuses to boot in production, so in a deployed system an invitation reached nobody
 * and nobody new could join a tenant. Measured against the compose stack before the
 * fix: POST /invites answered 201, Auth's outbox gained no row, Mailpit received
 * nothing; a password reset in the same tenant, as the control, queued one row.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - One email, whose link registers the invitee, who can then sign in. Read out of
 *     the inbox: a dev-sink token would pass with a broken link in the email.
 *   - The token, the link and the address are nowhere in clear in Auth's outbox row or
 *     in the bytes on the topic. Each search is shown to find what IS there -- the
 *     invite's id -- so an empty search cannot pass for a clean one.
 *   - The seal expires with the invitation, at an instant the DATABASE chose, and Auth
 *     judges the link on the database's clock too. Auth runs an hour fast here, with
 *     one-hour invitations: on a machine whose clocks agree, an expiry stamped by the
 *     process lands on the same millisecond and passes for the database's. Measured
 *     while building this: judged on Auth's clock, the link in a fresh email was refused
 *     as expired, and registration below is what would fail.
 *   - A re-invite: the newest link registers, the older is refused. Before the re-invite
 *     the older one is shown live, so its refusal is the re-invite's doing.
 *   - Two invitations to one address at once leave one live link. The interleaving is
 *     forced -- both requests are held on a table lock until both are waiting -- rather
 *     than hoped for.
 *   - Revoking after the email went out stops the emailed link; it was live before.
 *   - An invitation to a suppressed address is recorded SUPPRESSED and never attempted,
 *     and the counter that says it was judged moves.
 *   - A seal Auth did not sign, a reset's seal relabelled as an invitation, a role
 *     outside the closed set and an address carrying a line break each send nothing
 *     and reach no sent-log row. A seal this run makes the same way, correctly, IS
 *     delivered, so the four refusals are their defects and not a broken fixture.
 *   - An expired invitation, by its envelope or by its signed exp, sends nothing.
 *   - The budget refuses past its limit and not below it, per MAILBOX: the address in
 *     capitals, with a +tag, and at Gmail with dots, all spend one budget, while dots
 *     outside Gmail name different mailboxes and are not merged. Per tenant: a refusal
 *     in one tenant leaves another's admin free to invite the same address, so a 429
 *     says nothing about anyone else's invitations.
 *   - Inviting an address in one tenant leaves another tenant's live invitation to it
 *     alone. Invite has no row security, so the tenant in the supersede is all there is.
 *   - Only an ADMIN may invite: an attendee's token is refused, and writes nothing.
 *   - An address longer than SMTP can carry (255 characters) is refused; 254 is not.
 *   - Auth refuses to start with invitations longer than a week, and the topic keeps a
 *     request longer than that, set on the topic rather than inherited: an outage of
 *     the dispatcher cannot outlive an invitation's seal and lose it without a trace.
 *   - A dead-lettered invitation replayed after the sweeper ended its claim is sent,
 *     once. The first attempt is staged as the dispatcher leaves it -- a claim PENDING
 *     for two hours, a dead-letter copy written the way forward() writes one -- because
 *     Mailpit cannot be made to fail; the sweep and the replay are the documented
 *     commands, run through pnpm. A second replay sends nothing.
 *   - No token, link or address in Notification's output, in Auth's outside the dev
 *     sink's own lines, or in the dead-letter copies of the refused requests.
 *
 * READING EVERY PARTITION. Each invitation is keyed by its own invite id, so its
 * message can land on any partition. Notification's dispatch group is committed at the
 * end of the topic before it starts, and an expired canary is sent to EVERY partition
 * and counted before anything real is sent.
 *
 * Scoped to three tenants, consumer groups and addresses made for this run. The Auth
 * rows, the inbox messages, the groups and the contact cards registration published are
 * removed at the end. The settled sent-log and suppression rows are KEPT: the run's
 * sealed requests stay deliverable on the topic for an hour, and a Notification started
 * in that hour must find them already sent, suppressed or refused, not mail them again.
 *
 * Run (with Postgres, Kafka, the Schema Registry and Mailpit up, the schemas registered;
 * Auth, the relay and Notification built; both keypairs generated; compose's
 * Notification stopped): node scripts/verify-invitation-email.mjs
 */

import { spawn } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import kafkajs, { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { CompactEncrypt, SignJWT, compactDecrypt, importPKCS8, importSPKI, jwtVerify, calculateJwkThumbprint, exportJWK } from 'jose';
import { serviceEnv } from './lib/service-env.mjs';
import { commitAtEnd, deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';
import { contactsFromTheEnd, notificationGroups } from './lib/notification-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');
// The service's own hash, imported rather than rewritten: a copy here would only agree with itself.
const { addressHash } = await import(pathToFileURL(join(ROOT, 'services', 'notification', 'dist', 'dispatch', 'suppression.js')).href);
// The headers forward() writes on a dead-letter copy, and the reason the sweeper writes,
// read from the service for the same reason.
const { DLQ_HEADERS } = await import(pathToFileURL(join(ROOT, 'services', 'notification', 'dist', 'dispatch', 'retry.js')).href);
const { ABANDONED } = await import(pathToFileURL(join(ROOT, 'services', 'notification', 'dist', 'sweeper', 'sweep.js')).href);

// Not a named export of kafkajs's CommonJS build; verify-topic-configs reads it the same way.
const { ConfigResourceTypes } = kafkajs;

const TOPIC = 'auth.email-requested';
const DLQ = 'notification.dlq';
const CONTACT_TOPIC = 'auth.user-contact';
const AUDIENCE = 'eventhub-notification';
const TYPE = 'auth-email+jwt';
const DECRYPT_OPTIONS = { keyManagementAlgorithms: ['RSA-OAEP-256'], contentEncryptionAlgorithms: ['A256GCM'] };
const MAILPIT = process.env['MAILPIT_URL'] ?? 'http://127.0.0.1:8025';
const SUBJECT = 'You have been invited to EventHub';
/**
 * An hour, not the default week. Every request this run leaves on the topic can be
 * delivered by any Notification until it expires, and the run's tenants are gone at the
 * end, so its leftovers should die soon. Still an hour apart from Auth's clock skew, so
 * an expiry from the wrong clock reads two hours instead of one.
 */
const TTL_HOURS = 1;

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_invitemail_${RUN}`;
const BUDGET_TENANT = `tenant_invitebudget_${RUN}`;
const OTHER_TENANT = `tenant_inviteother_${RUN}`;
const TENANTS = [TENANT, BUDGET_TENANT, OTHER_TENANT];
const slugOf = (tenant) => tenant.replace(/_/g, '-');
const ADMIN_ID = `user_inviteadmin_${RUN}`;
const at = (name) => `${name}-${RUN}@mailcheck.example`;
/** A Gmail local part, so dots can be shown to count for nothing there. Only ever delivered to Mailpit. */
const G = `budgetg${RUN}`;
/** A domain made of 63-character labels, for an address exactly as long as asked. */
const longAddress = (length) => {
  const local = `long-${RUN}`;
  const tail = length - local.length - 1 - 3 * 64 - '.example'.length;
  return `${local}@${'a'.repeat(63)}.${'b'.repeat(63)}.${'c'.repeat(63)}.${'d'.repeat(tail)}.example`;
};
const A = {
  invitee: at('invitee'),
  reinvitee: at('reinvitee'),
  race: at('race'),
  revokee: at('revokee'),
  suppressed: at('suppressed'),
  replay: at('replay'),
  victim: at('victim'),
  bcc: at('bcc'),
  control: at('sealcontrol'),
  expired: at('expired'),
  notAdmin: at('notadmin'),
  x: at('budget-x'),
  xTagged: `budget-x-${RUN}+tag@mailcheck.example`,
  gmail: [`${G}@gmail.com`, `${G.slice(0, 3)}.${G.slice(3)}+promo@googlemail.com`, `${G[0]}.${G.slice(1)}@gmail.com`],
  dotted: [`budget.dot-${RUN}@mailcheck.example`, `budgetdot-${RUN}@mailcheck.example`, `bud.get.dot-${RUN}@mailcheck.example`],
  w: at('budget-w'),
  fill: [1, 2, 3].map((i) => at(`budget-fill${i}`)),
  long254: longAddress(254),
  long255: longAddress(255),
};
const ALL_ADDRESSES = [...Object.values(A).flat()];
const PASSWORD = `invited-password-${RUN}`;

/**
 * Small budgets, so the run can reach them. The main tenant makes eight invitations; the
 * budget tenant makes exactly ten and is then refused.
 */
const PER_TENANT = 10;
const PER_ADDRESS = 2;

const GROUP = `verify-invitation-email-${RUN}`;
const GROUPS = notificationGroups(GROUP);
const SCAN_TOPIC_GROUP = `${GROUP}-scan-topic`;
const SCAN_DLQ_GROUP = `${GROUP}-scan-dlq`;

const NOTIFICATION_PORT = 3391;
const RELAY_METRICS_PORT = 3392;
const AUTH_PORT = 3393;

const authEnv = serviceEnv(ROOT, 'auth', {
  PORT: String(AUTH_PORT),
  NODE_ENV: 'development',
  // How this run learns each real token, to search for it and to compare links with.
  AUTH_DEV_TOKEN_SINK: '1',
  LOG_LEVEL: 'debug',
  INVITES_PER_TENANT_PER_HOUR: String(PER_TENANT),
  INVITES_PER_ADDRESS_PER_DAY: String(PER_ADDRESS),
  INVITE_TTL_HOURS: String(TTL_HOURS),
  // An hour fast, so an expiry taken from Auth's clock cannot pass for the database's.
  NODE_OPTIONS: `--import=${pathToFileURL(join(ROOT, 'scripts', 'lib', 'skewed-clock.mjs')).href}`,
});
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['AUTH_RELAY_DATABASE_URL'] ?? 'postgresql://eventhub_auth_relay:eventhub_auth_relay@localhost:5432/eventhub_auth',
  RELAY_FORBIDDEN_TABLE: 'User',
  RELAY_METRICS_PORT: String(RELAY_METRICS_PORT),
});
const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(NOTIFICATION_PORT),
  ...GROUPS.env,
  DISPATCH_START_FROM: 'latest',
  AUTH_PUBLIC_KEY_B64: Buffer.from(key('auth', 'public.pem')).toString('base64'),
});
const APP_ROLE = new URL(authEnv.APP_DATABASE_URL).username;

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

const authCall = (path, body, headers = {}) =>
  fetch(`http://127.0.0.1:${AUTH_PORT}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  }).then(async (r) => ({ status: r.status, retryAfter: r.headers.get('retry-after'), text: await r.text() }));
const json = (text) => {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
};

const dispatch = async () => (await (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/metrics`)).json()).dispatch;

/** The dev sink's invite links, oldest first, for this run's tenants. */
function sinkInvites(log) {
  const found = [];
  for (const line of log.split(/\r?\n/)) {
    const entry = json(line);
    if (entry?.kind === 'invite' && entry.devOnlyResetUrl && TENANTS.includes(entry.tenantId)) found.push(entry);
  }
  return found;
}

/** The invitation link on its own line in an email's text, whole, compared with === and never includes(). */
const inviteLinkIn = (text) => text?.match(/^(https?:\/\/\S*accept-invite\?token=\S+?)\r?$/m)?.[1] ?? null;
const tokenOf = (link) => (link ? (new URL(link).searchParams.get('token') ?? '') : '');

function hrefIn(html) {
  const raw = html?.match(/href="([^"]*)"/)?.[1];
  if (raw === undefined) return null;
  return raw.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
}

async function messageById(id) {
  const res = await fetch(`${MAILPIT}/api/v1/message/${encodeURIComponent(id)}`);
  return res.ok ? res.json() : null;
}
async function headersById(id) {
  const res = await fetch(`${MAILPIT}/api/v1/message/${encodeURIComponent(id)}/headers`);
  return res.ok ? res.json() : null;
}
/**
 * The messages to exactly this address. Mailpit's to: search can match a part of an
 * address, and this run's addresses contain each other (invitee-, reinvitee-), so the
 * results are filtered on the whole recipient.
 */
async function inbox(address) {
  const res = await fetch(`${MAILPIT}/api/v1/search?query=${encodeURIComponent(`to:${address}`)}`);
  if (!res.ok) throw new Error(`Mailpit search answered ${res.status}`);
  const wanted = address.trim().toLowerCase();
  return ((await res.json()).messages ?? []).filter((m) => (m.To ?? []).some((t) => (t.Address ?? '').toLowerCase() === wanted));
}
/** Waits for `want` emails to an address, then gives one more the chance to arrive. Newest first. */
async function mailTo(address, want) {
  const arrived = await waitFor(async () => {
    const m = await inbox(address);
    return m.length >= want ? m : null;
  }, 30_000, 500);
  const extra = arrived ? await waitFor(async () => (await inbox(address)).length > want, 3_000, 500) : null;
  const messages = [...(arrived ?? [])].sort((a, b) => Date.parse(b.Created) - Date.parse(a.Created));
  return { count: extra ? want + 1 : messages.length, messages };
}
const header = (headers, name) => {
  const k = Object.keys(headers ?? {}).find((h) => h.toLowerCase() === name.toLowerCase());
  return k ? headers[k] : undefined;
};

async function main() {
  for (const port of [NOTIFICATION_PORT, AUTH_PORT, RELAY_METRICS_PORT]) {
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

  const kafka = new Kafka({ clientId: `verify-invitation-email-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  // Another Notification on the real groups shares the sent-log and would deliver this
  // run's messages first, from code this run never started.
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

  const authApp = new pg.Client({ connectionString: authEnv.APP_DATABASE_URL });
  const authOwner = new pg.Client({ connectionString: authEnv.DATABASE_URL });
  const watcher = new pg.Client({ connectionString: authEnv.DATABASE_URL });
  const relayDb = new pg.Client({ connectionString: relayEnv.RELAY_DATABASE_URL });
  const notifyApp = new pg.Client({ connectionString: notificationEnv.APP_DATABASE_URL });
  const notifyOwner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  const clients = [authApp, authOwner, watcher, relayDb, notifyApp, notifyOwner];
  await Promise.all(clients.map((c) => c.connect()));
  const producer = kafka.producer();
  await producer.connect();
  const registry = new SchemaRegistry({ host: relayEnv.SCHEMA_REGISTRY_URL });

  const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
  const notificationPrivate = await importPKCS8(key('notification', 'private.pem'), 'RSA-OAEP-256');
  const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
  const authSigning = await importPKCS8(key('auth', 'private.pem'), 'RS256');
  const authPublic = await importSPKI(key('auth', 'public.pem'), 'RS256');
  // Anyone can encrypt to Notification's public key; this is a signer that is not Auth.
  const impostorSigning = await importPKCS8(key('notification', 'private.pem'), 'RS256');

  /** An access token for this run's admin of a tenant, signed the way Auth signs one. Long, because Auth's clock is an hour fast. */
  const adminToken = (tenant) =>
    new SignJWT({ tenantId: tenant, tenantSlug: slugOf(tenant), role: 'ADMIN' })
      .setProtectedHeader({ alg: 'RS256' })
      .setSubject(ADMIN_ID)
      .setIssuer(authEnv.JWT_ISSUER)
      .setAudience(authEnv.JWT_AUDIENCE)
      .setIssuedAt()
      .setExpirationTime(Math.floor(Date.now() / 1000) + 3 * 3600)
      .sign(authSigning);
  const tokens = {};
  for (const t of TENANTS) tokens[t] = await adminToken(t);
  const asAdmin = (tenant) => ({ Authorization: `Bearer ${tokens[tenant]}`, 'X-Tenant-Slug': slugOf(tenant) });
  // The same token with the role an invitation most often grants, for the check that it cannot invite.
  const attendeeToken = await new SignJWT({ tenantId: TENANT, tenantSlug: slugOf(TENANT), role: 'ATTENDEE' })
    .setProtectedHeader({ alg: 'RS256' })
    .setSubject(`user_inviteattendee_${RUN}`)
    .setIssuer(authEnv.JWT_ISSUER)
    .setAudience(authEnv.JWT_AUDIENCE)
    .setIssuedAt()
    .setExpirationTime(Math.floor(Date.now() / 1000) + 3 * 3600)
    .sign(authSigning);

  /** A sealed request made the way Auth makes one, with whatever each case needs wrong. */
  async function seal({ messageId, subject, tenant = TENANT, claims, signingKey = authSigning, exp }) {
    const signed = await new SignJWT({ tenantId: tenant, ...claims })
      .setProtectedHeader({ alg: 'RS256', typ: TYPE })
      .setSubject(subject)
      .setJti(messageId)
      .setIssuer(authEnv.JWT_ISSUER)
      .setAudience(AUDIENCE)
      .setIssuedAt()
      .setExpirationTime(exp)
      .sign(signingKey);
    return new CompactEncrypt(new TextEncoder().encode(signed))
      .setProtectedHeader({ alg: 'RSA-OAEP-256', enc: 'A256GCM', cty: 'JWT', kid })
      .encrypt(notificationPublic);
  }
  async function produce({ messageId, subject, kind = 'INVITATION', expiresAt, sealed, tenant = TENANT, partition }) {
    const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);
    const value = { messageId, occurredAt: new Date().toISOString(), tenantId: tenant, userId: subject, kind, expiresAt, sealed };
    await producer.send({
      topic: TOPIC,
      messages: [{ key: subject, ...(partition === undefined ? {} : { partition }), value: await registry.encode(schemaId, value), headers: { 'message-id': messageId, 'tenant-id': tenant } }],
    });
  }

  const outboxRow = async (aggregateId) =>
    (
      await relayDb.query(
        `SELECT o.id, o.topic, o."eventType", o."aggregateId", o.payload, o.payload::text AS text, o."publishedAt"
           FROM "OutboxMessage" o WHERE o."aggregateId" = $1 ORDER BY o.seq`,
        [aggregateId],
      )
    ).rows;
  const inviteRows = async (tenant, address) =>
    (
      await authApp.query(
        `SELECT i.id, i.role::text AS role, i."revokedAt" IS NOT NULL AS revoked, i."acceptedAt" IS NOT NULL AS accepted,
                i."expiresAt" > (now() AT TIME ZONE 'UTC') AS unexpired
           FROM "Invite" i WHERE i."tenantId" = $1 AND i.email = $2 ORDER BY i."createdAt"`,
        [tenant, address],
      )
    ).rows;
  const liveInvites = async (tenant, address) => (await inviteRows(tenant, address)).filter((r) => !r.revoked && !r.accepted);
  const deliveriesOf = async (tenant, messageIds) =>
    (
      await inTenant(
        notifyApp,
        tenant,
        `SELECT d."sourceMessageId", d."recipientId", d.category, d."templateKey", d."idempotencyKey", d.status::text AS status, d.attempts, d."providerMessageId"
           FROM "NotificationDelivery" d WHERE d."tenantId" = $1 AND d."sourceMessageId" = ANY($2::text[])`,
        [tenant, messageIds],
      )
    ).rows;

  /**
   * POST /invites, and everything that tells this run which invitation it made: the
   * dev sink's link, the Invite row its token hashes to, and the outbox row keyed by it.
   */
  async function invite(tenant, address, role = 'ATTENDEE') {
    const before = sinkInvites(auth.log()).length;
    const res = await authCall('/invites', { email: address, role }, asAdmin(tenant));
    if (res.status !== 201) return { status: res.status, retryAfter: res.retryAfter, body: json(res.text) };
    // Matched by address and tenant as well as position: a line from a request this run
    // made directly can reach the captured output after this one began.
    const normalised = address.trim().toLowerCase();
    const entry = await waitFor(async () => sinkInvites(auth.log()).slice(before).find((e) => e.email === normalised && e.tenantId === tenant), 5_000, 50);
    const link = entry?.devOnlyResetUrl ?? null;
    const token = tokenOf(link);
    const inviteRow = token ? (await authApp.query(`SELECT i.id FROM "Invite" i WHERE i."tokenHash" = $1`, [sha256(token)])).rows[0] : undefined;
    const rows = inviteRow ? await outboxRow(inviteRow.id) : [];
    if (link) secrets.push(link, token);
    return { status: res.status, body: json(res.text), link, token, inviteId: inviteRow?.id ?? null, rows, row: rows[0] ?? null, sinkMessage: entry?.msg ?? '' };
  }

  /**
   * POST /register with an emailed link. Every user it creates is remembered, whether or
   * not this run expected one, so the cleanup withdraws that user's contact card from
   * the compacted topic even on a run where a refusal failed.
   */
  async function register(link) {
    const res = await authCall('/register', { token: tokenOf(link), password: PASSWORD });
    const user = json(res.text);
    if (res.status === 201 && user?.id) {
      const [row] = (await authApp.query('SELECT "tenantId" FROM "Invite" WHERE "tokenHash" = $1', [sha256(tokenOf(link))])).rows;
      registered.push({ tenant: row?.tenantId ?? TENANT, id: user.id });
    }
    return res;
  }

  /** Relays, then waits until each of these outbox rows is published: the effect, whoever's pass it was. */
  async function published(rowIds) {
    const relay = await relayOnce();
    const ok = await waitFor(async () => {
      const { rows } = await relayDb.query(`SELECT count(*)::int AS n FROM "OutboxMessage" o WHERE o.id = ANY($1::text[]) AND o."publishedAt" IS NOT NULL`, [rowIds]);
      return rows[0].n === rowIds.length;
    }, 30_000, 250);
    return { ok: Boolean(ok), code: relay.code, log: relay.log };
  }

  /** Every message carrying one of these message ids, on a topic, read by a group committed at its end when the run began. */
  async function scan(topic, groupId, ids, timeoutMs = 30_000) {
    const want = new Set(ids);
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });
    const found = new Map();
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ message }) => {
        const id = message.headers?.['message-id']?.toString();
        if (id && want.has(id)) found.set(id, message);
      },
    });
    await waitFor(async () => found.size >= want.size, timeoutMs, 250);
    await consumer.disconnect();
    return found;
  }

  const secrets = [];
  const registered = [];
  let notification = { child: { kill: () => {}, exitCode: null, signalCode: null }, log: () => '' };
  const auth = start('auth', ['dist/index.js'], authEnv);
  let holder = null;

  try {
    console.log('  the fixture');
    for (const tenant of TENANTS) {
      await authApp.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [tenant, `Invitations ${tenant}`, slugOf(tenant)]);
    }
    // The scans read only what this run writes.
    await commitAtEnd(admin, SCAN_TOPIC_GROUP, [TOPIC]);
    await commitAtEnd(admin, SCAN_DLQ_GROUP, [DLQ]);

    const authUp = await waitFor(async () => (await fetch(`http://127.0.0.1:${AUTH_PORT}/health`)).ok, 60_000, 500);
    check(Boolean(authUp), authUp ? `CONTROL: a private Auth with the dev sink on is listening on :${AUTH_PORT}` : `Auth never came up:\n${excerpt(auth.log())}`);
    check(auth.log().includes('clock skewed by +3600s'), "CONTROL: Auth's clock is an hour fast, so an expiry stamped by it cannot pass for the database's");
    if (failures > 0) return;

    // The invitation the replay section dead-letters. Published BEFORE the dispatch group
    // is committed at the topic's end, so this run's Notification never reads it: the
    // only first attempt it gets is the one staged below.
    const staged = await invite(TENANT, A.replay);
    const stagedPub = staged.row ? await published([staged.row.id]) : { ok: false };
    check(staged.status === 201 && stagedPub.ok, `CONTROL: an invitation to dead-letter later is issued and published before Notification reads the topic (${staged.status})`);
    if (failures > 0) return;

    // Before Notification starts: its dispatch group begins exactly at the topic's end.
    await commitAtEnd(admin, GROUP, [TOPIC]);
    const contacts = await contactsFromTheEnd(admin, ROOT, GROUPS);
    notification = start('notification', ['dist/index.js'], notificationEnv);
    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${NOTIFICATION_PORT} (database, consumer and SMTP)` : `Notification never became ready:\n${excerpt(notification.log())}`);
    if (failures > 0) return;

    const joined = await waitFor(async () => {
      const { groups } = await admin.describeGroups([GROUP]);
      return groups[0]?.state === 'Stable' && groups[0].members.length > 0;
    }, 60_000, 500);
    check(Boolean(joined), 'CONTROL: its dispatch group has joined');
    const own = await contacts.joined();
    check(own.ok, own.message);
    const partitions = (await admin.fetchTopicMetadata({ topics: [TOPIC] })).topics[0].partitions.map((p) => p.partitionId);
    const base = await dispatch();
    for (const partition of partitions) {
      const id = `canary-${randomUUID()}`;
      await produce({ messageId: id, subject: id, expiresAt: new Date(Date.now() - 60_000).toISOString(), sealed: 'canary', partition });
    }
    const reading = await waitFor(async () => (await dispatch()).expiredSkippedTotal >= base.expiredSkippedTotal + partitions.length, 45_000, 500);
    check(Boolean(reading), `CONTROL: it counted an expired INVITATION canary on each of the ${partitions.length} partitions, so it reads every one an invitation can land on`);
    if (failures > 0) return;

    // ---------------------------------------------------------------------------------
    console.log('');
    console.log('  an invitation is queued sealed, with the invitation, holding only ciphertext and identifiers');
    const first = await invite(TENANT, A.invitee);
    check(first.status === 201, `POST /invites answered 201 (${first.status})`);
    check((first.token ?? '').length >= 40, `CONTROL: the dev sink gave this run the real token to search for (${(first.token ?? '').length} characters)`);
    check(Boolean(first.inviteId), `CONTROL: the token hashes to one Invite row (${first.inviteId})`);
    check(first.rows.length === 1, `exactly one outbox row is keyed by that invitation (${first.rows.length})`);
    if (failures > 0) return;
    const row = first.row;
    check(row.topic === TOPIC && row.eventType === 'InvitationRequested', `an InvitationRequested for ${row.topic} (${row.eventType})`);
    const payloadKeys = Object.keys(row.payload).sort().join(', ');
    check(payloadKeys === 'expiresAt, kind, sealed, tenantId, userId', `its payload holds only ${payloadKeys}`);
    check(row.payload.kind === 'INVITATION' && row.payload.userId === first.inviteId && row.payload.tenantId === TENANT, `kind ${row.payload.kind}, for invite ${row.payload.userId}, in the tenant`);
    const storedLeaks = [['the token', first.token], ['the link', first.link], ['the address', A.invitee]].filter(([, s]) => row.text.includes(s));
    check(row.text.includes(first.inviteId), 'CONTROL: a search of the stored payload finds what is there in clear, the invite id');
    check(storedLeaks.length === 0, storedLeaks.length === 0 ? 'and finds neither the token, the link nor the address' : `and finds ${storedLeaks.map(([n]) => n).join(', ')}`);

    const opened = await compactDecrypt(row.payload.sealed, notificationPrivate, DECRYPT_OPTIONS).then(
      (r) => new TextDecoder().decode(r.plaintext),
      (err) => ({ error: err.code ?? err.message }),
    );
    check(typeof opened === 'string', typeof opened === 'string' ? "Notification's private key opens the seal" : `Notification's key cannot open it (${opened.error})`);
    if (typeof opened !== 'string') return;
    const verified = await jwtVerify(opened, authPublic, { algorithms: ['RS256'], issuer: authEnv.JWT_ISSUER, audience: AUDIENCE, typ: TYPE }).then(
      (v) => v.payload,
      (err) => ({ error: err.code ?? err.message }),
    );
    check(!verified.error, verified.error ? `Auth's signature did not verify (${verified.error})` : "the JWS inside verifies against Auth's key, as an auth-email+jwt for Notification");
    if (verified.error) return;
    check(verified.sub === first.inviteId && verified.kind === 'INVITATION' && verified.tenantId === TENANT, `its subject is the invitation, not a user (${verified.sub}, ${verified.kind})`);
    check(verified.email === A.invitee && verified.link === first.link && verified.role === 'ATTENDEE', 'it carries the address, exactly the link Auth issued, and the role, signed');
    check(verified.jti === row.id, `bound to its own message by jti (${verified.jti})`);

    console.log('');
    console.log('  the seal expires with the invitation, at an instant the database chose');
    const [inviteClock] = (
      await authApp.query(
        `SELECT to_char(i."expiresAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "expiresAt",
                extract(epoch FROM (i."expiresAt" - (now() AT TIME ZONE 'UTC')))::float8 AS "secondsLeft"
           FROM "Invite" i WHERE i.id = $1`,
        [first.inviteId],
      )
    ).rows;
    check(inviteClock?.expiresAt === row.payload.expiresAt, `the invitation's expiresAt, rendered by Postgres, is the envelope's (${inviteClock?.expiresAt} / ${row.payload.expiresAt})`);
    check(
      verified.exp * 1000 <= Date.parse(row.payload.expiresAt) && verified.exp * 1000 > Date.parse(row.payload.expiresAt) - 1000,
      `the signed exp is that instant to the second, rounded down, never after it (${new Date(verified.exp * 1000).toISOString()})`,
    );
    const lead = inviteClock ? inviteClock.secondsLeft - TTL_HOURS * 3600 : NaN;
    check(
      Math.abs(lead) < 120,
      `the configured ${TTL_HOURS} h from the DATABASE's now, not from Auth's clock an hour ahead (${Number.isFinite(lead) ? `${lead.toFixed(0)} s off` : 'no row'})`,
    );

    console.log('');
    console.log('  the relay publishes it, and the bytes on the topic hold nothing to steal');
    const pub1 = await published([row.id]);
    check(pub1.ok, `CONTROL: the row is published (relay --once exited ${pub1.code})`);
    if (!pub1.ok) {
      console.error(excerpt(pub1.log));
      return;
    }
    const onTopic = (await scan(TOPIC, SCAN_TOPIC_GROUP, [row.id])).get(row.id);
    check(Boolean(onTopic), `CONTROL: the message is on ${TOPIC}, found by its message-id header`);
    if (!onTopic) return;
    check(onTopic.key?.toString() === first.inviteId, `its Kafka key is the invitation (${onTopic.key?.toString()})`);
    const headerText = Object.entries(onTopic.headers ?? {}).map(([k, v]) => `${k}=${v?.toString()}`).join(' ');
    check(onTopic.value.includes(Buffer.from(first.inviteId)), 'CONTROL: a search of the bytes finds what is there in clear, the invite id');
    const wireLeaks = [['the token', first.token], ['the link', first.link], ['the address', A.invitee]].filter(([, s]) => onTopic.value.includes(Buffer.from(s)) || headerText.includes(s));
    check(wireLeaks.length === 0, wireLeaks.length === 0 ? 'and finds neither the token, the link nor the address, in the value or the headers' : `and finds ${wireLeaks.map(([n]) => n).join(', ')}`);
    const decoded = await registry.decode(onTopic.value);
    check(decoded.kind === 'INVITATION' && decoded.sealed === row.payload.sealed, 'it decodes against the registered schema, kind INVITATION, with the same seal');

    console.log('');
    console.log('  Notification sends it once, with the link Auth issued and nothing the invitee did not need');
    const mail1 = await mailTo(A.invitee, 1);
    check(mail1.count === 1, `exactly one email reached the invited address (${mail1.count})`);
    if (mail1.count === 0) {
      console.error(excerpt(notification.log()));
      return;
    }
    const email1 = await messageById(mail1.messages[0].ID);
    const headers1 = await headersById(mail1.messages[0].ID);
    check(email1?.Subject === SUBJECT, `its subject is "${email1?.Subject}"`);
    const textLink = inviteLinkIn(email1?.Text);
    check(textLink === first.link, `its text carries exactly the link Auth issued${textLink === first.link ? '' : textLink ? ' (it carries a different one)' : ' (it carries none)'}`);
    check(hrefIn(email1?.HTML) === first.link, 'and so does its HTML link');
    check(/as an attendee\./.test(email1?.Text ?? ''), 'it names the role it grants');
    // Two invitations to one address can arrive in either order: each is keyed by its own id.
    check(/replaces any earlier invitation/.test(email1?.Text ?? '') && /replaces any earlier invitation/.test(email1?.HTML ?? ''), 'it says it replaces any earlier invitation, which may arrive after it');
    check(
      /also emailed/.test(first.sinkMessage) && !/would be emailed/.test(first.sinkMessage),
      `the dev sink's own line says the link is also emailed, not that it would be ("${first.sinkMessage}")`,
    );
    const told = [['the tenant id', TENANT], ['the tenant slug', slugOf(TENANT)], ['the inviting admin', ADMIN_ID], ['the invite id', first.inviteId]].filter(([, s]) => `${email1?.Text}\n${email1?.HTML}`.includes(s));
    check(told.length === 0, told.length === 0 ? 'and nothing the invitee did not need: no tenant, no inviter, no internal id' : `it also tells the reader ${told.map(([n]) => n).join(', ')}`);
    check(header(headers1, 'X-EventHub-Message-Id')?.[0] === row.id, 'CONTROL: its headers were read: X-EventHub-Message-Id is the outbox message id');
    check(!header(headers1, 'List-Unsubscribe') && !header(headers1, 'List-Unsubscribe-Post'), 'it carries no List-Unsubscribe: an invitation is not a subscription');
    const [delivery1] = await deliveriesOf(TENANT, [row.id]);
    check(
      delivery1?.status === 'SENT' && delivery1.idempotencyKey === `invitation:${row.id}:email:${first.inviteId}` && delivery1.category === 'invitation' && delivery1.recipientId === first.inviteId,
      `the sent-log records it SENT under ${delivery1?.idempotencyKey}, category ${delivery1?.category}, for the invitation`,
    );

    console.log('');
    console.log('  the link from the inbox registers the invitee, who can then sign in');
    const reg1 = await register(textLink);
    const user1 = json(reg1.text);
    check(reg1.status === 201 && user1?.email === A.invitee && user1?.role === 'ATTENDEE', `the emailed link registers ${user1?.email} as ${user1?.role} (${reg1.status})`);
    const login1 = await authCall('/login', { email: A.invitee, password: PASSWORD }, { 'X-Tenant-Slug': slugOf(TENANT) });
    check(login1.status === 200, `and the new user signs in (${login1.status})`);
    const again1 = await register(textLink);
    check(again1.status === 400, `the same link a second time is refused (${again1.status})`);

    // ---------------------------------------------------------------------------------
    console.log('');
    console.log('  a re-invite: the newest link works, and the older one does not');
    const older = await invite(TENANT, A.reinvitee, 'ATTENDEE');
    const olderLive = (await liveInvites(TENANT, A.reinvitee)).map((r) => r.id);
    check(older.status === 201 && olderLive.length === 1 && olderLive[0] === older.inviteId, `CONTROL: the first invitation is live before the second is sent (${olderLive.length} live)`);
    const newer = await invite(TENANT, A.reinvitee, 'ORGANIZER');
    check(newer.status === 201 && Boolean(newer.inviteId) && newer.inviteId !== older.inviteId, `the re-invite answered ${newer.status}, as a new invitation`);
    const pubRe = await published([older.row?.id, newer.row?.id].filter(Boolean));
    check(pubRe.ok, 'CONTROL: both requests are published');
    const mailRe = await mailTo(A.reinvitee, 2);
    check(mailRe.count === 2, `two emails reached the address, one per invitation (${mailRe.count})`);
    const reTexts = await Promise.all(mailRe.messages.map(async (m) => (await messageById(m.ID))?.Text ?? ''));
    const olderEmailed = reTexts.map(inviteLinkIn).find((l) => l === older.link) ?? null;
    const newerEmailed = reTexts.map(inviteLinkIn).find((l) => l === newer.link) ?? null;
    check(Boolean(olderEmailed && newerEmailed), 'CONTROL: each link Auth issued is in one of them');
    check(reTexts.some((t) => t.includes(newer.link) && /as an organiser\./.test(t)), "the newer email names the newer invitation's role");
    const afterRe = await inviteRows(TENANT, A.reinvitee);
    check(
      afterRe.length === 2 && afterRe.filter((r) => !r.revoked).map((r) => r.id).join() === newer.inviteId,
      `of the address's ${afterRe.length} invitations only the newest is live (${afterRe.filter((r) => !r.revoked).length} live)`,
    );
    const oldReg = await register(olderEmailed);
    check(oldReg.status === 400 && json(oldReg.text)?.error === 'invalid or expired invite', `the older emailed link is refused (${oldReg.status} ${json(oldReg.text)?.error ?? oldReg.text})`);
    const newReg = await register(newerEmailed);
    const user2 = json(newReg.text);
    check(newReg.status === 201 && user2?.role === 'ORGANIZER', `the newer emailed link registers, at the newer role (${newReg.status}, ${user2?.role})`);

    console.log('');
    console.log('  two invitations to one address at once leave one live link');
    holder = new pg.Client({ connectionString: authEnv.DATABASE_URL });
    await holder.connect();
    await holder.query('BEGIN');
    // Holds every writer to Invite at its first write, so both requests are in flight
    // at once rather than one finishing before the other starts.
    await holder.query('LOCK TABLE "Invite" IN SHARE MODE');
    const racing = [authCall('/invites', { email: A.race, role: 'ATTENDEE' }, asAdmin(TENANT)), authCall('/invites', { email: A.race, role: 'ATTENDEE' }, asAdmin(TENANT))];
    const bothWaiting = await waitFor(
      async () => (await watcher.query('SELECT count(*)::int AS n FROM pg_stat_activity WHERE usename = $1 AND cardinality(pg_blocking_pids(pid)) > 0', [APP_ROLE])).rows[0].n >= 2,
      15_000,
      100,
    );
    await holder.query('COMMIT');
    const raced = await Promise.all(racing);
    check(Boolean(bothWaiting), 'CONTROL: both requests were waiting at the same moment, so the race was made to happen');
    check(raced.every((r) => r.status === 201), `both answered 201 (${raced.map((r) => r.status).join(', ')})`);
    const raceRows = await inviteRows(TENANT, A.race);
    check(raceRows.length === 2 && raceRows.filter((r) => !r.revoked).length === 1, `${raceRows.length} invitations were made and ${raceRows.filter((r) => !r.revoked).length} is live`);

    console.log('');
    console.log('  revoking after the email went out stops the emailed link');
    const revokee = await invite(TENANT, A.revokee);
    const pubRv = await published([revokee.row?.id].filter(Boolean));
    const mailRv = await mailTo(A.revokee, 1);
    const revokeLink = inviteLinkIn((await messageById(mailRv.messages[0]?.ID))?.Text);
    check(pubRv.ok && mailRv.count === 1 && revokeLink === revokee.link, `CONTROL: the email went out with the link (${mailRv.count} email)`);
    const liveBefore = await liveInvites(TENANT, A.revokee);
    check(liveBefore.length === 1 && liveBefore[0].unexpired, 'CONTROL: and the invitation behind it is live');
    const revoked = await authCall('/invites/revoke', { email: `  ${A.revokee.toUpperCase()} ` }, asAdmin(TENANT));
    check(revoked.status === 200 && json(revoked.text)?.revoked === 1, `the admin revokes it, naming the address in capitals with spaces (${revoked.status}, revoked ${json(revoked.text)?.revoked})`);
    const revokedReg = await register(revokeLink);
    check(revokedReg.status === 400, `the emailed link is refused (${revokedReg.status})`);

    console.log('');
    console.log('  an invitation to a suppressed address is recorded SUPPRESSED, and not sent');
    await inTenant(
      notifyOwner,
      TENANT,
      `INSERT INTO "EmailSuppression" (id, "tenantId", "addressHash", reason, "smtpStatus", "expiresAt") VALUES ($1, $2, $3, 'HARD_BOUNCE', '5.1.1', now() + interval '1 day')`,
      [randomUUID(), TENANT, addressHash(A.suppressed)],
    );
    const beforeSuppressed = await dispatch();
    const suppressed = await invite(TENANT, A.suppressed);
    const pubSp = await published([suppressed.row?.id].filter(Boolean));
    check(pubSp.ok, 'CONTROL: its request is published');
    // Whatever it decided, it decided: a row that is no longer PENDING.
    const settledRow = await waitFor(async () => {
      const [d] = await deliveriesOf(TENANT, [suppressed.row?.id ?? '']);
      return d && d.status !== 'PENDING' ? d : null;
    }, 30_000);
    check(Boolean(settledRow), `CONTROL: Notification read it and settled its delivery (${settledRow?.status ?? 'no settled row'})`);
    check(
      settledRow?.status === 'SUPPRESSED' && settledRow.attempts === 0 && settledRow.templateKey === 'invitation',
      `recorded SUPPRESSED with no attempt (${settledRow?.status}, ${settledRow?.attempts} attempt(s))`,
    );
    check((await dispatch()).suppressedTotal === beforeSuppressed.suppressedTotal + 1, 'and counted suppressed');
    await sleep(2_000);
    check((await inbox(A.suppressed)).length === 0, 'and nothing reached the address');

    // ---------------------------------------------------------------------------------
    console.log('');
    console.log('  a seal Auth did not sign, a relabelled reset, a role outside the set and a line break in the address send nothing');
    // Minutes, not the hour Auth's own requests get: a forged request still unexpired when
    // the next verifier's Notification reads the topic would be dead-lettered again there.
    const soon = new Date(Date.now() + 3 * 60_000);
    const cases = {};
    const mk = async (name, { claims, signingKey, kind = 'INVITATION' }) => {
      const messageId = randomUUID();
      const subject = `inv_forged_${name}_${RUN}`;
      cases[name] = messageId;
      await produce({ messageId, subject, kind, expiresAt: soon.toISOString(), sealed: await seal({ messageId, subject, claims, signingKey, exp: soon }) });
    };
    const beforeForged = await dispatch();
    await mk('unsigned', { claims: { email: A.victim, link: 'https://evil.example/accept-invite?token=forged', kind: 'INVITATION', role: 'ADMIN' }, signingKey: impostorSigning });
    await mk('relabelled', { claims: { email: A.victim, link: `https://evil.example/reset-password?token=${RUN}`, kind: 'PASSWORD_RESET' } });
    await mk('role', { claims: { email: A.victim, link: `http://localhost:3000/accept-invite?token=role-${RUN}`, kind: 'INVITATION', role: 'OWNER' } });
    await mk('crlf', { claims: { email: `${A.victim}\r\nBcc: ${A.bcc}`, link: `http://localhost:3000/accept-invite?token=crlf-${RUN}`, kind: 'INVITATION', role: 'ATTENDEE' } });
    // The control: made by the same helper, correctly. It must arrive.
    const controlLink = `http://localhost:3000/accept-invite?token=control-${RUN}`;
    await mk('control', { claims: { email: A.control, link: controlLink, kind: 'INVITATION', role: 'ATTENDEE' } });
    const refusedAll = await waitFor(async () => {
      const m = await dispatch();
      return m.poisonSkippedTotal >= beforeForged.poisonSkippedTotal + 4 && m.sentTotal >= beforeForged.sentTotal + 1 ? m : null;
    }, 30_000);
    check(refusedAll?.poisonSkippedTotal === beforeForged.poisonSkippedTotal + 4, `all four were refused as poison (${(refusedAll ?? (await dispatch())).poisonSkippedTotal - beforeForged.poisonSkippedTotal})`);
    const controlMail = await mailTo(A.control, 1);
    check(controlMail.count === 1 && inviteLinkIn((await messageById(controlMail.messages[0]?.ID))?.Text) === controlLink, `CONTROL: the seal made the same way, correctly, was delivered (${controlMail.count})`);
    const forgedIds = ['unsigned', 'relabelled', 'role', 'crlf'].map((n) => cases[n]);
    check((await deliveriesOf(TENANT, forgedIds)).length === 0, 'none of the four reached the sent-log');
    check((await inbox(A.victim)).length === 0 && (await inbox(A.bcc)).length === 0, 'nothing reached the address they named, nor the one smuggled after a line break');

    console.log('');
    console.log('  an expired invitation sends nothing');
    const beforeExpired = await dispatch();
    const past = new Date(Date.now() - 60_000);
    const stale = randomUUID();
    await produce({ messageId: stale, subject: `inv_stale_${RUN}`, expiresAt: past.toISOString(), sealed: await seal({ messageId: stale, subject: `inv_stale_${RUN}`, claims: { email: A.expired, link: `http://localhost:3000/accept-invite?token=stale-${RUN}`, kind: 'INVITATION', role: 'ATTENDEE' }, exp: past }) });
    const late = randomUUID();
    // The envelope says it is still valid; the signed exp, which is what counts, says not.
    await produce({ messageId: late, subject: `inv_late_${RUN}`, expiresAt: soon.toISOString(), sealed: await seal({ messageId: late, subject: `inv_late_${RUN}`, claims: { email: A.expired, link: `http://localhost:3000/accept-invite?token=late-${RUN}`, kind: 'INVITATION', role: 'ATTENDEE' }, exp: past }) });
    const skipped = await waitFor(async () => {
      const m = await dispatch();
      return m.expiredSkippedTotal >= beforeExpired.expiredSkippedTotal + 2 ? m : null;
    }, 30_000);
    check(skipped?.expiredSkippedTotal === beforeExpired.expiredSkippedTotal + 2, 'an expired envelope, and a current envelope around an expired seal, were both skipped as expired');
    await sleep(2_000);
    check((await deliveriesOf(TENANT, [stale, late])).length === 0 && (await inbox(A.expired)).length === 0, 'with no sent-log row and no email');

    // ---------------------------------------------------------------------------------
    console.log('');
    console.log('  a dead-lettered invitation, replayed after the sweeper ended its claim, is sent once');
    const stagedId = staged.row.id;
    const original = (await scan(TOPIC, SCAN_TOPIC_GROUP, [stagedId])).get(stagedId);
    check(Boolean(original), 'CONTROL: the invitation issued before Notification read the topic is on it');
    check((await deliveriesOf(TENANT, [stagedId])).length === 0, 'CONTROL: and Notification never saw it: no delivery row');
    if (!original) return;
    // The first attempt, as the dispatcher leaves one whose sends all failed: a claim
    // PENDING since two hours ago, attempts counted, and the message on notification.dlq
    // with the headers forward() writes.
    await inTenant(
      notifyApp,
      TENANT,
      `INSERT INTO "NotificationDelivery" (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId", attempts, "lastError", "createdAt")
       VALUES ($1, $2, $3, 'email', 'invitation', 'invitation', $4, $5, 4, 'connect ECONNREFUSED', now() - interval '2 hours')`,
      [randomUUID(), TENANT, staged.inviteId, `invitation:${stagedId}:email:${staged.inviteId}`, stagedId],
    );
    const plain = Object.fromEntries(Object.entries(original.headers ?? {}).map(([k, v]) => [k, v?.toString() ?? '']));
    await producer.send({
      topic: DLQ,
      messages: [
        {
          key: original.key,
          value: original.value,
          headers: {
            ...plain,
            [DLQ_HEADERS.reason]: 'retries exhausted after 4 attempts: connect ECONNREFUSED',
            [DLQ_HEADERS.consumerGroup]: `${GROUP}-retry`,
            [DLQ_HEADERS.originalTopic]: TOPIC,
            [DLQ_HEADERS.sourceTopic]: 'notification.retry.3',
            [DLQ_HEADERS.sourcePartition]: '0',
            [DLQ_HEADERS.sourceOffset]: '0',
            [DLQ_HEADERS.attempts]: '4',
            [DLQ_HEADERS.firstFailedAt]: new Date(Date.now() - 2 * 3_600_000).toISOString(),
          },
        },
      ],
    });
    /** One of Notification's scripts, run the way its usage line documents: through pnpm, which hands it a `--`. */
    const pnpmRun = (script, args = '') =>
      new Promise((done) => {
        const child = spawn(`pnpm --filter @eventhub/notification run ${script}${args ? ` -- ${args}` : ''}`, {
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
    const swept = await pnpmRun('sweep:once');
    const [ended] = await inTenant(notifyApp, TENANT, `SELECT d.status::text AS status, d."lastError" FROM "NotificationDelivery" d WHERE d."tenantId" = $1 AND d."sourceMessageId" = $2`, [TENANT, stagedId]).then((r) => r.rows);
    check(swept.code === 0 && ended?.status === 'FAILED' && ended.lastError === ABANDONED, `CONTROL: sweep:once ended the two-hour-old claim as abandoned (exit ${swept.code}; ${ended?.status}, "${ended?.lastError}")`);
    const beforeReplay = await dispatch();
    const replayed = await pnpmRun('dlq:replay', stagedId);
    check(replayed.code === 0 && replayed.log.includes(`replayed ${stagedId}`), `dlq:replay put it back on ${TOPIC} (exit ${replayed.code})`);
    const replayMail = await mailTo(A.replay, 1);
    check(replayMail.count === 1, `exactly one email reached the address (${replayMail.count})`);
    check(inviteLinkIn((await messageById(replayMail.messages[0]?.ID))?.Text) === staged.link, 'carrying the link Auth issued');
    const [resent] = await deliveriesOf(TENANT, [stagedId]);
    check(resent?.status === 'SENT', `the abandoned claim was taken back and recorded SENT (${resent?.status})`);
    check((await dispatch()).reopenedTotal === beforeReplay.reopenedTotal + 1, 'and counted as taken back');
    const beforeSecond = await dispatch();
    const again = await pnpmRun('dlq:replay', stagedId);
    const reread = await waitFor(async () => {
      const m = await dispatch();
      return m.duplicatesSkippedTotal > beforeSecond.duplicatesSkippedTotal ? m : null;
    }, 45_000, 500);
    check(again.code === 0 && Boolean(reread), `CONTROL: a second replay was read, and skipped as already sent (exit ${again.code})`);
    await sleep(2_000);
    check((await inbox(A.replay)).length === 1, 'and the inbox still holds one email');

    // ---------------------------------------------------------------------------------
    console.log('');
    console.log(`  the budget: ${PER_ADDRESS} a day per mailbox and ${PER_TENANT} an hour per tenant, keyed by tenant`);
    const x1 = await invite(BUDGET_TENANT, A.x);
    const x2 = await invite(BUDGET_TENANT, A.x);
    check(x1.status === 201 && x2.status === 201, `CONTROL: up to the address's limit is not refused (${x1.status}, ${x2.status})`);
    const variant = await invite(BUDGET_TENANT, `  ${A.x.toUpperCase()}  `);
    check(variant.status === 429 && Number(variant.retryAfter) > 0, `the same address in capitals with spaces is refused past it (${variant.status}, Retry-After ${variant.retryAfter})`);
    const tagged = await invite(BUDGET_TENANT, A.xTagged);
    check(tagged.status === 429, `and so is the same mailbox with a +tag, which is one person, not a new budget (${tagged.status})`);
    const gmail = [];
    for (const address of A.gmail) gmail.push(await invite(BUDGET_TENANT, address));
    check(gmail[0].status === 201 && gmail[1].status === 201, `CONTROL: at Gmail, two spellings of one mailbox are let through up to its limit (${gmail[0].status}, ${gmail[1].status})`);
    check(gmail[2].status === 429, `and a third, with the dots moved, is refused: Gmail ignores dots (${gmail[2].status})`);
    const dotted = [];
    for (const address of A.dotted) dotted.push((await invite(BUDGET_TENANT, address)).status);
    check(dotted.every((s) => s === 201), `outside Gmail, three spellings that differ only in dots are three mailboxes, and none is refused (${dotted.join(', ')})`);
    const fills = [];
    for (const address of A.fill) fills.push((await invite(BUDGET_TENANT, address)).status);
    check(fills.every((s) => s === 201), `CONTROL: up to the tenant's limit is not refused (${fills.join(', ')})`);
    const w = await invite(BUDGET_TENANT, A.w);
    check(w.status === 429 && Number(w.retryAfter) > 0, `past the tenant's limit a fresh address is refused (${w.status})`);
    check(JSON.stringify(variant.body) === JSON.stringify(w.body), `with the same body either way (${JSON.stringify(w.body)})`);
    const budgetXLive = (await liveInvites(BUDGET_TENANT, A.x)).length;
    const wOther = await invite(OTHER_TENANT, A.w);
    const xOther = await invite(OTHER_TENANT, A.x);
    check(wOther.status === 201 && xOther.status === 201, `another tenant's admin may still invite both addresses: one tenant spends only its own budget, and a refusal says nothing about anyone else's (${wOther.status}, ${xOther.status})`);
    const refusedRows = [...(await inviteRows(BUDGET_TENANT, A.w)), ...(await inviteRows(BUDGET_TENANT, A.x))];
    check(refusedRows.length === 2, `a refusal makes no invitation: the budget tenant holds ${refusedRows.length} for the two addresses (2 for x, 0 for w)`);

    console.log('');
    console.log("  inviting an address in one tenant leaves another tenant's invitation to it alone");
    // Invite has no row security, so the tenant in the supersede's WHERE is its only scope.
    const budgetXAfter = (await liveInvites(BUDGET_TENANT, A.x)).length;
    check(budgetXLive === 1, `CONTROL: the budget tenant held one live invitation to the address before (${budgetXLive})`);
    check(budgetXAfter === 1, `and still holds it after the other tenant invited the same address (${budgetXAfter} live)`);

    console.log('');
    console.log('  only an ADMIN may invite');
    const outboxCount = async () => (await relayDb.query('SELECT count(*)::int AS n FROM "OutboxMessage" o WHERE o."tenantId" = $1', [TENANT])).rows[0].n;
    const outboxBefore = await outboxCount();
    const asAttendee = await authCall('/invites', { email: A.notAdmin, role: 'ADMIN' }, { Authorization: `Bearer ${attendeeToken}`, 'X-Tenant-Slug': slugOf(TENANT) });
    check(asAttendee.status === 403, `an attendee's token is refused (${asAttendee.status})`);
    check((await inviteRows(TENANT, A.notAdmin)).length === 0 && (await outboxCount()) === outboxBefore, 'and writes no invitation and queues no email');

    console.log('');
    console.log('  an address longer than SMTP can carry is refused');
    check(A.long254.length === 254 && A.long255.length === 255, `CONTROL: the two addresses are ${A.long254.length} and ${A.long255.length} characters`);
    const tooLong = await invite(OTHER_TENANT, A.long255);
    check(tooLong.status === 400 && (await inviteRows(OTHER_TENANT, A.long255)).length === 0, `255 characters is a 400, and no invitation (${tooLong.status})`);
    const longest = await invite(OTHER_TENANT, A.long254);
    check(longest.status === 201, `CONTROL: 254 characters, the most SMTP carries, is accepted, so the refusal is the length (${longest.status})`);

    console.log('');
    console.log('  no invitation outlives the topic that carries it');
    const tooLongLived = start('auth', ['dist/index.js'], { ...authEnv, PORT: String(AUTH_PORT + 1), INVITE_TTL_HOURS: '169' });
    const exited = await waitFor(async () => (tooLongLived.child.exitCode !== null ? { code: tooLongLived.child.exitCode } : null), 30_000, 250);
    if (!exited) tooLongLived.child.kill();
    check(Boolean(exited) && exited.code !== 0 && tooLongLived.log().includes('INVITE_TTL_HOURS'), `Auth refuses to start with invitations longer than a week (${exited ? `exit ${exited.code}` : 'still running'})`);
    const [described] = (await admin.describeConfigs({ includeSynonyms: false, resources: [{ type: ConfigResourceTypes.TOPIC, name: TOPIC }] })).resources;
    const retention = described?.configEntries.find((c) => c.configName === 'retention.ms');
    check(
      retention?.isDefault === false && Number(retention.configValue) > 168 * 3_600_000,
      `${TOPIC} keeps a request longer than the longest invitation, set on the topic and not inherited (${retention?.configValue} ms, ${retention?.isDefault ? 'the broker default' : 'set on the topic'})`,
    );

    // ---------------------------------------------------------------------------------
    console.log('');
    console.log('  nothing to steal in either service\'s output, nor in the dead-letter copies');
    const nOut = notification.log().toLowerCase();
    const allLinks = [...new Set(secrets.filter(Boolean))];
    check(nOut.includes('invitation email sent'), 'CONTROL: Notification\'s output is captured (it logged a send)');
    const nLeaks = [...ALL_ADDRESSES.map((a) => a.toLowerCase()), ...allLinks.map((s) => s.toLowerCase())].filter((s) => nOut.includes(s));
    check(nLeaks.length === 0, nLeaks.length === 0 ? `and holds none of ${allLinks.length} secrets nor ${ALL_ADDRESSES.length} addresses` : `and holds ${nLeaks.length} of them`);
    const authLines = auth.log().split(/\r?\n/).filter((l) => !json(l)?.devOnlyResetUrl);
    check(authLines.some((l) => l.includes('invitation refused: over budget')), "CONTROL: Auth's output is captured (it logged a refusal)");
    const aLeaks = [...ALL_ADDRESSES.map((a) => a.toLowerCase()), ...allLinks.map((s) => s.toLowerCase())].filter((s) => authLines.some((l) => l.toLowerCase().includes(s)));
    check(aLeaks.length === 0, aLeaks.length === 0 ? "and outside the dev sink's own lines Auth's holds no token, link or address" : `and Auth's holds ${aLeaks.length} of them outside the dev sink`);
    const dead = await scan(DLQ, SCAN_DLQ_GROUP, forgedIds);
    check(dead.size === forgedIds.length, `CONTROL: the four refused requests are on ${DLQ}, found by message id (${dead.size})`);
    const dlqText = [...dead.values()].map((m) => `${m.value?.toString('latin1') ?? ''} ${Object.entries(m.headers ?? {}).map(([k, v]) => `${k}=${v?.toString()}`).join(' ')}`).join('\n').toLowerCase();
    check(![A.victim, A.bcc].some((a) => dlqText.includes(a)) && !dlqText.includes('evil.example'), 'and their values and headers hold no address and no link in clear: the value kept is the seal');
    /**
     * EACH REFUSED FOR ITS OWN DEFECT, read from its dead-letter reason. Two guards can
     * refuse one message -- an unknown role is also one the template has no words for --
     * so a count of refusals cannot show either guard missing; the reason can.
     */
    const expectedReason = {
      unsigned: "Auth's signature did not verify",
      relabelled: 'not a kind of email this service sends',
      role: 'the invitation names no role it could grant',
      crlf: 'the seal carries an address that cannot be put in a header',
    };
    const reasons = Object.fromEntries(Object.keys(expectedReason).map((n) => [n, dead.get(cases[n])?.headers?.['dlq-reason']?.toString() ?? '']));
    const wrong = Object.keys(expectedReason).filter((n) => !reasons[n].startsWith(expectedReason[n]));
    check(wrong.length === 0, wrong.length === 0 ? 'each was refused for its own defect: signature, kind, role, line break' : `refused for another reason: ${wrong.map((n) => `${n} (${reasons[n] || 'none'})`).join('; ')}`);
  } catch (err) {
    const children = [['Notification', notification], ['Auth', auth]];
    const cause = err?.cause ? ` [${err.cause.code ?? err.cause.message ?? String(err.cause)}]` : '';
    const exited = children.filter(([, p]) => p.child.exitCode !== null || p.child.signalCode !== null).map(([name, p]) => ` (${name} had exited: ${p.child.exitCode ?? p.child.signalCode})`);
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}${cause}${exited.join('')}`);
    for (const [name, p] of children) console.error(`  ${name}'s output:\n${excerpt(p.log())}`);
  } finally {
    if (holder) {
      await holder.query('ROLLBACK').catch(() => {});
      await holder.end().catch(() => {});
    }
    notification.child.kill();
    auth.child.kill();

    // Every message to every address this run used, searched again rather than trusted.
    const mailIds = [];
    for (const address of ALL_ADDRESSES) mailIds.push(...(await inbox(address).catch(() => [])).map((m) => m.ID));
    const mailDeleted = mailIds.length === 0
      ? true
      : await fetch(`${MAILPIT}/api/v1/messages`, { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ IDs: mailIds }) }).then((r) => r.ok, () => false);

    // The contact cards registration published, withdrawn from the compacted topic.
    const contactKeys = registered.map((u) => `${u.tenant}:${u.id}`);
    const tombstoned = contactKeys.length === 0 ? 0 : await producer.send({ topic: CONTACT_TOPIC, messages: contactKeys.map((k) => ({ key: k, value: null })) }).then(() => contactKeys.length, () => 0);

    /**
     * THE SETTLED SENT-LOG AND SUPPRESSION ROWS ARE KEPT. This run's sealed requests stay
     * on the topic, deliverable until they expire an hour from now. Deleting the rows that
     * say they were sent, suppressed or refused would hand every one of them to the next
     * Notification that reads the topic in that hour -- the suppressed address included --
     * which is #142 the other way round. Only a claim still PENDING, which no outcome
     * settled, is removed. Each row is a hash, ids and a status: no address.
     */
    const settledCount = async () => {
      const n = { deliveries: 0, suppressions: 0 };
      for (const tenant of TENANTS) {
        n.deliveries += (await inTenant(notifyOwner, tenant, `SELECT count(*)::int AS n FROM "NotificationDelivery" WHERE "tenantId" = $1 AND status <> 'PENDING'`, [tenant]).catch(() => ({ rows: [{ n: 0 }] }))).rows[0].n;
        n.suppressions += (await inTenant(notifyOwner, tenant, 'SELECT count(*)::int AS n FROM "EmailSuppression" WHERE "tenantId" = $1', [tenant]).catch(() => ({ rows: [{ n: 0 }] }))).rows[0].n;
      }
      return n;
    };
    const settledBefore = await settledCount();
    let deleted = 0;
    for (const tenant of TENANTS) {
      deleted += (await relayDb.query('DELETE FROM "OutboxMessage" WHERE "tenantId" = $1', [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      deleted += (await inTenant(notifyOwner, tenant, `DELETE FROM "NotificationDelivery" WHERE "tenantId" = $1 AND status = 'PENDING'`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      deleted += (await inTenant(notifyApp, tenant, 'DELETE FROM "ContactCard" WHERE "tenantId" = $1', [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      for (const table of ['RefreshToken', 'Invite', 'User']) {
        deleted += (await inTenant(authApp, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      }
      deleted += (await authApp.query('DELETE FROM "Tenant" WHERE id = $1', [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
    }
    const left = { outbox: 0, invites: 0, pending: 0, inbox: 0 };
    for (const tenant of TENANTS) {
      left.outbox += (await relayDb.query('SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1', [tenant])).rows[0].n;
      left.invites += (await authApp.query('SELECT count(*)::int AS n FROM "Invite" WHERE "tenantId" = $1', [tenant])).rows[0].n;
      left.pending += (await inTenant(notifyOwner, tenant, `SELECT count(*)::int AS n FROM "NotificationDelivery" WHERE "tenantId" = $1 AND status = 'PENDING'`, [tenant])).rows[0].n;
    }
    const kept = await settledCount();
    for (const address of ALL_ADDRESSES) left.inbox += (await inbox(address).catch(() => [])).length;
    const groupsLeft = await deleteGroupsWhenEmpty(admin, [...GROUPS.all, SCAN_TOPIC_GROUP, SCAN_DLQ_GROUP]);
    check(
      deleted > 0 && mailDeleted && tombstoned === contactKeys.length && Object.values(left).every((n) => n === 0),
      `cleanup: ${deleted} rows and ${mailIds.length} emails deleted, ${tombstoned} contact card(s) withdrawn, and nothing left (${JSON.stringify(left)}); ` +
        `consumer groups ${groupsLeft.length === 0 ? 'deleted' : `left to expire: ${groupsLeft.join(', ')}`}`,
    );
    check(
      settledBefore.deliveries > 0 && settledBefore.suppressions > 0 && kept.deliveries === settledBefore.deliveries && kept.suppressions === settledBefore.suppressions,
      `cleanup KEPT every settled row while the run's requests can still be delivered: ${kept.deliveries} of ${settledBefore.deliveries} deliveries ` +
        `and ${kept.suppressions} of ${settledBefore.suppressions} suppression(s), for the run's three tenants`,
    );
    await producer.disconnect().catch(() => {});
    await admin.disconnect().catch(() => {});
    await Promise.all(clients.map((c) => c.end().catch(() => {})));
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('an invitation reaches the person invited once, sealed on the way, with a link that registers them; nothing refused is sent');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
