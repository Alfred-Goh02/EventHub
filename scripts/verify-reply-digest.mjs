/**
 * Asserts the hourly limit on reply emails and the digest that carries the rest.
 *
 * Decided 2026-09-27: at most five reply emails per person per hour; past that, replies
 * are held and go out as one digest an hour, with counts and links and none of the
 * replies' words.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - Five replies are emailed and the sixth and seventh are held: five in the inbox, and
 *     two held items. The held counter moving is the proof the held ones were read, since
 *     "five emails" is also what a consumer that stopped after five would produce.
 *   - A held reply read again is held once: the counter moves, the items do not.
 *   - The limit is per tenant: the same user id in another tenant is emailed.
 *   - Two consumers cannot both take the fifth place. Replies to one person arrive on the
 *     replies consumer and its retry consumer at once; four on each, and still exactly
 *     five are sent.
 *   - The digest: one email per person, saying how many replies on which post, linking
 *     to the post on the web app's origin, with the same one-click unsubscribe, and none
 *     of the replies' words — each reply here carries a marker the digest must not.
 *   - Re-asked when the digest goes: someone who turned replies off in the hour, or whose
 *     card went, is sent nothing and their held items are dropped; a suppressed address
 *     is recorded SUPPRESSED and not sent to.
 *   - A digest taken and never settled — the process died between stamping the items and
 *     sending — is settled by the next pass, and a digest the sent-log says went out is
 *     not sent again.
 *   - Held means held: nothing goes out until a pass runs. The pass is run by hand here,
 *     in its documented form, so no check races a timer; then a Notification with a
 *     short interval shows the service runs passes on its own.
 *   - The digest's credential reads three columns of the held items, in every tenant, and
 *     nothing else; the service refuses to start on a credential that can do more.
 *
 * Community's messages are produced here, as verify-reply-emails.mjs produces them, and
 * contact cards are sealed and stored the way the contact consumer stores them.
 *
 * Scoped to two tenants created for this run; their rows, inbox messages and consumer
 * groups are removed at the end, and what was deleted is reported. A pass by hand also
 * settles any other tenant's held replies, so nothing here reads its printed totals.
 *
 * Run (with Postgres, Kafka, the Schema Registry and Mailpit up; Notification built;
 * both keypairs generated): node scripts/verify-reply-digest.mjs
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
const TIER_1 = 'notification.retry.1';
const MAILPIT = process.env['MAILPIT_URL'] ?? 'http://127.0.0.1:8025';
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_digest_${RUN}`;
const OTHER = `tenant_digest_other_${RUN}`;
const SPACE = `space_digest_${RUN}`;
const GROUP = `verify-reply-digest-${RUN}`;
const PORT = 3987;
const APP_URL = `https://app-${RUN}.digest.example`;
/** In every reply's body, so a digest that quoted any of them would show it. */
const WORDS = `words-${RUN}-must-not-appear`;

/** Everyone in the run, and the address their card holds. */
const who = (name, tenant = TENANT) => ({ id: `user_${name}_${RUN}`, tenant, email: `digest-${name}-${RUN}-${tenant === TENANT ? 'a' : 'b'}@mailcheck.example` });
const ACTOR = who('actor');
const D = who('busy');
const D2 = { ...who('busy', OTHER) };
const F = who('raced');
const G = who('optsout');
const H = who('cardgone');
const S = who('suppressed');
const J = who('resumed');
const Z = who('scheduled');

const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(PORT),
  KAFKA_CONSUMER_GROUP: GROUP,
  CONTACTS_CONSUMER_GROUP: `${GROUP}-contacts`,
  DISPATCH_START_FROM: 'latest',
  DISPATCH_RETRY_DELAYS_MS: '3000,3000,3000',
  NOTIFICATION_PUBLIC_URL: `http://127.0.0.1:${PORT}`,
  PUBLIC_APP_URL: APP_URL,
  AUTH_PUBLIC_KEY_B64: Buffer.from(key('auth', 'public.pem')).toString('base64'),
});
// REPLY_HOURLY_LIMIT and DIGEST_INTERVAL_MS are .env.example's: five, and an hour, so
// the in-process flusher never runs during the checks and the limit is the decided one.
const LIMIT = Number(notificationEnv.REPLY_HOURLY_LIMIT);

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

function verifyToken(token) {
  const [v, body, sig] = token.split('.');
  if (v !== 'v1' || !body || !sig) return null;
  const expected = createHmac('sha256', notificationEnv.UNSUBSCRIBE_SECRET).update(`v1.${body}`).digest();
  const given = Buffer.from(sig, 'base64url');
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;
  return JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
}

function start(env) {
  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', 'notification'), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  const exited = new Promise((done) => child.on('exit', (code) => done(code)));
  return { child, log: () => log, exited };
}

/** The pass by hand, exactly as the README documents it. */
function digestOnce() {
  return new Promise((done) => {
    const child = spawn('pnpm --filter @eventhub/notification run digest:once', { cwd: ROOT, env: notificationEnv, shell: true, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    child.stdout.on('data', (d) => (out += d));
    child.stderr.on('data', (d) => (out += d));
    child.on('exit', (code) => done({ code, out }));
  });
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

  // A running Notification shares the sent-log, the held items and Mailpit: it would
  // send this run's replies, and its own flusher could send this run's digests.
  const realGroup = serviceEnv(ROOT, 'notification').KAFKA_CONSUMER_GROUP;
  const kafka = new Kafka({ clientId: `verify-digest-${RUN}`, brokers: notificationEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const busy = await busyGroups(admin, [realGroup, `${realGroup}-retry`, `${realGroup}-replies`, `${realGroup}-replies-retry`]);
  if (busy.length > 0) {
    console.error(`FAIL: ${busy.join(' and ')} has active members, so another Notification is running. Stop it first (in compose: docker compose stop notification).`);
    process.exitCode = 1;
    await admin.disconnect();
    return;
  }

  const owner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  await owner.connect();
  const digestRole = new pg.Client({ connectionString: notificationEnv.DIGEST_DATABASE_URL });
  await digestRole.connect();
  const producer = kafka.producer();
  await producer.connect();
  const registry = new SchemaRegistry({ host: notificationEnv.SCHEMA_REGISTRY_URL });
  const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);

  const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
  const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
  const authSigning = await importPKCS8(key('auth', 'private.pem'), 'RS256');

  async function storeCard(user) {
    const messageId = randomUUID();
    const occurredAt = '2026-09-27T00:00:00.000Z';
    const signed = await new SignJWT({ tenantId: user.tenant, email: user.email, occurredAt })
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
      user.tenant,
      `INSERT INTO "ContactCard" ("tenantId", "userId", sealed, "messageId", "occurredAt") VALUES ($1, $2, $3, $4, ($5::timestamptz AT TIME ZONE 'UTC'))`,
      [user.tenant, user.id, sealed, messageId, occurredAt],
    );
  }

  function reply(user, fields = {}) {
    return {
      messageId: randomUUID(),
      occurredAt: new Date().toISOString(),
      tenantId: user.tenant,
      spaceId: SPACE,
      kind: 'COMMENT_CREATED',
      actorId: ACTOR.id,
      postId: `post_${RUN}_1`,
      commentId: `c_${randomBytes(4).toString('hex')}`,
      parentCommentId: `p_${RUN}`,
      body: `a reply with ${WORDS}`,
      recipientId: user.id,
      ...fields,
    };
  }
  const produce = async (value, { topic = TOPIC, headers = {} } = {}) =>
    producer.send({
      topic,
      messages: [{ key: SPACE, value: await registry.encode(schemaId, value), headers: { 'message-id': value.messageId, 'event-type': 'community.comment_created', ...headers } }],
    });
  const dueNow = () => ({ 'retry-original-topic': TOPIC, 'retry-attempt': '1', 'retry-not-before': String(Date.now()), 'retry-first-failed-at': new Date().toISOString() });

  const metrics = async (port = PORT) => (await (await fetch(`http://127.0.0.1:${port}/metrics`)).json());
  const heldItems = async (user) =>
    (await inTenant(owner, user.tenant, `SELECT i."postId", i."toComment", i."digestId" FROM "DigestItem" i WHERE i."tenantId" = $1 AND i."userId" = $2`, [user.tenant, user.id])).rows;
  const deliveries = async (user, templateKey) =>
    (await inTenant(
      owner,
      user.tenant,
      `SELECT d.status::text AS status, d."sourceMessageId" FROM "NotificationDelivery" d WHERE d."tenantId" = $1 AND d."recipientId" = $2 AND d."templateKey" = $3`,
      [user.tenant, user.id, templateKey],
    )).rows;
  /** The emails in an inbox whose subject says they are a digest. */
  const digests = async (user) => (await inbox(user.email)).filter((m) => /more repl/.test(m.Subject));

  const everyone = [ACTOR, D, D2, F, G, H, S, J, Z];
  let service = start(notificationEnv);

  try {
    console.log('  the fixture');
    for (const user of [D, D2, F, G, H, S, J, Z]) await storeCard(user);
    await inTenant(
      owner,
      TENANT,
      `INSERT INTO "EmailSuppression" (id, "tenantId", "addressHash", reason, "expiresAt") VALUES ($1, $2, $3, 'HARD_BOUNCE', now() + interval '1 day')`,
      [randomUUID(), TENANT, createHash('sha256').update(S.email.trim().toLowerCase()).digest('hex')],
    );
    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${PORT}, limit ${LIMIT} an hour` : `it never became ready:\n${excerpt(service.log())}`);
    if (!ready) return;
    const groups = [`${GROUP}-replies`, `${GROUP}-replies-retry`];
    const joined = await waitFor(async () => {
      const { groups: described } = await admin.describeGroups(groups);
      return described.length === groups.length && described.every((g) => g.state === 'Stable' && g.members.length > 0);
    }, 60_000, 500);
    check(Boolean(joined), 'CONTROL: its replies group and replies retry group have both joined');
    const reading = await waitFor(async () => {
      const before = (await metrics()).replies.notAReplyTotal;
      await produce(reply(D, { kind: 'POST_CREATED', recipientId: null }), { headers: { 'event-type': 'community.post_created' } });
      return waitFor(async () => (await metrics()).replies.notAReplyTotal > before, 5_000);
    }, 60_000, 500);
    check(Boolean(reading), 'CONTROL: it read a canary on the partition every reply below lands on');
    if (failures > 0) return;

    console.log('');
    console.log(`  ${LIMIT} an hour, and the rest held`);
    const heldBefore = (await metrics()).replies.heldTotal;
    const busyReplies = [...Array.from({ length: LIMIT + 1 }, () => reply(D)), reply(D, { postId: `post_${RUN}_2`, parentCommentId: null })];
    for (const r of busyReplies) await produce(r);
    const allRead = await waitFor(async () => (await metrics()).replies.heldTotal >= heldBefore + 2, 45_000);
    await waitFor(async () => (await inbox(D.email)).length >= LIMIT, 30_000);
    await sleep(1_500);
    const dMails = (await inbox(D.email)).length;
    const dHeld = await heldItems(D);
    const dSent = await deliveries(D, 'community-reply');
    check(
      Boolean(allRead) && dMails === LIMIT && dHeld.length === 2 && dSent.length === LIMIT,
      `seven replies: ${LIMIT} emailed and 2 held (inbox ${dMails}, held ${dHeld.length}, reply deliveries ${dSent.length})`,
    );
    const heldAgainBefore = (await metrics()).replies.heldTotal;
    await produce(busyReplies[busyReplies.length - 1]);
    const readAgain = await waitFor(async () => (await metrics()).replies.heldTotal > heldAgainBefore, 30_000);
    check(Boolean(readAgain) && (await heldItems(D)).length === 2, `a held reply read again is held once (held ${(await heldItems(D)).length})`);
    await produce(reply(D2));
    const otherTenant = await waitFor(async () => (await inbox(D2.email)).length === 1, 30_000);
    check(Boolean(otherTenant), 'the same user id in another tenant has an hour of its own: emailed, not held');

    console.log('');
    console.log('  two consumers cannot both take the last place');
    const raced = Array.from({ length: 8 }, () => reply(F));
    await Promise.all(raced.map((r, i) => (i % 2 === 0 ? produce(r) : produce(r, { topic: TIER_1, headers: dueNow() }))));
    await waitFor(async () => (await inbox(F.email)).length + (await heldItems(F)).length >= 8, 45_000);
    await sleep(2_000);
    const fMails = (await inbox(F.email)).length;
    const fHeld = (await heldItems(F)).length;
    check(fMails === LIMIT && fHeld === 3, `eight replies, half on each consumer at once: exactly ${LIMIT} sent and 3 held (sent ${fMails}, held ${fHeld})`);

    console.log('');
    console.log('  held means held, until a pass');
    // G and H: one over the limit each. G then turns replies off, and H's card goes.
    for (const user of [G, H]) for (let i = 0; i <= LIMIT; i++) await produce(reply(user));
    const bothHeld = await waitFor(async () => (await heldItems(G)).length === 1 && (await heldItems(H)).length === 1, 45_000);
    // Without it, "no digest and nothing held" below would hold of two people who were
    // never held anything at all.
    check(Boolean(bothHeld), `CONTROL: one held reply each for the two people whose circumstances change (${(await heldItems(G)).length}, ${(await heldItems(H)).length})`);
    await inTenant(owner, TENANT, `INSERT INTO "NotificationPreference" ("tenantId", "userId", category, channel, enabled) VALUES ($1, $2, 'COMMUNITY_REPLY', 'EMAIL', false)`, [TENANT, G.id]);
    await inTenant(owner, TENANT, `DELETE FROM "ContactCard" WHERE "tenantId" = $1 AND "userId" = $2`, [TENANT, H.id]);
    // S: held replies to a suppressed address, planted, since a suppressed delivery does
    // not use up the hour and so nothing would ever be held for it.
    for (const post of [`post_${RUN}_s`, `post_${RUN}_s`]) {
      await inTenant(owner, TENANT, `INSERT INTO "DigestItem" ("tenantId", "userId", category, "sourceMessageId", "postId", "toComment") VALUES ($1, $2, 'COMMUNITY_REPLY', $3, $4, true)`, [TENANT, S.id, randomUUID(), post]);
    }
    // J: a digest a previous pass took and never settled, which the sent-log says went.
    const settledId = randomUUID();
    await inTenant(owner, TENANT, `INSERT INTO "DigestItem" ("tenantId", "userId", category, "sourceMessageId", "postId", "toComment", "digestId") VALUES ($1, $2, 'COMMUNITY_REPLY', $3, $4, false, $5)`, [TENANT, J.id, randomUUID(), `post_${RUN}_j`, settledId]);
    await inTenant(
      owner,
      TENANT,
      `INSERT INTO "NotificationDelivery" (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId", status, "sentAt")
       VALUES ($1, $2, $3, 'email', 'community', 'community-digest', $4, $5, 'SENT', now())`,
      [randomUUID(), TENANT, J.id, `community-digest:${settledId}:email:${J.id}`, settledId],
    );
    await sleep(1_000);
    check(
      (await digests(D)).length === 0 && (await heldItems(D)).length === 2,
      `nothing goes out before a pass: no digest, and both of the first person's replies still held (${(await heldItems(D)).length})`,
    );

    console.log('');
    console.log('  one pass, run as the README documents it');
    const pass = await digestOnce();
    check(pass.code === 0 && /sent \d+ digest\(s\)/.test(pass.out), `digest:once exits 0 and says what it did (exit ${pass.code}: ${pass.out.trim().split(/\r?\n/).pop()?.slice(0, 140)})`);

    const dDigests = await waitFor(async () => {
      const found = await digests(D);
      return found.length >= 1 ? found : null;
    }, 20_000);
    const dDigest = dDigests?.[0] ? await message(dDigests[0].ID) : null;
    const dHeaders = dDigests?.[0] ? await headersOf(dDigests[0].ID) : {};
    check(dDigests?.length === 1 && dDigest?.Subject === '2 more replies to you', `one digest for the busy person, and it says how many (${dDigests?.length ?? 0}: ${dDigest?.Subject})`);
    const post1 = `${APP_URL}/posts/post_${RUN}_1`;
    const post2 = `${APP_URL}/posts/post_${RUN}_2`;
    check(
      Boolean(dDigest) &&
        dDigest.Text.includes('1 reply to your comment') && dDigest.Text.includes(post1) &&
        dDigest.Text.includes('1 comment on your post') && dDigest.Text.includes(post2) &&
        dDigest.HTML.includes(`href="${post1}"`) && dDigest.HTML.includes(`href="${post2}"`),
      'a line per post, reply or comment, each linking to the post on the web app',
    );
    check(
      Boolean(dDigest) && !dDigest.Text.includes(WORDS) && !dDigest.HTML.includes(WORDS),
      'and none of the replies\' words: counts and links only',
    );
    const unsubscribeUrl = (dHeaders['List-Unsubscribe']?.[0] ?? '').match(/^<(.+)>$/)?.[1] ?? '';
    const claims = unsubscribeUrl ? verifyToken(new URL(unsubscribeUrl).searchParams.get('token') ?? '') : null;
    check(
      dHeaders['List-Unsubscribe-Post']?.[0] === 'List-Unsubscribe=One-Click' && claims?.t === TENANT && claims?.u === D.id && claims?.c === 'COMMUNITY_REPLY' && dDigest?.Text.includes(unsubscribeUrl),
      'with the same one-click unsubscribe, for this person and replies only, in the headers and the body',
    );
    const dDigestRows = await deliveries(D, 'community-digest');
    check(
      (await heldItems(D)).length === 0 && dDigestRows.length === 1 && dDigestRows[0].status === 'SENT',
      `its held replies are gone, and the digest is one SENT delivery (${dDigestRows.map((r) => r.status).join(', ') || 'none'})`,
    );
    const fDigest = await waitFor(async () => (await digests(F)).length === 1 ? (await digests(F))[0] : null, 20_000);
    check(fDigest?.Subject === '3 more replies to you', `the raced person's three held replies went in one digest (${fDigest?.Subject})`);

    const gone = async (user) => (await heldItems(user)).length === 0;
    check((await digests(G)).length === 0 && (await gone(G)), 'turned replies off during the hour: no digest, and the held reply dropped');
    check((await digests(H)).length === 0 && (await gone(H)), 'no card any more: no digest, and the held reply dropped');
    const sRows = await deliveries(S, 'community-digest');
    check(
      (await inbox(S.email)).length === 0 && sRows.length === 1 && sRows[0].status === 'SUPPRESSED' && (await gone(S)),
      `a suppressed address: recorded ${sRows[0]?.status ?? 'nothing'}, nothing sent, the held replies settled`,
    );
    check(
      (await inbox(J.email)).length === 0 && (await gone(J)),
      'a digest taken by an earlier pass that the sent-log says went: settled, and not sent again',
    );

    const beforeSecond = { d: (await inbox(D.email)).length, f: (await inbox(F.email)).length };
    const second = await digestOnce();
    await sleep(1_000);
    check(
      second.code === 0 && (await inbox(D.email)).length === beforeSecond.d && (await inbox(F.email)).length === beforeSecond.f,
      'a second pass with nothing waiting sends nothing',
    );

    console.log('');
    console.log("  the digest's credential finds who is waiting, in every tenant, and nothing else");
    // Held items in two tenants, readable in one statement without any tenant set.
    await inTenant(owner, OTHER, `INSERT INTO "DigestItem" ("tenantId", "userId", category, "sourceMessageId", "postId", "toComment") VALUES ($1, $2, 'COMMUNITY_REPLY', $3, 'post_x', true)`, [OTHER, D2.id, randomUUID()]);
    await inTenant(owner, TENANT, `INSERT INTO "DigestItem" ("tenantId", "userId", category, "sourceMessageId", "postId", "toComment") VALUES ($1, $2, 'COMMUNITY_REPLY', $3, 'post_x', true)`, [TENANT, Z.id, randomUUID()]);
    const seen = (await digestRole.query(`SELECT i."tenantId", i."userId", i."createdAt" FROM "DigestItem" i WHERE i."tenantId" = ANY($1)`, [[TENANT, OTHER]])).rows;
    check(new Set(seen.map((r) => r.tenantId)).size === 2, `CONTROL: it sees who is waiting in both tenants at once (${seen.length} row(s))`);
    const refusal = async (sql, params = []) => {
      await digestRole.query('BEGIN');
      try {
        await digestRole.query("SELECT set_config('app.tenant_id', $1, true)", [TENANT]);
        await digestRole.query(sql, params);
        return 'allowed';
      } catch (err) {
        return err.code ?? err.message;
      } finally {
        await digestRole.query('ROLLBACK').catch(() => {});
      }
    };
    const refusals = [
      ['which post a held reply was on', 'SELECT "postId" FROM "DigestItem" LIMIT 1'],
      ['the sent-log', 'SELECT 1 FROM "NotificationDelivery" LIMIT 1'],
      ['contact cards', 'SELECT 1 FROM "ContactCard" LIMIT 1'],
      ['a held reply of its own', `INSERT INTO "DigestItem" ("tenantId", "userId", category, "sourceMessageId", "postId", "toComment") VALUES ('${TENANT}', 'x', 'x', 'x', 'x', true)`],
      ['deleting held replies', 'DELETE FROM "DigestItem"'],
    ];
    for (const [what, sql] of refusals) {
      const got = await refusal(sql);
      check(got === '42501', `it cannot read or write ${what} (${got})`);
    }
    const wide = start({ ...notificationEnv, PORT: String(PORT + 1), DIGEST_DATABASE_URL: notificationEnv.APP_DATABASE_URL, KAFKA_CONSUMER_GROUP: `${GROUP}-wide`, CONTACTS_CONSUMER_GROUP: `${GROUP}-wide-contacts` });
    const wideExit = await Promise.race([wide.exited, sleep(45_000).then(() => 'still running')]);
    if (wideExit === 'still running') {
      wide.child.kill();
      await wide.exited;
    }
    check(
      wideExit !== 'still running' && wideExit !== 0 && /refusing to flush digests/.test(wide.log()),
      `given the app role instead, the service refuses to start, and says why (exit ${wideExit}: ${wide.log().match(/refusing to flush digests: [^.\n]*/)?.[0] ?? 'no such message'})`,
    );

    console.log('');
    console.log('  and the service runs passes on its own');
    service.child.kill();
    await service.exited;
    service = start({ ...notificationEnv, DIGEST_INTERVAL_MS: '3000' });
    const again = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    const scheduled = again ? await waitFor(async () => ((await digests(Z)).length === 1 ? true : null), 30_000) : null;
    const m = again ? await metrics() : null;
    check(
      Boolean(scheduled) && (m?.digest?.digestsSentTotal ?? 0) >= 1 && (await gone(Z)),
      `with a three-second interval, Z's held reply went out without being asked (${m?.digest?.digestsSentTotal ?? 0} digest(s) in ${m?.digest?.passesTotal ?? 0} pass(es))`,
    );
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Notification's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    let removed = 0;
    let left = 0;
    const tables = ['DigestItem', 'NotificationDelivery', 'ContactCard', 'NotificationPreference', 'EmailSuppression'];
    for (const tenant of [TENANT, OTHER]) {
      for (const table of tables) {
        removed += (await inTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      }
      for (const table of tables) {
        left += (await inTenant(owner, tenant, `SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rows: [{ n: -1 }] }))).rows[0].n;
      }
    }
    const ids = [];
    for (const user of everyone) ids.push(...(await inbox(user.email).catch(() => [])).map((x) => x.ID));
    if (ids.length > 0) {
      await fetch(`${MAILPIT}/api/v1/messages`, { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ IDs: ids }) }).catch(() => {});
    }
    check(removed > 0 && left === 0, `cleanup: this run's rows are gone (${removed} deleted, ${left} left), and ${ids.length} email(s) removed from Mailpit`);
    const stuck = await deleteGroupsWhenEmpty(admin, [
      GROUP, `${GROUP}-retry`, `${GROUP}-replies`, `${GROUP}-replies-retry`, `${GROUP}-contacts`,
      `${GROUP}-wide`, `${GROUP}-wide-retry`, `${GROUP}-wide-replies`, `${GROUP}-wide-replies-retry`, `${GROUP}-wide-contacts`,
    ]);
    if (stuck.length > 0) console.log(`    note  consumer group(s) left to expire: ${stuck.join(', ')}`);
    await Promise.allSettled([producer.disconnect(), admin.disconnect(), owner.end(), digestRole.end()]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('five reply emails an hour, the rest in one digest of counts and links, re-checked when it goes, sent once');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
