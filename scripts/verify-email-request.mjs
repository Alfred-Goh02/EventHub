/**
 * Asserts the path a password-reset email takes out of Auth. The request commits
 * with the reset and reaches Kafka through the relay. It carries the link sealed,
 * so that only Notification can open it and only Auth could have written it.
 *
 * WHY SEALED. Decided on 2026-09-15: the link travels over Kafka encrypted to
 * Notification's public key, inside a JWS signed by Auth. A raw token on a topic
 * hands an account to anyone who can read the topic. Encryption alone would let
 * anyone who can produce to it have Notification mail a link of their choosing.
 *
 * WHERE A LEAK IS LOOKED FOR, AND WITH WHAT. The raw token, the full link and the
 * address are searched for in the outbox row and in the bytes on the topic. The
 * token comes from Auth's own dev sink, in a private instance started here, so the
 * search looks for the real secret rather than a guess at its shape.
 *
 * THE LINK MUST STILL WORK. A seal around a dead link would pass every check above.
 * So the link opened from the seal resets the password, and the new password signs
 * in.
 *
 * Scoped to a tenant and a user created for this run, and removed at the end.
 *
 * Run (with Postgres, Kafka and the Schema Registry up, Auth and the relay built,
 * and both keypairs generated): node scripts/verify-email-request.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import {
  CompactEncrypt,
  SignJWT,
  calculateJwkThumbprint,
  compactDecrypt,
  decodeProtectedHeader,
  exportJWK,
  importPKCS8,
  importSPKI,
  jwtVerify,
} from 'jose';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const TOPIC = 'auth.email-requested';
const AUDIENCE = 'eventhub-notification';
const TYPE = 'auth-email+jwt';
const DECRYPT_OPTIONS = { keyManagementAlgorithms: ['RSA-OAEP-256'], contentEncryptionAlgorithms: ['A256GCM'] };

const AUTH_PORT = 3095;
const authEnv = serviceEnv(ROOT, 'auth', {
  PORT: String(AUTH_PORT),
  NODE_ENV: 'development',
  // The dev sink is how this run learns the real token to search for. It logs at
  // debug, so the level has to be there too.
  AUTH_DEV_TOKEN_SINK: '1',
  LOG_LEVEL: 'debug',
});
const relayEnv = serviceEnv(ROOT, 'relay', {
  RELAY_DATABASE_URL:
    process.env['AUTH_RELAY_DATABASE_URL'] ??
    'postgresql://eventhub_auth_relay:eventhub_auth_relay@localhost:5432/eventhub_auth',
  RELAY_FORBIDDEN_TABLE: 'User',
  RELAY_METRICS_PORT: '3094',
});

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_mailcheck_${RUN}`;
/** Its subdomain, which is how Auth is told the tenant. */
const TENANT_SLUG = `mailcheck-${RUN}`;
const USER_ID = `user_mailcheck_${RUN}`;
const EMAIL = `reset-${RUN}@mailcheck.example`;
const NEW_PASSWORD = `new-password-${RUN}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');

/** A transaction with app.tenant_id set, committed. */
async function inTenant(client, tenant, fn) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const value = await fn(client);
    await client.query('COMMIT');
    return value;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** The same, always rolled back, for statements that must be refused. */
async function attempt(client, tenant, sql, params) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const r = await client.query(sql, params);
    return { ok: true, rowCount: r.rowCount };
  } catch (err) {
    return { ok: false, code: err.code, message: err.message };
  } finally {
    await client.query('ROLLBACK').catch(() => {});
  }
}

function startAuth() {
  const child = spawn(process.execPath, ['dist/index.js'], {
    cwd: join(ROOT, 'services', 'auth'),
    env: authEnv,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  return { child, log: () => log };
}

async function waitForHealth(auth) {
  for (let i = 0; i < 80; i += 1) {
    try {
      if ((await fetch(`http://localhost:${AUTH_PORT}/health`)).ok) return true;
    } catch {
      // not listening yet
    }
    if (auth.child.exitCode !== null) return false;
    await sleep(250);
  }
  return false;
}

const post = (path, body, headers = {}) =>
  fetch(`http://localhost:${AUTH_PORT}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  }).then(async (r) => ({ status: r.status, body: await r.text() }));

/** The reset link the dev sink printed for this run's tenant, if it has yet. */
function sinkLink(log) {
  for (const line of log.split(/\r?\n/)) {
    try {
      const entry = JSON.parse(line);
      if (entry.devOnlyResetUrl && entry.tenantId === TENANT) return entry.devOnlyResetUrl;
    } catch {
      // not a JSON log line
    }
  }
  return null;
}

function relayOnce() {
  return new Promise((done) => {
    const child = spawn(process.execPath, ['dist/index.js', '--once'], {
      cwd: join(ROOT, 'services', 'relay'),
      env: relayEnv,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let log = '';
    child.stdout.on('data', (d) => (log += d));
    child.stderr.on('data', (d) => (log += d));
    const timer = setTimeout(() => child.kill(), 60_000);
    child.on('exit', (code) => {
      clearTimeout(timer);
      done({ code, log });
    });
  });
}

async function consumeMessage(messageId) {
  const kafka = new Kafka({ clientId: `verify-email-request-${RUN}`, brokers: relayEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const groupId = `verify-email-request-${RUN}`;
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  const found = await new Promise((done) => {
    const timer = setTimeout(() => done(null), 30_000);
    consumer.run({
      eachMessage: async ({ message }) => {
        if (message.headers?.['message-id']?.toString() === messageId) {
          clearTimeout(timer);
          done(message);
        }
      },
    });
  });
  await consumer.disconnect();
  const admin = kafka.admin();
  await admin.connect();
  await admin.deleteGroups([groupId]).catch(() => {});
  await admin.disconnect();
  return found;
}

async function main() {
  const app = new pg.Client({ connectionString: authEnv.APP_DATABASE_URL });
  const relayDb = new pg.Client({ connectionString: relayEnv.RELAY_DATABASE_URL });
  await app.connect();
  await relayDb.connect();
  const auth = startAuth();

  const outboxRows = () =>
    relayDb
      .query(
        `SELECT o.id, o.topic, o."eventType", o."aggregateId", o.payload, o.payload::text AS text, o."publishedAt"
           FROM "OutboxMessage" o WHERE o."tenantId" = $1 ORDER BY o.seq`,
        [TENANT],
      )
      .then((r) => r.rows);

  try {
    console.log('  the fixture');
    await app.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [TENANT, `Mail check ${RUN}`, TENANT_SLUG]);
    await inTenant(app, TENANT, (c) =>
      c.query(`INSERT INTO "User" (id, "tenantId", email, "passwordHash", "updatedAt") VALUES ($1, $2, $3, 'not-a-hash', now())`, [USER_ID, TENANT, EMAIL]),
    );
    const up = await waitForHealth(auth);
    check(up, up ? `CONTROL: a private Auth with the dev sink on is listening on :${AUTH_PORT}` : `Auth never came up:\n${auth.log().slice(-1500)}`);
    if (failures > 0) return;

    console.log('');
    console.log('  an address that is not a user queues nothing');
    const unknown = await post('/forgot-password', { email: `nobody-${RUN}@mailcheck.example` }, { 'X-Tenant-Slug': TENANT_SLUG });
    check(unknown.status === 202, `an unknown address still gets 202 (got ${unknown.status})`);
    check((await outboxRows()).length === 0, 'and writes no outbox row');

    console.log('');
    console.log('  a real reset queues one request, holding only ciphertext and identifiers');
    const known = await post('/forgot-password', { email: EMAIL }, { 'X-Tenant-Slug': TENANT_SLUG });
    check(known.status === 202, `the user's reset gets 202 (got ${known.status})`);
    let link = null;
    for (let i = 0; i < 40 && !link; i += 1) {
      link = sinkLink(auth.log());
      if (!link) await sleep(125);
    }
    const token = link ? new URL(link).searchParams.get('token') ?? '' : '';
    check(token.length >= 40, `CONTROL: the dev sink gave this run the real token to search for (${token.length} characters)`);
    const rows = await outboxRows();
    // The positive control for "writes no outbox row" above: the same query sees this one.
    check(rows.length === 1, `CONTROL: exactly one outbox row for the tenant (found ${rows.length})`);
    if (failures > 0) return;

    const row = rows[0];
    check(row.topic === TOPIC && row.eventType === 'PasswordResetRequested', `it is a PasswordResetRequested for ${row.topic}`);
    check(row.aggregateId === USER_ID, `keyed by the user (${row.aggregateId})`);
    const keys = Object.keys(row.payload).sort().join(', ');
    check(keys === 'expiresAt, kind, sealed, tenantId, userId', `its payload holds only ${keys}`);
    const leaked = [['the token', token], ['the link', link], ['the address', EMAIL]].filter(([, s]) => row.text.includes(s));
    check(leaked.length === 0, leaked.length === 0 ? 'the stored payload contains neither the token, the link nor the address' : `the stored payload contains ${leaked.map(([n]) => n).join(', ')}`);

    const rewrite = await attempt(app, TENANT, `UPDATE "OutboxMessage" SET payload = '{}'::jsonb WHERE id = $1`, [row.id]);
    check(!rewrite.ok && rewrite.code === '42501', `Auth's app role cannot rewrite a queued message (${rewrite.ok ? `updated ${rewrite.rowCount}` : rewrite.code})`);
    const remove = await attempt(app, TENANT, `DELETE FROM "OutboxMessage" WHERE id = $1`, [row.id]);
    check(!remove.ok && remove.code === '42501', `nor delete one (${remove.ok ? `deleted ${remove.rowCount}` : remove.code})`);
    const peek = await relayDb.query('SELECT 1 FROM "User" LIMIT 1').then(() => 'read it', (err) => err.code);
    check(peek === '42501', `the relay role, which reads every tenant's outbox, cannot read User (${peek})`);

    console.log('');
    console.log("  the seal: only Notification's key opens it, and only Auth's key signed it");
    const sealed = row.payload.sealed;
    const header = decodeProtectedHeader(sealed);
    const notificationPublic = await importSPKI(key('notification', 'public.pem'), 'RSA-OAEP-256', { extractable: true });
    const kid = await calculateJwkThumbprint(await exportJWK(notificationPublic));
    check(header.alg === 'RSA-OAEP-256' && header.enc === 'A256GCM' && header.cty === 'JWT', `a JWE with alg ${header.alg}, enc ${header.enc}, cty ${header.cty}`);
    check(header.kid === kid, `named by the thumbprint of Notification's public key (${header.kid})`);

    const withAuthKey = await compactDecrypt(sealed, await importPKCS8(key('auth', 'private.pem'), 'RSA-OAEP-256'), DECRYPT_OPTIONS).then(
      () => 'opened',
      (err) => err.code ?? err.name,
    );
    check(withAuthKey !== 'opened', `Auth's own private key cannot open it (${withAuthKey})`);

    // A check rather than a throw: sealed to the wrong key, this used to end the run
    // with a stack trace instead of saying which property had failed.
    const opened = await compactDecrypt(sealed, await importPKCS8(key('notification', 'private.pem'), 'RSA-OAEP-256'), DECRYPT_OPTIONS).then(
      (result) => result,
      (err) => ({ error: err.code ?? err.message }),
    );
    check(!opened.error, opened.error ? `Notification's private key cannot open it (${opened.error})` : "Notification's private key opens it");
    if (opened.error) return;
    const signed = new TextDecoder().decode(opened.plaintext);
    const authPublic = await importSPKI(key('auth', 'public.pem'), 'RS256');
    const verified = await jwtVerify(signed, authPublic, { algorithms: ['RS256'], issuer: authEnv.JWT_ISSUER, audience: AUDIENCE, typ: TYPE }).then(
      (v) => v,
      (err) => ({ error: err.code ?? err.message }),
    );
    check(!verified.error, verified.error ? `Notification's key opened it but Auth's signature did not verify: ${verified.error}` : "the JWS inside verifies against Auth's public key");
    if (verified.error) return;

    const claims = verified.payload;
    check(claims.sub === USER_ID && claims.tenantId === TENANT && claims.kind === 'PASSWORD_RESET', `for ${claims.sub} in ${claims.tenantId}, kind ${claims.kind}`);
    check(claims.email === EMAIL, 'addressed to the user');
    check(claims.link === link, 'carrying exactly the link Auth issued');
    check(claims.jti === row.id, `bound to its own message by jti (${claims.jti})`);
    check(Math.abs(claims.exp * 1000 - Date.parse(row.payload.expiresAt)) < 1000, 'expiring when the payload says it does');

    const asAccessToken = await jwtVerify(signed, authPublic, { algorithms: ['RS256'], issuer: authEnv.JWT_ISSUER, audience: authEnv.JWT_AUDIENCE }).then(
      () => 'accepted',
      (err) => err.code,
    );
    check(asAccessToken !== 'accepted', `it is not accepted as an access token (${asAccessToken})`);

    // What the signature is for: anyone can encrypt to Notification's public key.
    const impostorKey = await importPKCS8(key('notification', 'private.pem'), 'RS256');
    const impostor = await new SignJWT({ tenantId: TENANT, email: 'victim@mailcheck.example', link: 'https://evil.example/reset', kind: 'PASSWORD_RESET' })
      .setProtectedHeader({ alg: 'RS256', typ: TYPE })
      .setSubject(USER_ID)
      .setJti(row.id)
      .setIssuer(authEnv.JWT_ISSUER)
      .setAudience(AUDIENCE)
      .setIssuedAt()
      .setExpirationTime('10m')
      .sign(impostorKey);
    const forged = await new CompactEncrypt(new TextEncoder().encode(impostor))
      .setProtectedHeader({ alg: 'RSA-OAEP-256', enc: 'A256GCM', cty: 'JWT', kid })
      .encrypt(notificationPublic);
    const forgedOpened = await compactDecrypt(forged, await importPKCS8(key('notification', 'private.pem'), 'RSA-OAEP-256'), DECRYPT_OPTIONS);
    const forgedVerified = await jwtVerify(new TextDecoder().decode(forgedOpened.plaintext), authPublic, {
      algorithms: ['RS256'],
      issuer: authEnv.JWT_ISSUER,
      audience: AUDIENCE,
      typ: TYPE,
    }).then(() => 'accepted', (err) => err.code);
    check(forgedVerified !== 'accepted', `a seal anyone else made opens, and then fails Auth's signature (${forgedVerified})`);

    console.log('');
    console.log('  the relay publishes it, and the bytes on the topic hold nothing to steal');
    const relay = await relayOnce();
    const [afterRelay] = await outboxRows();
    check(Boolean(afterRelay?.publishedAt), `CONTROL: the row is published (relay --once exited ${relay.code})`);
    if (!afterRelay?.publishedAt) {
      console.error(relay.log.slice(-1500));
      return;
    }
    const message = await consumeMessage(row.id);
    check(Boolean(message), `CONTROL: the message is on ${TOPIC}, found by its message-id header`);
    if (!message) return;

    check(message.key?.toString() === USER_ID, `its Kafka key is the user (${message.key?.toString()})`);
    check(message.headers['tenant-id']?.toString() === TENANT, 'its tenant-id header is the tenant');
    const onWire = [['the token', token], ['the link', link], ['the address', EMAIL]].filter(([, s]) => message.value.includes(Buffer.from(s)));
    check(onWire.length === 0, onWire.length === 0 ? 'the message bytes contain neither the token, the link nor the address' : `the message bytes contain ${onWire.map(([n]) => n).join(', ')}`);
    const decoded = await new SchemaRegistry({ host: relayEnv.SCHEMA_REGISTRY_URL }).decode(message.value);
    check(decoded.messageId === row.id && decoded.sealed === sealed && decoded.userId === USER_ID, 'it decodes against the registered schema to the same message and seal');

    console.log('');
    console.log('  the sealed link is the live credential');
    const reset = await post('/reset-password', { token: new URL(claims.link).searchParams.get('token'), password: NEW_PASSWORD });
    check(reset.status === 200, `the link from inside the seal resets the password (${reset.status})`);
    const login = await post('/login', { email: EMAIL, password: NEW_PASSWORD }, { 'X-Tenant-Slug': TENANT_SLUG });
    check(login.status === 200, `and the new password signs in (${login.status})`);
  } finally {
    auth.child.kill();
    await relayDb.query('DELETE FROM "OutboxMessage" WHERE "tenantId" = $1', [TENANT]).catch(() => {});
    await inTenant(app, TENANT, (c) => c.query('DELETE FROM "User" WHERE id = $1', [USER_ID])).catch(() => {});
    await app.query('DELETE FROM "Tenant" WHERE id = $1', [TENANT]).catch(() => {});
    const left = await relayDb.query('SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1', [TENANT]).then((r) => r.rows[0].n, () => -1);
    const tenantLeft = await app.query('SELECT count(*)::int AS n FROM "Tenant" WHERE id = $1', [TENANT]).then((r) => r.rows[0].n, () => -1);
    check(left === 0 && tenantLeft === 0, `cleanup: this run's outbox rows and tenant are gone (${left} rows, ${tenantLeft} tenant left)`);
    await Promise.all([app.end(), relayDb.end()].map((p) => p.catch(() => {})));
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a reset is queued sealed: nothing on the way can read the link, and the link works');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
