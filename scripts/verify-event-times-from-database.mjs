/**
 * Asserts that the times this system stamps on rows, and the event times it
 * publishes, are the database's clock -- the one every reader compares them with.
 *
 * THEY WERE NOT. Prisma 7 with @prisma/adapter-pg fills `@default(now())` IN THE
 * CLIENT, from the Node process's clock, and sends the value in the INSERT. So every
 * outbox row written by `emit()` carried the app server's time, and the relay
 * publishes an outbox row's createdAt as the event's `occurredAt`. Measured with
 * Ticketing an hour fast: OutboxMessage.createdAt and Reservation.createdAt came out
 * +3600.0 s against Postgres's now(). Live stamped its live.interactions messages with
 * `new Date()` directly, and Analytics stores that as occurred_at.
 *
 * The fix is a default Postgres fills: `@default(dbgenerated("CURRENT_TIMESTAMP"))`.
 * Prisma then leaves the column out of the INSERT and reads it back. That value is
 * now() in the writing transaction: when it BEGAN, on the database's clock, not when
 * it committed. A transaction that waited on a lock keeps its earlier start, so
 * occurredAt is a time to report, never an order (see publisher.ts).
 *
 * TWO HALVES.
 *
 *   STATIC. Every services/<name>/prisma/schema.prisma, derived from the directory,
 *   comments stripped first: no `@default(now())`, and every DateTime default is
 *   dbgenerated("CURRENT_TIMESTAMP"). A rule a schema can be checked against, so a
 *   column nothing below exercises is held to it too. And no migration after this
 *   change may contain `SET DEFAULT CURRENT_TIMESTAMP`: introspection reads that
 *   default back as now(), so `prisma migrate dev` offers the same 34 statements in
 *   every migration it writes from now on, and they change nothing.
 *
 *   BEHAVIOURAL. Each service runs with its clock an HOUR FAST
 *   (scripts/lib/skewed-clock.mjs), because on a machine whose clocks agree a stamp
 *   from the process lands on the database's millisecond and no check can tell the
 *   two apart. Every comparison is made in SQL, `extract(epoch FROM col - now())`.
 *     - Ticketing: a hold through the API. Reservation.createdAt and the
 *       reservation.held outbox row's createdAt.
 *     - Event: an event created through the API by an organiser. Event.createdAt and
 *       its outbox row's createdAt.
 *     - Live: a question and an upvote over the WebSocket. Question.createdAt,
 *       QuestionVote.createdAt, and the occurredAt of both live.interactions
 *       messages, read off the topic and compared with now() and with the row.
 *     - Auth: its emit(), imported from dist into a child process with the same
 *       skew, inside a transaction that measures the row and rolls back. Auth's
 *       outbox carries sealed email requests for Notification; a rolled-back emit
 *       has no side effect that outlives the run.
 *     - Auth: the 90-day ceiling on a refresh family, measured from its oldest
 *       token's createdAt -- the database's clock now, which it compared with
 *       Date.now(). Two families aged by SQL to half an hour either side of it.
 *
 * WHY scripts/verify-relay-occurred-at.mjs NEVER SAW THIS. It inserts its outbox row
 * with raw SQL, so Postgres fills createdAt and the row is right whatever Prisma
 * would have done. It proves the relay publishes createdAt faithfully; this proves
 * createdAt is the database's to begin with.
 *
 * @updatedAt has no database-side form in Prisma, so those columns stay on the
 * process's clock. Nothing reads one; the schemas and CLAUDE.md say nothing may.
 *
 * Starts its own Auth, Ticketing, Event and Live, on this run's consumer group and tenant.
 * Needs Postgres, Kafka and Redis up, and auth, event, ticketing and live built:
 *   node scripts/verify-event-times-from-database.mjs
 * The static half alone, against this tree or a copy of one (no services needed):
 *   node scripts/verify-event-times-from-database.mjs --static [root]
 */

import { spawn } from 'node:child_process';
import { createHash, createPrivateKey, randomBytes, randomUUID, sign } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
import WebSocket from 'ws';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const THIS_FILE = fileURLToPath(import.meta.url);
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_times_${RUN}`;

/** Every process this run starts, an hour fast. */
const SKEW = `--import=${pathToFileURL(join(ROOT, 'scripts', 'lib', 'skewed-clock.mjs')).href}`;
const SKEW_LINE = 'clock skewed by +3600s for this process';

/** How far behind the database's now() a row's stamp may be by the time it is read. The skew is 3600. */
const NEAR_S = 10;
/** A message is read off the topic a little later than its row is. */
const NEAR_TOPIC_S = 30;

// ---------------------------------------------------------------------------
// The child: Auth's emit(), an hour fast, in a transaction that rolls back.
// Everything below it is the parent.
// ---------------------------------------------------------------------------
if (process.argv[2] === '--auth-emit') {
  const out = await authEmit(process.argv[3], process.argv[4]).catch((err) => ({
    error: err instanceof Error ? err.stack : String(err),
  }));
  console.log(`RESULT ${JSON.stringify(out)}`);
  process.exit(0);
}

async function authEmit(id, tenantId) {
  const dist = join(ROOT, 'services', 'auth', 'dist');
  if (!existsSync(join(dist, 'outbox.js'))) throw new Error('services/auth/dist is missing: build Auth first');
  // Auth's own client and Auth's own emit(), so the INSERT is the one the service sends.
  const { prisma } = await import(pathToFileURL(join(dist, 'db.js')).href);
  const { emit } = await import(pathToFileURL(join(dist, 'outbox.js')).href);

  class RolledBack extends Error {}
  const processNow = new Date().toISOString();
  let measured = null;
  try {
    await prisma.$transaction(async (tx) => {
      await tx.$executeRaw`SELECT set_config('app.tenant_id', ${tenantId}, true)`;
      await emit(tx, {
        id,
        tenantId,
        topic: 'auth.email-requested',
        eventType: 'ClockProbe',
        aggregateId: id,
        payload: { probe: id },
      });
      // now() is this transaction's start, the value DEFAULT CURRENT_TIMESTAMP takes in it.
      const rows = await tx.$queryRaw`
        SELECT extract(epoch FROM "createdAt" - now())::float8 AS "createdAt",
               extract(epoch FROM ${processNow}::timestamptz - now())::float8 AS "processClock"
          FROM "OutboxMessage" WHERE id = ${id}`;
      measured = rows[0] ?? null;
      throw new RolledBack();
    });
  } catch (err) {
    if (!(err instanceof RolledBack)) throw err;
  } finally {
    await prisma.$disconnect();
  }
  return { found: measured !== null, ...(measured ?? {}) };
}

// ---------------------------------------------------------------------------
// The parent.
// ---------------------------------------------------------------------------

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const near = (s, within = NEAR_S) => typeof s === 'number' && s > -within && s < 1;
const fmt = (s) => (typeof s === 'number' ? `${s >= 0 ? '+' : ''}${s.toFixed(3)} s` : String(s));

const PUBLIC_B64 = readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64');
const PRIVATE = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
const b64url = (v) => Buffer.from(v).toString('base64url');
/** Good for two hours: each service checks exp on ITS clock, an hour fast. */
function token(env, userId, role) {
  const now = Math.floor(Date.now() / 1000);
  const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const body = b64url(
    JSON.stringify({ sub: userId, tenantId: TENANT, tenantSlug: TENANT.replace(/_/g, '-'), role, iss: env.JWT_ISSUER, aud: env.JWT_AUDIENCE, iat: now, exp: now + 7200 }),
  );
  return `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), PRIVATE))}`;
}

/** Statements inside this run's tenant, the context the service writes in. */
async function inTenant(db, fn) {
  await db.query('BEGIN');
  try {
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
    const out = await fn();
    await db.query('COMMIT');
    return out;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** A service of this run's, an hour fast, on its own port. */
async function start(service, port, overrides = {}) {
  const env = serviceEnv(ROOT, service, { PORT: String(port), AUTH_PUBLIC_KEY_B64: PUBLIC_B64, NODE_OPTIONS: SKEW, ...overrides });
  const taken = await fetch(`http://127.0.0.1:${port}/health`).then(() => true, () => false);
  if (taken) {
    check(false, `something already answers on :${port}; stop it before running this`);
    return { env, up: false, stop: async () => {} };
  }
  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', service), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  let up = false;
  for (let i = 0; i < 80 && !up && child.exitCode === null; i += 1) {
    const r = await fetch(`http://127.0.0.1:${port}/health`).then((x) => x.json(), () => null);
    up = r?.service === service;
    if (!up) await sleep(500);
  }
  check(up, up ? `CONTROL: this run's own ${service} answers on :${port}` : `${service} never came up:\n${log.slice(-1500)}`);
  if (up) check(log.includes(SKEW_LINE), `CONTROL: its clock runs an hour fast, so a stamp from it cannot pass for the database's`);
  const stop = async () => {
    if (child.exitCode === null) {
      child.kill();
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
  };
  return { env, up: up && log.includes(SKEW_LINE), stop, log: () => log };
}

// ---------------------------------------------------------------------------
// STATIC
// ---------------------------------------------------------------------------

/**
 * Prisma schema source with every `//` and `///` comment removed, and string
 * literals kept whole, so a `//` inside one is not taken for a comment.
 */
function stripComments(src) {
  let out = '';
  let inString = false;
  for (let i = 0; i < src.length; i += 1) {
    const c = src[i];
    if (inString) {
      out += c;
      if (c === '\\') out += src[(i += 1)] ?? '';
      else if (c === '"') inString = false;
    } else if (c === '"') {
      inString = true;
      out += c;
    } else if (c === '/' && src[i + 1] === '/') {
      while (i < src.length && src[i] !== '\n') i += 1;
      if (src[i] === '\n') out += '\n';
    } else {
      out += c;
    }
  }
  return out;
}

const NOW_DEFAULT = /@default\s*\(\s*now\s*\(\s*\)\s*\)/;
const DB_DEFAULT = /@default\s*\(\s*dbgenerated\s*\(\s*"CURRENT_TIMESTAMP"\s*\)\s*\)/;
const DATETIME_FIELD = /^\s*(\w+)\s+DateTime[?]?\s+(.*)$/;
const SERVICES_WITH_SCHEMAS = ['auth', 'community', 'event', 'live', 'notification', 'ticketing'];

/**
 * SQL with `--` and `/* *\/` comments removed, and quoted strings and identifiers kept
 * whole. A migration that explains, in a comment, why it does NOT set a default is
 * not one that sets it.
 */
function stripSqlComments(sql) {
  let out = '';
  let quote = null;
  for (let i = 0; i < sql.length; i += 1) {
    const c = sql[i];
    if (quote) {
      out += c;
      if (c === quote) quote = null;
    } else if (c === "'" || c === '"') {
      quote = c;
      out += c;
    } else if (c === '-' && sql[i + 1] === '-') {
      while (i < sql.length && sql[i] !== '\n') i += 1;
      if (sql[i] === '\n') out += '\n';
    } else if (c === '/' && sql[i + 1] === '*') {
      // Its newlines are kept, so a line number after it is still the file's.
      const end = sql.indexOf('*/', i + 2);
      const stop = end === -1 ? sql.length : end + 2;
      out += sql.slice(i, stop).replace(/[^\n]/g, '');
      i = stop - 1;
    } else {
      out += c;
    }
  }
  return out;
}

/**
 * The newest migration in the repository when every schema's timestamp defaults
 * became dbgenerated("CURRENT_TIMESTAMP"). No migration up to it sets one.
 */
const NOISE_AFTER = '20261010090000';
const SET_DEFAULT_NOW = /SET\s+DEFAULT\s+CURRENT_TIMESTAMP/i;

function staticCheck(root = ROOT) {
  console.log('');
  console.log('  every schema: a timestamp default is the database\'s');

  // The detector's own control: a comment is not a use, a string is not a comment.
  const sample = stripComments('a DateTime @default(now()) // @default(now())\n/// @default(now())\nb String @default("x//y")\n');
  check(
    sample === 'a DateTime @default(now()) \n\nb String @default("x//y")\n',
    `CONTROL: comments are stripped before the scan, and a "//" inside a string is kept (${JSON.stringify(sample)})`,
  );

  const schemas = readdirSync(join(root, 'services'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(root, 'services', d.name, 'prisma', 'schema.prisma')))
    .map((d) => d.name)
    .sort();
  check(
    SERVICES_WITH_SCHEMAS.every((s) => schemas.includes(s)),
    `CONTROL: the schemas are read from services/*/prisma/schema.prisma, and the ${schemas.length} found include all six (${schemas.join(', ')})`,
  );

  const judged = {};
  let updatedAt = 0;
  for (const service of schemas) {
    const code = stripComments(readFileSync(join(root, 'services', service, 'prisma', 'schema.prisma'), 'utf8')).split(/\r?\n/);
    const nows = [];
    const defaults = [];
    const others = [];
    code.forEach((line, i) => {
      if (NOW_DEFAULT.test(line)) nows.push(`line ${i + 1}: ${line.trim()}`);
      if (/@updatedAt\b/.test(line)) updatedAt += 1;
      const field = DATETIME_FIELD.exec(line);
      if (field && /@default\s*\(/.test(field[2])) {
        defaults.push(field[1]);
        if (!DB_DEFAULT.test(field[2])) others.push(`line ${i + 1}: ${line.trim()}`);
      }
    });
    judged[service] = defaults.length;
    check(nows.length === 0, `${service}: no @default(now()), which Prisma fills from the process's clock${nows.length ? ` -- ${nows.join('; ')}` : ''}`);
    check(
      others.length === 0,
      `${service}: each of its ${defaults.length} DateTime default(s) is dbgenerated("CURRENT_TIMESTAMP")${others.length ? ` -- not: ${others.join('; ')}` : ''}`,
    );
  }
  check(
    SERVICES_WITH_SCHEMAS.every((s) => judged[s] > 0),
    `CONTROL: the scan found DateTime defaults to judge in each of the six (${Object.entries(judged).map(([s, n]) => `${s} ${n}`).join(', ')})`,
  );
  console.log(`    note  ${updatedAt} @updatedAt column(s) stay on the process's clock: Prisma has no database-side form, and nothing may compare one`);

  /**
   * THE NOISE THE FIX LEAVES BEHIND. Introspection reads DEFAULT CURRENT_TIMESTAMP
   * back as @default(now()), so `prisma migrate diff` and `prisma migrate dev` see a
   * difference that applying it does not remove (measured: the same statements again
   * after running them), and offer `ALTER ... SET DEFAULT CURRENT_TIMESTAMP` for all
   * 34 columns in every migration they write from now on. Each is a default the
   * column already has. Refused in any migration newer than the fix.
   */
  console.log('');
  console.log("  every migration since: none carries Prisma's SET DEFAULT CURRENT_TIMESTAMP noise");
  const flagged = stripSqlComments(`-- SET DEFAULT CURRENT_TIMESTAMP\n/* SET DEFAULT\nCURRENT_TIMESTAMP */\nALTER TABLE "Q" ALTER COLUMN "createdAt" SET DEFAULT CURRENT_TIMESTAMP;\n`);
  const found = [...flagged.matchAll(new RegExp(SET_DEFAULT_NOW, 'gi'))].map((m) => flagged.slice(0, m.index).split('\n').length);
  check(
    found.length === 1 && found[0] === 4,
    `CONTROL: the detector finds the statement, on its own line, and not the same words in a -- or /* */ comment (line(s) ${found.join(', ') || 'none'})`,
  );
  let read = 0;
  let newer = 0;
  const offenders = [];
  for (const service of readdirSync(join(root, 'services')).sort()) {
    const dir = join(root, 'services', service, 'prisma', 'migrations');
    if (!existsSync(dir)) continue;
    for (const name of readdirSync(dir).sort()) {
      const file = join(dir, name, 'migration.sql');
      const stamp = /^(\d{14})_/.exec(name)?.[1];
      if (!stamp || !existsSync(file)) continue;
      read += 1;
      if (stamp <= NOISE_AFTER) continue;
      newer += 1;
      const code = stripSqlComments(readFileSync(file, 'utf8'));
      const hits = [...code.matchAll(new RegExp(SET_DEFAULT_NOW, 'gi'))].map((m) => code.slice(0, m.index).split('\n').length);
      if (hits.length) offenders.push(`services/${service}/prisma/migrations/${name}/migration.sql, line(s) ${hits.join(', ')}`);
    }
  }
  check(read > 0, `CONTROL: ${read} migrations read from services/*/prisma/migrations, ${newer} of them newer than ${NOISE_AFTER}`);
  check(
    offenders.length === 0,
    offenders.length === 0
      ? `no migration newer than ${NOISE_AFTER} sets a timestamp default to CURRENT_TIMESTAMP`
      : `delete the SET DEFAULT CURRENT_TIMESTAMP lines from ${offenders.join('; ')}. They are Prisma's introspection noise: the column's default is already CURRENT_TIMESTAMP, and Prisma will offer them again in every migration`,
  );
}

// ---------------------------------------------------------------------------
// AUTH
// ---------------------------------------------------------------------------

async function authCheck() {
  console.log('');
  console.log("  Auth: emit()'s outbox row");
  const env = serviceEnv(ROOT, 'auth', { NODE_OPTIONS: SKEW });
  const id = `clockprobe_${RUN}`;
  const child = await new Promise((done) => {
    const p = spawn(process.execPath, [THIS_FILE, '--auth-emit', id, TENANT], { cwd: ROOT, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    p.stdout.on('data', (d) => (stdout += d));
    p.stderr.on('data', (d) => (stderr += d));
    p.on('close', (code) => done({ code, stdout, stderr }));
  });
  const line = child.stdout.split(/\r?\n/).find((l) => l.startsWith('RESULT '));
  const out = line ? JSON.parse(line.slice('RESULT '.length)) : { error: `no result (exit ${child.code}): ${child.stderr.slice(-1500)}` };
  check(!out.error && child.code === 0, `CONTROL: Auth's dist/outbox.js emit() ran in a child process${out.error ? `: ${out.error}` : ''}`);
  check(child.stderr.includes(SKEW_LINE), 'CONTROL: its clock runs an hour fast');
  check(
    typeof out.processClock === 'number' && out.processClock > 3590 && out.processClock < 3610,
    `CONTROL: and Postgres puts that process's new Date() an hour ahead of now() (${fmt(out.processClock)}), so the fixture can show the failure`,
  );
  check(out.found === true, 'CONTROL: the row emit() wrote was there to measure, inside the transaction');
  check(
    typeof out.createdAt === 'number' && Math.abs(out.createdAt) < 1,
    `its createdAt is the database's now() (${fmt(out.createdAt)}; from the process's clock it would be +3600)`,
  );

  // Nothing to delete: the transaction rolled back. Asked as the role that wrote it.
  const db = new pg.Client({ connectionString: env.APP_DATABASE_URL });
  await db.connect();
  try {
    const left = await inTenant(db, () => db.query(`SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1`, [TENANT]));
    check(left.rows[0].n === 0, `cleanup: the probe rolled back, and ${left.rows[0].n} outbox row(s) are left in this run's tenant`);
  } finally {
    await db.end();
  }
}

/**
 * The 90-day ceiling on a refresh family is measured from min("createdAt"), which is
 * now Postgres's clock. It was compared with Date.now() in JS, so the ceiling moved by
 * however far Auth's clock and the database's disagree. Each family is aged by SQL, on
 * the database's clock, half an hour either side of the ceiling; Auth is an hour fast,
 * so by its own clock both are past it.
 */
async function authCeilingCheck() {
  console.log('');
  console.log("  Auth: the 90-day ceiling on a refresh family, on the database's clock");
  const svc = await start('auth', 3994);
  const db = new pg.Client({ connectionString: svc.env.APP_DATABASE_URL });
  await db.connect();
  const families = [];
  let successors = 0;
  try {
    if (!svc.up) return;
    const post = async (path, body, headers = {}) => {
      const res = await fetch(`http://127.0.0.1:3994${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...headers },
        body: JSON.stringify(body),
      });
      return { status: res.status, body: await res.json().catch(() => null) };
    };
    // The seeded account (services/auth/prisma/seed.ts), which every run of CI seeds.
    const login = () => post('/login', { email: 'alice@acme.com', password: 'hunter2222' }, { 'X-Tenant-Slug': 'acme' });
    const sha256 = (t) => createHash('sha256').update(t).digest('hex');
    /** Ages the token's whole family to 90 days plus `offset`, on the database's clock. */
    const age = async (token, offset) => {
      const r = await db.query(
        `UPDATE "RefreshToken" SET "createdAt" = now() - interval '90 days' + $2::interval
          WHERE "familyId" = (SELECT "familyId" FROM "RefreshToken" WHERE "tokenHash" = $1)`,
        [sha256(token), offset],
      );
      const f = await db.query(`SELECT "familyId" FROM "RefreshToken" WHERE "tokenHash" = $1`, [sha256(token)]);
      if (f.rows[0]) families.push(f.rows[0].familyId);
      return r.rowCount;
    };

    const inside = await login();
    const outside = await login();
    check(
      inside.status === 200 && outside.status === 200 && Boolean(inside.body?.refreshToken && outside.body?.refreshToken),
      `CONTROL: the seeded account signs in twice, two families (${inside.status}, ${outside.status})`,
    );
    if (!inside.body?.refreshToken || !outside.body?.refreshToken) return;
    const agedInside = await age(inside.body.refreshToken, '30 minutes');
    const agedOutside = await age(outside.body.refreshToken, '-30 minutes');
    check(agedInside === 1 && agedOutside === 1, `CONTROL: each family is aged by SQL (${agedInside}, ${agedOutside} row)`);

    const kept = await post('/refresh', { refreshToken: inside.body.refreshToken });
    if (kept.status === 200) successors += 1;
    check(
      kept.status === 200,
      `a family 30 minutes short of 90 days by the database's clock still refreshes (${kept.status}; by this Auth's clock, an hour fast, it is 30 minutes past)`,
    );
    const refused = await post('/refresh', { refreshToken: outside.body.refreshToken });
    if (refused.status === 200) successors += 1;
    check(refused.status === 401, `CONTROL: one 30 minutes past 90 days is refused, so the ceiling still holds (${refused.status})`);
  } catch (err) {
    check(false, `Auth's ceiling stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    await svc.stop();
    // RefreshToken is read before any tenant exists, so it has no tenant policy; the
    // two families are this run's by id.
    const deleted = await db
      .query(`DELETE FROM "RefreshToken" WHERE "familyId" = ANY($1)`, [families])
      .then((r) => r.rowCount, (err) => err.message);
    const left = await db
      .query(`SELECT count(*)::int AS n FROM "RefreshToken" WHERE "familyId" = ANY($1)`, [families])
      .then((r) => r.rows[0].n, () => -1);
    if (families.length) {
      // One per family, and a successor for each refresh that was let through.
      const expected = families.length + successors;
      check(deleted === expected && left === 0, `cleanup: deleted ${deleted} of ${expected} refresh token(s) of this run's ${families.length} families; ${left} left`);
    }
    await db.end();
  }
}

// ---------------------------------------------------------------------------
// EVENT
// ---------------------------------------------------------------------------

async function eventCheck() {
  console.log('');
  console.log('  Event: an event created through the API');
  const svc = await start('event', 3992);
  const db = new pg.Client({ connectionString: svc.env.APP_DATABASE_URL });
  await db.connect();
  let eventId = null;
  try {
    if (!svc.up) return;
    const res = await fetch('http://127.0.0.1:3992/events', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token(svc.env, `user_times_organiser_${RUN}`, 'ORGANIZER')}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ title: `Times ${RUN}`, capacity: 10, startsAt: '2027-06-01T10:00:00.000Z', endsAt: '2027-06-01T18:00:00.000Z' }),
    });
    const body = await res.json().catch(() => null);
    eventId = body?.id ?? null;
    check(res.status === 201 && typeof eventId === 'string', `CONTROL: an organiser creates an event (${res.status} ${eventId})`);
    if (!eventId) return;
    const r = (
      await inTenant(db, () =>
        db.query(
          `SELECT extract(epoch FROM e."createdAt" - now())::float8 AS "event",
                  (SELECT extract(epoch FROM o."createdAt" - now())::float8 FROM "OutboxMessage" o
                    WHERE o."tenantId" = e."tenantId" AND o."aggregateId" = e.id AND o."eventType" = 'event.snapshot') AS "outbox",
                  (SELECT count(*) FROM "OutboxMessage" o WHERE o."tenantId" = e."tenantId" AND o."aggregateId" = e.id)::int AS "rows"
             FROM "Event" e WHERE e.id = $1`,
          [eventId],
        ),
      )
    ).rows[0] ?? {};
    check(r.rows === 1, `CONTROL: its snapshot is in the outbox, one row (${r.rows})`);
    check(near(r.outbox), `the outbox row's createdAt, published as the event's occurredAt, is the database's now() (${fmt(r.outbox)})`);
    check(near(r.event), `the event's createdAt is the database's now() (${fmt(r.event)})`);
  } catch (err) {
    check(false, `Event stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    await svc.stop();
    const deleted = await inTenant(db, async () => ({
      OutboxMessage: (await db.query(`DELETE FROM "OutboxMessage" WHERE "tenantId" = $1`, [TENANT])).rowCount,
      Event: (await db.query(`DELETE FROM "Event" WHERE "tenantId" = $1`, [TENANT])).rowCount,
    })).catch((err) => ({ error: err.message }));
    const left = await inTenant(db, () =>
      db.query(`SELECT (SELECT count(*) FROM "Event" WHERE "tenantId" = $1)::int + (SELECT count(*) FROM "OutboxMessage" WHERE "tenantId" = $1)::int AS n`, [TENANT]),
    ).then((x) => x.rows[0].n, () => -1);
    if (eventId) {
      check(
        !deleted.error && deleted.Event === 1 && deleted.OutboxMessage === 1 && left === 0,
        `cleanup: deleted ${deleted.error ?? `${deleted.Event} event and ${deleted.OutboxMessage} outbox row`}; ${left} left`,
      );
    }
    await db.end();
  }
}

// ---------------------------------------------------------------------------
// TICKETING
// ---------------------------------------------------------------------------

async function ticketingCheck(groups) {
  console.log('');
  console.log('  Ticketing: a hold taken through the API');
  const group = `verify-event-times-ticketing-${RUN}`;
  groups.push(group);
  const event = `evt_times_${RUN}`;
  const type = `tt_times_${RUN}`;
  const env = serviceEnv(ROOT, 'ticketing', {});
  const db = new pg.Client({ connectionString: env.APP_DATABASE_URL });
  await db.connect();
  let svc = null;
  let reservationId = null;
  try {
    await inTenant(db, async () => {
      await db.query(
        `INSERT INTO "EventProjection" (id, "tenantId", title, slug, status, capacity, "startsAt", "endsAt", "organiserId", deleted, "sourceVersion", "updatedAt")
         VALUES ($1, $2, $1, $1, 'PUBLISHED', 100, now() + interval '30 days', now() + interval '31 days', $3, false, 1, now())`,
        [event, TENANT, `user_times_owner_${RUN}`],
      );
      await db.query(
        `INSERT INTO "TicketType" (id, "tenantId", "eventId", name, "priceCents", total, remaining, held, sold, "updatedAt")
         VALUES ($1, $2, $3, $1, 1000, 10, 10, 0, 0, now())`,
        [type, TENANT, event],
      );
    });

    svc = await start('ticketing', 3991, { KAFKA_CONSUMER_GROUP: group, PROJECTION_START_FROM: 'latest' });
    if (!svc.up) return;
    const res = await fetch('http://127.0.0.1:3991/reservations', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token(svc.env, `user_times_buyer_${RUN}`, 'ATTENDEE')}`,
        'Content-Type': 'application/json',
        'Idempotency-Key': `times-${RUN}`,
      },
      body: JSON.stringify({ eventId: event, items: [{ ticketTypeId: type, quantity: 1 }] }),
    });
    const body = await res.json().catch(() => null);
    reservationId = body?.id ?? null;
    check(res.status === 201 && typeof reservationId === 'string', `CONTROL: a buyer takes a hold (${res.status} ${reservationId})`);
    if (!reservationId) return;
    const r = (
      await inTenant(db, () =>
        db.query(
          `SELECT extract(epoch FROM r."createdAt" - now())::float8 AS "reservation",
                  (SELECT extract(epoch FROM o."createdAt" - now())::float8 FROM "OutboxMessage" o
                    WHERE o."tenantId" = r."tenantId" AND o."eventType" = 'reservation.held' AND o.payload->>'reservationId' = r.id) AS "outbox",
                  (SELECT count(*) FROM "OutboxMessage" o
                    WHERE o."tenantId" = r."tenantId" AND o."eventType" = 'reservation.held' AND o.payload->>'reservationId' = r.id)::int AS "rows"
             FROM "Reservation" r WHERE r.id = $1`,
          [reservationId],
        ),
      )
    ).rows[0] ?? {};
    check(r.rows === 1, `CONTROL: reservation.held is in the outbox, one row (${r.rows})`);
    check(near(r.outbox), `the reservation.held outbox row's createdAt, published as occurredAt, is the database's now() (${fmt(r.outbox)})`);
    check(near(r.reservation), `the reservation's createdAt, which a buyer is shown and pages by, is the database's now() (${fmt(r.reservation)})`);
  } catch (err) {
    check(false, `Ticketing stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    if (svc) await svc.stop();
    // No payment was made, so everything this run wrote can go.
    const deleted = await inTenant(db, async () => {
      const n = {};
      for (const table of ['ReservationItem', 'Reservation', 'TicketType', 'EventProjection', 'IdempotencyKey', 'OutboxMessage']) {
        n[table] = (await db.query(`DELETE FROM "${table}" WHERE "tenantId" = $1`, [TENANT])).rowCount;
      }
      return n;
    }).catch((err) => ({ error: err.message }));
    const left = await inTenant(db, () =>
      db.query(
        `SELECT (SELECT count(*) FROM "Reservation" WHERE "tenantId" = $1)::int + (SELECT count(*) FROM "TicketType" WHERE "tenantId" = $1)::int
              + (SELECT count(*) FROM "EventProjection" WHERE "tenantId" = $1)::int + (SELECT count(*) FROM "OutboxMessage" WHERE "tenantId" = $1)::int
              + (SELECT count(*) FROM "Payment" WHERE "tenantId" = $1)::int AS n`,
        [TENANT],
      ),
    ).then((x) => x.rows[0].n, () => -1);
    check(
      !deleted.error && deleted.EventProjection === 1 && deleted.TicketType === 1 && (reservationId === null || (deleted.Reservation === 1 && deleted.OutboxMessage === 1)) && left === 0,
      `cleanup: deleted ${deleted.error ?? Object.entries(deleted).map(([t, n]) => `${n} ${t}`).join(', ')}; ${left} left, payments included`,
    );
    await db.end();
  }
}

// ---------------------------------------------------------------------------
// LIVE
// ---------------------------------------------------------------------------

function connect(port, tok) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/live`, ['eventhub.v1', `eh.token.${tok}`]);
  const seen = [];
  ws.on('message', (raw) => {
    try {
      seen.push(JSON.parse(raw.toString()));
    } catch {
      seen.push({ t: 'unparseable' });
    }
  });
  const ready = new Promise((res, rej) => {
    ws.once('open', () => res(ws));
    ws.once('error', rej);
    ws.once('close', (code) => rej(new Error(`closed before open: ${code}`)));
  });
  return { ws, seen, ready, of: (t) => seen.filter((m) => m.t === t) };
}
async function waitFor(fn, ms) {
  const until = Date.now() + ms;
  for (;;) {
    const v = fn();
    if (v || Date.now() >= until) return v;
    await sleep(100);
  }
}

async function liveCheck(kafka, admin, groups) {
  console.log('');
  console.log('  Live: a question and an upvote, and what reached live.interactions');
  const TOPIC = 'live.interactions';
  const group = `verify-event-times-live-${RUN}`;
  groups.push(group);
  const eventId = `evt_times_live_${RUN}`;
  const env = serviceEnv(ROOT, 'live', {});
  const db = new pg.Client({ connectionString: env.APP_DATABASE_URL });
  await db.connect();
  const found = [];
  let consumer = null;
  let svc = null;
  let client = null;
  let questionId = null;
  try {
    // From where every partition ends now, on a group of this run's.
    const before = await admin.fetchTopicOffsets(TOPIC);
    consumer = kafka.consumer({ groupId: group });
    let joined = false;
    consumer.on(consumer.events.GROUP_JOIN, () => (joined = true));
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC });
    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const m = JSON.parse(message.value.toString());
          if (m?.tenantId === TENANT) found.push(m);
        } catch {}
      },
    });
    for (const p of before) consumer.seek({ topic: TOPIC, partition: p.partition, offset: p.high });
    check(await waitFor(() => joined, 30_000), `CONTROL: a consumer of this run's joined ${TOPIC}, from its end`);

    svc = await start('live', 3993, { LIVE_DRAIN_DELAY_MS: '0' });
    if (!svc.up) return;
    client = connect(3993, token(svc.env, `user_times_asker_${RUN}`, 'ATTENDEE'));
    await client.ready;
    client.ws.send(JSON.stringify({ t: 'subscribe', eventId }));
    await waitFor(() => client.of('subscribed').length > 0, 5_000);
    client.ws.send(JSON.stringify({ t: 'question.create', eventId, body: `what time is it? ${RUN}`, clientMsgId: randomUUID() }));
    await waitFor(() => client.of('question.created').length > 0, 10_000);
    questionId = client.of('question.created')[0]?.question?.id ?? null;
    check(typeof questionId === 'string', `CONTROL: the question was stored and came back over the socket (${questionId})`);
    if (!questionId) return;
    client.ws.send(JSON.stringify({ t: 'question.upvote', eventId, questionId }));
    const upvoted = await waitFor(() => client.of('question.upvoted').find((m) => m.id === questionId), 10_000);
    check(upvoted?.voteCount === 1, `CONTROL: and upvoted, its first vote (${JSON.stringify(upvoted ?? client.of('error'))})`);

    // The rows at once, before any wait for the topic.
    const rows = (
      await inTenant(db, () =>
        db.query(
          `SELECT extract(epoch FROM q."createdAt" - now())::float8 AS "question",
                  extract(epoch FROM v."createdAt" - now())::float8 AS "vote"
             FROM "Question" q JOIN "QuestionVote" v ON v."questionId" = q.id
            WHERE q.id = $1`,
          [questionId],
        ),
      )
    ).rows[0] ?? {};
    check(near(rows.question), `the question's createdAt, which the list is ordered by, is the database's now() (${fmt(rows.question)})`);
    check(near(rows.vote), `the vote's createdAt is the database's now() (${fmt(rows.vote)})`);

    const kinds = () => new Set(found.filter((m) => m.questionId === questionId).map((m) => m.kind));
    await waitFor(() => kinds().has('question.created') && kinds().has('question.upvoted'), 30_000);
    const created = found.find((m) => m.questionId === questionId && m.kind === 'question.created');
    const vote = found.find((m) => m.questionId === questionId && m.kind === 'question.upvoted');
    check(Boolean(created && vote), `CONTROL: both interactions reached ${TOPIC} (${[...kinds()].join(', ') || 'neither'})`);

    const r = (
      await inTenant(db, () =>
        db.query(
          `SELECT extract(epoch FROM $2::timestamptz - now())::float8 AS "createdMessage",
                  extract(epoch FROM $3::timestamptz - now())::float8 AS "voteMessage",
                  to_char(q."createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "questionAt",
                  to_char(v."createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "voteAt"
             FROM "Question" q JOIN "QuestionVote" v ON v."questionId" = q.id
            WHERE q.id = $1`,
          [questionId, created?.occurredAt ?? null, vote?.occurredAt ?? null],
        ),
      )
    ).rows[0] ?? {};
    check(
      near(r.createdMessage, NEAR_TOPIC_S) && created?.occurredAt === r.questionAt,
      `question.created's occurredAt is the database's: ${fmt(r.createdMessage)} from now(), and the question's own createdAt (${created?.occurredAt} = ${r.questionAt})`,
    );
    check(
      near(r.voteMessage, NEAR_TOPIC_S) && vote?.occurredAt === r.voteAt,
      `question.upvoted's occurredAt is the database's: ${fmt(r.voteMessage)} from now(), and the vote's own createdAt (${vote?.occurredAt} = ${r.voteAt})`,
    );
  } catch (err) {
    check(false, `Live stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    client?.ws.close();
    if (svc) await svc.stop();
    if (consumer) await consumer.disconnect().catch(() => {});
    await db.end();
    // The app role may not delete a question. The owner may, inside this run's tenant:
    // FORCE binds the owner to the same policy, so without the tenant it would match none.
    const owner = new pg.Client({ connectionString: env.DATABASE_URL });
    await owner.connect();
    const deleted = await inTenant(owner, async () => ({
      QuestionVote: (await owner.query(`DELETE FROM "QuestionVote" WHERE "tenantId" = $1`, [TENANT])).rowCount,
      Question: (await owner.query(`DELETE FROM "Question" WHERE "tenantId" = $1`, [TENANT])).rowCount,
    })).catch((err) => ({ error: err.message }));
    const left = await inTenant(owner, () => owner.query(`SELECT count(*)::int AS n FROM "Question" WHERE "tenantId" = $1`, [TENANT])).then((x) => x.rows[0].n, () => -1);
    await owner.end();
    if (questionId) {
      check(
        !deleted.error && deleted.Question === 1 && deleted.QuestionVote === 1 && left === 0,
        `cleanup: deleted ${deleted.error ?? `${deleted.Question} question and ${deleted.QuestionVote} vote`}; ${left} left`,
      );
    }
  }
}

async function main() {
  // `--static [root]`: the static half only, against another tree's services/ if one is
  // named. How a planted migration is shown red without committing it.
  if (process.argv[2] === '--static') {
    staticCheck(process.argv[3] ? resolve(process.argv[3]) : ROOT);
    console.log('');
    if (failures > 0) console.error(`FAIL: ${failures} assertion(s) failed`);
    else console.log('the static half holds');
    return;
  }

  console.log(`  run ${RUN}, tenant ${TENANT}`);
  staticCheck();

  const brokers = serviceEnv(ROOT, 'ticketing', {}).KAFKA_BROKERS.split(',');
  const kafka = new Kafka({ clientId: `verify-event-times-${RUN}`, brokers, logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  const groups = [];
  try {
    for (const run of [authCheck, authCeilingCheck, eventCheck, () => ticketingCheck(groups), () => liveCheck(kafka, admin, groups)]) {
      try {
        await run();
      } catch (err) {
        check(false, `a section stopped early: ${err instanceof Error ? err.stack : String(err)}`);
      }
    }
  } finally {
    const left = await deleteGroupsWhenEmpty(admin, groups).catch((err) => [`${groups.join(', ')} (${err.message})`]);
    if (left.length) console.log(`    note  consumer group(s) left to expire: ${left.join(', ')}`);
    await admin.disconnect();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log("every row stamp and event time checked here is the database's clock, whatever the app server's says");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
