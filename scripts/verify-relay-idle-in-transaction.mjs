/**
 * Asserts that a connection the database terminates WHILE CHECKED OUT does not
 * kill the process — the half verify-pool-resilience.mjs cannot see.
 *
 * THE CRASH, observed during a verify-community-posts.mjs run:
 *
 *   error: terminating connection due to idle-in-transaction timeout
 *   Emitted 'error' event on Client instance at:
 *       at Client._handleErrorEvent (pg/lib/client.js:417:10)
 *   severity: 'FATAL'
 *
 * The relay exited 1 mid-drain and left the row unpublished with lastError
 * null. db/init/01-bootstrap.sql gives every relay and sweeper role
 * idle_in_transaction_session_timeout = '30s', on purpose: an open transaction
 * pins xmin and blocks VACUUM across the cluster. The relay did not survive the
 * valve firing.
 *
 * WHY THE EXISTING GUARD MISSED IT. attachPoolErrorHandler listens on the POOL,
 * and pg-pool forwards only errors from IDLE clients. On checkout it removes its
 * idle listener from the Client and re-adds it on release, so a checked-out
 * Client has no 'error' listener at all. pg's Client emits 'error' only when no
 * query is in flight — with one in flight it rejects the query instead — and "no
 * query in flight, transaction open" is the definition of idle in transaction.
 * The comment on the guard said the opposite, which is what stopped anyone
 * looking. withClient() is the fix, in each of the three services that carry a
 * pool helper.
 *
 * FORCED, NOT WAITED FOR. Each connection here asks for a 1-second timeout in
 * its startup packet (`options=-c idle_in_transaction_session_timeout=1000`),
 * which overrides the role's 30s for that session only. Nothing global changes,
 * and the session's setting is read back and asserted — 30s is the role's
 * default, so seeing 1s is what proves the override applied.
 *
 * FIVE PARTS, each with its own control:
 *
 *   A  the unguarded shape dies. A child process does exactly what the three
 *      relay call sites did — pool.connect(), BEGIN, go idle — and must exit on
 *      an unhandled 'error'. This is the negative control that runs every time:
 *      if the timeout stopped firing, or pg stopped emitting there, B would pass
 *      for a reason that has nothing to do with withClient.
 *   B  the same thing through every withClient in the repository survives,
 *      logs the SQLSTATE, and the pool serves the next checkout.
 *   C  withClient removes its listener. pg reuses Client objects, so one added
 *      per checkout accumulates; counted over twelve checkouts of ONE client.
 *   D  the shipped drain, holding a real outbox row across a publish, is
 *      terminated mid-publish, rolls back, and the next pass publishes the row.
 *   E  a ratchet: every pool.connect() in services/ is inside a withClient.
 *
 * Run: node scripts/verify-relay-idle-in-transaction.mjs
 */

import { spawn } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';

const APP_URL =
  process.env['EVENT_APP_DATABASE_URL'] ??
  'postgresql://eventhub_event_app:eventhub_event_app@localhost:5432/eventhub_event';
const RELAY_URL =
  process.env['RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_event_relay:eventhub_event_relay@localhost:5432/eventhub_event';

/** This session only: a 1s valve instead of the role's 30s. */
const FAST = `${RELAY_URL}${RELAY_URL.includes('?') ? '&' : '?'}options=${encodeURIComponent(
  '-c idle_in_transaction_session_timeout=1000',
)}`;

/** Resolved from this file, not cwd — a scanning verifier must know where the tree is. */
const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const THIS_FILE = fileURLToPath(import.meta.url);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(predicate, ms) {
  const until = Date.now() + ms;
  while (Date.now() < until) {
    if (predicate()) return true;
    await sleep(25);
  }
  return predicate();
}

// ---------------------------------------------------------------------------
// The child: the pre-fix shape, run in its own process because it is meant to
// die. Everything below `main` is the parent.
// ---------------------------------------------------------------------------
if (process.argv.includes('--unguarded-checkout')) {
  const pool = new pg.Pool({ connectionString: FAST, application_name: 'relay-idle-verify-child', max: 1 });
  // What every pool in the repository already had: the IDLE half of the guard.
  pool.on('error', (err) => console.log(`POOL_HANDLER_SAW ${err.code}`));
  const client = await pool.connect();
  const { rows } = await client.query(`SELECT current_setting('idle_in_transaction_session_timeout') AS t`);
  console.log(`CHILD_TIMEOUT ${rows[0].t}`);
  await client.query('BEGIN');
  await client.query('SELECT 1');
  await sleep(3000);
  console.log('CHILD_SURVIVED');
  client.release();
  await pool.end();
  process.exit(0);
}

const tenantId = 'tenant_acme';
const PREFIX = 'idleverify_';
const run = `${PREFIX}${Date.now()}`;

/**
 * Sets the exit code where the check fails, not in a summary at the end — a
 * crash later in the run must not be able to leave a FAIL printed and exit 0.
 */
let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/** A logger that remembers what it was told, so "it logged the termination" is a fact. */
function capture() {
  const lines = [];
  const logger = {
    debug() {},
    info() {},
    warn(o, m) {
      lines.push({ level: 'warn', ...o, msg: m });
    },
    error(o, m) {
      lines.push({ level: 'error', ...o, msg: m });
    },
    child() {
      return logger;
    },
  };
  return { logger, lines, saw: (code) => lines.some((l) => l.code === code) };
}

// Node prints this when listeners pile up on one emitter; part C watches for it.
const warnings = [];
process.on('warning', (w) => warnings.push(w.name));

// ---------------------------------------------------------------------------
// Where the helpers are: derived from the source, never listed by hand.
// ---------------------------------------------------------------------------
function sourceFiles(dir, out = []) {
  for (const entry of readdirSync(dir)) {
    if (entry === 'node_modules' || entry === 'dist' || entry === 'generated') continue;
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) sourceFiles(full, out);
    else if (entry.endsWith('.ts')) out.push(full);
  }
  return out;
}

const DEFINITION = 'export async function withClient';
const files = sourceFiles(join(REPO_ROOT, 'services'));
const helperSources = files.filter((f) => readFileSync(f, 'utf8').includes(DEFINITION));

// ---------------------------------------------------------------------------
// A. The unguarded shape dies.
// ---------------------------------------------------------------------------
async function partA() {
  console.log('  A. a checked-out client terminated between queries kills an unguarded process');

  const child = spawn(process.execPath, [THIS_FILE, '--unguarded-checkout'], {
    env: { ...process.env, RELAY_DATABASE_URL: RELAY_URL },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stdout = '';
  let stderr = '';
  child.stdout.on('data', (d) => (stdout += d));
  child.stderr.on('data', (d) => (stderr += d));
  const killer = setTimeout(() => child.kill(), 20_000);
  const code = await new Promise((r) => child.on('close', r));
  clearTimeout(killer);

  const timeout = /CHILD_TIMEOUT (\S+)/.exec(stdout)?.[1] ?? '(never read)';
  check(timeout === '1s', `the child's session carries the forced 1s timeout (read back: ${timeout}; the role's own is 30s)`);
  check(code !== 0 && code !== null, `the child exited ${code} — the unguarded checkout is fatal`);
  check(
    stderr.includes("Unhandled 'error' event") && stderr.includes('idle-in-transaction timeout'),
    stderr.includes("Unhandled 'error' event")
      ? "and it died of exactly the reported crash: Unhandled 'error' event, idle-in-transaction timeout"
      : `and it did NOT die of the reported crash. stderr: ${stderr.split(/\r?\n/).slice(0, 4).join(' | ')}`,
  );
  check(
    !stdout.includes('POOL_HANDLER_SAW'),
    "pool.on('error') never saw it — the pool-level guard cannot cover a checked-out client",
  );
  check(!stdout.includes('CHILD_SURVIVED'), 'the child never reached the line after the idle wait');
}

// ---------------------------------------------------------------------------
// B and C, for each withClient the source defines.
// ---------------------------------------------------------------------------
function distOf(src) {
  const rel = relative(REPO_ROOT, src).split(/[\\/]/); // services/<svc>/src/...
  const i = rel.indexOf('src');
  return join(REPO_ROOT, ...rel.slice(0, i), 'dist', ...rel.slice(i + 1)).replace(/\.ts$/, '.js');
}

async function partB(label, withClient) {
  const cap = capture();
  const pool = new pg.Pool({ connectionString: FAST, application_name: 'relay-idle-verify', max: 1 });
  pool.on('error', () => {}); // the idle half, as every real pool has

  let setting = null;
  let rejected = null;
  try {
    await withClient(pool, cap.logger, label, async (client) => {
      const { rows } = await client.query(
        `SELECT current_setting('idle_in_transaction_session_timeout') AS t`,
      );
      setting = rows[0].t;
      await client.query('BEGIN');
      await client.query('SELECT 1');
      // Idle in transaction until the server kills it. Without withClient's
      // listener the process dies right here, and this verifier with it.
      await waitFor(() => cap.saw('25P03'), 6000);
      await client.query('SELECT 2');
      await client.query('COMMIT');
    });
  } catch (err) {
    rejected = err;
  }

  check(setting === '1s', `${label}: the session carries the forced 1s timeout (read back: ${setting})`);
  check(cap.saw('25P03'), `${label}: the termination was logged with its SQLSTATE, 25P03`);
  check(
    rejected !== null,
    rejected
      ? `${label}: the next query on the dead client rejected ("${rejected.message}"), so the caller's own rollback-and-rethrow runs`
      : `${label}: the query after the idle wait SUCCEEDED — the timeout never fired, so nothing above was tested`,
  );

  const next = await withClient(pool, cap.logger, label, (c) => c.query('SELECT 1 AS ok'));
  check(next.rows[0]?.ok === 1, `${label}: the pool served the next checkout on a fresh connection`);
  await pool.end();
}

async function partC(label, withClient) {
  const cap = capture();
  const pool = new pg.Pool({ connectionString: RELAY_URL, application_name: 'relay-idle-verify', max: 1 });
  pool.on('error', () => {});

  const clients = new Set();
  const counts = [];
  for (let i = 0; i < 12; i += 1) {
    const c = await withClient(pool, cap.logger, label, async (client) => client);
    clients.add(c);
    counts.push(c.listenerCount('error'));
  }
  await pool.end();

  // The control: counting listeners on twelve DIFFERENT clients would show one
  // each and prove nothing about accumulation.
  check(clients.size === 1, `${label}: twelve checkouts reused one Client object (${clients.size} distinct)`);
  check(
    counts.every((n) => n === counts[0]),
    `${label}: 'error' listeners on it after each release: ${counts.join(',')} — must not grow`,
  );
}

// ---------------------------------------------------------------------------
// D. The shipped drain.
// ---------------------------------------------------------------------------
/** A payload the real relay can encode, so the compose relay taking it harms nothing. */
function payload(eventId) {
  return {
    eventId,
    tenantId,
    title: 'Idle Transaction Check',
    slug: 'idle-transaction-check',
    description: null,
    status: 'PUBLISHED',
    capacity: 42,
    startsAt: '2027-06-01T10:00:00.000Z',
    endsAt: '2027-06-01T18:00:00.000Z',
    organiserId: 'u_idle_verify',
    version: 1,
    changeType: 'PUBLISHED',
    deleted: false,
  };
}

async function asApp(app, fn) {
  await app.query('BEGIN');
  await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
  try {
    const r = await fn();
    await app.query('COMMIT');
    return r;
  } catch (err) {
    await app.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

async function partD(relay, app) {
  console.log('');
  console.log('  D. the shipped drain, terminated mid-publish, rolls back and publishes on the next pass');

  const id = `${run}_v1`;
  await asApp(app, () =>
    app.query(
      `INSERT INTO "OutboxMessage"
         (id, "tenantId", topic, "eventType", "schemaVersion", "aggregateId", payload)
       VALUES ($1, $2, 'events.activity', 'event.snapshot', 1, $3, $4::jsonb)`,
      [id, tenantId, run, JSON.stringify(payload(run))],
    ),
  );

  const cap = capture();
  const pool = relay.createPool(FAST, cap.logger);
  const opts = { batchSize: 100, maxAttempts: 5 };

  /**
   * The stub never lets a FOREIGN row be marked published: it throws, which is
   * a transient failure and costs that row a short backoff. Returning success
   * for a row this run did not write would mark a real message delivered
   * without sending it.
   */
  let reached = false;
  const holding = {
    async publish(row) {
      if (row.aggregateId !== run) throw new Error('not this verifier\'s row (deliberately refused)');
      reached = true;
      // Hold the transaction idle until the server kills it, then return at
      // once: the row lock went with the backend, and every millisecond after
      // that is a window for another relay to take the row.
      await waitFor(() => cap.saw('25P03'), 6000);
    },
  };

  let firstPass = null;
  try {
    await relay.drainOnce(pool, holding, cap.logger, opts);
    firstPass = 'resolved';
  } catch (err) {
    firstPass = err.message;
  }

  check(reached, 'the fixture control: the first pass claimed THIS row and reached its publish');
  check(cap.saw('25P03'), 'the termination arrived mid-publish and was logged with its SQLSTATE, 25P03');
  check(
    firstPass !== 'resolved' && firstPass !== null,
    `the first pass REJECTED rather than exiting the process ("${firstPass}"), which the relay loop logs and retries`,
  );

  const { rows: between } = await asApp(app, () =>
    app.query(
      `SELECT "publishedAt" IS NULL AS unpublished, "deadLetteredAt" IS NULL AS alive, attempts
         FROM "OutboxMessage" WHERE id = $1`,
      [id],
    ),
  );
  check(
    between[0]?.unpublished === true && between[0]?.alive === true && between[0]?.attempts === 0,
    `the transaction rolled back: row unpublished, not dead-lettered, attempts 0 (${JSON.stringify(between[0])})`,
  );

  const sent = [];
  const recording = {
    async publish(row) {
      if (row.aggregateId !== run) throw new Error('not this verifier\'s row (deliberately refused)');
      sent.push(row.id);
    },
  };
  await relay.drainOnce(pool, recording, cap.logger, opts);

  const { rows: after } = await asApp(app, () =>
    app.query(`SELECT "publishedAt" IS NOT NULL AS published FROM "OutboxMessage" WHERE id = $1`, [id]),
  );
  check(
    sent.includes(id) && after[0]?.published === true,
    sent.includes(id)
      ? 'the next pass, on the same pool, published the row and marked it'
      : `the next pass did not publish the row (published=${after[0]?.published}). If published is true, ` +
          'another relay took it in the gap — the compose relay drains this outbox; CI runs none',
  );
  await pool.end();
}

// ---------------------------------------------------------------------------
// E. The ratchet.
// ---------------------------------------------------------------------------
function partE() {
  console.log('');
  console.log('  E. every pool checkout in services/ goes through a withClient');

  const CHECKOUT = /\b\w*[Pp]ool\.connect\s*\(/;
  const inside = [];
  const outside = [];
  let calls = 0;

  for (const file of files) {
    const lines = readFileSync(file, 'utf8').split(/\r?\n/);
    let defAt = -Infinity;
    lines.forEach((raw, i) => {
      // Comments do not check anything out; the doc comments quote the call.
      const trimmed = raw.trim();
      if (trimmed.startsWith('*') || trimmed.startsWith('//') || trimmed.startsWith('/*')) return;
      const line = raw.split('//')[0];
      if (line.includes(DEFINITION)) defAt = i;
      else if (/\bwithClient\s*\(/.test(line)) calls += 1;
      if (!CHECKOUT.test(line)) return;
      const where = `${relative(REPO_ROOT, file).replace(/\\/g, '/')}:${i + 1}`;
      // The one checkout allowed: withClient's own, in the first lines of its body.
      if (i - defAt <= 8) inside.push(where);
      else outside.push(where);
    });
  }

  check(files.length > 20, `scanned ${files.length} source files`);
  check(
    inside.length === helperSources.length && inside.length >= 3,
    `found the checkout inside each of the ${helperSources.length} withClient definitions (${inside.length}) — the scan matches real code`,
  );
  check(calls > 0, `${calls} call site(s) go through withClient`);
  check(
    outside.length === 0,
    outside.length === 0
      ? 'no checkout bypasses it'
      : `checkouts with no 'error' listener while held — each is one terminated connection away from exiting:\n        ${outside.join('\n        ')}`,
  );
}

// ---------------------------------------------------------------------------
async function main() {
  const app = new pg.Client({ connectionString: APP_URL });
  app.on('error', (err) => console.log(`    (app client saw ${err.code ?? err.message})`));
  await app.connect();

  const relayDb = new pg.Client({ connectionString: RELAY_URL });
  relayDb.on('error', () => {});
  await relayDb.connect();

  try {
    /**
     * Leftovers from a run that died — which is what part D does against a
     * relay without the fix. Removed as the app role WITH the tenant set, the
     * context they were written in, and reported by what was deleted.
     */
    const stale = await asApp(app, () =>
      app.query(`DELETE FROM "OutboxMessage" WHERE "aggregateId" LIKE $1`, [`${PREFIX}%`]),
    );
    if (stale.rowCount > 0) console.log(`  (removed ${stale.rowCount} row(s) left by an earlier run)`);

    /**
     * Part D drains with a stub. It refuses foreign rows, but a pending row
     * that is not ours would still be claimed and backed off by a test, so an
     * outbox with real work in it is not one to run against.
     */
    const { rows: pending } = await relayDb.query(
      `SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL`,
    );
    if (pending[0].n > 0) {
      console.error(
        `REFUSING TO RUN: ${pending[0].n} undelivered outbox row(s) are pending. Part D drains with a stub ` +
          'publisher; run the real relay first (node services/relay/dist/index.js --once), then re-run.',
      );
      process.exitCode = 1;
      return;
    }

    await partA();

    console.log('');
    console.log('  B. the same termination through every withClient: survived, logged, pool still serves');
    const helpers = [];
    for (const src of helperSources) {
      const dist = distOf(src);
      const label = relative(REPO_ROOT, src).replace(/\\/g, '/');
      if (!existsSync(dist)) {
        check(false, `${label}: no build at ${relative(REPO_ROOT, dist)} — run pnpm build first`);
        continue;
      }
      const mod = await import(pathToFileURL(dist).href);
      if (typeof mod.withClient !== 'function') {
        check(false, `${label}: the build exports no withClient`);
        continue;
      }
      helpers.push([label, mod.withClient]);
    }
    check(helpers.length >= 3, `${helpers.length} withClient helper(s) built and loaded, derived from the source`);
    for (const [label, withClient] of helpers) await partB(label, withClient);

    console.log('');
    console.log('  C. withClient removes its listener: no accumulation across checkouts of one client');
    for (const [label, withClient] of helpers) await partC(label, withClient);
    check(
      !warnings.includes('MaxListenersExceededWarning'),
      `no MaxListenersExceededWarning (${warnings.length} warning(s) seen in total)`,
    );

    const relay = {
      ...(await import(pathToFileURL(join(REPO_ROOT, 'services/relay/dist/db.js')).href)),
      ...(await import(pathToFileURL(join(REPO_ROOT, 'services/relay/dist/drain.js')).href)),
    };
    await partD(relay, app);

    partE();
  } finally {
    const done = await asApp(app, () =>
      app.query(`DELETE FROM "OutboxMessage" WHERE "aggregateId" LIKE $1`, [`${run}%`]),
    ).catch(() => null);
    console.log('');
    console.log(`  cleanup: ${done ? done.rowCount : 'FAILED —'} row(s) deleted for ${run}`);
    await app.end().catch(() => {});
    await relayDb.end().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a connection terminated while checked out is logged and replaced, not fatal');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
