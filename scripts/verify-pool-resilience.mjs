/**
 * Asserts that a terminated idle database connection does not kill a service.
 *
 * `pg.Pool` is an EventEmitter, and an EventEmitter that emits `'error'` with no
 * listener THROWS — in Node, an uncaught exception and an exit. Not one of the
 * five raw pools in this repository had a listener. Reproduced by terminating a
 * single idle backend:
 *
 *   node:events:487
 *         throw er; // Unhandled 'error' event
 *   error: terminating connection due to administrator command
 *
 * Nothing exotic gets you there: a Postgres restart, a managed failover, a
 * maintenance script running pg_terminate_backend, idle_session_timeout, or a
 * load balancer reaping an idle TCP connection.
 *
 * WHAT MAKES THIS WORTH A TEST RATHER THAN A PATCH is which pool does the
 * damage. Prisma's adapter installs its own handler, so terminating the
 * APPLICATION connection is survived cleanly — the obvious experiment passes.
 * The two raw pools in the webhook route are the unguarded ones, and either of
 * them takes the WHOLE HTTP SERVICE down: reservations, payments, health, all
 * of it, over a pool that exists to resolve one column for one webhook.
 *
 * So this has two halves:
 *
 *   BEHAVIOUR  terminate the webhook lookup pool's connection and assert the
 *              service is still serving afterwards, and that the pool actually
 *              replaced the connection rather than merely surviving idle.
 *
 *   RATCHET    every `new pg.Pool(` in the repository must be wrapped in
 *              attachPoolErrorHandler(). This is the half that catches the
 *              SIXTH pool — the one somebody adds in a year, in a file nobody
 *              thinks of as a database file, exactly as the webhook route was.
 *
 * The termination is done with the pool's OWN role rather than a superuser: a
 * Postgres role may signal its own backends, so no extra credential is needed,
 * and the CI job already has this one.
 *
 * Run: node scripts/verify-pool-resilience.mjs
 */

import { createHmac } from 'node:crypto';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import pg from 'pg';

const SWEEPER_URL =
  process.env['SWEEPER_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_sweeper:eventhub_ticketing_sweeper@localhost:5432/eventhub_ticketing';
const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';
const SECRET = process.env['PAYMENT_WEBHOOK_SECRET'] ?? 'dev_webhook_secret';

/** The application_name the webhook's cross-tenant lookup pool connects under. */
const LOOKUP_POOL = 'webhook-tenant-lookup';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * A correctly signed webhook for a payment that does not exist.
 *
 * It is answered 200 with "unknown payment", which is the point: the route
 * still had to consult the lookup pool to find that out, so the connection we
 * are about to terminate exists, and nothing in the database is changed.
 */
async function probeWebhook() {
  const ts = Math.floor(Date.now() / 1000);
  const body = JSON.stringify({
    paymentId: `pay_pool_probe_${ts}`,
    type: 'charge.succeeded',
    providerRef: 'probe',
  });
  const sig = createHmac('sha256', SECRET).update(`${ts}.${body}`).digest('hex');

  const res = await fetch(`${TICKETING}/webhooks/payments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': `t=${ts},v1=${sig}` },
    body,
  });
  return { status: res.status, body: await res.json() };
}

async function health() {
  try {
    const res = await fetch(`${TICKETING}/health`);
    return res.status;
  } catch {
    return 0;
  }
}

// ---------------------------------------------------------------------------
// Half one: the behaviour.
// ---------------------------------------------------------------------------
async function behaviour() {
  console.log('  a terminated idle connection must not take the service with it');

  const before = await health();
  if (before !== 200) fail(`ticketing is not healthy to begin with (got ${before})`);

  // Make the lookup pool open a connection.
  const first = await probeWebhook();
  check(first.status === 200, `a signed webhook is answered (${first.status})`);

  const admin = new pg.Client({ connectionString: SWEEPER_URL });
  // Even the verifier's own client needs one of these. A pg.Client is an
  // EventEmitter too, and this script terminates connections for a living.
  admin.on('error', (err) => console.log(`    (admin client saw ${err.code ?? err.message})`));
  await admin.connect();

  const { rows } = await admin.query(
    `SELECT pid FROM pg_stat_activity
      WHERE application_name = $1 AND pid <> pg_backend_pid()`,
    [LOOKUP_POOL],
  );

  if (rows.length === 0) {
    await admin.end();
    fail(
      `no backend found with application_name '${LOOKUP_POOL}'. Either the service ` +
        'predates the application_name on that pool, or the webhook probe above did not ' +
        'reach it — this assertion cannot run blind.',
    );
  }
  console.log(`    found ${rows.length} idle backend(s) for the ${LOOKUP_POOL} pool`);

  for (const { pid } of rows) {
    await admin.query('SELECT pg_terminate_backend($1)', [pid]);
  }
  await admin.end();
  console.log('    terminated them');

  // A crash is not instant: the FATAL travels back over the socket, pg emits,
  // and Node exits. Give it long enough to actually die if it is going to.
  await sleep(2000);

  const after = await health();
  check(after === 200, `the service is still serving after the termination (health ${after})`);
  if (after !== 200) {
    fail('the service died. That is the whole bug: an idle side-connection took the API down.');
  }

  /**
   * Surviving is not enough. A process could stay up and leave the pool broken,
   * which would be a slower version of the same outage. This sends a second
   * webhook, which must go through the lookup pool again — so it passes only if
   * a REPLACEMENT connection was opened.
   */
  const second = await probeWebhook();
  check(
    second.status === 200,
    `the pool replaced the connection and served the next request (${second.status})`,
  );
}

// ---------------------------------------------------------------------------
// Half two: the ratchet.
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

function ratchet() {
  console.log('');
  console.log('  every raw pool in the repository must be guarded');

  const files = sourceFiles('services');
  const unguarded = [];
  let guarded = 0;

  for (const file of files) {
    const text = readFileSync(file, 'utf8');
    const lines = text.split(/\r?\n/);

    lines.forEach((line, i) => {
      if (!line.includes('new pg.Pool(')) return;

      /**
       * Comments do not construct pools.
       *
       * The first run of this scan reported the two files that DOCUMENT the
       * rule as violations of it, because both doc comments quote
       * `new pg.Pool(` while explaining what must wrap it. A textual rule has
       * to say what counts as code, or the clearest explanation of a hazard
       * becomes an instance of it.
       */
      const trimmed = line.trim();
      if (trimmed.startsWith('*') || trimmed.startsWith('//') || trimmed.startsWith('/*')) return;

      /**
       * The rule: the pool constructor must be an ARGUMENT to
       * attachPoolErrorHandler, which means the call opens on this line or the
       * one above it. Deliberately syntactic rather than clever — a rule that
       * needs a parser is a rule that gets skipped when it breaks.
       */
      const context = `${lines[i - 1] ?? ''}\n${line}`;
      if (context.includes('attachPoolErrorHandler(')) guarded += 1;
      else unguarded.push(`${file}:${i + 1}`);
    });
  }

  check(
    guarded > 0,
    `found ${guarded} guarded pool(s) — the scan is looking at real code, not an empty list`,
  );
  check(
    unguarded.length === 0,
    unguarded.length === 0
      ? 'no unguarded pools'
      : `unguarded pools, each one an uncaught 'error' away from exiting:\n        ` +
        unguarded.join('\n        '),
  );
}

async function main() {
  await behaviour();
  ratchet();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('a terminated idle connection is replaced, not fatal, and no pool is unguarded');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
