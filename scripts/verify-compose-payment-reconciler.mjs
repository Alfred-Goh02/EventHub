/**
 * Asserts that the COMPOSE STACK settles a charge the provider never answered, by
 * itself.
 *
 * Every other reconciler verifier runs a reconciler of its own -- the binary with
 * `--once`, or reconcileOnce() imported -- so each one proves that reconciliation
 * works and none proves that anything runs it. Nothing did: compose started the hold
 * sweeper and never the reconciler. The webhook is the accelerator and the
 * reconciler the guaranteed route, so a provider that never answered left the
 * payment UNKNOWN and the reservation PENDING_PAYMENT for good, and the sweeper skips
 * PENDING_PAYMENT by design. Measured on the local stack before the fix: 8 payments
 * UNKNOWN with settledAt NULL, the oldest started 35 days earlier. Seats that never
 * came back, and charges that went through and were never confirmed.
 *
 * So this verifier starts NOTHING. It signs in through the gateway as a user of a
 * scratch tenant, takes two holds and pays for them with amounts the fake provider
 * does not answer (src/payments/provider.ts):
 *
 *   2 x 1001 = 2002  ...02  no answer, and the charge DID go through
 *   3 x 1001 = 3003  ...03  no answer, and the charge never arrived
 *
 * Both are 202 and UNKNOWN at charge time; only a lookup tells them apart. Then it
 * waits -- for the reconciler's own grace and interval, read from its container --
 * and requires:
 *
 *   - CONTROL, first: the container is running and NOT restarting, and a
 *     `payment-reconciler` backend is connected from its address. Running alone is
 *     what Docker reports of a container crash-looping on settings it refuses;
 *   - CONTROL, before waiting: both are UNKNOWN and unsettled, both reservations
 *     PENDING_PAYMENT, all five seats held. A payment that was never unsettled is
 *     also settled afterwards.
 *   - ...02 is SUCCEEDED with the provider's reference, its reservation CONFIRMED and
 *     its two seats moved held -> sold exactly once, with one reservation.confirmed
 *     and one tickets.purchased for it;
 *   - ...03 is FAILED with the LOOKUP's reason, not the charge's, and its reservation
 *     is a plain hold again -- its three seats still held, nothing sold, nothing
 *     emitted. That is what settleAttempt does with a charge that never happened: it
 *     returns the hold, and the sweeper returns the seats when the hold runs out;
 *   - each was settled more than twice the API's own provider timeout after it
 *     started, measured in the database. Below that the reconciler is looking up
 *     charges the API may still be waiting on. Its startup check compares grace
 *     with ITS copy of the timeout; this compares it with the API container's;
 *   - CONTROL: the reconciler CONTAINER's own log names each payment and what it did
 *     with it. A reconciler run on the host -- every other verifier of this job runs
 *     one -- would settle these rows too, and the rows alone would look the same.
 *
 * Through the gateway, as a buyer would, and as a user of a tenant made for the run
 * and removed at the end with everything in it. The cleanup reports what it DELETED,
 * and requires it: a red run leaves two UNKNOWN payments, and they must not outlive
 * it for some later reconciler to find.
 *
 * NO DEPENDENCIES, like the other compose verifiers: the compose CI job installs
 * nothing. Postgres is reached with `docker exec psql` -- as the app role inside the
 * scratch tenant for the fixture and the reads, and as the superuser for the cleanup,
 * because the app role may not delete a Payment.
 *
 * Run (with `docker compose up -d --wait` done): node scripts/verify-compose-payment-reconciler.mjs
 */

import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { send, sleep } from './lib/kong.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const PROXY = process.env['KONG_PROXY_URL'] ?? 'http://127.0.0.1:8000';
const POSTGRES = process.env['POSTGRES_CONTAINER'] ?? 'eventhub-postgres';
const RECONCILER = process.env['RECONCILER_CONTAINER'] ?? 'eventhub-ticketing-reconciler';
const API = process.env['TICKETING_CONTAINER'] ?? 'eventhub-ticketing';
const DATABASE = 'eventhub_ticketing';
const APP_ROLE = 'eventhub_ticketing_app';

/** A scratch tenant per run: nothing this verifier reads or deletes can be anyone else's. */
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_recprobe_${RUN}`;
const SLUG = `recprobe-${RUN}`;
const HOST = `${SLUG}.eventhub.localhost`;
const USER = { id: `user_recprobe_${RUN}`, email: `recprobe-${RUN}@mailcheck.example` };
const PASSWORD = 'hunter2222';
const EVENT_ID = `recprobe_${RUN}_evt`;
const TICKET_TYPE_ID = `recprobe_${RUN}_tt`;

const TOTAL = 10;
const PRICE_CENTS = 1001;
/** 2 x 1001 = 2002: the provider took the money and the answer was lost. */
const PAID_QTY = 2;
/** 3 x 1001 = 3003: the charge never reached the provider. */
const LOST_QTY = 3;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

function run(cmd, args, input) {
  return new Promise((resolvePromise, reject) => {
    const child = execFile(cmd, args, { maxBuffer: 64 * 1024 * 1024 }, (err, stdout, stderr) =>
      err ? reject(new Error(`${err.message}\n${stderr}`)) : resolvePromise({ stdout: stdout.trim(), stderr }),
    );
    if (input !== undefined) child.stdin.end(input);
  });
}

const psql = (user, database, sql) =>
  run('docker', ['exec', '-i', POSTGRES, 'psql', '-U', user, '-d', database, '-v', 'ON_ERROR_STOP=1', '-q', '-tA'], sql).then((r) => r.stdout);

/**
 * One transaction as the app role, inside this run's tenant. SET LOCAL returns no
 * row, so the output is only what `sql` selects. Every value interpolated in this
 * file is generated by it.
 */
const asTenant = (sql) => psql(APP_ROLE, DATABASE, `BEGIN;\nSET LOCAL app.tenant_id = '${TENANT}';\n${sql}\nCOMMIT;\n`);
const asSuperuser = (database, sql) => psql('eventhub', database, sql);

/** A container's environment as it is running, or null if there is no such container. */
async function containerEnv(name) {
  const out = await run('docker', ['inspect', '-f', '{{json .Config.Env}}', name]).then((r) => r.stdout, () => null);
  if (out === null) return null;
  return Object.fromEntries(JSON.parse(out).map((kv) => [kv.slice(0, kv.indexOf('=')), kv.slice(kv.indexOf('=') + 1)]));
}

/** Docker's view of a container, or null if there is no such container. */
async function containerState(name) {
  const state = await run('docker', ['inspect', '-f', '{{json .State}}', name]).then((r) => JSON.parse(r.stdout), () => null);
  if (state === null) return null;
  const restarts = await run('docker', ['inspect', '-f', '{{.RestartCount}}', name]).then((r) => Number(r.stdout), () => NaN);
  return { running: state.Running === true, restarting: state.Restarting === true, status: state.Status, restarts };
}

/**
 * Whether a `payment-reconciler` backend is connected FROM the reconciler container.
 *
 * `State.Running` is no evidence the process is up: Docker keeps it true while a
 * crash-looping container waits to be restarted, and a reconciler that refuses its
 * settings exits before it ever connects. So this asks Postgres, as the superuser
 * because it sees every backend, the way verify-compose-hold-sweeper.mjs finds its
 * sweeper: reconciler/index.ts names its pool `payment-reconciler`, and the address
 * says which container the connection came from. A reconciler run on the host connects
 * from the gateway's address, which is no container's.
 *
 * Polled for up to one interval and a margin: the pool lets an idle connection go
 * after 30 s, so with an interval longer than that the backend exists only around
 * each pass.
 */
async function reconcilerConnected(withinMs) {
  const deadline = Date.now() + withinMs;
  let seen = { addrs: [], ips: [] };
  for (;;) {
    const addrs = (
      await asSuperuser(
        DATABASE,
        `SELECT DISTINCT host(client_addr) FROM pg_stat_activity WHERE application_name = 'payment-reconciler' AND client_addr IS NOT NULL`,
      ).catch(() => '')
    )
      .split('\n')
      .filter(Boolean);
    // A container between restarts has no address, and the template prints
    // "invalid IP" for it; only addresses are kept.
    const ips = await run('docker', ['inspect', '-f', '{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}', RECONCILER]).then(
      (r) => r.stdout.split(/\s+/).filter((s) => /^[0-9a-f.:]+$/i.test(s) && /[.:]/.test(s)),
      () => [],
    );
    seen = { addrs, ips };
    if (addrs.some((a) => ips.includes(a)) || Date.now() >= deadline) return { ...seen, connected: addrs.some((a) => ips.includes(a)) };
    await sleep(500);
  }
}

/**
 * The reconciler's grace and interval: its container's own settings, or where the
 * container sets none, the defaults reconciler/index.ts parses them with -- read from
 * that file, so the wait below is never sized against a hand-copied number. A raised
 * grace would otherwise run past a fixed deadline and report "nothing asked the
 * provider" about a reconciler that was about to.
 */
async function reconcilerTiming() {
  const env = await containerEnv(RECONCILER);
  const source = readFileSync(join(ROOT, 'services/ticketing/src/reconciler/index.ts'), 'utf8');
  const read = (name) => {
    if (env?.[name]) return { ms: Number(env[name]), from: `${RECONCILER}'s environment` };
    const m = source.match(new RegExp(`${name}: z\\.coerce\\.number\\(\\)[^\\n]*\\.default\\(([\\d_]+)\\)`));
    return m ? { ms: Number(m[1].replace(/_/g, '')), from: "reconciler/index.ts's default" } : { ms: NaN, from: 'nowhere' };
  };
  return { grace: read('RECONCILE_GRACE_MS'), interval: read('RECONCILE_INTERVAL_MS') };
}

/**
 * The API's provider timeout: its container's PAYMENT_TIMEOUT_MS, or when that is
 * unset, the default pay.ts falls back to -- read from pay.ts, so a changed default
 * is not compared against a stale copy of it here.
 */
async function apiTimeoutMs() {
  const env = await containerEnv(API);
  if (env?.['PAYMENT_TIMEOUT_MS']) return { ms: Number(env['PAYMENT_TIMEOUT_MS']), from: `${API}'s environment` };
  const source = readFileSync(join(ROOT, 'services/ticketing/src/routes/pay.ts'), 'utf8');
  const m = source.match(/if \(raw === undefined \|\| raw === ''\) return (\d+);/);
  return m ? { ms: Number(m[1]), from: `pay.ts's default (${API} does not set it)` } : { ms: NaN, from: 'nowhere: pay.ts no longer has the default this reads' };
}

/** A request through the gateway, as this run's tenant. */
async function api(path, { method = 'GET', token, body, headers = {} } = {}) {
  const text = body === undefined ? undefined : JSON.stringify(body);
  return send(PROXY, path, {
    host: HOST,
    method,
    body: text,
    headers: {
      ...headers,
      ...(token ? { authorization: `Bearer ${token}` } : {}),
      ...(text === undefined ? {} : { 'content-type': 'application/json', 'content-length': String(Buffer.byteLength(text)) }),
    },
  });
}

const STATE_SQL = `
  SELECT json_build_object(
    'payments', (
      SELECT coalesce(json_object_agg(p.id, json_build_object(
               'status', p.status,
               'settled', p."settledAt" IS NOT NULL,
               'providerRef', p."providerRef",
               'failureReason', p."failureReason",
               'amountCents', p."amountCents",
               'settledAfterMs', (extract(epoch from p."settledAt" - p."startedAt") * 1000)::bigint,
               'reservation', r.status,
               'paymentStartedAtCleared', r."paymentStartedAt" IS NULL)), '{}')
        FROM "Payment" p JOIN "Reservation" r ON r.id = p."reservationId"),
    'inventory', (
      SELECT json_build_object('total', total, 'remaining', remaining, 'held', held, 'sold', sold)
        FROM "TicketType" WHERE id = '${TICKET_TYPE_ID}'),
    'messages', (
      SELECT coalesce(json_agg(json_build_object(
               'eventType', "eventType", 'reservationId', payload->>'reservationId', 'paymentId', payload->>'paymentId')), '[]')
        FROM "OutboxMessage" WHERE "eventType" IN ('reservation.confirmed', 'tickets.purchased'))
  );`;

const state = async () => JSON.parse(await asTenant(STATE_SQL));

/** Every `settled an unanswered charge` line the reconciler container logged, by payment. */
async function reconcilerSettled() {
  const { stdout, stderr } = await run('docker', ['logs', '--since', '15m', RECONCILER]).catch(() => ({ stdout: '', stderr: '' }));
  const settled = new Map();
  for (const line of `${stdout}\n${stderr}`.split('\n')) {
    let entry;
    try {
      entry = JSON.parse(line);
    } catch {
      continue;
    }
    if (entry.msg === 'settled an unanswered charge' && entry.paymentId) settled.set(entry.paymentId, entry.settled);
  }
  return settled;
}

async function seed() {
  // The seeded account's own hash, so this user has a password without hashing one:
  // the compose job has no dependencies.
  const hash = await asSuperuser('eventhub_auth', `SELECT "passwordHash" FROM "User" WHERE email = 'alice@acme.com' AND "tenantId" = 'tenant_acme'`);
  check(hash.startsWith('$argon2'), `CONTROL: the seeded account's hash was read (${hash.slice(0, 12)}…)`);
  // A new tenant is one INSERT: Kong passes the subdomain, and Auth resolves it.
  await asSuperuser(
    'eventhub_auth',
    `INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ('${TENANT}', 'Reconciler probe ${RUN}', '${SLUG}', now());
     INSERT INTO "User" (id, "tenantId", email, "passwordHash", role, "updatedAt")
     VALUES ('${USER.id}', '${TENANT}', '${USER.email}', '${hash}', 'ATTENDEE', now());`,
  );
  await asTenant(`
    INSERT INTO "EventProjection"
      (id, "tenantId", title, slug, status, capacity, "startsAt", "endsAt", "organiserId", "sourceVersion", "updatedAt")
    VALUES ('${EVENT_ID}', '${TENANT}', 'Reconciler probe', 'reconciler-probe-${RUN}', 'PUBLISHED', ${TOTAL},
            now() + interval '30 days', now() + interval '31 days', 'u_recprobe', 1, now());
    INSERT INTO "TicketType"
      (id, "tenantId", "eventId", name, "priceCents", total, remaining, held, sold, "updatedAt")
    VALUES ('${TICKET_TYPE_ID}', '${TENANT}', '${EVENT_ID}', 'General', ${PRICE_CENTS}, ${TOTAL}, ${TOTAL}, 0, 0, now());`);
}

/**
 * Children first, as the superuser, every DELETE scoped to this run's tenant by name:
 * the superuser is not bound by row security, so nothing here leans on a policy to
 * narrow it. Each statement counts what it removed and the remainder is counted
 * after, because "0 deleted, 0 left" is the bug a cleanup is written against.
 */
async function cleanup() {
  // Labelled by database as well as table: both have an OutboxMessage, and keyed by
  // the table name alone Auth's 0 overwrote Ticketing's count, so the report said no
  // outbox row was deleted while the run's four had just been.
  const count = (table, label = table) =>
    `WITH d AS (DELETE FROM "${table}" WHERE "tenantId" = '${TENANT}' RETURNING 1) SELECT '${label}', count(*) FROM d;`;
  const ticketing = ['OutboxMessage', 'Payment', 'IdempotencyKey', 'ReservationItem', 'Reservation', 'TicketType', 'EventProjection'];
  const removed = {};
  const ticketingOut = await asSuperuser(DATABASE, ticketing.map((t) => count(t)).join('\n')).catch((err) => {
    check(false, `the ticketing cleanup ran (${err.message.split('\n')[0]})`);
    return '';
  });
  // A User cascades to its refresh tokens, and a Tenant to its users; they are
  // still counted one table at a time, so the report says what each step removed.
  const authOut = await asSuperuser(
    'eventhub_auth',
    `${count('RefreshToken', 'auth RefreshToken')}\n${count('OutboxMessage', 'auth OutboxMessage')}\n${count('User', 'auth User')}
     WITH d AS (DELETE FROM "Tenant" WHERE id = '${TENANT}' RETURNING 1) SELECT 'auth Tenant', count(*) FROM d;`,
  ).catch((err) => {
    check(false, `the auth cleanup ran (${err.message.split('\n')[0]})`);
    return '';
  });
  for (const line of `${ticketingOut}\n${authOut}`.split('\n').filter(Boolean)) {
    const [table, n] = line.split('|');
    removed[table] = Number(n);
  }
  const left = Number(
    await asSuperuser(
      DATABASE,
      `SELECT ${ticketing.map((t) => `(SELECT count(*) FROM "${t}" WHERE "tenantId" = '${TENANT}')`).join(' + ')};`,
    ).catch(() => NaN),
  ) + Number(await asSuperuser('eventhub_auth', `SELECT (SELECT count(*) FROM "User" WHERE "tenantId" = '${TENANT}') + (SELECT count(*) FROM "Tenant" WHERE id = '${TENANT}');`).catch(() => NaN));
  const deleted = Object.values(removed).reduce((a, b) => a + b, 0);
  console.log(`  cleanup: ${deleted} deleted (${Object.entries(removed).map(([t, n]) => `${t} ${n}`).join(', ')}), ${left} left`);
  // The fixture alone is an event, a ticket type, two reservations with an item
  // each, two payments, a user and a tenant: ten rows, whether or not anything
  // was settled.
  check(deleted >= 10 && removed['Payment'] === 2, `CONTROL: the cleanup deleted this run's rows (${deleted}, at least 10, two of them payments)`);
  check(left === 0, `nothing of tenant ${TENANT} is left behind (${left})`);
}

async function main() {
  console.log(`  scratch tenant ${TENANT}, through the gateway as ${HOST}`);

  const timing = await reconcilerTiming();
  /**
   * Due at the grace, taken within an interval of it; three intervals and fifteen
   * seconds more is room for a pass in progress and a slow runner. Without both
   * numbers there is nothing to size it by, and that is a failure of its own.
   */
  const derived = timing.grace.ms + 3 * timing.interval.ms + 15_000;
  check(
    Number.isFinite(derived),
    `the wait is sized by the reconciler's own settings: grace ${timing.grace.ms} ms (${timing.grace.from}), ` +
      `interval ${timing.interval.ms} ms (${timing.interval.from})`,
  );
  const DEADLINE_MS = Number.isFinite(derived) ? derived : 120_000;

  const container = await containerState(RECONCILER);
  check(
    container?.running === true && container?.restarting === false,
    container === null
      ? `CONTROL: there is no ${RECONCILER} container`
      : `CONTROL: ${RECONCILER} is ${container.status}, running=${container.running}, restarting=${container.restarting} ` +
          `(${container.restarts} restart(s) so far)`,
  );
  const connected = await reconcilerConnected(timing.interval.ms + 5_000 || 35_000);
  check(
    connected.connected,
    `CONTROL: a payment-reconciler backend is connected from ${RECONCILER}'s address -- the process got as far as its ` +
      `database, which a container restarting on settings it refuses never does (backends from ${connected.addrs.join(', ') || 'nowhere'}; ` +
      `container at ${connected.ips.join(', ') || 'no address'})`,
  );
  const timeout = await apiTimeoutMs();

  const ids = { paidRes: null, lostRes: null, paidPay: null, lostPay: null };
  try {
    await seed();

    const login = await api('/auth/login', { method: 'POST', body: { email: USER.email, password: PASSWORD } });
    const token = login.json?.accessToken;
    check(login.status === 200 && Boolean(token), `the scratch tenant's user signs in through the gateway (${login.status})`);
    if (!token) return;

    const holdAndPay = async (quantity, tag) => {
      const hold = await api('/reservations', {
        method: 'POST',
        token,
        headers: { 'idempotency-key': `recprobe-${RUN}-${tag}` },
        body: { eventId: EVENT_ID, items: [{ ticketTypeId: TICKET_TYPE_ID, quantity }] },
      });
      const pay = hold.json?.id ? await api(`/reservations/${hold.json.id}/pay`, { method: 'POST', token }) : null;
      return { hold, pay };
    };
    const paid = await holdAndPay(PAID_QTY, 'paid');
    const lost = await holdAndPay(LOST_QTY, 'lost');
    ids.paidRes = paid.hold.json?.id ?? null;
    ids.lostRes = lost.hold.json?.id ?? null;
    ids.paidPay = paid.pay?.json?.paymentId ?? null;
    ids.lostPay = lost.pay?.json?.paymentId ?? null;

    console.log('');
    console.log('  before: the provider answered neither charge');
    check(
      paid.hold.status === 201 && lost.hold.status === 201,
      `both holds are taken (${paid.hold.status}, ${lost.hold.status})`,
    );
    check(
      paid.pay?.status === 202 && lost.pay?.status === 202 && Boolean(ids.paidPay && ids.lostPay),
      `both payments answer 202, "being confirmed" (${paid.pay?.status}, ${lost.pay?.status})`,
    );
    const before = await state();
    const bp = before.payments[ids.paidPay];
    const bl = before.payments[ids.lostPay];
    check(
      bp?.status === 'UNKNOWN' && bl?.status === 'UNKNOWN' && !bp.settled && !bl.settled,
      `CONTROL: both are UNKNOWN and unsettled (${bp?.status}/${bp?.settled}, ${bl?.status}/${bl?.settled})`,
    );
    check(
      bp?.amountCents === PAID_QTY * PRICE_CENTS && bl?.amountCents === LOST_QTY * PRICE_CENTS,
      `for the amounts that pick the provider's two silent outcomes (${bp?.amountCents}, ${bl?.amountCents})`,
    );
    check(
      bp?.reservation === 'PENDING_PAYMENT' && bl?.reservation === 'PENDING_PAYMENT',
      `CONTROL: both reservations are PENDING_PAYMENT, which the sweeper never touches (${bp?.reservation}, ${bl?.reservation})`,
    );
    check(
      before.inventory?.remaining === TOTAL - PAID_QTY - LOST_QTY && before.inventory?.held === PAID_QTY + LOST_QTY && before.inventory?.sold === 0,
      `CONTROL: all ${PAID_QTY + LOST_QTY} seats are held (remaining=${before.inventory?.remaining}, held=${before.inventory?.held}, sold=${before.inventory?.sold})`,
    );

    console.log('');
    console.log(`  waiting up to ${DEADLINE_MS / 1000}s for the stack to settle them; this script runs no reconciler`);
    const started = Date.now();
    let after = before;
    while (Date.now() - started < DEADLINE_MS) {
      await sleep(1000);
      after = await state();
      if (after.payments[ids.paidPay]?.settled && after.payments[ids.lostPay]?.settled) break;
    }
    const waited = ((Date.now() - started) / 1000).toFixed(1);
    const ap = after.payments[ids.paidPay];
    const al = after.payments[ids.lostPay];
    console.log(`  after ${waited}s:`);

    check(
      ap?.status === 'SUCCEEDED' && ap?.providerRef === `fake_ch_${ids.paidPay}`,
      ap?.settled
        ? `...02 is SUCCEEDED with the provider's reference (${ap.status}, ${ap.providerRef})`
        : `...02 is still ${ap?.status}: nothing in the running stack asked the provider about it`,
    );
    check(
      ap?.reservation === 'CONFIRMED' && ap?.paymentStartedAtCleared,
      `and its reservation is CONFIRMED (${ap?.reservation}, paymentStartedAt cleared=${ap?.paymentStartedAtCleared})`,
    );
    check(
      al?.status === 'FAILED' && al?.failureReason === 'no_such_charge',
      al?.settled
        ? `...03 is FAILED with the lookup's answer, not the charge's (${al.status}, ${al.failureReason})`
        : `...03 is still ${al?.status} (${al?.failureReason}): nothing in the running stack asked the provider about it`,
    );
    check(
      al?.reservation === 'HELD' && al?.paymentStartedAtCleared,
      `and its reservation is a plain hold again, for the sweeper to expire (${al?.reservation}, paymentStartedAt cleared=${al?.paymentStartedAtCleared})`,
    );

    const inv = after.inventory ?? {};
    check(
      inv.remaining === TOTAL - PAID_QTY - LOST_QTY && inv.held === LOST_QTY && inv.sold === PAID_QTY,
      `only the paid seats moved held -> sold, exactly once (remaining ${before.inventory?.remaining} -> ${inv.remaining}, ` +
        `held ${before.inventory?.held} -> ${inv.held}, sold ${before.inventory?.sold} -> ${inv.sold})`,
    );
    // No "remaining + held + sold is still TOTAL" check: TicketType_counts_sum is a CHECK
    // constraint, so it cannot fail here, and a double held -> sold keeps the sum. The
    // exact counts above are the assertion.

    const confirmedFor = (res) => after.messages.filter((m) => m.eventType === 'reservation.confirmed' && m.reservationId === res).length;
    const purchasedFor = (pay) => after.messages.filter((m) => m.eventType === 'tickets.purchased' && m.paymentId === pay).length;
    check(
      confirmedFor(ids.paidRes) === 1 && purchasedFor(ids.paidPay) === 1,
      `one reservation.confirmed and one tickets.purchased for the sale (${confirmedFor(ids.paidRes)}, ${purchasedFor(ids.paidPay)})`,
    );
    check(
      confirmedFor(ids.lostRes) === 0 && purchasedFor(ids.lostPay) === 0,
      `nothing emitted for the charge that never arrived (${confirmedFor(ids.lostRes)}, ${purchasedFor(ids.lostPay)})`,
    );

    const floor = 2 * timeout.ms;
    const after2x = (p) => Number.isFinite(floor) && p?.settledAfterMs !== null && p?.settledAfterMs !== undefined && p.settledAfterMs > floor;
    check(
      after2x(ap) && after2x(al),
      `each was settled more than twice the API's provider timeout after it started -- ${ap?.settledAfterMs ?? 'never'} ms and ` +
        `${al?.settledAfterMs ?? 'never'} ms, against 2 x ${timeout.ms} ms from ${timeout.from}`,
    );

    const logged = await reconcilerSettled();
    check(
      logged.get(ids.paidPay) === 'confirmed' && logged.get(ids.lostPay) === 'released',
      `CONTROL: ${RECONCILER}'s own log names both payments and what it did with them ` +
        `(${ids.paidPay}: ${logged.get(ids.paidPay) ?? 'not named'}, ${ids.lostPay}: ${logged.get(ids.lostPay) ?? 'not named'})`,
    );
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    console.log('');
    await cleanup();
  }

  console.log('');
  if (failures > 0) {
    console.error(
      `${failures} check(s) failed. If a CONTROL on the reconciler failed, its own log says why: docker compose logs ticketing-reconciler`,
    );
    return;
  }
  console.log('the compose stack settles an unanswered charge on its own, in both directions, and only after the API stopped waiting');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
