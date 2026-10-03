/**
 * Asserts that unresolved charges are resolved by ASKING the provider, and in
 * both directions.
 *
 * This is the job that decides what happens to a customer whose card may or may
 * not have been charged. Getting it wrong is not a stuck row — it is either
 * charging someone for a seat that was then resold, or marking a seat sold that
 * nobody paid for. Both are invisible until somebody complains.
 *
 * The fake provider distinguishes two attempts that look IDENTICAL at charge
 * time — both return "no answer" — and differ only in what actually happened at
 * the provider. That is the whole point: if the client could tell them apart, no
 * reconciliation would be needed.
 *
 * Run: node scripts/verify-payment-reconciliation.mjs
 */

import { spawnSync } from 'node:child_process';
import pg from 'pg';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const SWEEPER_URL =
  process.env['SWEEPER_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_sweeper:eventhub_ticketing_sweeper@localhost:5432/eventhub_ticketing';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';

const TENANT = 'tenant_acme';
/** The seeded tenant's subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';

// See src/payments/provider.ts. Both of these return "no answer" from charge().
const PRICE_PAID = 1002; // ...and the money DID move
const PRICE_LOST = 1003; // ...and the charge never arrived

const run = `rec_${Date.now()}`;
const eventId = `${run}_evt`;
const ttPaid = `${run}_tt_paid`;
const ttLost = `${run}_tt_lost`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const db = new pg.Client({ connectionString: APP_URL });

async function tx(fn) {
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  try {
    const out = await fn();
    await db.query('COMMIT');
    return out;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** Runs the real reconciler binary, so this tests the shipped process. */
function reconcile(graceMs) {
  const r = spawnSync(process.execPath, ['dist/reconciler/index.js', '--once'], {
    cwd: 'services/ticketing',
    env: {
      ...process.env,
      SWEEPER_DATABASE_URL: SWEEPER_URL,
      RECONCILE_GRACE_MS: String(graceMs),
      // The reconciler refuses to start unless the grace period is more than
      // twice the provider timeout -- below that it examines charges still in
      // flight and releases the seats of customers mid-checkout. So the two
      // are set together here rather than leaving the default 8s timeout to
      // contradict a deliberately short grace.
      // 50ms was too tight and made this test flaky on a loaded runner.
      //
      // The reconciler wraps its provider in withTimeout(PAYMENT_TIMEOUT_MS),
      // and a lookup that loses that race returns `unknown` -- which is
      // CORRECT behaviour, and leaves the attempt unsettled. So on a busy CI
      // box the reconciler would report 'still unresolved' and the assertions
      // below would fail for a reason that has nothing to do with what they
      // test. Observed on main twice.
      //
      // 300ms is still far below anything a real provider needs, and the
      // grace period is raised with it to stay clear of the
      // grace > 2x timeout floor the reconciler enforces.
      PAYMENT_TIMEOUT_MS: '300',
      LOG_LEVEL: 'error',
    },
    encoding: 'utf8',
  });
  if (r.status !== 0) fail(`reconciler exited ${r.status}: ${r.stderr}`);
  return r;
}

const state = (reservationId) =>
  tx(async () => {
    const r = await db.query(`SELECT status FROM "Reservation" WHERE id = $1`, [reservationId]);
    const p = await db.query(
      `SELECT status, "settledAt", "providerRef", "failureReason"
         FROM "Payment" WHERE "reservationId" = $1 ORDER BY "startedAt"`,
      [reservationId],
    );
    const o = await db.query(
      `SELECT topic, "eventType" FROM "OutboxMessage"
        WHERE payload->>'reservationId' = $1 AND "eventType" <> 'reservation.held'
        ORDER BY seq`,
      [reservationId],
    );
    return { status: r.rows[0]?.status, payments: p.rows, fromPayment: o.rows };
  });

const inventory = (id) =>
  tx(async () => {
    const { rows } = await db.query(
      `SELECT total, remaining, held, sold FROM "TicketType" WHERE id = $1`,
      [id],
    );
    return rows[0];
  });

async function main() {
  await db.connect();

  await tx(async () => {
    await db.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Reconciliation','reconciliation','PUBLISHED',50,now(),now(),'u_test',1,now())`,
      [eventId, TENANT],
    );
    for (const [id, price] of [
      [ttPaid, PRICE_PAID],
      [ttLost, PRICE_LOST],
    ]) {
      await db.query(
        `INSERT INTO "TicketType"
           (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
         VALUES ($1,$2,$3,'General',$4,10,10,0,0,now())`,
        [id, TENANT, eventId, price],
      );
    }
  });

  const loginRes = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await loginRes.json();
  if (!accessToken) fail('could not log in');
  const auth = { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' };

  const holdAndPay = async (ticketTypeId, tag) => {
    const h = await fetch(`${TICKETING}/reservations`, {
      method: 'POST',
      headers: { ...auth, 'Idempotency-Key': `${run}_${tag}` },
      body: JSON.stringify({ eventId, items: [{ ticketTypeId, quantity: 1 }] }),
    });
    const held = await h.json();
    if (h.status !== 201) fail(`hold ${tag}: got ${h.status} ${JSON.stringify(held)}`);

    const p = await fetch(`${TICKETING}/reservations/${held.id}/pay`, {
      method: 'POST',
      headers: auth,
    });
    if (p.status !== 202) fail(`pay ${tag}: expected 202 (no answer), got ${p.status}`);
    return held.id;
  };

  const paidRes = await holdAndPay(ttPaid, 'paid');
  const lostRes = await holdAndPay(ttLost, 'lost');

  // -------------------------------------------------------------------------
  console.log('\n1. both attempts look identical from this side');
  // -------------------------------------------------------------------------
  const before = { paid: await state(paidRes), lost: await state(lostRes) };
  check(before.paid.status === 'PENDING_PAYMENT', `paid-case PENDING_PAYMENT, got ${before.paid.status}`);
  check(before.lost.status === 'PENDING_PAYMENT', `lost-case PENDING_PAYMENT, got ${before.lost.status}`);
  check(before.paid.payments[0]?.status === 'UNKNOWN', `paid-case UNKNOWN, got ${before.paid.payments[0]?.status}`);
  check(before.lost.payments[0]?.status === 'UNKNOWN', `lost-case UNKNOWN, got ${before.lost.payments[0]?.status}`);

  // -------------------------------------------------------------------------
  console.log('\n2. the grace period leaves a charge that may still be in flight alone');
  // -------------------------------------------------------------------------
  // Without this, the reconciler would look up a charge the endpoint is still
  // waiting on, be told "no such charge", and release the seats of a customer
  // who is mid-checkout — the repair job would become the thing breaking it.
  reconcile(600_000);
  const afterGrace = await state(paidRes);
  check(afterGrace.status === 'PENDING_PAYMENT', `untouched inside the grace window, got ${afterGrace.status}`);
  check(afterGrace.payments[0]?.status === 'UNKNOWN', `still UNKNOWN, got ${afterGrace.payments[0]?.status}`);

  // -------------------------------------------------------------------------
  console.log('\n3. past the grace window, the provider is asked');
  // -------------------------------------------------------------------------
  /**
   * Wait past the grace window before asking the reconciler to act.
   *
   * The job only examines attempts older than RECONCILE_GRACE_MS, so whether it
   * sees these two depended on how long the preceding HTTP calls and process
   * spawn happened to take. That is not a property worth testing: it made the
   * run pass or fail on incidental latency rather than on behaviour.
   *
   * Sleeping makes the precondition explicit and deterministic.
   */
  await sleep(1200);
  reconcile(1000);

  const paid = await state(paidRes);
  const paidInv = await inventory(ttPaid);
  console.log('  the money moved:');
  check(paid.status === 'CONFIRMED', `reservation CONFIRMED, got ${paid.status}`);
  check(paid.payments[0]?.status === 'SUCCEEDED', `payment SUCCEEDED, got ${paid.payments[0]?.status}`);
  check(paid.payments[0]?.settledAt !== null, 'settled, so it leaves the reconciliation index');
  check(paid.payments[0]?.providerRef !== null, 'the provider reference was recorded');
  check(paidInv.held === 0 && paidInv.sold === 1, `held->sold moved (held=${paidInv.held} sold=${paidInv.sold})`);
  const paidTopics = paid.fromPayment.map((m) => `${m.topic}/${m.eventType}`);
  check(paidTopics.includes('tickets.reserved/reservation.confirmed'), `seat-state CONFIRMED emitted (${paidTopics.join(', ') || 'none'})`);
  check(paidTopics.includes('tickets.purchased/tickets.purchased'), 'purchase emitted');

  const lost = await state(lostRes);
  const lostInv = await inventory(ttLost);
  console.log('  the charge never arrived:');
  check(lost.status === 'HELD', `reservation back to HELD, got ${lost.status}`);
  check(lost.payments[0]?.status === 'FAILED', `payment FAILED, got ${lost.payments[0]?.status}`);
  check(lost.payments[0]?.settledAt !== null, 'a definite answer is settled');
  check(lostInv.held === 1 && lostInv.sold === 0, `seats still held, none sold (held=${lostInv.held} sold=${lostInv.sold})`);
  check(lost.fromPayment.length === 0, `nothing emitted for a charge that never happened, got ${lost.fromPayment.length}`);

  // -------------------------------------------------------------------------
  console.log('\n4. a second pass must not move anything twice');
  // -------------------------------------------------------------------------
  // A double held->sold satisfies both TicketType CHECK constraints — remaining
  // + held + sold stays equal to total, the numbers are simply wrong — so the
  // schema cannot catch this and the guard has to.
  /**
   * Wait past the grace window before asking the reconciler to act.
   *
   * The job only examines attempts older than RECONCILE_GRACE_MS, so whether it
   * sees these two depended on how long the preceding HTTP calls and process
   * spawn happened to take. That is not a property worth testing: it made the
   * run pass or fail on incidental latency rather than on behaviour.
   *
   * Sleeping makes the precondition explicit and deterministic.
   */
  await sleep(1200);
  reconcile(1000);
  const paidAgain = await inventory(ttPaid);
  const lostAgain = await inventory(ttLost);
  const paidState = await state(paidRes);

  check(paidAgain.sold === 1, `sold still 1, got ${paidAgain.sold}`);
  check(paidAgain.held === 0, `held still 0, got ${paidAgain.held}`);
  check(lostAgain.held === 1, `lost-case held still 1, got ${lostAgain.held}`);
  check(
    paidState.fromPayment.filter((m) => m.topic === 'tickets.purchased').length === 1,
    `exactly one purchase message, got ${paidState.fromPayment.filter((m) => m.topic === 'tickets.purchased').length}`,
  );
  check(
    paidAgain.remaining + paidAgain.held + paidAgain.sold === paidAgain.total,
    'inventory invariant holds',
  );

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('reconciliation verified: it asks the provider rather than guessing, honours the');
  console.log('grace window, and settles each attempt exactly once');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
