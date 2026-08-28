/**
 * Exercises every outcome of the payment path and asserts the state it leaves
 * behind.
 *
 * The interesting assertion is not that a successful charge confirms a
 * reservation. It is what happens when the provider does NOT answer: the seats
 * must stay held, the attempt must stay unsettled, and the sweeper must not
 * reclaim the seats out from under a customer whose card may already have been
 * charged. That case is invisible in a happy-path test and is the one that costs
 * money when it is wrong.
 *
 * Run: node scripts/verify-payment-path.mjs
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

// The fake provider picks its outcome from the last two digits of the amount,
// the way a provider's test card numbers do. See src/payments/provider.ts.
const PRICE_OK = 1000;
const PRICE_DECLINE = 1001;
const PRICE_UNKNOWN = 1002;

const run = `pay_${Date.now()}`;
const eventId = `${run}_evt`;
const ttOk = `${run}_tt_ok`;
const ttDecline = `${run}_tt_dec`;
const ttUnknown = `${run}_tt_unk`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const db = new pg.Client({ connectionString: APP_URL });

/** Every read and write goes through this: the app role sees nothing without it. */
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

const state = (reservationId) =>
  tx(async () => {
    const r = await db.query(
      `SELECT status, "paymentStartedAt", "confirmedAt" FROM "Reservation" WHERE id = $1`,
      [reservationId],
    );
    const p = await db.query(
      `SELECT id, status, "settledAt", "amountCents", "providerRef", "failureReason"
         FROM "Payment" WHERE "reservationId" = $1 ORDER BY "startedAt"`,
      [reservationId],
    );
    const o = await db.query(
      `SELECT topic, "eventType" FROM "OutboxMessage"
        WHERE payload->>'reservationId' = $1 ORDER BY seq`,
      [reservationId],
    );
    // `reservation.held` is the message the HOLD emitted, not the payment. Every
    // assertion below is about what the payment did, so it is filtered out —
    // counting it made two assertions read "no messages emitted, got 1" for
    // behaviour that was entirely correct.
    return {
      reservation: r.rows[0],
      payments: p.rows,
      outbox: o.rows,
      fromPayment: o.rows.filter((m) => m.eventType !== 'reservation.held'),
    };
  });

const inventory = (ticketTypeId) =>
  tx(async () => {
    const { rows } = await db.query(
      `SELECT total, remaining, held, sold FROM "TicketType" WHERE id = $1`,
      [ticketTypeId],
    );
    return rows[0];
  });

async function main() {
  await db.connect();

  await tx(async () => {
    await db.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Payment Path','payment-path','PUBLISHED',100,now(),now(),'u_test',1,now())`,
      [eventId, TENANT],
    );
    for (const [id, price] of [
      [ttOk, PRICE_OK],
      [ttDecline, PRICE_DECLINE],
      [ttUnknown, PRICE_UNKNOWN],
    ]) {
      await db.query(
        `INSERT INTO "TicketType"
           (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
         VALUES ($1,$2,$3,'General',$4,20,20,0,0,now())`,
        [id, TENANT, eventId, price],
      );
    }
  });

  const loginRes = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await loginRes.json();
  if (!accessToken) fail('could not log in');

  const auth = { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' };

  const hold = async (ticketTypeId, quantity, tag) => {
    const r = await fetch(`${TICKETING}/reservations`, {
      method: 'POST',
      headers: { ...auth, 'Idempotency-Key': `${run}_${tag}` },
      body: JSON.stringify({ eventId, items: [{ ticketTypeId, quantity }] }),
    });
    const body = await r.json();
    if (r.status !== 201) fail(`hold ${tag}: expected 201, got ${r.status} ${JSON.stringify(body)}`);
    return body.id;
  };

  const payFor = async (reservationId) => {
    const r = await fetch(`${TICKETING}/reservations/${reservationId}/pay`, {
      method: 'POST',
      headers: auth,
    });
    return { status: r.status, body: await r.json().catch(() => null) };
  };

  // -------------------------------------------------------------------------
  console.log('\n1. a successful charge');
  // -------------------------------------------------------------------------
  const okRes = await hold(ttOk, 2, 'ok');
  const okPay = await payFor(okRes);
  const okState = await state(okRes);
  const okInv = await inventory(ttOk);

  check(okPay.status === 200, `200, got ${okPay.status}`);
  check(okState.reservation.status === 'CONFIRMED', `reservation CONFIRMED, got ${okState.reservation.status}`);
  check(okState.reservation.confirmedAt !== null, 'confirmedAt is set');
  check(okState.payments.length === 1, `one payment attempt, got ${okState.payments.length}`);
  check(okState.payments[0]?.status === 'SUCCEEDED', `payment SUCCEEDED, got ${okState.payments[0]?.status}`);
  check(okState.payments[0]?.settledAt !== null, 'settledAt is set');
  check(okState.payments[0]?.amountCents === 2 * PRICE_OK, `charged ${2 * PRICE_OK}, got ${okState.payments[0]?.amountCents}`);
  check(okInv.held === 0 && okInv.sold === 2, `held->sold moved (held=${okInv.held} sold=${okInv.sold})`);
  check(okInv.remaining + okInv.held + okInv.sold === okInv.total, 'inventory invariant holds');

  const topics = okState.outbox.map((o) => `${o.topic}/${o.eventType}`);
  check(topics.includes('tickets.reserved/reservation.confirmed'), `seat-state CONFIRMED emitted (${topics.join(', ')})`);
  check(topics.includes('tickets.purchased/tickets.purchased'), 'purchase emitted');

  // -------------------------------------------------------------------------
  console.log('\n2. paying twice — the retry must not charge again');
  // -------------------------------------------------------------------------
  const replay = await payFor(okRes);
  const afterReplay = await state(okRes);
  const invAfterReplay = await inventory(ttOk);

  check(replay.status === 200, `200, got ${replay.status}`);
  check(replay.body?.replay === true, 'answered as a replay');
  check(afterReplay.payments.length === 1, `still one attempt, got ${afterReplay.payments.length}`);
  check(invAfterReplay.sold === 2, `sold still 2, got ${invAfterReplay.sold}`);

  // -------------------------------------------------------------------------
  console.log('\n3. a declined card');
  // -------------------------------------------------------------------------
  const decRes = await hold(ttDecline, 1, 'dec');
  const decPay = await payFor(decRes);
  const decState = await state(decRes);
  const decInv = await inventory(ttDecline);

  check(decPay.status === 402, `402, got ${decPay.status}`);
  check(decState.reservation.status === 'HELD', `back to HELD, got ${decState.reservation.status}`);
  check(decState.reservation.paymentStartedAt === null, 'paymentStartedAt cleared');
  check(decState.payments[0]?.status === 'FAILED', `payment FAILED, got ${decState.payments[0]?.status}`);
  check(decState.payments[0]?.settledAt !== null, 'a definite no is settled');
  check(decInv.held === 1 && decInv.sold === 0, `seats still held, none sold (held=${decInv.held} sold=${decInv.sold})`);
  check(decState.fromPayment.length === 0, `the payment emitted nothing, got ${decState.fromPayment.length}`);

  // A second card must be able to succeed, which is the reason the provider
  // idempotency key is per attempt rather than per reservation.
  await tx(async () => {
    await db.query(`UPDATE "ReservationItem" SET "unitPriceCents" = $2 WHERE "reservationId" = $1`, [
      decRes,
      PRICE_OK,
    ]);
  });
  const retryPay = await payFor(decRes);
  const retryState = await state(decRes);
  check(retryPay.status === 200, `retry after decline succeeds, got ${retryPay.status}`);
  check(retryState.payments.length === 2, `a SECOND attempt row exists, got ${retryState.payments.length}`);

  // -------------------------------------------------------------------------
  console.log('\n4. the provider does not answer');
  // -------------------------------------------------------------------------
  const unkRes = await hold(ttUnknown, 1, 'unk');
  const unkPay = await payFor(unkRes);
  const unkState = await state(unkRes);
  const unkInv = await inventory(ttUnknown);

  check(unkPay.status === 202, `202, got ${unkPay.status}`);
  check(unkState.reservation.status === 'PENDING_PAYMENT', `stays PENDING_PAYMENT, got ${unkState.reservation.status}`);
  check(unkState.payments[0]?.status === 'UNKNOWN', `payment UNKNOWN, got ${unkState.payments[0]?.status}`);
  check(unkState.payments[0]?.settledAt === null, 'NOT settled, so reconciliation will find it');
  check(unkInv.held === 1 && unkInv.sold === 0, `seats stay held (held=${unkInv.held} sold=${unkInv.sold})`);
  check(
    unkState.fromPayment.length === 0,
    `nothing emitted for an unresolved charge, got ${unkState.fromPayment.length}`,
  );

  // -------------------------------------------------------------------------
  console.log('\n5. the sweeper must not reclaim seats mid-charge');
  // -------------------------------------------------------------------------
  // Push the hold well past expiry. Under status HELD the sweeper would return
  // these seats immediately; the whole point of PENDING_PAYMENT is that it does
  // not, because the customer's card may already have been charged.
  await tx(async () => {
    await db.query(`UPDATE "Reservation" SET "expiresAt" = now() - interval '1 hour' WHERE id = $1`, [
      unkRes,
    ]);
  });

  const sweep = spawnSync(
    process.execPath,
    ['dist/sweeper/index.js', '--once'],
    {
      cwd: 'services/ticketing',
      env: { ...process.env, SWEEPER_DATABASE_URL: SWEEPER_URL, LOG_LEVEL: 'warn' },
      encoding: 'utf8',
    },
  );
  if (sweep.status !== 0) fail(`sweeper exited ${sweep.status}: ${sweep.stderr}`);

  const afterSweep = await state(unkRes);
  const invAfterSweep = await inventory(ttUnknown);
  check(afterSweep.reservation.status === 'PENDING_PAYMENT', `still PENDING_PAYMENT after a sweep, got ${afterSweep.reservation.status}`);
  check(invAfterSweep.held === 1, `seats still held after a sweep, got held=${invAfterSweep.held}`);

  // -------------------------------------------------------------------------
  console.log("\n6. another user in the same tenant cannot pay for it");
  // -------------------------------------------------------------------------
  // RLS scopes this database by TENANT, not by user — every policy reads
  // app.tenant_id and nothing else. So the userId filter in the endpoint is the
  // only thing standing between a tenant member and someone else's reservation.
  const otherRes = `${run}_other`;
  await tx(async () => {
    await db.query(
      `INSERT INTO "Reservation" (id,"tenantId","eventId","userId",status,"expiresAt","updatedAt")
       VALUES ($1,$2,$3,'u_somebody_else','HELD', now() + interval '1 hour', now())`,
      [otherRes, TENANT, eventId],
    );
    await db.query(
      `INSERT INTO "ReservationItem" (id,"tenantId","reservationId","ticketTypeId",quantity,"unitPriceCents")
       VALUES ($1,$2,$3,$4,1,$5)`,
      [`${otherRes}_i`, TENANT, otherRes, ttOk, PRICE_OK],
    );
    // Move the counters too. Seeding a hold without decrementing remaining
    // leaves inventory that no reservation explains, and
    // verify-inventory-reconciliation correctly fails on it — a test that
    // corrupts the thing a later test checks is worse than no test.
    await db.query(
      `UPDATE "TicketType" SET remaining = remaining - 1, held = held + 1,
              version = version + 1, "updatedAt" = now()
        WHERE id = $1`,
      [ttOk],
    );
  });

  const stolen = await payFor(otherRes);
  const otherState = await state(otherRes);
  check(stolen.status === 404, `404, got ${stolen.status}`);
  check(otherState.reservation.status === 'HELD', `untouched, got ${otherState.reservation.status}`);
  check(otherState.payments.length === 0, `no charge attempted, got ${otherState.payments.length}`);

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('payment path verified: charges settle, declines return the hold, and an');
  console.log('unanswered provider leaves the seats held for reconciliation to resolve');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
