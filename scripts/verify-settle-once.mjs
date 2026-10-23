/**
 * Asserts that two routes learning about the same charge settle it ONCE.
 *
 * The provider's webhook can arrive while the reconciler is asking about the
 * same payment. Both call settleAttempt(). Exactly one must do the work — and
 * the failure that makes this matter is invisible to the schema: a double
 * `held -> sold` keeps `remaining + held + sold = total` perfectly true. Both
 * TicketType CHECK constraints pass, because a constraint sees one row and
 * cannot notice the row was moved twice.
 *
 * WHY THIS EXISTS SEPARATELY from verify-payment-reconciliation.mjs, which has
 * a section titled "a second pass must not move anything twice".
 *
 * That section runs the reconciler again after the first pass settled
 * everything. The candidate query is `WHERE "settledAt" IS NULL`, so the second
 * pass selects NOTHING. Measured, with LOG_LEVEL raised to see it:
 *
 *   pass 1: examined=8 confirmed=8 released=0 stillUnknown=0
 *   pass 2: examined=0 confirmed=0 released=0 stillUnknown=0
 *
 * Nothing could move twice because nothing was looked at. That section is not
 * worthless — it proves settled attempts are excluded from the candidate query —
 * but it is not the assertion its heading makes, and the guard it appears to be
 * about is never reached.
 *
 * So this drives settleAttempt() directly, on two connections, with the second
 * arriving while the first transaction is still open. That is the real
 * interleaving: the reconciler takes NO lock when it selects candidates (no FOR
 * UPDATE SKIP LOCKED, unlike the relay's drain and the sweeper's claim), so two
 * processes genuinely do both select the same row.
 *
 * THERE ARE TWO GUARDS IN SERIES, and the first version of this test could not
 * tell them apart. Payment is guarded by `settledAt IS NULL`; Reservation is
 * guarded by `status = 'PENDING_PAYMENT'`, and inventory only moves if THAT
 * matched. So a test where both routes report the SAME outcome still passes
 * with the Payment guard deleted — the reservation guard catches it. I watched
 * exactly that: guard removed, every assertion still green.
 *
 * What the Payment guard uniquely protects is FIRST ANSWER WINS when the two
 * routes DISAGREE. So case 2 has the webhook arrive saying the charge FAILED
 * after the reconciler already settled it as succeeded. With the guard that is
 * a noop. Without it the Payment flips to FAILED while the reservation stays
 * CONFIRMED and the seat stays sold — a payment record saying the charge failed,
 * for a seat that was sold.
 *
 * First-wins is the right semantics even though a later answer might be more
 * accurate, because "later" is indistinguishable from "out of order", and the
 * sale has already been made and published. Flipping the payment without
 * undoing the sale is strictly worse than keeping it.
 *
 * Run: node scripts/verify-settle-once.mjs
 */

import pg from 'pg';
import { settleAttempt } from '../services/ticketing/dist/payments/settle.js';

const SWEEPER_URL =
  process.env['SWEEPER_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_sweeper:eventhub_ticketing_sweeper@localhost:5432/eventhub_ticketing';
const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';

const TENANT = 'tenant_acme';
const run = `so_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const logger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} };

const app = new pg.Client({ connectionString: APP_URL, application_name: 'verify-settle-once' });
app.on('error', () => {});

async function tx(fn) {
  await app.query('BEGIN');
  await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  try {
    const out = await fn();
    await app.query('COMMIT');
    return out;
  } catch (err) {
    await app.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** One event, one held seat, one unsettled attempt — a fresh set per case. */
async function seed(label) {
  const ids = {
    event: `${run}_${label}_evt`,
    ticketType: `${run}_${label}_tt`,
    reservation: `${run}_${label}_res`,
    payment: `${run}_${label}_pay`,
  };

  await tx(async () => {
    await app.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Settle Once',$3,'PUBLISHED',100,now(),now(),'u_test',1,now())`,
      [ids.event, TENANT, `settle-once-${label}-${run}`],
    );
    // A double `held -> sold` takes sold to 2 while remaining + held + sold
    // still equals total, which is exactly why the CHECK cannot catch it.
    await app.query(
      `INSERT INTO "TicketType"
         (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
       VALUES ($1,$2,$3,'General',1000,10,9,1,0,now())`,
      [ids.ticketType, TENANT, ids.event],
    );
    await app.query(
      `INSERT INTO "Reservation"
         (id,"tenantId","eventId","userId",status,"expiresAt","paymentStartedAt",version,"createdAt","updatedAt")
       VALUES ($1,$2,$3,'u_test','PENDING_PAYMENT', now() + interval '1 hour', now(), 1, now(), now())`,
      [ids.reservation, TENANT, ids.event],
    );
    await app.query(
      `INSERT INTO "ReservationItem"
         (id,"tenantId","reservationId","ticketTypeId",quantity,"unitPriceCents")
       VALUES ($1,$2,$3,$4,1,1000)`,
      [`${run}_${label}_item`, TENANT, ids.reservation, ids.ticketType],
    );
    /**
     * Started NOW, not two hours ago, and it used to be two hours ago.
     *
     * settleAttempt() never reads startedAt, so the age bought this test nothing --
     * and it made the row a candidate for every reconciler running, the moment it
     * committed. Compose runs one now (ticketing-reconciler), whose fake provider
     * answers "succeeded" for 1000, so a pass landing between this INSERT and the
     * race below settles the row first and both routes report noop. Measured with
     * that window widened to 6 s, one compose interval: "the first settlement did
     * the work, got 'noop'" -- while the seat-sold-once and one-purchase checks beside
     * it stayed green, for the compose reconciler's work. A payment started now is
     * inside any deployed reconciler's grace, which is more than twice the provider
     * timeout and so over 16 s by default.
     */
    await app.query(
      `INSERT INTO "Payment"
         (id,"tenantId","reservationId","userId","amountCents",currency,status,"startedAt","updatedAt")
       VALUES ($1,$2,$3,'u_test',1000,'SGD','UNKNOWN', now(), now())`,
      [ids.payment, TENANT, ids.reservation],
    );
  });

  return ids;
}

const inventory = (ticketTypeId) =>
  tx(async () => {
    const { rows } = await app.query(
      `SELECT total, remaining, held, sold FROM "TicketType" WHERE id = $1`,
      [ticketTypeId],
    );
    return rows[0];
  });

const paymentRow = (paymentId) =>
  tx(async () => {
    const { rows } = await app.query(
      `SELECT status, "providerRef", "failureReason" FROM "Payment" WHERE id = $1`,
      [paymentId],
    );
    return rows[0];
  });

const purchaseMessages = (reservationId) =>
  tx(async () => {
    const { rows } = await app.query(
      `SELECT count(*)::int AS n FROM "OutboxMessage"
        WHERE payload->>'reservationId' = $1 AND topic = 'tickets.purchased'`,
      [reservationId],
    );
    return rows[0].n;
  });

/**
 * Run two settlements against one attempt with overlapping transactions.
 *
 * The second is ISSUED while the first is still open, so it blocks on the row
 * lock rather than reading around it. It unblocks when the first commits, and
 * only then re-evaluates its own WHERE clause against what the first wrote —
 * which is the only reason the guard works at all.
 */
async function race(ids, firstOutcome, secondOutcome) {
  const target = {
    id: ids.payment,
    tenantId: TENANT,
    reservationId: ids.reservation,
    amountCents: 1000,
    currency: 'SGD',
  };

  const a = new pg.Client({ connectionString: SWEEPER_URL, application_name: 'settle-a' });
  const b = new pg.Client({ connectionString: SWEEPER_URL, application_name: 'settle-b' });
  a.on('error', () => {});
  b.on('error', () => {});
  await Promise.all([a.connect(), b.connect()]);

  await a.query('BEGIN');
  await b.query('BEGIN');

  const first = await settleAttempt(a, target, firstOutcome, logger);
  const pending = settleAttempt(b, target, secondOutcome, logger);
  await a.query('COMMIT');
  const second = await pending;
  await b.query('COMMIT');

  await Promise.all([a.end(), b.end()]);
  return { first, second };
}

async function main() {
  await app.connect();

  // -------------------------------------------------------------------------
  console.log('  1. both routes report the SAME outcome');
  // -------------------------------------------------------------------------
  const same = await seed('same');
  const succeeded = { status: 'succeeded', providerRef: `fake_ref_${same.payment}` };

  const r1 = await race(same, succeeded, succeeded);
  console.log(`     reconciler: ${r1.first}   webhook: ${r1.second}`);

  check(r1.first === 'confirmed', `the first settlement did the work, got '${r1.first}'`);
  check(
    r1.second === 'noop',
    r1.second === 'noop'
      ? "the second was refused, got 'noop'"
      : `the second ALSO did the work, got '${r1.second}'`,
  );

  const inv1 = await inventory(same.ticketType);
  check(inv1.sold === 1 && inv1.held === 0, `the seat sold once (held ${inv1.held}, sold ${inv1.sold})`);
  check(
    inv1.remaining + inv1.held + inv1.sold === inv1.total,
    `the CHECK constraint still holds (${inv1.remaining}+${inv1.held}+${inv1.sold}=${inv1.total}) — ` +
      'which is why it cannot catch a double settlement on its own',
  );
  check(
    (await purchaseMessages(same.reservation)) === 1,
    'exactly one tickets.purchased message',
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  2. the routes DISAGREE — the webhook says it failed, after the fact');
  // -------------------------------------------------------------------------
  /**
   * This is the case the Payment guard uniquely protects, and the reason case 1
   * alone is not enough: with the guard deleted, case 1 still passes because the
   * reservation guard stops the second settlement from moving inventory.
   * Nothing stops it rewriting the Payment row.
   */
  const split = await seed('split');
  const r2 = await race(
    split,
    { status: 'succeeded', providerRef: `fake_ref_${split.payment}` },
    { status: 'failed', reason: 'card_declined_late' },
  );
  console.log(`     reconciler: ${r2.first}   webhook: ${r2.second}`);

  check(r2.first === 'confirmed', `the first answer won, got '${r2.first}'`);
  check(r2.second === 'noop', `the contradicting answer was refused, got '${r2.second}'`);

  const pay2 = await paymentRow(split.payment);
  check(
    pay2.status === 'SUCCEEDED',
    pay2.status === 'SUCCEEDED'
      ? 'the payment is still SUCCEEDED'
      : `the payment was rewritten to ${pay2.status} — it now says the charge failed for a seat ` +
        'that was sold',
  );
  check(
    pay2.failureReason === null,
    `failureReason is ${JSON.stringify(pay2.failureReason)}, expected null`,
  );
  check(
    pay2.providerRef === `fake_ref_${split.payment}`,
    `providerRef still names the successful charge (${pay2.providerRef})`,
  );

  const inv2 = await inventory(split.ticketType);
  check(inv2.sold === 1 && inv2.held === 0, `the seat stayed sold (held ${inv2.held}, sold ${inv2.sold})`);

  await app.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('one attempt settles once, and the first answer is the one that stands');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
