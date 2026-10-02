/**
 * Asserts that an event cancelled AFTER a hold was taken cannot still be paid
 * for.
 *
 * POST /reservations has always refused to open a hold on an event that is not
 * PUBLISHED. POST /reservations/:id/pay checked NOTHING -- a grep for
 * eventProjection found it read in exactly one place outside the consumer, and
 * that place was the new-hold gate. So the expensive half of the flow was the
 * unguarded one: the organiser calls the event off, the cancellation propagates
 * to the projection, and a buyer who was already in checkout completes it and
 * is charged for something that is not happening.
 *
 * Taking money is the one step where "is this still real?" has to be asked
 * again, because it is the step that is expensive to undo. A hold costs nothing
 * to abandon; a charge costs a refund and a conversation.
 *
 * Two assertions, and the second is the one that is easy to get wrong:
 *
 *   REFUSED     the charge does not happen. 409, and no Payment row at all --
 *               not a FAILED one, because nothing was attempted.
 *
 *   ROLLED BACK the reservation is still HELD. The gate signals by THROWING so
 *               the phase-1 transaction takes the HELD -> PENDING_PAYMENT
 *               transition down with it. Returning instead would leave the
 *               reservation stranded in PENDING_PAYMENT for an event nobody can
 *               attend -- and the sweeper skips PENDING_PAYMENT by design, so
 *               those seats would be held until a human noticed.
 *
 * The first case run is the POSITIVE CONTROL: an identical reservation on the
 * same event, paid while it is still PUBLISHED, which must go through for 200.
 * Without it a verifier that 409s for some entirely unrelated reason -- bad
 * seeding, a broken login, a typo in the URL -- would pass while asserting
 * nothing at all.
 *
 * Run: node scripts/verify-cancelled-event-gate.mjs
 */

import pg from 'pg';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';

const TENANT = 'tenant_acme';
/** The seeded tenant's subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';

// The fake provider picks its outcome from the last two digits of the amount.
// 00 succeeds, which is what the positive control needs. See payments/provider.ts.
const PRICE_OK = 1000;

const run = `cg_${Date.now()}`;
const eventId = `${run}_evt`;

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

const stateOf = (reservationId) =>
  tx(async () => {
    const r = await db.query(`SELECT status FROM "Reservation" WHERE id = $1`, [reservationId]);
    const p = await db.query(
      `SELECT id, status FROM "Payment" WHERE "reservationId" = $1 ORDER BY "startedAt"`,
      [reservationId],
    );
    return { status: r.rows[0]?.status ?? null, payments: p.rows };
  });

const inventoryOf = (ticketTypeId) =>
  tx(async () => {
    const { rows } = await db.query(
      `SELECT remaining, held, sold FROM "TicketType" WHERE id = $1`,
      [ticketTypeId],
    );
    return rows[0];
  });

/** Put the projection into whatever state this case is about. */
const setProjection = (status, deleted) =>
  tx(async () => {
    await db.query(
      `UPDATE "EventProjection"
          SET status = $2, deleted = $3, "sourceVersion" = "sourceVersion" + 1, "updatedAt" = now()
        WHERE id = $1`,
      [eventId, status, deleted],
    );
  });

async function main() {
  await db.connect();

  // Three identical ticket types on one event, so each case gets its own seats
  // and no case can be affected by what an earlier one did to inventory.
  const CASES = [
    {
      key: 'control',
      label: 'PUBLISHED (positive control)',
      projection: ['PUBLISHED', false],
      expectStatus: 200,
      expectReservation: 'CONFIRMED',
      expectPayments: 1,
    },
    {
      key: 'cancelled',
      label: 'CANCELLED',
      projection: ['CANCELLED', false],
      expectStatus: 409,
      expectReservation: 'HELD',
      expectPayments: 0,
    },
    {
      key: 'deleted',
      label: 'PUBLISHED but soft-deleted',
      projection: ['PUBLISHED', true],
      expectStatus: 409,
      expectReservation: 'HELD',
      expectPayments: 0,
    },
  ];

  await tx(async () => {
    await db.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Cancelled Gate',$3,'PUBLISHED',100,now(),now(),'u_test',1,now())`,
      [eventId, TENANT, `cancelled-gate-${run}`],
    );
    for (const c of CASES) {
      await db.query(
        `INSERT INTO "TicketType"
           (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
         VALUES ($1,$2,$3,$4,$5,10,10,0,0,now())`,
        [`${run}_tt_${c.key}`, TENANT, eventId, `General ${c.key}`, PRICE_OK],
      );
    }
  });
  console.log(`  seeded a PUBLISHED event with ${CASES.length} ticket types`);

  const loginRes = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await loginRes.json();
  if (!accessToken) fail('could not log in');

  /**
   * EVERY hold is taken now, while the event is still PUBLISHED.
   *
   * This is the whole point of the bug. POST /reservations would refuse these
   * once the event is cancelled, so the only way to reach the state that
   * mattered is to be already in checkout when the cancellation lands -- which
   * is exactly the customer this gate protects.
   */
  for (const c of CASES) {
    const res = await fetch(`${TICKETING}/reservations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
        'Idempotency-Key': `${run}_hold_${c.key}`,
      },
      body: JSON.stringify({ eventId, items: [{ ticketTypeId: `${run}_tt_${c.key}`, quantity: 1 }] }),
    });
    const body = await res.json();
    if (res.status !== 201) {
      fail(`expected 201 for the ${c.key} hold, got ${res.status} ${JSON.stringify(body)}`);
    }
    c.reservationId = body.id;
  }
  console.log(`  took ${CASES.length} holds while the event was on sale`);

  for (const c of CASES) {
    console.log('');
    console.log(`  ${c.label}`);

    await setProjection(c.projection[0], c.projection[1]);
    const before = await inventoryOf(`${run}_tt_${c.key}`);

    const res = await fetch(`${TICKETING}/reservations/${c.reservationId}/pay`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
    });
    const body = await res.json();

    check(
      res.status === c.expectStatus,
      `POST /pay returned ${res.status}, expected ${c.expectStatus}` +
        (res.status === c.expectStatus ? '' : ` -- ${JSON.stringify(body)}`),
    );

    const after = await stateOf(c.reservationId);

    check(
      after.status === c.expectReservation,
      `reservation is ${after.status}, expected ${c.expectReservation}`,
    );

    check(
      after.payments.length === c.expectPayments,
      `${after.payments.length} payment row(s), expected ${c.expectPayments}` +
        (after.payments.length ? ` [${after.payments.map((p) => p.status).join(', ')}]` : ''),
    );

    const inv = await inventoryOf(`${run}_tt_${c.key}`);

    if (c.expectStatus === 409) {
      /**
       * The rollback assertion, stated in inventory terms as well as status.
       *
       * A gate that RETURNED instead of throwing would leave the reservation in
       * PENDING_PAYMENT with the same held/remaining counts, so the counters
       * alone would not catch it -- but the status check above would. Both are
       * here because they fail for different mistakes: the counters catch a gate
       * placed AFTER the inventory move, the status catches one that does not
       * roll back.
       */
      check(
        inv.held === before.held && inv.remaining === before.remaining && inv.sold === before.sold,
        `inventory unchanged (held ${inv.held}, remaining ${inv.remaining}, sold ${inv.sold})`,
      );
    } else {
      check(
        inv.sold === 1 && inv.held === 0,
        `the control sale settled (held ${inv.held}, sold ${inv.sold})`,
      );
    }
  }

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('a cancelled or deleted event cannot be charged for, and refusing leaves an ordinary hold');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
