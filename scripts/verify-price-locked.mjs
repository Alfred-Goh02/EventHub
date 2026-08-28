/**
 * Asserts that the price a buyer is quoted at hold time cannot be changed
 * underneath them.
 *
 * Without ReservationItem.unitPriceCents the charge amount would be read from
 * TicketType.priceCents at PAY time, which is a mutable row an organiser can
 * edit at any moment. Someone who put a $10 ticket in their cart could then be
 * charged $500 for it, having agreed to neither the price nor the change — and
 * nothing in the system would record that the number had moved.
 *
 * This holds a seat, edits the price behind the buyer's back, and asserts the
 * reservation still says what it said. It also checks the reverse direction: a
 * price DROP must not silently discount an existing hold either, because the
 * guarantee is "the amount is fixed at hold time", not "the amount is whatever
 * favours the buyer".
 *
 * Run: node scripts/verify-price-locked.mjs
 */

import pg from 'pg';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';

const TENANT = 'tenant_acme';
const ORIGINAL_PRICE = 1000;
const GOUGED_PRICE = 50_000;
const DISCOUNT_PRICE = 1;

const run = `pl_${Date.now()}`;
const eventId = `${run}_evt`;
const ticketTypeId = `${run}_tt`;

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

async function main() {
  const db = new pg.Client({ connectionString: APP_URL });
  await db.connect();

  // Every statement below runs inside a transaction with app.tenant_id set,
  // because the app role is subject to FORCE row-level security and sees
  // nothing at all without it.
  const tx = async (fn) => {
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
  };

  await tx(async () => {
    await db.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Price Lock Test','price-lock-test','PUBLISHED',10,now(),now(),'u_test',1,now())`,
      [eventId, TENANT],
    );
    await db.query(
      `INSERT INTO "TicketType"
         (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
       VALUES ($1,$2,$3,'General',$4,10,10,0,0,now())`,
      [ticketTypeId, TENANT, eventId, ORIGINAL_PRICE],
    );
  });
  console.log(`  seeded a ticket type at ${ORIGINAL_PRICE} cents`);

  const loginRes = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await loginRes.json();
  if (!accessToken) fail('could not log in');

  const res = await fetch(`${TICKETING}/reservations`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${accessToken}`,
      'Idempotency-Key': `${run}_hold`,
    },
    body: JSON.stringify({ eventId, items: [{ ticketTypeId, quantity: 2 }] }),
  });
  const hold = await res.json();
  if (res.status !== 201) fail(`expected 201 for the hold, got ${res.status}`);
  console.log(`  held 2 seats (reservation ${hold.id})`);

  const priceOnReservation = async () => {
    const { rows } = await db.query(
      `SELECT "unitPriceCents" FROM "ReservationItem" WHERE "reservationId" = $1`,
      [hold.id],
    );
    if (!rows.length) fail('the reservation has no items');
    return rows[0].unitPriceCents;
  };

  const captured = await tx(priceOnReservation);
  if (captured !== ORIGINAL_PRICE) {
    fail(`the hold captured ${captured} cents, expected ${ORIGINAL_PRICE}`);
  }
  console.log(`  the reservation captured ${captured} cents`);

  // The attack: raise the price while the buyer is still in checkout.
  for (const [label, newPrice] of [
    ['raised', GOUGED_PRICE],
    ['dropped', DISCOUNT_PRICE],
  ]) {
    await tx(async () => {
      await db.query(`UPDATE "TicketType" SET "priceCents" = $2, "updatedAt" = now() WHERE id = $1`, [
        ticketTypeId,
        newPrice,
      ]);
    });

    const after = await tx(priceOnReservation);
    console.log(`  price ${label} to ${newPrice}; reservation still says ${after}`);

    if (after !== ORIGINAL_PRICE) {
      fail(
        `the reservation followed the ticket type to ${after} cents. The buyer agreed to ` +
          `${ORIGINAL_PRICE} and the amount moved underneath them.`,
      );
    }
  }

  // And the amount owed is reconstructible from the reservation ALONE — no read
  // of TicketType at all. That is what reconciliation needs: an amount that
  // cannot have moved since, because nothing it depends on is mutable.
  const owed = await tx(async () => {
    const { rows } = await db.query(
      `SELECT sum(quantity * "unitPriceCents")::int AS cents
         FROM "ReservationItem" WHERE "reservationId" = $1`,
      [hold.id],
    );
    return rows[0].cents;
  });

  await db.end();

  const expected = 2 * ORIGINAL_PRICE;
  if (owed !== expected) fail(`amount owed came to ${owed}, expected ${expected}`);
  console.log(`  amount owed, computed from the reservation alone: ${owed} cents`);

  console.log('');
  console.log('price is locked at hold time: editing the ticket type cannot move what a buyer owes');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
