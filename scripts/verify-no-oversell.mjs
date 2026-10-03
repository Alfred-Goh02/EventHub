/**
 * Fires many concurrent reservations at a deliberately scarce ticket type and
 * asserts that inventory is never oversold.
 *
 * This is the assertion the whole reservation design exists for. The naive
 * read-then-write has no protection under READ COMMITTED — both racers read the
 * same count, both conclude they may proceed, and the seat is sold twice. This
 * proves the single guarded UPDATE actually prevents that under real
 * concurrency, rather than in theory.
 *
 * Run: node scripts/verify-no-oversell.mjs
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
const SEATS = 5;
const BUYERS = 40;

const run = `os_${Date.now()}`;
const eventId = `${run}_evt`;
const ticketTypeId = `${run}_tt`;

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

async function main() {
  // -------------------------------------------------------------------------
  // Seed a projection and a scarce ticket type directly. The consumer that
  // would normally populate EventProjection from Kafka is not built yet, and
  // this test is about inventory concurrency, not about the projection.
  // -------------------------------------------------------------------------
  const db = new pg.Client({ connectionString: APP_URL });
  await db.connect();
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  await db.query(
    `INSERT INTO "EventProjection"
       (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
     VALUES ($1,$2,'Oversell Test','oversell-test','PUBLISHED',$3,now(),now(),'u_test',1,now())`,
    [eventId, TENANT, SEATS],
  );
  await db.query(
    `INSERT INTO "TicketType"
       (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
     VALUES ($1,$2,$3,'General',1000,$4,$4,0,0,now())`,
    [ticketTypeId, TENANT, eventId, SEATS],
  );
  await db.query('COMMIT');
  console.log(`  seeded ${SEATS} seats`);

  // -------------------------------------------------------------------------
  // Log in once, then fire BUYERS concurrent requests for one seat each.
  // -------------------------------------------------------------------------
  const loginRes = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await loginRes.json();
  if (!accessToken) fail('could not log in');

  const attempts = Array.from({ length: BUYERS }, (_, i) =>
    fetch(`${TICKETING}/reservations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
        // A distinct key per buyer — this test is about inventory contention,
        // not about idempotent replay.
        'Idempotency-Key': `${run}_${i}`,
      },
      body: JSON.stringify({ eventId, items: [{ ticketTypeId, quantity: 1 }] }),
    }).then(async (r) => ({ status: r.status, body: await r.json().catch(() => null) })),
  );

  const results = await Promise.all(attempts);

  const created = results.filter((r) => r.status === 201).length;
  const soldOut = results.filter((r) => r.status === 409).length;
  const other = results.filter((r) => r.status !== 201 && r.status !== 409);

  console.log(`  ${BUYERS} concurrent buyers for ${SEATS} seats`);
  console.log(`    201 created:  ${created}`);
  console.log(`    409 sold out: ${soldOut}`);
  if (other.length) {
    console.log(`    other:        ${other.length} -> ${JSON.stringify(other.slice(0, 3))}`);
  }

  // -------------------------------------------------------------------------
  // The assertions.
  // -------------------------------------------------------------------------
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  const { rows } = await db.query(
    `SELECT total, remaining, held, sold FROM "TicketType" WHERE id = $1`,
    [ticketTypeId],
  );
  const { rows: itemRows } = await db.query(
    `SELECT coalesce(sum(i.quantity), 0)::int AS held_items
       FROM "ReservationItem" i
       JOIN "Reservation" r ON r."id" = i."reservationId" AND r."tenantId" = i."tenantId"
      WHERE i."ticketTypeId" = $1 AND r.status = 'HELD'`,
    [ticketTypeId],
  );
  await db.query('COMMIT');
  await db.end();

  const t = rows[0];
  console.log(`  final: total=${t.total} remaining=${t.remaining} held=${t.held} sold=${t.sold}`);

  if (created !== SEATS) fail(`expected exactly ${SEATS} successful reservations, got ${created}`);
  if (t.remaining < 0) fail(`remaining went NEGATIVE: ${t.remaining}`);
  if (t.remaining + t.held + t.sold !== t.total) {
    fail(`invariant broken: ${t.remaining} + ${t.held} + ${t.sold} != ${t.total}`);
  }
  if (Number(itemRows[0].held_items) !== t.held) {
    fail(`held (${t.held}) does not match summed HELD items (${itemRows[0].held_items})`);
  }
  if (other.length) fail(`${other.length} request(s) returned an unexpected status`);

  console.log('');
  console.log('no overselling: exactly the available seats were sold, invariant intact');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
