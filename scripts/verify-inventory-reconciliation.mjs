/**
 * Asserts that inventory counters agree with the reservations that explain them.
 *
 * The CHECK constraint on TicketType already guarantees the ARITHMETIC:
 *
 *     remaining + held + sold = total,  and none negative
 *
 * But arithmetic consistency is not the same as being CORRECT. The counters
 * could sum perfectly while describing reservations that do not exist — a
 * sweeper that returned inventory without expiring its reservation, a payment
 * path that moved held to sold for a row it never confirmed, a rollback that
 * left the counter moved. The constraint cannot see any of that, because it only
 * looks at one row.
 *
 * So this checks the counters against their EXPLANATION:
 *
 *     held = sum of quantities on HELD and PENDING_PAYMENT reservations
 *     sold = sum of quantities on CONFIRMED reservations
 *
 * PENDING_PAYMENT counts toward `held` deliberately: those seats are still
 * reserved and not yet paid for. They have merely stopped counting down.
 *
 * This is the single assertion worth running after ANY chaotic sequence —
 * concurrent buyers, an interrupted sweeper, a payment that failed halfway. If
 * money and inventory ever disagree, it shows up here.
 *
 * Run: node scripts/verify-inventory-reconciliation.mjs
 */

import pg from 'pg';

const OWNER_URL =
  process.env['TICKETING_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_owner:eventhub_ticketing_owner@localhost:5432/eventhub_ticketing';

/**
 * Runs as the OWNER with RLS suspended for the duration.
 *
 * A reconciliation check must see EVERY tenant's rows — it is asking a question
 * about the database as a whole, not about one customer. The owner is subject to
 * FORCE ROW LEVEL SECURITY like everyone else, and matches no tenant policy, so
 * without this it would cheerfully report zero rows and pass. That failure mode
 * is worth naming: a check that passes because it can see nothing is worse than
 * no check at all.
 */
const SUSPEND = `
  ALTER TABLE "TicketType"      NO FORCE ROW LEVEL SECURITY;
  ALTER TABLE "Reservation"     NO FORCE ROW LEVEL SECURITY;
  ALTER TABLE "ReservationItem" NO FORCE ROW LEVEL SECURITY;
`;
const RESTORE = `
  ALTER TABLE "TicketType"      FORCE ROW LEVEL SECURITY;
  ALTER TABLE "Reservation"     FORCE ROW LEVEL SECURITY;
  ALTER TABLE "ReservationItem" FORCE ROW LEVEL SECURITY;
`;

const RECONCILE = `
  SELECT
    t.id,
    t."tenantId",
    t.name,
    t.total,
    t.remaining,
    t.held,
    t.sold,
    coalesce(h.qty, 0)::int AS expected_held,
    coalesce(s.qty, 0)::int AS expected_sold
  FROM "TicketType" t
  LEFT JOIN (
    SELECT i."ticketTypeId", sum(i.quantity) AS qty
      FROM "ReservationItem" i
      JOIN "Reservation" r
        ON r.id = i."reservationId" AND r."tenantId" = i."tenantId"
     WHERE r.status IN ('HELD', 'PENDING_PAYMENT')
     GROUP BY i."ticketTypeId"
  ) h ON h."ticketTypeId" = t.id
  LEFT JOIN (
    SELECT i."ticketTypeId", sum(i.quantity) AS qty
      FROM "ReservationItem" i
      JOIN "Reservation" r
        ON r.id = i."reservationId" AND r."tenantId" = i."tenantId"
     WHERE r.status = 'CONFIRMED'
     GROUP BY i."ticketTypeId"
  ) s ON s."ticketTypeId" = t.id
  ORDER BY t."tenantId", t.id
`;

async function main() {
  const db = new pg.Client({ connectionString: OWNER_URL });
  await db.connect();

  let rows;
  try {
    await db.query(SUSPEND);
    ({ rows } = await db.query(RECONCILE));
  } finally {
    // Always restore, even if the query threw. Leaving FORCE off would silently
    // weaken tenant isolation for every subsequent connection.
    await db.query(RESTORE).catch((err) => {
      console.error('CRITICAL: failed to restore FORCE ROW LEVEL SECURITY:', err);
      process.exitCode = 1;
    });
    await db.end();
  }

  if (rows.length === 0) {
    console.log('  no ticket types to reconcile (nothing to check yet)');
    return;
  }

  const broken = [];
  for (const r of rows) {
    const problems = [];
    if (r.remaining + r.held + r.sold !== r.total) {
      problems.push(`arithmetic: ${r.remaining} + ${r.held} + ${r.sold} != ${r.total}`);
    }
    if (r.held !== r.expected_held) {
      problems.push(`held ${r.held} but HELD/PENDING_PAYMENT items sum to ${r.expected_held}`);
    }
    if (r.sold !== r.expected_sold) {
      problems.push(`sold ${r.sold} but CONFIRMED items sum to ${r.expected_sold}`);
    }
    if (problems.length) broken.push({ id: r.id, tenantId: r.tenantId, problems });
  }

  console.log(`  reconciled ${rows.length} ticket type(s) across all tenants`);
  for (const r of rows) {
    console.log(
      `    ${String(r.id).padEnd(24)} total=${String(r.total).padStart(4)} ` +
        `remaining=${String(r.remaining).padStart(4)} held=${String(r.held).padStart(3)} ` +
        `sold=${String(r.sold).padStart(3)}`,
    );
  }

  if (broken.length) {
    console.error('');
    console.error('RECONCILIATION FAILED — inventory does not match the reservations explaining it:');
    for (const b of broken) {
      console.error(`  ${b.tenantId}/${b.id}`);
      for (const p of b.problems) console.error(`    - ${p}`);
    }
    process.exit(1);
  }

  console.log('');
  console.log('inventory reconciles: counters match the reservations that explain them');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
