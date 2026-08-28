/**
 * Runs two sweepers at once against the same expired holds and asserts that
 * inventory is returned exactly ONCE.
 *
 * This is the test the payment-path review asked for, and it targets a failure
 * the schema cannot catch on its own: a DOUBLE INVENTORY RETURN passes both
 * TicketType CHECK constraints. Returning the same seats twice keeps
 * remaining + held + sold = total perfectly true — it just moves the total in
 * the wrong direction and oversells silently. The constraint only ever sees one
 * row, so it cannot notice.
 *
 * Two things make that impossible here, and this proves both:
 *
 *   FOR UPDATE SKIP LOCKED in the SAME transaction as the release, so a second
 *   sweeper never claims a row the first is working on. Claiming in one
 *   transaction and releasing in another would leave the rows unlocked in
 *   between and make the lock decorative.
 *
 *   AND status = 'HELD' on the expiring UPDATE, so even if a row were reached
 *   twice, only the first write moves inventory.
 *
 * Run: node scripts/verify-sweeper-concurrency.mjs
 */

import { spawn } from 'node:child_process';
import pg from 'pg';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const SWEEPER_URL =
  process.env['SWEEPER_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_sweeper:eventhub_ticketing_sweeper@localhost:5432/eventhub_ticketing';

const TENANT = 'tenant_acme';
const HOLDS = 30;
const SEATS_EACH = 2;
const TOTAL = HOLDS * SEATS_EACH;

const run = `swp_${Date.now()}`;
const eventId = `${run}_evt`;
const ticketTypeId = `${run}_tt`;

const fail = (m) => { console.error(`FAIL: ${m}`); process.exit(1); };

function runSweeper(label) {
  return new Promise((resolve) => {
    const child = spawn('node', ['dist/sweeper/index.js', '--once'], {
      cwd: 'services/ticketing',
      env: {
        ...process.env,
        SWEEPER_DATABASE_URL: SWEEPER_URL,
        SWEEPER_BATCH_SIZE: '100',
        // info, so each sweeper's own tally is visible — seeing HOW the work
        // divided between them is the point of the test.
        LOG_LEVEL: 'info',
      },
    });
    let out = '';
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { out += d; });
    child.on('close', () => resolve({ label, out }));
  });
}

async function main() {
  const db = new pg.Client({ connectionString: APP_URL });
  await db.connect();

  // ---------------------------------------------------------------------
  // Seed holds that are ALREADY expired, so both sweepers see them at once.
  // ---------------------------------------------------------------------
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  await db.query(
    `INSERT INTO "EventProjection"
       (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
     VALUES ($1,$2,'Sweeper Race','sweeper-race','PUBLISHED',$3,now(),now(),'u1',1,now())`,
    [eventId, TENANT, TOTAL],
  );
  await db.query(
    `INSERT INTO "TicketType"
       (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
     VALUES ($1,$2,$3,'General',1000,$4,0,$4,0,now())`,
    [ticketTypeId, TENANT, eventId, TOTAL],
  );

  for (let i = 0; i < HOLDS; i += 1) {
    const rid = `${run}_r${i}`;
    await db.query(
      `INSERT INTO "Reservation"
         (id,"tenantId","eventId","userId",status,"expiresAt","updatedAt")
       VALUES ($1,$2,$3,'u_race','HELD', now() - interval '1 minute', now())`,
      [rid, TENANT, eventId],
    );
    await db.query(
      `INSERT INTO "ReservationItem" (id,"tenantId","reservationId","ticketTypeId",quantity)
       VALUES ($1,$2,$3,$4,$5)`,
      [`${rid}_i`, TENANT, rid, ticketTypeId, SEATS_EACH],
    );
  }
  await db.query('COMMIT');
  console.log(`  seeded ${HOLDS} expired holds totalling ${TOTAL} seats (all currently held)`);

  // ---------------------------------------------------------------------
  // Two sweepers, at the same time.
  // ---------------------------------------------------------------------
  const results = await Promise.all([runSweeper('A'), runSweeper('B')]);
  for (const r of results) {
    const m = r.out.match(/"claimed":(\d+),"expired":(\d+),"skipped":(\d+),"seatsReturned":(\d+)/);
    console.log(`  sweeper ${r.label}: ${m ? m[0] : '(no result line)'}`);
  }

  // ---------------------------------------------------------------------
  // The assertions.
  // ---------------------------------------------------------------------
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  const { rows: tt } = await db.query(
    `SELECT total, remaining, held, sold FROM "TicketType" WHERE id = $1`,
    [ticketTypeId],
  );
  const { rows: st } = await db.query(
    `SELECT status, count(*)::int AS n FROM "Reservation" WHERE "eventId" = $1 GROUP BY status`,
    [eventId],
  );
  const { rows: msgs } = await db.query(
    `SELECT count(*)::int AS n FROM "OutboxMessage"
      WHERE "aggregateId" = $1 AND "eventType" = 'reservation.expired'`,
    [eventId],
  );
  await db.query('COMMIT');
  await db.end();

  const t = tt[0];
  console.log(`  final: total=${t.total} remaining=${t.remaining} held=${t.held} sold=${t.sold}`);
  console.log(`  statuses: ${st.map((r) => `${r.status}=${r.n}`).join(', ')}`);
  console.log(`  release messages: ${msgs[0].n}`);

  // Returned EXACTLY once: every seat back, nothing beyond.
  if (t.remaining !== TOTAL) {
    fail(
      t.remaining > TOTAL
        ? `DOUBLE RELEASE: remaining is ${t.remaining}, more than the ${TOTAL} that exist`
        : `only ${t.remaining} of ${TOTAL} seats were returned`,
    );
  }
  if (t.held !== 0) fail(`held should be 0 after every hold expired, got ${t.held}`);
  if (t.remaining + t.held + t.sold !== t.total) fail('arithmetic invariant broken');

  const expired = st.find((r) => r.status === 'EXPIRED')?.n ?? 0;
  if (expired !== HOLDS) fail(`expected ${HOLDS} EXPIRED reservations, got ${expired}`);

  // One release message per reservation — not two, and not none.
  if (msgs[0].n !== HOLDS) {
    fail(`expected ${HOLDS} release messages, got ${msgs[0].n} — a duplicate means a row was swept twice`);
  }

  console.log('');
  console.log('two concurrent sweepers returned every seat exactly once');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
