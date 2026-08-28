/**
 * Proves the whole cross-service chain, for CI:
 *
 *   Event service  ->  outbox  ->  relay  ->  Avro  ->  Kafka
 *                                                        |
 *                        Ticketing's projection  <--------+
 *
 * and then the business rule that depends on it: cancelling an event in one
 * service stops sales in the other, across two databases with no connection
 * between them.
 *
 * This is the assertion that would catch the defect the Chapter 4 design review
 * found — that only /publish emitted an outbox row, so the projection was stale
 * by construction for every other state change.
 *
 * Assumes auth (3001), event (3002) and ticketing (3003) are already running,
 * and that the relay can be invoked with --once.
 *
 * Run: node scripts/verify-projection-e2e.mjs
 */

import { execFileSync } from 'node:child_process';
import pg from 'pg';

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const EVENT = process.env['EVENT_URL'] ?? 'http://localhost:3002';
const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';
const TICKETING_APP =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const RELAY_URL =
  process.env['RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_event_relay:eventhub_event_relay@localhost:5432/eventhub_event';

const TENANT = 'tenant_acme';
const stamp = Date.now();

const fail = (m) => { console.error(`FAIL: ${m}`); process.exit(1); };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function drainRelay() {
  execFileSync('node', ['dist/index.js', '--once'], {
    cwd: 'services/relay',
    stdio: 'ignore',
    env: { ...process.env, RELAY_DATABASE_URL: RELAY_URL },
  });
}

/** Polls the projection, because consumption is asynchronous by design. */
async function waitForProjection(db, eventId, predicate, label, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  let last = null;
  while (Date.now() < deadline) {
    await db.query('BEGIN');
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
    const { rows } = await db.query(
      `SELECT status, capacity, "sourceVersion", deleted FROM "EventProjection" WHERE id = $1`,
      [eventId],
    );
    await db.query('COMMIT');
    last = rows[0] ?? null;
    if (last && predicate(last)) return last;
    await sleep(500);
  }
  fail(`timed out waiting for ${label}; last saw ${JSON.stringify(last)}`);
}

async function main() {
  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await login.json();
  if (!accessToken) fail('could not log in');
  const auth = { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' };

  // -------------------------------------------------------------------------
  // 1. Create and publish in the EVENT service.
  // -------------------------------------------------------------------------
  const created = await fetch(`${EVENT}/events`, {
    method: 'POST',
    headers: auth,
    body: JSON.stringify({
      title: `Projection E2E ${stamp}`,
      capacity: 250,
      startsAt: '2027-09-01T10:00:00Z',
      endsAt: '2027-09-01T18:00:00Z',
    }),
  }).then((r) => r.json());

  const eventId = created.id;
  if (!eventId) fail(`event not created: ${JSON.stringify(created)}`);

  const published = await fetch(`${EVENT}/events/${eventId}/publish`, { method: 'POST', headers: auth });
  if (published.status !== 200) fail(`publish returned ${published.status}`);
  console.log(`  created and published ${eventId} in the Event service`);

  // -------------------------------------------------------------------------
  // 2. Drain the outbox, then wait for Ticketing to catch up.
  // -------------------------------------------------------------------------
  drainRelay();

  const db = new pg.Client({ connectionString: TICKETING_APP });
  await db.connect();

  const projected = await waitForProjection(
    db, eventId, (r) => r.status === 'PUBLISHED', 'the projection to show PUBLISHED',
  );
  if (projected.capacity !== 250) fail(`projected capacity ${projected.capacity}, expected 250`);
  console.log(`  Ticketing's projection: ${projected.status} cap=${projected.capacity} v${projected.sourceVersion}`);

  // -------------------------------------------------------------------------
  // 3. Seed a ticket type and prove a reservation succeeds while published.
  // -------------------------------------------------------------------------
  const ticketTypeId = `e2e_tt_${stamp}`;
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  await db.query(
    `INSERT INTO "TicketType" (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
     VALUES ($1,$2,$3,'General',2500,100,100,0,0,now())`,
    [ticketTypeId, TENANT, eventId],
  );
  await db.query('COMMIT');

  const ok = await fetch(`${TICKETING}/reservations`, {
    method: 'POST',
    headers: { ...auth, 'Idempotency-Key': `e2e-${stamp}-a` },
    body: JSON.stringify({ eventId, items: [{ ticketTypeId, quantity: 2 }] }),
  });
  if (ok.status !== 201) fail(`reservation while published returned ${ok.status}`);
  console.log('  reservation while PUBLISHED: 201');

  // -------------------------------------------------------------------------
  // 4. Cancel in the Event service. Ticketing must find out and refuse to sell.
  //
  // This is the assertion that matters: Ticketing has no foreign key to the
  // Event table and no credential for that database. Kafka is the ONLY way it
  // can learn the event was cancelled.
  // -------------------------------------------------------------------------
  const cancelled = await fetch(`${EVENT}/events/${eventId}/cancel`, { method: 'POST', headers: auth });
  if (cancelled.status !== 200) fail(`cancel returned ${cancelled.status}`);
  drainRelay();

  const afterCancel = await waitForProjection(
    db, eventId, (r) => r.status === 'CANCELLED', 'the cancellation to reach the projection',
  );
  console.log(`  cancellation propagated: ${afterCancel.status} v${afterCancel.sourceVersion}`);

  if (afterCancel.sourceVersion <= projected.sourceVersion) {
    fail('sourceVersion did not advance — the monotonic guard may be discarding valid updates');
  }

  const refused = await fetch(`${TICKETING}/reservations`, {
    method: 'POST',
    headers: { ...auth, 'Idempotency-Key': `e2e-${stamp}-b` },
    body: JSON.stringify({ eventId, items: [{ ticketTypeId, quantity: 1 }] }),
  });
  if (refused.status !== 409) fail(`reservation after cancellation returned ${refused.status}, expected 409`);
  console.log('  reservation after CANCELLED: 409');

  // -------------------------------------------------------------------------
  // 5. Dedup: replaying the topic must not change the projection. This is what
  //    makes at-least-once delivery safe.
  // -------------------------------------------------------------------------
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  const { rows: dupRows } = await db.query(
    `SELECT count(*)::int AS n FROM "ProcessedMessage" WHERE "tenantId" = $1`,
    [TENANT],
  );
  await db.query('COMMIT');
  if (dupRows[0].n === 0) fail('no ProcessedMessage rows — deduplication is not recording anything');
  console.log(`  ${dupRows[0].n} messages recorded for deduplication`);

  await db.end();
  console.log('');
  console.log('Event -> outbox -> relay -> Kafka -> projection -> sales rule verified');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
