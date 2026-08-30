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
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
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

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';

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
  // 5. Dedup: REPLAYING A MESSAGE THE CONSUMER HAS ALREADY APPLIED must change
  //    nothing. This is what makes at-least-once delivery safe.
  //
  //    This used to count rows in ProcessedMessage and call it done. That
  //    asserts the consumer WRITES bookkeeping, not that the bookkeeping is ever
  //    READ -- delete the ON CONFLICT check and the count stays exactly the
  //    same, because the rows are still inserted. The comment above it described
  //    a replay test that did not exist.
  //
  //    So: take a messageId the consumer has demonstrably already processed,
  //    and publish a DIFFERENT payload under it.
  // -------------------------------------------------------------------------
  const eventDb = new pg.Client({ connectionString: RELAY_URL });
  await eventDb.connect();
  const { rows: lastPublished } = await eventDb.query(
    `SELECT id, payload FROM "OutboxMessage"
      WHERE "aggregateId" = $1 AND "publishedAt" IS NOT NULL
      ORDER BY seq DESC LIMIT 1`,
    [eventId],
  );
  await eventDb.end();

  if (lastPublished.length === 0) fail('no published outbox row to replay — nothing to test dedup with');

  // The relay sets messageId = the outbox row id, which is precisely why that id
  // is stable across relay retries and usable as a dedup key.
  const replayId = lastPublished[0].id;
  const original = lastPublished[0].payload;

  const before = await waitForProjection(db, eventId, () => true, 'the projection before replay');

  /**
   * The replay carries a HIGHER version and a different title, deliberately.
   *
   * If it were an identical copy, the projection would be unchanged whether
   * dedup worked or not, and the test would prove nothing. If it carried a lower
   * or equal version, the MONOTONIC GUARD would reject it and the test would be
   * measuring that instead.
   *
   * A higher version means the version guard would happily let this through. The
   * only thing that can stop it is dedup, so an unchanged projection isolates
   * exactly the mechanism under test.
   */
  const registry = new SchemaRegistry({ host: REGISTRY });
  // The same resolution the relay uses, so the replay is encoded byte-identically
  // to a real message rather than against some other schema version.
  const schemaId = await registry.getLatestSchemaId('events.activity-value');

  const value = await registry.encode(schemaId, {
    ...original,
    messageId: replayId,
    occurredAt: new Date().toISOString(),
    version: Number(original.version ?? 0) + 100,
    title: 'REPLAYED — this must never reach the projection',
  });

  const kafka = new Kafka({ clientId: 'verify-dedup', brokers: BROKERS, logLevel: logLevel.ERROR });
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({ topic: 'events.activity', messages: [{ key: eventId, value }] });
  await producer.disconnect();

  // Give the consumer more than enough time to have applied it if it were going
  // to. A pass here is "nothing happened", so the wait has to be long enough
  // that nothing-happened is a conclusion rather than a race.
  await sleep(8000);

  const after = await waitForProjection(db, eventId, () => true, 'the projection after replay');

  if (after.sourceVersion !== before.sourceVersion) {
    fail(
      `the replay was applied: sourceVersion moved ${before.sourceVersion} -> ${after.sourceVersion}. ` +
        'A message the consumer had already processed changed the projection.',
    );
  }
  if (after.status !== before.status || after.capacity !== before.capacity) {
    fail(`the replay changed the projection: ${JSON.stringify(before)} -> ${JSON.stringify(after)}`);
  }

  // And the title specifically, since that is what the replay tried to overwrite.
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  const { rows: titleRows } = await db.query(
    `SELECT title FROM "EventProjection" WHERE id = $1`,
    [eventId],
  );
  const { rows: dupRows } = await db.query(
    `SELECT count(*)::int AS n FROM "ProcessedMessage" WHERE "tenantId" = $1`,
    [TENANT],
  );
  await db.query('COMMIT');

  if (String(titleRows[0]?.title ?? '').startsWith('REPLAYED')) {
    fail('the replayed title reached the projection — dedup did not stop it');
  }
  if (dupRows[0].n === 0) fail('no ProcessedMessage rows — deduplication is not recording anything');

  console.log(
    `  replayed messageId ${replayId} with version +100 and a new title; projection unchanged ` +
      `(v${after.sourceVersion}, ${dupRows[0].n} messages recorded)`,
  );

  await db.end();
  console.log('');
  console.log('Event -> outbox -> relay -> Kafka -> projection -> sales rule verified');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
