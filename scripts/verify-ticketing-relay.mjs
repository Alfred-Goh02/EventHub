/**
 * Drains Ticketing's outbox with the SAME relay binary as the Event service's,
 * and asserts the messages decode.
 *
 * This exists because of a defect the payment-path review found: the reservation
 * handler had been writing rows to tickets.reserved since it was built, while
 * no Avro schema was registered for that topic and nothing was draining that
 * table. Every one of those rows would have dead-lettered the moment a relay was
 * pointed at it — eight had already accumulated.
 *
 * It also demonstrates the point of storing `topic` as a column on the outbox
 * row: the relay reads the destination off each row and produces to it keyed by
 * aggregateId, so draining a second service's outbox needs a different
 * connection string and nothing else. No routing table, no per-service relay.
 *
 * Run: node scripts/verify-ticketing-relay.mjs
 */

import { execFileSync } from 'node:child_process';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const RELAY_URL =
  process.env['TICKETING_RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_relay:eventhub_ticketing_relay@localhost:5432/eventhub_ticketing';
/**
 * A SECOND connection, as the app role, purely to seed.
 *
 * The relay credential deliberately cannot INSERT into OutboxMessage -- it
 * drains, it does not produce -- so seeding through it fails with
 * aclcheck_error, which is the grant working exactly as intended. Messages are
 * therefore written the way the service writes them: as the app role, inside a
 * transaction with app.tenant_id set, because OutboxMessage is under FORCE row
 * security and its tenant policy is what the INSERT must satisfy.
 */
const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';

const TENANT = 'tenant_acme';

const fail = (m) => { console.error(`FAIL: ${m}`); process.exit(1); };

async function main() {
  const db = new pg.Client({ connectionString: RELAY_URL });
  await db.connect();

  /**
   * Seed one message per topic rather than depending on leftovers.
   *
   * This used to return early with "nothing to drain" whenever the outbox
   * happened to be empty — and whether it is empty depends entirely on which CI
   * steps ran first. So on a good day this asserted the relay works, and on a
   * different ordering it asserted nothing at all and said so in a line that
   * reads like success.
   *
   * It also never touched tickets.purchased, which now carries money through the
   * same path and had no coverage here whatsoever.
   *
   * Seeding makes the run self-sufficient: it always drains something, and it
   * always drains BOTH topics. The rows are written straight to the outbox, which
   * is exactly what the reservation and settlement paths do — the relay cannot
   * tell the difference, because reading `topic` off the row is its whole routing
   * story.
   */
  const marker = `relaycheck_${Date.now()}`;
  const seeder = new pg.Client({ connectionString: APP_URL });
  await seeder.connect();
  await seeder.query('BEGIN');
  await seeder.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  await seeder.query(
    `INSERT INTO "OutboxMessage"
       ("id","tenantId",topic,"eventType","schemaVersion","aggregateId",payload,"createdAt")
     VALUES
       (gen_random_uuid()::text, $1, 'tickets.reserved', 'reservation.expired', 1, $2, $3::jsonb, now()),
       (gen_random_uuid()::text, $1, 'tickets.purchased', 'tickets.purchased', 1, $2, $4::jsonb, now())`,
    [
      TENANT,
      marker,
      JSON.stringify({
        reservationId: `${marker}_res`,
        tenantId: TENANT,
        eventId: marker,
        userId: 'u_relaycheck',
        state: 'EXPIRED',
        items: [{ ticketTypeId: `${marker}_tt`, quantity: 2 }],
        expiresAt: null,
      }),
      JSON.stringify({
        paymentId: `${marker}_pay`,
        reservationId: `${marker}_res`,
        tenantId: TENANT,
        eventId: marker,
        userId: 'u_relaycheck',
        amountCents: 2000,
        currency: 'SGD',
        items: [{ ticketTypeId: `${marker}_tt`, quantity: 2, unitPriceCents: 1000 }],
        providerRef: null,
      }),
    ],
  );
  await seeder.query('COMMIT');
  await seeder.end();

  const { rows: before } = await db.query(
    `SELECT
       count(*) FILTER (WHERE topic = 'tickets.reserved')::int AS reserved,
       count(*)::int AS total
     FROM "OutboxMessage"
     WHERE topic IN ('tickets.reserved', 'tickets.purchased')
       AND "publishedAt" IS NULL AND "deadLetteredAt" IS NULL`,
  );
  const pending = before[0].total;
  // Counted separately because the decode step below subscribes to
  // tickets.reserved only -- comparing what it saw against a total that
  // includes purchases would fail for the wrong reason.
  const pendingReserved = before[0].reserved;

  // The seed guarantees at least two. Zero here means the INSERT did not land,
  // which would make everything below vacuous.
  if (pending < 2) {
    fail(`expected at least the 2 seeded messages, found ${pending} — the seed did not land`);
  }
  console.log(`  ${pending} undelivered message(s) across tickets.reserved and tickets.purchased`);

  // The same binary. Only the connection string and the forbidden-table name
  // differ, because the relay is routing-free by design.
  /**
   * Drain until OUR messages are through, not until the table is empty.
   *
   * The relay claims RELAY_BATCH_SIZE rows per run, ordered by seq, so a single
   * --once leaves anything beyond the first batch behind. Asserting "nothing is
   * pending" therefore only worked while the outbox happened to be smaller than
   * one batch -- true in CI, false on any machine that has been used, where a
   * few hundred rows accumulate from earlier runs and the seeded messages are
   * the NEWEST and so the last to be claimed.
   *
   * Bounded, so a genuinely stuck relay fails rather than looping forever.
   */
  const MAX_DRAINS = 12;
  let drains = 0;
  for (; drains < MAX_DRAINS; drains += 1) {
      execFileSync('node', ['dist/index.js', '--once'], {
        cwd: 'services/relay',
        stdio: 'inherit',
        env: {
          ...process.env,
          RELAY_DATABASE_URL: RELAY_URL,
          // Must be a table that EXISTS here, or the privilege assertion would pass
          // on "relation does not exist" without testing anything.
          RELAY_FORBIDDEN_TABLE: 'Reservation',
          RELAY_METRICS_PORT: '3011',
        },
      });

    const { rows: mine } = await db.query(
      `SELECT count(*)::int AS n FROM "OutboxMessage"
        WHERE "aggregateId" = $1 AND "publishedAt" IS NULL AND "deadLetteredAt" IS NULL`,
      [marker],
    );
    if (mine[0].n === 0) break;
  }
  console.log(`  drained ${drains + 1} batch(es) to reach the seeded messages`);

  const { rows: after } = await db.query(
    `SELECT
       count(*) FILTER (WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL)::int AS still_pending,
       count(*) FILTER (WHERE "deadLetteredAt" IS NOT NULL)::int AS dead,
       count(*)::int AS seeded
     FROM "OutboxMessage" WHERE "aggregateId" = $1`,
    [marker],
  );

  // The control: if the seed did not land, every count below is 0 and the
  // assertions pass while testing nothing.
  if (after[0].seeded !== 2) {
    fail(`expected 2 seeded messages, found ${after[0].seeded} — the assertions below would be vacuous`);
  }
  await db.end();

  if (after[0].dead > 0) {
    fail(`${after[0].dead} reservation message(s) dead-lettered — the Avro schema does not match the payload`);
  }
  if (after[0].still_pending > 0) {
    fail(`${after[0].still_pending} seeded message(s) still undelivered after ${MAX_DRAINS} drains`);
  }
  console.log('  both seeded messages published, none dead-lettered');

  // Decode from the wire, resolving the writer's schema from the message itself.
  const kafka = new Kafka({ clientId: 'verify-reserved', brokers: BROKERS, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: REGISTRY });
  const consumer = kafka.consumer({ groupId: `verify-reserved-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: 'tickets.reserved', fromBeginning: true });

  const seen = [];
  await consumer.run({
    eachMessage: async ({ message }) => {
      seen.push(await registry.decode(message.value));
    },
  });
  await new Promise((r) => setTimeout(r, 8000));
  await consumer.disconnect();

  if (seen.length < pendingReserved) {
    fail(`decoded only ${seen.length} message(s) from tickets.reserved, expected at least ${pendingReserved}`);
  }

  for (const m of seen) {
    if (!m.messageId) fail('a message is missing messageId — consumers could not dedupe it');
    if (!m.tenantId) fail('a message is missing tenantId — a consumer could not re-establish tenant scope');
    if (!m.state) fail('a message is missing state — a consumer could not tell a hold from a release');
    if (!Array.isArray(m.items) || m.items.length === 0) fail('a message has no items');
  }

  console.log(`  decoded ${seen.length} message(s), all carrying messageId, tenantId, state and items`);
  console.log('');
  console.log("Ticketing's outbox drains with the same relay binary and the messages decode");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
