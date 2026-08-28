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
const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';

const fail = (m) => { console.error(`FAIL: ${m}`); process.exit(1); };

async function main() {
  const db = new pg.Client({ connectionString: RELAY_URL });
  await db.connect();

  const { rows: before } = await db.query(
    `SELECT count(*)::int AS pending FROM "OutboxMessage"
      WHERE topic = 'tickets.reserved' AND "publishedAt" IS NULL AND "deadLetteredAt" IS NULL`,
  );
  const pending = before[0].pending;

  if (pending === 0) {
    console.log('  no undelivered reservation messages — nothing to drain');
    await db.end();
    return;
  }
  console.log(`  ${pending} undelivered message(s) on tickets.reserved`);

  // The same binary. Only the connection string and the forbidden-table name
  // differ, because the relay is routing-free by design.
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

  const { rows: after } = await db.query(
    `SELECT
       count(*) FILTER (WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL)::int AS still_pending,
       count(*) FILTER (WHERE "deadLetteredAt" IS NOT NULL)::int AS dead
     FROM "OutboxMessage" WHERE topic = 'tickets.reserved'`,
  );
  await db.end();

  if (after[0].dead > 0) {
    fail(`${after[0].dead} reservation message(s) dead-lettered — the Avro schema does not match the payload`);
  }
  if (after[0].still_pending > 0) {
    fail(`${after[0].still_pending} message(s) still undelivered after a drain`);
  }
  console.log('  all published, none dead-lettered');

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

  if (seen.length < pending) {
    fail(`decoded only ${seen.length} message(s) from tickets.reserved, expected at least ${pending}`);
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
