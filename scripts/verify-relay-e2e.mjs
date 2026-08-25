/**
 * End-to-end proof of the transactional outbox, for CI.
 *
 *   insert an outbox row  ->  run the relay once  ->  assert it published
 *                         ->  assert the message is decodable from Kafka
 *
 * This is the assertion that ties the chapter together. Every piece is checked
 * separately elsewhere — the schema contract, the broker round-trip, the RLS
 * policies — but "does a row written by a service actually become a message a
 * consumer can read?" is only true if all of them hold at once.
 *
 * Run: node scripts/verify-relay-e2e.mjs
 */

import { execFileSync } from 'node:child_process';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const APP_URL =
  process.env['EVENT_APP_DATABASE_URL'] ??
  'postgresql://eventhub_event_app:eventhub_event_app@localhost:5432/eventhub_event';
const RELAY_URL =
  process.env['RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_event_relay:eventhub_event_relay@localhost:5432/eventhub_event';
const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';

const marker = `e2e_${Date.now()}`;
const tenantId = 'tenant_acme';

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

async function main() {
  // ---------------------------------------------------------------------
  // 1. Write an outbox row as the SERVICE role, with tenant context set.
  //    Not as the owner: OutboxMessage's policies are role-targeted, so the
  //    owner matches none of them and would silently insert nothing visible.
  // ---------------------------------------------------------------------
  const app = new pg.Client({ connectionString: APP_URL });
  await app.connect();
  await app.query('BEGIN');
  await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
  await app.query(
    `INSERT INTO "OutboxMessage"
       (id, "tenantId", topic, "eventType", "schemaVersion", "aggregateId", payload)
     VALUES ($1, $2, 'events.activity', 'event.snapshot', 1, $3, $4::jsonb)`,
    [
      marker,
      tenantId,
      marker,
      JSON.stringify({
        eventId: marker,
        tenantId,
        title: 'E2E Relay Check',
        slug: 'e2e-relay-check',
        description: null,
        status: 'PUBLISHED',
        capacity: 42,
        startsAt: '2027-06-01T10:00:00.000Z',
        endsAt: '2027-06-01T18:00:00.000Z',
        organiserId: 'u_e2e',
        version: 1,
        changeType: 'PUBLISHED',
        deleted: false,
      }),
    ],
  );
  await app.query('COMMIT');
  await app.end();
  console.log(`  inserted outbox row ${marker}`);

  // ---------------------------------------------------------------------
  // 2. Drain once.
  // ---------------------------------------------------------------------
  execFileSync('node', ['dist/index.js', '--once'], {
    cwd: 'services/relay',
    stdio: 'inherit',
    env: { ...process.env, RELAY_DATABASE_URL: RELAY_URL },
  });

  // ---------------------------------------------------------------------
  // 3. The row must now be marked published.
  // ---------------------------------------------------------------------
  const relay = new pg.Client({ connectionString: RELAY_URL });
  await relay.connect();
  const { rows } = await relay.query(
    `SELECT "publishedAt" IS NOT NULL AS published, "deadLetteredAt" IS NOT NULL AS dead, attempts, "lastError"
       FROM "OutboxMessage" WHERE id = $1`,
    [marker],
  );
  await relay.end();

  const row = rows[0];
  if (!row) fail('the outbox row vanished');
  if (row.dead) fail(`row was dead-lettered: ${row.lastError}`);
  if (!row.published) fail(`row was not published (attempts=${row.attempts}, lastError=${row.lastError})`);
  console.log('  row marked published');

  // ---------------------------------------------------------------------
  // 4. The message must be on Kafka AND decodable by someone who did not
  //    encode it — resolving the writer's schema from the wire format.
  // ---------------------------------------------------------------------
  const kafka = new Kafka({ clientId: 'e2e', brokers: BROKERS, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: REGISTRY });
  const consumer = kafka.consumer({ groupId: `e2e-${marker}` });
  await consumer.connect();
  await consumer.subscribe({ topic: 'events.activity', fromBeginning: true });

  let found = null;
  await consumer.run({
    eachMessage: async ({ message }) => {
      if (message.key?.toString() !== marker) return;
      found = {
        value: await registry.decode(message.value),
        messageId: message.headers?.['message-id']?.toString(),
        tenantHeader: message.headers?.['tenant-id']?.toString(),
      };
    },
  });

  await new Promise((r) => setTimeout(r, 8000));
  await consumer.disconnect();

  if (!found) fail('no message with the expected key arrived on events.activity');

  if (found.value.eventId !== marker) fail(`decoded eventId mismatch: ${found.value.eventId}`);
  if (found.value.capacity !== 42) fail(`decoded capacity mismatch: ${found.value.capacity}`);
  if (found.value.tenantId !== tenantId) fail(`decoded tenantId mismatch: ${found.value.tenantId}`);
  // The envelope the RELAY adds from columns, not from the stored payload.
  if (found.messageId !== marker) fail(`message-id header missing or wrong: ${found.messageId}`);
  if (!found.value.messageId) fail('messageId missing from the decoded payload');
  if (!found.value.occurredAt) fail('occurredAt missing from the decoded payload');

  console.log('  message decoded from Kafka with the expected values');
  console.log('  envelope present: messageId + occurredAt + headers');
  console.log('');
  console.log('outbox -> relay -> Avro -> Kafka -> consumer verified');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
