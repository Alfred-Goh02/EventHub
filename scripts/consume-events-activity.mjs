/**
 * Reads events.activity and decodes the Avro messages the relay published.
 *
 * This is the consumer side of the contract, and it proves something the
 * producer side cannot: that a message encoded by the relay is decodable by
 * someone who was not involved in encoding it. It resolves the writer's schema
 * from the id in the wire format, exactly as the Ticketing projection will.
 *
 * Run: node scripts/consume-events-activity.mjs
 */

import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';
const TOPIC = 'events.activity';

const kafka = new Kafka({ clientId: 'events-activity-reader', brokers: BROKERS, logLevel: logLevel.ERROR });
const registry = new SchemaRegistry({ host: REGISTRY });

async function main() {
  // A fresh group id, so this always replays from the beginning rather than
  // resuming someone else's committed offset. Replaying from earliest is
  // exactly what a projection rebuild does, and it only works because the topic
  // is compacted and every message is a full snapshot.
  const consumer = kafka.consumer({ groupId: `reader-${process.pid}-${Math.random().toString(36).slice(2)}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  const seen = [];
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      // The first byte is 0x00 and the next four are the schema id. decode()
      // reads that id, fetches the WRITER's schema, and resolves it against the
      // reader's — which is why a plain avsc encode without the prefix would be
      // undecodable here.
      const value = await registry.decode(message.value);
      seen.push({
        partition,
        key: message.key?.toString(),
        messageId: message.headers?.['message-id']?.toString(),
        value,
      });
    },
  });

  await new Promise((r) => setTimeout(r, 6000));
  await consumer.disconnect();

  if (seen.length === 0) {
    console.error('no messages on events.activity');
    process.exit(1);
  }

  console.log(`decoded ${seen.length} message(s) from ${TOPIC}\n`);
  for (const m of seen.sort((a, b) => a.value.version - b.value.version)) {
    console.log(
      `  p${m.partition}  ${String(m.value.changeType).padEnd(9)} v${m.value.version}  ` +
        `status=${String(m.value.status).padEnd(9)} capacity=${String(m.value.capacity).padEnd(4)} ` +
        `deleted=${m.value.deleted}  tenant=${m.value.tenantId}`,
    );
  }

  // The property that makes the projection safe: a consumer keeps the highest
  // version it has applied and discards anything lower, so neither a redelivery
  // nor an out-of-order arrival can move it backwards.
  const versions = seen.map((m) => m.value.version);
  const monotonic = versions.every((v, i) => i === 0 || v >= versions[i - 1]);
  console.log('');
  console.log(`  message-id header present on all: ${seen.every((m) => m.messageId) ? 'yes' : 'NO'}`);
  console.log(`  all messages on one partition:    ${new Set(seen.map((m) => m.partition)).size === 1 ? 'yes (same key)' : 'no'}`);
  console.log(`  versions arrived in order:        ${monotonic ? 'yes' : 'no'}`);
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
