/**
 * Produces and consumes a few messages to prove the broker actually works, and
 * to demonstrate the property the whole partition-key design rests on:
 *
 *   messages sharing a key always land on the same partition, and ordering is
 *   guaranteed only WITHIN a partition.
 *
 * That is why the outbox stores an aggregateId and uses it as the key — all
 * messages about one event stay ordered relative to each other, while different
 * events spread across partitions and can be consumed in parallel.
 *
 * Run: node scripts/kafka-smoke.mjs
 */

import { Kafka, logLevel } from 'kafkajs';

const BROKER = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';
const TOPIC = 'events.activity';

const kafka = new Kafka({
  clientId: 'eventhub-smoke',
  brokers: [BROKER],
  logLevel: logLevel.ERROR,
});

async function main() {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: TOPIC,
    messages: [
      { key: 'evt_alpha', value: JSON.stringify({ n: 1 }) },
      { key: 'evt_alpha', value: JSON.stringify({ n: 2 }) },
      { key: 'evt_alpha', value: JSON.stringify({ n: 3 }) },
      { key: 'evt_beta', value: JSON.stringify({ n: 4 }) },
      { key: 'evt_gamma', value: JSON.stringify({ n: 5 }) },
    ],
  });
  await producer.disconnect();
  console.log(`produced 5 messages to ${TOPIC} via ${BROKER}`);

  // A unique group id each run, so this always reads from the beginning rather
  // than resuming a committed offset from a previous run.
  const consumer = kafka.consumer({ groupId: `smoke-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  const seen = [];
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      seen.push({
        partition,
        key: message.key?.toString() ?? '(none)',
        n: JSON.parse(message.value.toString()).n,
      });
    },
  });

  await new Promise((r) => setTimeout(r, 5000));
  await consumer.disconnect();

  const mine = seen.filter((m) => m.key.startsWith('evt_'));
  console.log('');
  for (const m of mine.sort((a, b) => a.n - b.n)) {
    console.log(`  partition=${m.partition}  key=${m.key.padEnd(10)} n=${m.n}`);
  }

  const alphaPartitions = new Set(mine.filter((m) => m.key === 'evt_alpha').map((m) => m.partition));
  const allPartitions = new Set(mine.map((m) => m.partition));

  console.log('');
  console.log(`same key on one partition: ${alphaPartitions.size === 1 ? 'YES' : 'NO'}`);
  console.log(`different keys spread over: ${allPartitions.size} partition(s)`);

  if (alphaPartitions.size !== 1) {
    console.error('FAILED: messages with the same key were split across partitions');
    process.exit(1);
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
