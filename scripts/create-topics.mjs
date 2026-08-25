/**
 * Creates the project's Kafka topics.
 *
 * Checked in for the same reason db/init/01-bootstrap.sql is: infrastructure
 * created by hand exists on one machine and nowhere else. The broker runs with
 * KAFKA_AUTO_CREATE_TOPICS_ENABLE=false deliberately — auto-creation hides typos
 * by silently making a topic named after your mistake, with default settings
 * nobody chose.
 *
 * Run: node scripts/create-topics.mjs
 */

import { Kafka } from 'kafkajs';

const BROKER = process.env['KAFKA_BROKERS'] ?? 'localhost:9092';

/**
 * Three partitions, not one.
 *
 * Partitions are the unit of parallelism: one consumer in a group can own each
 * partition, so a single-partition topic caps you at one consumer no matter how
 * many you run. Ordering is guaranteed only WITHIN a partition, which is why
 * the partition key matters — messages sharing a key land together and stay
 * ordered relative to each other.
 *
 * Replication factor 1 because this is a single broker. Production is 3, with
 * min.insync.replicas=2 and acks=all for anything involving money.
 */
const TOPICS = [
  {
    topic: 'events.activity',
    numPartitions: 3,
    replicationFactor: 1,
    note: 'Event lifecycle. Keyed by event id, so all messages about one event stay ordered.',
  },
  {
    topic: 'tickets.reserved',
    numPartitions: 3,
    replicationFactor: 1,
    note: 'A seat hold was taken. Keyed by event id — reservations for one event must be ordered.',
  },
  {
    topic: 'tickets.purchased',
    numPartitions: 3,
    replicationFactor: 1,
    note: 'Money changed hands. Keyed by event id.',
  },
  {
    topic: 'live.interactions',
    numPartitions: 3,
    replicationFactor: 1,
    note: 'Live Q&A activity, for durable analytics. Real-time fan-out is Redis, not this.',
  },
  {
    topic: 'community.feed',
    numPartitions: 3,
    replicationFactor: 1,
    note: 'Community posts and reactions.',
  },
];

const kafka = new Kafka({ clientId: 'eventhub-admin', brokers: [BROKER] });
const admin = kafka.admin();

async function main() {
  await admin.connect();
  console.log(`connected to ${BROKER}`);

  const existing = await admin.listTopics();
  const missing = TOPICS.filter((t) => !existing.includes(t.topic));

  if (missing.length === 0) {
    console.log('all topics already exist');
  } else {
    await admin.createTopics({
      topics: missing.map(({ topic, numPartitions, replicationFactor }) => ({
        topic,
        numPartitions,
        replicationFactor,
      })),
      waitForLeaders: true,
    });
    for (const t of missing) console.log(`created ${t.topic}`);
  }

  const meta = await admin.fetchTopicMetadata({
    topics: TOPICS.map((t) => t.topic),
  });

  console.log('');
  for (const t of meta.topics) {
    const note = TOPICS.find((x) => x.topic === t.name)?.note ?? '';
    console.log(`${t.name.padEnd(20)} partitions=${t.partitions.length}  ${note}`);
  }

  await admin.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
