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
    // COMPACTED, not deleted.
    //
    // Kafka retains the latest message per KEY forever, so a consumer can
    // rebuild its entire projection by replaying from the beginning — no HTTP
    // bootstrap path and no cold-start hole. Under the default cleanup.policy
    // of `delete`, a service deployed a week later would simply never learn
    // about events published before its retention window, with no error
    // anywhere to say so.
    //
    // This is exactly why the payload is a full SNAPSHOT rather than a delta:
    // replaying deltas requires every delta, and compaction keeps only the last
    // message per key.
    //
    // The cost is losing change history on this topic — which is why the ticket
    // topics below stay on `delete`. For those, the SEQUENCE is the information.
    configEntries: [
      { name: 'cleanup.policy', value: 'compact' },
      // Compact aggressively in dev so the behaviour is observable rather than
      // theoretical. Production would leave these at their defaults.
      { name: 'min.cleanable.dirty.ratio', value: '0.1' },
      { name: 'segment.ms', value: '60000' },
    ],
    note: 'Event snapshots, COMPACTED. Keyed by event id.',
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
  {
    topic: 'events.activity.dlq',
    numPartitions: 3,
    replicationFactor: 1,
    // Where an undecodable message goes so it stops blocking its partition.
    //
    // Deliberately NOT compacted: the whole point is to keep every failure for
    // a human to look at, and compaction would discard all but the last per key.
    note: 'Dead letters from the events.activity consumer. Inspect, never auto-retry.',
  },
  {
    topic: 'smoke.roundtrip',
    numPartitions: 3,
    replicationFactor: 1,
    // The broker smoke test produces plain JSON. It must not touch a real topic:
    // events.activity carries Avro in the Confluent wire format (a magic byte
    // plus a 4-byte schema id), so a JSON message on it is undecodable — and an
    // undecodable message blocks its partition until someone intervenes.
    note: 'Throwaway target for scripts/kafka-smoke.mjs. Plain JSON, never Avro.',
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
      topics: missing.map(({ topic, numPartitions, replicationFactor, configEntries }) => ({
        topic,
        numPartitions,
        replicationFactor,
        ...(configEntries ? { configEntries } : {}),
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
