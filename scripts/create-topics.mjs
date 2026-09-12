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
import kafkajs from 'kafkajs';

/**
 * The enum for a topic config resource.
 *
 * Reached through the DEFAULT import rather than a named one: kafkajs is
 * CommonJS, and under ESM only some of its exports survive as named bindings.
 * `Kafka` does; `ConfigResourceTypes` does not, and importing it that way fails
 * at load with "Named export not found" rather than at the point of use.
 */
const TOPIC_RESOURCE = kafkajs.ConfigResourceTypes.TOPIC;

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
    //
    // And NOT deleted either, which it was. With no retention.ms of its own the
    // topic inherited the broker default -- log.retention.hours=168 -- so the
    // only surviving record of a message that could never be processed was
    // thrown away after seven days. A dead letter nobody looked at inside a week
    // was simply gone, and the topic whose stated purpose is "keep every failure"
    // kept none of them.
    //
    // -1 is infinite. That is safe here in a way it would not be on a traffic
    // topic: this one only receives messages that CANNOT be processed, so its
    // size is bounded by how many genuinely broken payloads exist rather than by
    // throughput. If it ever grows, that is the alarm, not the problem.
    configEntries: [{ name: 'retention.ms', value: '-1' }],
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

/**
 * THE NEGATIVE-TIMEOUT WARNING IS KAFKAJS'S, NOT OURS.
 *
 * Every script here that talks to the broker prints this at least once, and so
 * do the relay and Ticketing in their container logs:
 *
 *   TimeoutNegativeWarning: -1789219377436 is a negative number.
 *   Timeout duration was set to 1.
 *
 * The magnitude is the clue -- that number is roughly Date.now(), so something
 * computed `something - now` where `something` was near zero. It is kafkajs
 * 2.2.4, src/network/requestQueue/index.js:
 *
 *   let scheduleAt = this.throttledUntil - Date.now()      // throttledUntil = -1
 *   if (!this.throttleCheckTimeoutId) {
 *     if (this.pending.length > 0) {
 *       scheduleAt = scheduleAt > 0 ? scheduleAt : CHECK_PENDING_REQUESTS_INTERVAL
 *     }
 *     this.throttleCheckTimeoutId = setTimeout(..., scheduleAt)
 *   }
 *
 * `throttledUntil` is initialised to -1 and only moves if the broker actually
 * throttles us, which it never does here. So scheduleAt is `-1 - Date.now()`.
 * The clamp that would fix it sits inside `if (this.pending.length > 0)`, and
 * the queue is empty in exactly the ordinary case: a request fulfilled with
 * nothing else waiting behind it. Node clamps the delay to 1ms and warns.
 *
 * So the cost is one needless 1ms timer and a line of noise. It is not a lost
 * message, a stalled queue, or a clock problem.
 *
 * LEFT ALONE DELIBERATELY. 2.2.4 is the newest stable kafkajs -- everything
 * published after it is a 2.3.0 beta -- and taking a beta broker client to
 * silence a cosmetic warning is a bad trade. Suppressing TimeoutNegativeWarning
 * process-wide would work and would also hide a genuine negative timeout in our
 * own code, which is a warning worth keeping.
 */
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

  /**
   * Reconcile the CONFIG of topics that already exist.
   *
   * Without this the script only ever applied configEntries at creation, and
   * then printed "all topics already exist" and exited 0. So every config in
   * this file was a description of what the topics were made with once, not of
   * what they are -- change a value here and nothing happens, successfully.
   *
   * That is how events.activity.dlq ended up on the broker default retention:
   * the entry had no retention.ms, and adding one later would have changed
   * nothing on any cluster where the topic was already there.
   *
   * CONFIG ONLY, never partitions. Raising the partition count of a COMPACTED,
   * keyed topic silently rehashes which partition a key lands on, so the
   * ordering guarantee that events.activity rests on -- one aggregate, one
   * partition -- would break for every event that already exists. That is a
   * migration somebody has to plan, not something a setup script should do
   * because a number in a file changed.
   */
  const configured = TOPICS.filter((t) => t.configEntries?.length);

  if (configured.length > 0) {
    const current = await admin.describeConfigs({
      includeSynonyms: false,
      resources: configured.map((t) => ({ type: TOPIC_RESOURCE, name: t.topic })),
    });

    const drifted = [];

    for (const t of configured) {
      const live = current.resources.find((r) => r.resourceName === t.topic);
      for (const want of t.configEntries) {
        const have = live?.configEntries.find((c) => c.configName === want.name);
        if (have?.configValue !== String(want.value)) {
          drifted.push({
            topic: t.topic,
            name: want.name,
            from: have?.configValue ?? '(unset)',
            to: String(want.value),
          });
        }
      }
    }

    if (drifted.length === 0) {
      console.log('topic configs already match this file');
    } else {
      await admin.alterConfigs({
        validateOnly: false,
        resources: configured.map((t) => ({
          type: TOPIC_RESOURCE,
          name: t.topic,
          configEntries: t.configEntries.map((c) => ({
            name: c.name,
            value: String(c.value),
          })),
        })),
      });
      for (const d of drifted) {
        console.log(`reconciled ${d.topic}: ${d.name} ${d.from} -> ${d.to}`);
      }
    }
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
