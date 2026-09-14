import type { Admin } from 'kafkajs';

export interface PartitionLag {
  topic: string;
  partition: number;
  /** The group's committed offset, or null if it has never committed here. */
  committed: number | null;
  highWatermark: number;
  lag: number | null;
}

export interface ConsumerLag {
  /** Messages behind across the partitions whose position is known. */
  total: number | null;
  partitions: PartitionLag[];
}

/**
 * How far ingest is behind, in MESSAGES: each partition's high-water mark minus
 * the group's committed offset, across every topic ingest reads. It is the one
 * number that says the analytics pipeline is broken — dashboards go stale exactly
 * as fast as it grows.
 *
 * Offsets, not wall-clock time, for the reason Search's indexer measures them
 * (services/search/src/indexer/lag.ts): an idle topic and a dead consumer look
 * identical by "time since the last message", and Ticketing once reported 12329
 * seconds of lag on a healthy idle system that way. Adapted here rather than
 * shared, because that one reads a single topic and no shared Kafka package exists
 * yet; a third consumer needing it is the moment to move it into packages/.
 *
 * A partition the group has never committed on has no position, reported as null
 * rather than guessed.
 */
export async function consumerLag(admin: Admin, groupId: string, topics: string[]): Promise<ConsumerLag> {
  const [topicOffsets, groupOffsets] = await Promise.all([
    Promise.all(topics.map(async (topic) => ({ topic, partitions: await admin.fetchTopicOffsets(topic) }))),
    admin.fetchOffsets({ groupId, topics }),
  ]);

  const committedBy = new Map<string, number>();
  for (const t of groupOffsets) {
    for (const p of t.partitions) committedBy.set(`${t.topic}:${p.partition}`, Number(p.offset));
  }

  const partitions = topicOffsets.flatMap(({ topic, partitions: offsets }) =>
    offsets.map((p): PartitionLag => {
      const highWatermark = Number(p.high);
      const committed = committedBy.get(`${topic}:${p.partition}`);
      const known = committed !== undefined && committed >= 0;
      return {
        topic,
        partition: p.partition,
        committed: known ? committed : null,
        highWatermark,
        lag: known ? Math.max(0, highWatermark - committed) : null,
      };
    }),
  );

  const known = partitions.filter((p) => p.lag !== null);
  return {
    total: known.length === 0 ? null : known.reduce((n, p) => n + (p.lag ?? 0), 0),
    partitions,
  };
}
