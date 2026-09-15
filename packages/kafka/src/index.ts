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
 * How far a consumer group is behind, in MESSAGES: each partition's high-water
 * mark minus the group's committed offset, across every topic the group reads.
 *
 * OFFSETS, NOT WALL-CLOCK TIME. Ticketing once reported "lag" as now minus the last
 * message's timestamp, and read 12329 seconds on a healthy, caught-up, idle system.
 * By that measure an idle topic and a dead consumer look the same. Offsets tell
 * them apart: an idle topic has nothing past the committed offset, while a dead
 * consumer's high-water mark keeps moving away from it.
 *
 * A partition the group has never committed on has no position, and it is reported
 * as null rather than guessed. Under 'earliest' everything retained there is
 * unread; under 'latest' none of it ever will be. Which is true is not something
 * this function can know.
 *
 * SHARED, because three consumers need it: Search's indexer, Analytics' ingest and
 * Notification's dispatch. It was copied once, from Search into Analytics, and the
 * copy's own comment said a third consumer would be the moment to share it.
 */
export async function consumerLag(admin: Admin, groupId: string, topics: readonly string[]): Promise<ConsumerLag> {
  const [topicOffsets, groupOffsets] = await Promise.all([
    Promise.all(topics.map(async (topic) => ({ topic, partitions: await admin.fetchTopicOffsets(topic) }))),
    admin.fetchOffsets({ groupId, topics: [...topics] }),
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
