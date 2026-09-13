import type { Admin } from 'kafkajs';

export interface PartitionLag {
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
 * How far the indexer is behind, in MESSAGES: each partition's high-water mark
 * minus the group's committed offset. This is the staleness of the read model.
 *
 * Offsets, not wall-clock time. Ticketing once reported "lag" as now minus the
 * last message's timestamp and read 12329 seconds on a healthy, caught-up, idle
 * system — an idle topic and a dead consumer look identical by that measure.
 * Offsets tell them apart: an idle topic has nothing past the committed offset,
 * a dead consumer has a high-water mark that keeps moving away from it.
 *
 * A partition the group has never committed on has no position, and that is
 * reported as null rather than guessed. Under 'earliest' everything retained
 * there is unread; under 'latest' none of it ever will be. Which one is true is
 * not something this function can know.
 */
export async function consumerLag(admin: Admin, groupId: string, topic: string): Promise<ConsumerLag> {
  const [topicOffsets, groupOffsets] = await Promise.all([
    admin.fetchTopicOffsets(topic),
    admin.fetchOffsets({ groupId, topics: [topic] }),
  ]);

  const committedBy = new Map<number, number>();
  for (const t of groupOffsets) {
    for (const p of t.partitions) committedBy.set(p.partition, Number(p.offset));
  }

  const partitions = topicOffsets.map((p): PartitionLag => {
    const highWatermark = Number(p.high);
    const committed = committedBy.get(p.partition);
    const known = committed !== undefined && committed >= 0;
    return {
      partition: p.partition,
      committed: known ? committed : null,
      highWatermark,
      lag: known ? Math.max(0, highWatermark - committed) : null,
    };
  });

  const known = partitions.filter((p) => p.lag !== null);
  return {
    total: known.length === 0 ? null : known.reduce((n, p) => n + (p.lag ?? 0), 0),
    partitions,
  };
}
