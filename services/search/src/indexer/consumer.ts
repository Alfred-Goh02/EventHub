import { setTimeout as sleep } from 'node:timers/promises';
import { Kafka, logLevel, type EachBatchPayload, type KafkaMessage } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { Logger } from '@eventhub/logger';
import { eventSnapshot } from './contract.js';
import { toIndexAction, type IndexAction } from './to-document.js';
import { bulkIndex, type ItemOutcome } from './bulk.js';
import { consumerLag, type ConsumerLag } from './lag.js';

const TOPIC = 'events.activity';
const DLQ = 'events.activity.dlq';

/** Documents per bulk request. Snapshots are small; this is well under 5 MB. */
const CHUNK = 500;
const RETRY_ATTEMPTS = 5;
const RETRY_BASE_MS = 200;

const counters = { indexed: 0, stale: 0, deadLettered: 0, retries: 0 };
let assignedPartitions = 0;
let lastIndexedOccurredAt: number | null = null;

/**
 * What /metrics reports.
 *
 * versionConflictsTotal is a SIGNAL, not an error count: a low steady rate is
 * ordinary redelivery, and a spike means a rebalance loop or a producer replay.
 *
 * There is deliberately no "lag seconds" computed as now minus the last message's
 * occurredAt. Ticketing shipped exactly that once and it read 12329 seconds on a
 * healthy, fully caught-up, idle system — an idle topic and a dead consumer look
 * identical by that measure. lastIndexedOccurredAt is reported as the raw fact
 * it is; real lag needs committed offsets against the high-water mark.
 */
export function indexerMetrics(): Record<string, unknown> {
  return {
    indexedTotal: counters.indexed,
    versionConflictsTotal: counters.stale,
    deadLetteredTotal: counters.deadLettered,
    bulkRetriesTotal: counters.retries,
    assignedPartitions,
    lastIndexedOccurredAt:
      lastIndexedOccurredAt === null ? null : new Date(lastIndexedOccurredAt).toISOString(),
  };
}

/**
 * Registry failures that are the NETWORK, not the message.
 *
 * Ticketing's consumer dead-letters every decode failure. Most are genuine
 * poison — no magic byte, an unknown schema id, a truncated buffer — but a
 * registry that is briefly unreachable fails the same call, and dead-lettering
 * on that loses good events to a restart. The client caches schemas by id, so
 * this only bites on the first message of a schema, which is exactly when a
 * deploy is most likely to be restarting things.
 */
function isTransient(err: unknown): boolean {
  const text = err instanceof Error ? `${err.name} ${err.message}` : String(err);
  const status = (err as { status?: number })?.status;
  return (
    (typeof status === 'number' && status >= 500) ||
    /ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|timed? ?out/i.test(text)
  );
}

type Decoded =
  | { ok: true; action: IndexAction; occurredAt: number }
  | { ok: false; reason: string };

/**
 * Consumes events.activity into the search index.
 *
 * THE COMMIT RULE: an offset is resolved only once every message up to it has a
 * FINAL outcome — indexed, stale, or dead-lettered. A document the cluster could
 * not take right now is none of those, so the batch throws, nothing past it is
 * committed, and the consumer redelivers. Lag grows while OpenSearch is down,
 * which is correct: when it returns, the consumer resumes from the last committed
 * offset, and external versioning makes the replay safe.
 *
 * THE DEAD-LETTER RULE: only what can never succeed. Undecodable, contract
 * violations, a document the mapping rejects. Never a cluster outage.
 */
export function createIndexer(opts: {
  brokers: string[];
  registryUrl: string;
  groupId: string;
  startFrom: 'earliest' | 'latest';
  logger: Logger;
}) {
  const kafka = new Kafka({ clientId: 'search-indexer', brokers: opts.brokers, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: opts.registryUrl });
  const consumer = kafka.consumer({ groupId: opts.groupId });
  const producer = kafka.producer();
  // Reads offsets for the lag measurement. Its own connection, so a slow
  // metadata request never waits behind a fetch.
  const admin = kafka.admin();

  /**
   * Parks a message where an operator can find it, with enough to replay it.
   *
   * events.activity.dlq is SHARED with Ticketing's projection consumer: a poison
   * message on the topic fails both, and both park it. dlq-consumer-group is what
   * tells an operator — and scripts/verify-dlq-malformed.mjs — whose failure a
   * given copy records.
   */
  async function deadLetter(partition: number, message: KafkaMessage, reason: string): Promise<void> {
    await producer.send({
      topic: DLQ,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'dlq-reason': reason,
            'dlq-consumer-group': opts.groupId,
            'dlq-source-topic': TOPIC,
            'dlq-source-partition': String(partition),
            'dlq-source-offset': message.offset,
          },
        },
      ],
    });
    counters.deadLettered += 1;
    opts.logger.error({ partition, offset: message.offset, reason }, 'message sent to the dead-letter topic');
  }

  async function decode(message: KafkaMessage): Promise<Decoded> {
    if (!message.value) {
      // A compaction tombstone. Events are soft-deleted, so nothing should
      // produce one — and this consumer could not apply it anyway: the key is
      // the eventId alone, and without the tenant there is no routing to delete
      // with. Parking it is how someone finds out a producer broke the contract.
      return { ok: false, reason: 'null value (a tombstone); events.activity events are soft-deleted' };
    }

    let raw: unknown;
    try {
      raw = await registry.decode(message.value);
    } catch (err) {
      if (isTransient(err)) throw err;
      return { ok: false, reason: `undecodable: ${err instanceof Error ? err.message : String(err)}` };
    }

    const parsed = eventSnapshot.safeParse(raw);
    if (!parsed.success) {
      const issues = parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ');
      return { ok: false, reason: `contract violation: ${issues}` };
    }

    return { ok: true, action: toIndexAction(parsed.data), occurredAt: Date.parse(parsed.data.occurredAt) };
  }

  async function processBatch(payload: EachBatchPayload): Promise<void> {
    const { batch, resolveOffset, heartbeat, isRunning, isStale, commitOffsetsIfNecessary } = payload;

    for (let start = 0; start < batch.messages.length; start += CHUNK) {
      if (!isRunning() || isStale()) return;
      const chunk = batch.messages.slice(start, start + CHUNK);

      // Sequential, not Promise.all: a transient registry failure should stop at
      // the message it hit, not race the rest.
      const decoded: Decoded[] = [];
      for (const message of chunk) decoded.push(await decode(message));

      let pending: Array<{ at: number; action: IndexAction; occurredAt: number }> = [];
      for (const [at, d] of decoded.entries()) {
        if (d.ok) pending.push({ at, action: d.action, occurredAt: d.occurredAt });
        else await deadLetter(batch.partition, chunk[at]!, d.reason);
      }

      for (let attempt = 1; pending.length > 0; attempt++) {
        let outcomes: ItemOutcome[] | null = null;
        try {
          outcomes = await bulkIndex(pending.map((p) => p.action));
        } catch (err) {
          if (attempt >= RETRY_ATTEMPTS) throw err;
          opts.logger.warn({ err, attempt }, 'bulk request failed; retrying');
        }

        const retry: typeof pending = [];
        if (outcomes === null) {
          retry.push(...pending);
        } else {
          for (const [j, outcome] of outcomes.entries()) {
            const p = pending[j]!;
            if (outcome.kind === 'indexed') {
              counters.indexed += 1;
              lastIndexedOccurredAt = Math.max(lastIndexedOccurredAt ?? 0, p.occurredAt);
            } else if (outcome.kind === 'stale') {
              counters.stale += 1;
            } else if (outcome.kind === 'rejected') {
              await deadLetter(batch.partition, chunk[p.at]!, `rejected by OpenSearch (${outcome.status}): ${outcome.reason}`);
            } else {
              retry.push(p);
            }
          }
        }

        if (retry.length === 0) break;
        if (attempt >= RETRY_ATTEMPTS) {
          throw new Error(
            `${retry.length} document(s) still refused by the cluster after ${attempt} attempts; ` +
              'not committing, so they are redelivered',
          );
        }
        counters.retries += 1;
        await sleep(RETRY_BASE_MS * 2 ** (attempt - 1));
        await heartbeat();
        pending = retry;
      }

      // Every message in this chunk now has a final outcome.
      resolveOffset(chunk[chunk.length - 1]!.offset);
      await heartbeat();
      await commitOffsetsIfNecessary();
    }
  }

  return {
    async start(): Promise<void> {
      await producer.connect();
      await admin.connect();
      await consumer.connect();

      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
        assignedPartitions = Object.values(payload.memberAssignment).reduce(
          (n, partitions) => n + partitions.length,
          0,
        );
      });

      /**
       * Where a group with NO committed offsets starts. 'earliest' is the normal
       * setting and the whole reason the topic is compacted: a new deployment, or
       * a wiped index, rebuilds by replaying from the beginning, with no backfill
       * endpoint. Once a group has committed, this is ignored.
       */
      await consumer.subscribe({ topic: TOPIC, fromBeginning: opts.startFrom === 'earliest' });

      await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: processBatch,
      });

      opts.logger.info({ topic: TOPIC, group: opts.groupId, startFrom: opts.startFrom }, 'indexer running');
    },

    /** Messages behind, per partition. See lag.ts. */
    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, TOPIC);
    },

    async stop(): Promise<void> {
      await consumer.disconnect().catch(() => {});
      await producer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
    },
  };
}
