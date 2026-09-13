import { Kafka, logLevel, type EachBatchPayload } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { ClickHouseClient } from '@clickhouse/client';
import { ZodError } from 'zod';
import type { Logger } from '@eventhub/logger';
import { INGEST_TOPICS, isIngestTopic, toRows, type ActivityRow } from './mappers.js';
import { writeActivity } from './writer.js';

interface DlqRow {
  source_topic: string;
  kafka_partition: number;
  kafka_offset: number;
  error: string;
  payload: string;
}

const counters = { batches: 0, rowsInserted: 0, duplicatesSkipped: 0, deadLettered: 0, insertFailures: 0 };
let assignedPartitions = 0;
let lastFlushMs: number | null = null;

export function ingestMetrics(): Record<string, unknown> {
  return {
    batchesTotal: counters.batches,
    rowsInsertedTotal: counters.rowsInserted,
    duplicatesSkippedTotal: counters.duplicatesSkipped,
    deadLetteredTotal: counters.deadLettered,
    insertFailuresTotal: counters.insertFailures,
    assignedPartitions,
    lastFlushMs,
  };
}

/** Registry and network failures that are not the message's fault. Same rule as Search's indexer. */
function isTransient(err: unknown): boolean {
  const text = err instanceof Error ? `${err.name} ${err.message}` : String(err);
  const status = (err as { status?: number })?.status;
  return (
    (typeof status === 'number' && status >= 500) ||
    /ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|timed? ?out/i.test(text)
  );
}

/**
 * What an operator reads in ingest_dlq.error. A ZodError's own message is its
 * issues as pretty-printed JSON, several lines per field; this is one line per
 * issue naming the field, the way Search's indexer reports a contract violation.
 */
function describe(err: unknown): string {
  const text =
    err instanceof ZodError
      ? `contract violation: ${err.issues.map((i) => `${i.path.join('.') || '(root)'}: ${i.message}`).join('; ')}`
      : err instanceof Error
        ? err.message
        : String(err);
  return text.slice(0, 2_000);
}

/**
 * Consumes the ticket and live-interaction topics into analytics.activity_events.
 *
 * ONE KAFKA BATCH, ONE INSERT. Every ClickHouse insert creates a data part, and a
 * part per message is how "too many parts" happens. kafkajs hands over one batch
 * per topic-partition per fetch, and the fetch waits up to a second for data to
 * accumulate (maxWaitTimeInMs with minBytes), so a quiet topic produces about one
 * insert a second per partition rather than one per message, and a busy one
 * produces large inserts. That is the size-or-time trigger, pushed down to the
 * fetch so it needs no buffer of its own.
 *
 * THE ORDER IS FIXED: insert, wait for ClickHouse to acknowledge, then resolve and
 * commit. Anything that fails between those redelivers, so the write must be safe
 * to repeat. writer.ts is how, and why a token made from offsets was not enough.
 *
 * WHAT CAN NEVER BE A ROW goes to analytics.ingest_dlq with its raw bytes, topic,
 * partition and offset, and the partition moves on. A ClickHouse or registry
 * outage is not that: it throws, nothing is committed, lag grows, and the batch is
 * redelivered when the store is back.
 */
export function createIngestor(opts: {
  brokers: string[];
  registryUrl: string;
  groupId: string;
  startFrom: 'earliest' | 'latest';
  ch: ClickHouseClient;
  logger: Logger;
}) {
  const kafka = new Kafka({ clientId: 'analytics-ingest', brokers: opts.brokers, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: opts.registryUrl });
  const consumer = kafka.consumer({
    groupId: opts.groupId,
    // Wait up to a second for up to a megabyte: the batching trigger.
    maxWaitTimeInMs: 1_000,
    minBytes: 1_000_000,
    sessionTimeout: 30_000,
  });

  async function decode(topic: string, value: Buffer | null): Promise<unknown> {
    if (value === null) throw new Error('null value (a tombstone)');
    if (!isIngestTopic(topic)) throw new Error(`not an ingest topic: ${topic}`);
    return INGEST_TOPICS[topic] === 'avro' ? registry.decode(value) : JSON.parse(value.toString('utf8'));
  }

  async function processBatch(payload: EachBatchPayload): Promise<void> {
    const { batch, resolveOffset, heartbeat, isRunning, isStale, commitOffsetsIfNecessary } = payload;
    if (batch.messages.length === 0) return;

    const rows: ActivityRow[] = [];
    const dead: DlqRow[] = [];

    for (const message of batch.messages) {
      // A partition taken away mid-batch: stop without resolving anything, so
      // nothing past here is committed and the new owner starts from the same place.
      if (!isRunning() || isStale()) return;
      const at = { topic: batch.topic, partition: batch.partition, offset: message.offset };
      try {
        rows.push(...toRows(at, await decode(batch.topic, message.value)));
      } catch (err) {
        if (isTransient(err)) throw err;
        dead.push({
          source_topic: batch.topic,
          kafka_partition: batch.partition,
          kafka_offset: Number(message.offset),
          error: describe(err),
          payload: message.value?.toString('base64') ?? '',
        });
      }
    }

    const last = batch.messages[batch.messages.length - 1]!.offset;
    const where = { topic: batch.topic, partition: batch.partition, lastOffset: last };

    const started = Date.now();
    let written;
    try {
      written = await writeActivity(opts.ch, rows);
      if (dead.length > 0) {
        // Not deduplicated: a redelivered poison message is parked twice, which
        // costs an operator one extra row to read and changes no rollup.
        await opts.ch.insert({ table: 'ingest_dlq', values: dead, format: 'JSONEachRow' });
      }
    } catch (err) {
      counters.insertFailures += 1;
      opts.logger.warn({ err, ...where }, 'write failed; not committing, the batch will be redelivered');
      throw err;
    }
    lastFlushMs = Date.now() - started;

    counters.batches += 1;
    counters.rowsInserted += written.inserted;
    counters.duplicatesSkipped += written.duplicates;
    counters.deadLettered += dead.length;
    if (dead.length > 0) opts.logger.error({ ...where, count: dead.length }, 'messages sent to analytics.ingest_dlq');

    // Only now, with ClickHouse's acknowledgement in hand.
    //
    // WITH THE OFFSETS SPELLED OUT. commitOffsetsIfNecessary() with no argument
    // commits only once kafkajs's autoCommitInterval or autoCommitThreshold is
    // reached, and with autoCommit off both are null — so it never commits. The
    // first version called it that way, and scripts/verify-analytics-ingest.mjs
    // found the group's committed offsets still at -1 after everything was
    // written: every restart would have replayed from wherever 'earliest' or
    // 'latest' put it. Given offsets, it commits them. A committed offset is the
    // NEXT one to read, hence the + 1.
    resolveOffset(last);
    await heartbeat();
    await commitOffsetsIfNecessary({
      topics: [{ topic: batch.topic, partitions: [{ partition: batch.partition, offset: (BigInt(last) + 1n).toString() }] }],
    });
  }

  return {
    async start(): Promise<void> {
      await consumer.connect();
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
        assignedPartitions = Object.values(payload.memberAssignment).reduce((n, p) => n + p.length, 0);
      });
      await consumer.subscribe({ topics: Object.keys(INGEST_TOPICS), fromBeginning: opts.startFrom === 'earliest' });
      await consumer.run({ autoCommit: false, eachBatchAutoResolve: false, eachBatch: processBatch });
      opts.logger.info({ group: opts.groupId, topics: Object.keys(INGEST_TOPICS), startFrom: opts.startFrom }, 'ingest running');
    },

    async stop(): Promise<void> {
      await consumer.disconnect().catch(() => {});
    },
  };
}
