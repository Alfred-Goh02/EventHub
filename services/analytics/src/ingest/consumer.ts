import { Kafka, logLevel, type EachBatchPayload } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { ClickHouseClient } from '@clickhouse/client';
import { ZodError } from 'zod';
import type { Logger } from '@eventhub/logger';
import { INGEST_TOPICS, isIngestTopic, toRows } from './mappers.js';
import { writeMessages, type MessageRows } from './writer.js';
import { consumerLag, type ConsumerLag } from '@eventhub/kafka';

interface DlqRow {
  source_topic: string;
  kafka_partition: number;
  kafka_offset: number;
  error: string;
  payload: string;
}

const counters = {
  batches: 0,
  rowsInserted: 0,
  duplicatesSkipped: 0,
  deadLettered: 0,
  insertFailures: 0,
  batchFailures: 0,
  crashes: 0,
};
let assignedPartitions = 0;
let lastFlushMs: number | null = null;

/**
 * Whether the consumer is doing its job, for /ready.
 *
 * The first version set one flag once the consumer started and never cleared it,
 * so a consumer crash-looping on a batch it could never write, or stopped for good
 * by an error kafkajs will not retry, answered ready — the silent stop the probe
 * listener exists to expose (found by review).
 */
const health = { subscribed: false, failing: false, stopped: false, lastError: null as string | null };

export function ingestMetrics(): Record<string, unknown> {
  return {
    batchesTotal: counters.batches,
    rowsInsertedTotal: counters.rowsInserted,
    duplicatesSkippedTotal: counters.duplicatesSkipped,
    deadLetteredTotal: counters.deadLettered,
    insertFailuresTotal: counters.insertFailures,
    /** Batches that threw, for any reason, and so will be redelivered. */
    batchFailuresTotal: counters.batchFailures,
    /** Times kafkajs gave up retrying a batch and restarted (or stopped) the consumer. */
    crashesTotal: counters.crashes,
    assignedPartitions,
    lastFlushMs,
  };
}

export interface IngestHealth {
  /** Subscribed, not stopped, and the last batch did not fail (or the group has been rejoined since). */
  consuming: boolean;
  failing: boolean;
  stopped: boolean;
  lastError: string | null;
}

export function ingestHealth(): IngestHealth {
  return {
    consuming: health.subscribed && !health.failing && !health.stopped,
    failing: health.failing,
    stopped: health.stopped,
    lastError: health.lastError,
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
 *
 * WELL-FORMED, ALWAYS. The text is partly the message's own: zod echoes the value
 * it received, and JSON.parse quotes the token it choked on one UTF-16 unit at a
 * time. A lone surrogate in that text made ClickHouse refuse the dead-letter row
 * itself (CANNOT_PARSE_ESCAPE_SEQUENCE) — the same stalled partition a refused
 * activity row once caused, one table over, found by review. Cutting to a length
 * can also split a surrogate pair, so toWellFormed runs after the slice, replacing
 * any lone surrogate with U+FFFD.
 */
function describe(err: unknown): string {
  const text =
    err instanceof ZodError
      ? `contract violation: ${err.issues.map((i) => `${i.path.join('.') || '(root)'}: ${i.message}`).join('; ')}`
      : err instanceof Error
        ? err.message
        : String(err);
  return text.slice(0, 2_000).toWellFormed();
}

export interface BatchProcessorOptions {
  ch: ClickHouseClient;
  /** Turns a message's bytes into the value its mapper expects. */
  decode: (topic: string, value: Buffer | null) => Promise<unknown>;
  logger: Logger;
}

/**
 * One Kafka batch: decode, map, write, and only then commit.
 *
 * ONE KAFKA BATCH, ONE INSERT. Every ClickHouse insert creates a data part, and a
 * part per message is how "too many parts" happens. kafkajs hands over one batch
 * per topic-partition per fetch, and the fetch waits up to a second for data to
 * accumulate (maxWaitTimeInMs with minBytes), so a quiet topic produces about one
 * insert a second per partition rather than one per message.
 *
 * THE ORDER IS FIXED: insert, wait for ClickHouse to acknowledge, then resolve and
 * commit. Anything that fails between those redelivers, so the write must be safe
 * to repeat. writer.ts is how, and why a token made from offsets was not enough.
 *
 * WHAT CAN NEVER BE A ROW goes to analytics.ingest_dlq with its raw bytes, topic,
 * partition and offset, and the partition moves on. A message is found to be one
 * in two places: the mapper refuses it, or ClickHouse refuses its rows (see
 * writeMessages). A ClickHouse or registry outage is neither: it throws, nothing
 * is committed, lag grows, and the batch is redelivered when the store is back.
 *
 * Exported apart from the Kafka wiring so scripts/verify-analytics-ingest.mjs can
 * hand it batches whose dependencies fail on cue and watch what it commits.
 */
export function createBatchProcessor(opts: BatchProcessorOptions): (payload: EachBatchPayload) => Promise<void> {
  async function processBatch(payload: EachBatchPayload): Promise<void> {
    const { batch, resolveOffset, heartbeat, isRunning, isStale, commitOffsetsIfNecessary } = payload;
    if (batch.messages.length === 0) return;

    const mapped: MessageRows[] = [];
    const dead: DlqRow[] = [];
    const park = (offset: string, payload: Buffer | null, error: string): void => {
      dead.push({
        source_topic: batch.topic,
        kafka_partition: batch.partition,
        kafka_offset: Number(offset),
        error,
        payload: payload?.toString('base64') ?? '',
      });
    };

    for (const message of batch.messages) {
      // A partition taken away mid-batch: stop without resolving anything, so
      // nothing past here is committed and the new owner starts from the same place.
      if (!isRunning() || isStale()) return;
      const at = { topic: batch.topic, partition: batch.partition, offset: message.offset };
      try {
        mapped.push({ offset: message.offset, payload: message.value, rows: toRows(at, await opts.decode(batch.topic, message.value)) });
      } catch (err) {
        if (isTransient(err)) throw err;
        park(message.offset, message.value, describe(err));
      }
    }

    const last = batch.messages[batch.messages.length - 1]!.offset;
    const where = { topic: batch.topic, partition: batch.partition, lastOffset: last };

    const started = Date.now();
    let written;
    try {
      written = await writeMessages(opts.ch, mapped);
      for (const r of written.refused) park(r.offset, r.payload, r.error.toWellFormed());
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
    health.failing = false;
  }

  return async (payload) => {
    try {
      await processBatch(payload);
    } catch (err) {
      counters.batchFailures += 1;
      health.failing = true;
      health.lastError = describe(err);
      throw err;
    }
  };
}

/**
 * Consumes the ticket and live-interaction topics into analytics.activity_events.
 * See createBatchProcessor for what happens to each batch.
 */
export function createIngestor(opts: {
  brokers: string[];
  registryUrl: string;
  groupId: string;
  startFrom: 'earliest' | 'latest';
  ch: ClickHouseClient;
  logger: Logger;
  /**
   * Called when kafkajs stops the consumer for good — an error it will not retry.
   * The process can do nothing useful after that, so the caller exits and lets the
   * orchestrator start a fresh one; staying up would be a live process consuming
   * nothing.
   */
  onFatal: (err: unknown) => void;
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
  // Reads offsets for the lag measurement. Its own connection, so a slow
  // metadata request never waits behind a fetch.
  const admin = kafka.admin();

  async function decode(topic: string, value: Buffer | null): Promise<unknown> {
    if (value === null) throw new Error('null value (a tombstone)');
    if (!isIngestTopic(topic)) throw new Error(`not an ingest topic: ${topic}`);
    return INGEST_TOPICS[topic] === 'avro' ? registry.decode(value) : JSON.parse(value.toString('utf8'));
  }

  const processBatch = createBatchProcessor({ ch: opts.ch, decode, logger: opts.logger });

  return {
    async start(): Promise<void> {
      await admin.connect();
      await consumer.connect();
      consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
        assignedPartitions = Object.values(payload.memberAssignment).reduce((n, p) => n + p.length, 0);
        // A fresh start on the partitions. A batch that fails again sets it back.
        health.failing = false;
      });
      consumer.on(consumer.events.CRASH, ({ payload }) => {
        counters.crashes += 1;
        assignedPartitions = 0;
        if (payload.restart) {
          opts.logger.error({ err: payload.error }, 'consumer crashed; kafkajs is restarting it');
          return;
        }
        health.stopped = true;
        health.lastError = describe(payload.error);
        opts.onFatal(payload.error);
      });
      await consumer.subscribe({ topics: Object.keys(INGEST_TOPICS), fromBeginning: opts.startFrom === 'earliest' });
      // Ready from here, not from the group join run() waits for below: a join can
      // wait out a dead member's 30-second session.
      health.subscribed = true;
      await consumer.run({ autoCommit: false, eachBatchAutoResolve: false, eachBatch: processBatch });
      opts.logger.info({ group: opts.groupId, topics: Object.keys(INGEST_TOPICS), startFrom: opts.startFrom }, 'ingest running');
    },

    /** Messages behind, per partition of every ingest topic. See @eventhub/kafka. */
    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, Object.keys(INGEST_TOPICS));
    },

    async stop(): Promise<void> {
      health.subscribed = false;
      await consumer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
    },
  };
}
