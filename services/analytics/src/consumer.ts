import { setTimeout as sleep } from 'node:timers/promises';
import express from 'express';
import { createLogger } from '@eventhub/logger';
import { ingestEnv as env } from './ingest/env.js';
import { createClickHouse } from './clickhouse.js';
import { createIngestor, ingestHealth, ingestMetrics } from './ingest/consumer.js';
import { partCounts } from './ingest/parts.js';

/**
 * The ingest process — a separate entrypoint from the read API, and in
 * production a separate Deployment from the same image.
 *
 * Separate because the two have different scaling drivers and failure modes.
 * Ingest scales with partitions and is throughput-bound; the API scales with
 * dashboard traffic and is latency-bound. In one process, a large flush would
 * delay HTTP responses on the same event loop, and a memory spike would take the
 * API down with it.
 */
const logger = createLogger('analytics-ingest');

const ch = createClickHouse({
  url: env.CLICKHOUSE_URL,
  database: env.CLICKHOUSE_DATABASE,
  username: env.CLICKHOUSE_USER,
  password: env.CLICKHOUSE_PASSWORD,
});

const ingestor = createIngestor({
  brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
  registryUrl: env.SCHEMA_REGISTRY_URL,
  groupId: env.KAFKA_CONSUMER_GROUP,
  startFrom: env.INGEST_START_FROM,
  ch,
  logger,
  onFatal: (err) => {
    logger.fatal({ err }, 'the consumer stopped for good; exiting so the process is restarted');
    void ch.close().finally(() => process.exit(1));
  },
});

/**
 * A listener that exists only so the process can be probed. Without one,
 * Kubernetes treats a consumer that has silently stopped consuming as healthy.
 */
const app = express();
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'analytics-ingest' });
});
/**
 * The counters, plus the two numbers that decide whether this pipeline is healthy:
 * consumer lag, which says it is broken, and part counts, which say it is about to
 * be.
 *
 * Both take round trips — to the brokers and to ClickHouse — so they are cached for
 * five seconds and bounded at two, the way Search bounds its lag: a /metrics
 * request must never be the thing that hangs when Kafka or ClickHouse does.
 */
let slow: { at: number; lag: unknown; parts: unknown } | null = null;

const bounded = <T>(work: Promise<T>): Promise<T | { error: string }> =>
  Promise.race([work, sleep(2_000).then(() => ({ error: 'timed out after 2s' }))]).catch((err: unknown) => ({
    error: err instanceof Error ? err.message : String(err),
  }));

app.get('/metrics', async (_req, res) => {
  if (slow === null || Date.now() - slow.at > 5_000) {
    const [lag, parts] = await Promise.all([bounded(ingestor.lag()), bounded(partCounts(ch, env.CLICKHOUSE_DATABASE))]);
    slow = { at: Date.now(), lag, parts };
  }
  res.json({ ingest: { ...ingestMetrics(), lag: slow.lag, parts: slow.parts } });
});

/**
 * Ready while the consumer is subscribed and its batches are not failing, and
 * while ClickHouse accepts this process's credentials.
 *
 * Subscribed, not "has joined its group": a join can wait out a dead member's
 * session timeout — 30 seconds here, and a hard kill routinely leaves one behind —
 * and a probe that failed for that long would restart a healthy process into the
 * same wait. (The first version's comment said this while its flag waited for the
 * join anyway; review caught it.)
 *
 * ping({ select: true }) rather than ping(): ClickHouse answers GET /ping without
 * authenticating. Measured with a wrong password, ping() said success and
 * ping({ select: true }) said AUTHENTICATION_FAILED.
 */
app.get('/ready', async (_req, res) => {
  const clickhouse = await ch
    .ping({ select: true })
    .then((r) => r.success)
    .catch(() => false);
  const { consuming, failing, stopped, lastError } = ingestHealth();
  const ready = consuming && clickhouse;
  res
    .status(ready ? 200 : 503)
    .json({ status: ready ? 'ready' : 'not ready', consumer: consuming, clickhouse, failing, stopped, lastError });
});

async function startIngestor(): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await ingestor.start();
      return;
    } catch (err) {
      const waitMs = Math.min(30_000, 1_000 * 2 ** attempt);
      logger.error({ err, attempt, waitMs }, 'ingest failed to start; retrying');
      await ingestor.stop();
      await sleep(waitMs);
    }
  }
}

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  // Stopping the consumer first lets an in-flight batch finish its insert and
  // commit; closing ClickHouse first would fail that insert and redeliver it.
  await ingestor.stop();
  await ch.close().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

app.listen(env.INGEST_PORT, () => {
  logger.info(`analytics-ingest probes listening on port ${env.INGEST_PORT}`);
});
void startIngestor();
