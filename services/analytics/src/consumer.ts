import { setTimeout as sleep } from 'node:timers/promises';
import express from 'express';
import { createLogger } from '@eventhub/logger';
import { ingestEnv as env } from './ingest/env.js';
import { createClickHouse } from './clickhouse.js';
import { createIngestor, ingestMetrics } from './ingest/consumer.js';

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
});

/**
 * A listener that exists only so the process can be probed. Without one,
 * Kubernetes treats a consumer that has silently stopped consuming as healthy.
 */
const app = express();
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'analytics-ingest' });
});
app.get('/metrics', (_req, res) => {
  res.json({ ingest: ingestMetrics() });
});

let ingestStarted = false;

/**
 * Ready once the consumer has connected to Kafka and subscribed, and ClickHouse
 * answers as this process's user.
 *
 * Not "has joined its group". A join can wait out a dead member's session
 * timeout — 30 seconds here, and a hard kill routinely leaves one behind — and a
 * probe that failed for that long would restart a healthy process into the same
 * wait.
 */
app.get('/ready', async (_req, res) => {
  const clickhouse = await ch
    .ping()
    .then((r) => r.success)
    .catch(() => false);
  const ready = ingestStarted && clickhouse;
  res.status(ready ? 200 : 503).json({ status: ready ? 'ready' : 'not ready', consumer: ingestStarted, clickhouse });
});

async function startIngestor(): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await ingestor.start();
      ingestStarted = true;
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
