import { setTimeout as sleep } from 'node:timers/promises';
import express from 'express';
import { createLogger } from '@eventhub/logger';
import { env } from './env.js';
import { clusterInfo } from './opensearch.js';
import { createIndexer, indexerMetrics } from './indexer/consumer.js';

const logger = createLogger('search-service');
const app = express();

app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'search' });
});

/**
 * Readiness reports whether the CLUSTER is reachable, which is a different
 * question from whether this process is alive.
 *
 * Search is the one service in the fleet that is allowed to be degraded rather
 * than broken: its store is a derived read model, so losing it costs the search
 * box and nothing else. Saying so here — rather than failing /health and being
 * restarted in a loop — is what makes that degradation orderly.
 */
app.get('/ready', (_req, res) => {
  clusterInfo()
    .then((info) => res.json({ ready: true, opensearch: info }))
    .catch((err: unknown) => {
      logger.warn({ err }, 'opensearch unreachable');
      res.status(503).json({ ready: false });
    });
});

/**
 * The indexer's counters. JSON rather than Prometheus text, the same as Live's
 * /metrics and for the same reason: nothing scrapes this yet, and a format with
 * no consumer is a guess.
 */
app.get('/metrics', (_req, res) => {
  res.json({ indexer: indexerMetrics() });
});

app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

const indexer = createIndexer({
  brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
  registryUrl: env.SCHEMA_REGISTRY_URL,
  groupId: env.KAFKA_CONSUMER_GROUP,
  startFrom: env.SEARCH_INDEXER_START_FROM,
  logger,
});

/**
 * Keeps trying to start the indexer rather than exiting when Kafka is not there.
 *
 * The same reasoning as listening before the cluster is reachable: a broker that
 * is restarting should delay indexing, not crash-loop the service that answers
 * search. Once running, kafkajs restarts the consumer after its own failures.
 */
async function startIndexer(): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await indexer.start();
      return;
    } catch (err) {
      const waitMs = Math.min(30_000, 1_000 * 2 ** attempt);
      logger.error({ err, attempt, waitMs }, 'indexer failed to start; retrying');
      await indexer.stop();
      await sleep(waitMs);
    }
  }
}

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  // Leaving the group cleanly hands its partitions to another replica now,
  // rather than after the session timeout.
  await indexer.stop();
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main(): Promise<void> {
  /**
   * LISTEN FIRST, then look for the cluster.
   *
   * This used to await the cluster before listening and exit 1 when it was not
   * there, which contradicted the comment above /ready. A pod started during an
   * OpenSearch maintenance window would crash, be restarted, and crash again —
   * the restart loop that separating liveness from readiness exists to prevent.
   * Now the process comes up, /health answers, and /ready says 503 until the
   * cluster can be reached.
   */
  app.listen(env.PORT, () => {
    logger.info(`search-service listening on port ${env.PORT}`);
  });

  // Log what we are actually talking to. A version mismatch between the client
  // and the cluster produces failures that look like bugs in the query.
  try {
    const info = await clusterInfo();
    logger.info(info, 'connected to opensearch');
  } catch (err) {
    logger.warn({ err }, 'opensearch unreachable at startup; /ready reports 503 until it is');
  }

  void startIndexer();
}

main().catch((err) => {
  logger.error({ err }, 'search-service failed to start');
  process.exit(1);
});
