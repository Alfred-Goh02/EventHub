import { createServer } from 'node:http';
import { createLogger } from '@eventhub/logger';
import { loadRelayEnv } from './env.js';
import { assertRelayIsLeastPrivilege, createPool } from './db.js';
import { createPublisher } from './publisher.js';
import { drainOnce, readMetrics } from './drain.js';

const env = loadRelayEnv();
const logger = createLogger('outbox-relay');

const pool = createPool(env.RELAY_DATABASE_URL);
const publisher = createPublisher({
  brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
  registryUrl: env.SCHEMA_REGISTRY_URL,
});

let stopping = false;

/**
 * Drain, then sleep only if there was nothing to do.
 *
 * A busy relay loops immediately so a backlog drains at full speed; an idle one
 * waits. Sleeping a fixed interval regardless would cap throughput at
 * batchSize per interval no matter how far behind it is.
 */
async function loop(): Promise<void> {
  while (!stopping) {
    try {
      const result = await drainOnce(pool, publisher, logger, {
        batchSize: env.RELAY_BATCH_SIZE,
        maxAttempts: env.RELAY_MAX_ATTEMPTS,
      });

      if (result.claimed > 0) {
        logger.info(result, 'drained');
      }

      if (result.claimed === 0) {
        await sleep(env.RELAY_POLL_INTERVAL_MS);
      }
    } catch (err) {
      // The loop must not die. A broker restart or a dropped connection is
      // normal; the relay's job is to still be running when it comes back.
      logger.error({ err }, 'drain failed, retrying');
      await sleep(env.RELAY_POLL_INTERVAL_MS);
    }
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const metricsServer = createServer((req, res) => {
  if (req.url !== '/metrics') {
    res.writeHead(404).end();
    return;
  }

  readMetrics(pool)
    .then((m) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      // Alert on oldestUndeliveredSeconds, not backlog. 500 rows draining fast
      // is healthy; three rows stuck for an hour is an outage.
      res.end(JSON.stringify(m));
    })
    .catch((err) => {
      logger.error({ err }, 'metrics query failed');
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'metrics unavailable' }));
    });
});

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  stopping = true;
  metricsServer.close();
  await publisher.disconnect().catch(() => {});
  await pool.end().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

/**
 * `--once` drains a single batch and exits.
 *
 * Exists so CI can assert the whole path — outbox row in, Kafka message out —
 * without babysitting a long-running process, and so a human can drain a
 * backlog deliberately without leaving a daemon behind.
 */
const runOnce = process.argv.includes('--once');

async function main(): Promise<void> {
  // Fails closed: refuses to start if the credential can read business tables
  // or bypass row security.
  await assertRelayIsLeastPrivilege(pool, env.RELAY_FORBIDDEN_TABLE);

  await publisher.connect();

  if (runOnce) {
    const result = await drainOnce(pool, publisher, logger, {
      batchSize: env.RELAY_BATCH_SIZE,
      maxAttempts: env.RELAY_MAX_ATTEMPTS,
    });
    logger.info(result, 'drained once');
    await publisher.disconnect();
    await pool.end();
    // Non-zero if anything was dead-lettered, so CI notices rather than passing
    // a run in which messages were silently abandoned.
    process.exit(result.deadLettered > 0 ? 1 : 0);
  }

  metricsServer.listen(env.RELAY_METRICS_PORT, () => {
    logger.info(
      { metricsPort: env.RELAY_METRICS_PORT, batchSize: env.RELAY_BATCH_SIZE },
      'outbox relay started',
    );
  });

  await loop();
}

main().catch((err) => {
  logger.error({ err }, 'relay failed to start');
  process.exit(1);
});
