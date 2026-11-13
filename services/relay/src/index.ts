import { createServer } from 'node:http';
import { createLogger } from '@eventhub/logger';
import { loadRelayEnv } from './env.js';
import { assertRelayIsLeastPrivilege, createPool } from './db.js';
import { createPublisher } from './publisher.js';
import { drainOnce, readMetrics } from './drain.js';
import { pruneOnce } from './prune.js';

const env = loadRelayEnv();
const logger = createLogger('outbox-relay');

const pool = createPool(env.RELAY_DATABASE_URL, logger);
const publisher = createPublisher({
  brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
  registryUrl: env.SCHEMA_REGISTRY_URL,
});

let stopping = false;

/**
 * When the retention sweep last ran. Zero means "not yet", so the first pass
 * happens shortly after start rather than an hour in -- on a table nobody has
 * ever pruned, waiting an hour to begin is the wrong default.
 */
let lastPruneAt = 0;

/**
 * The drain currently in progress, if any.
 *
 * `stopping` is only read BETWEEN loop iterations, so on SIGTERM the shutdown
 * path used to disconnect the publisher and end the pool while a batch was
 * mid-publish. The transaction then aborts, so nothing is corrupted — the
 * attempt increments roll back with it — but rows already handed to Kafka are
 * republished on restart, and the operator sees a stack of connection errors on
 * every deploy that look like a fault rather than a shutdown.
 *
 * Holding the promise lets shutdown wait for the batch to finish committing.
 */
let inFlight: Promise<unknown> | null = null;

/** How long a shutdown waits for an in-flight batch before giving up on it. */
const SHUTDOWN_GRACE_MS = 10_000;

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
      // Kept in a typed local as well as the untyped `inFlight` handle, so
      // awaiting it here still narrows to DrainResult.
      const pending = drainOnce(pool, publisher, logger, {
        batchSize: env.RELAY_BATCH_SIZE,
        maxAttempts: env.RELAY_MAX_ATTEMPTS,
      });
      inFlight = pending;
      const result = await pending;
      inFlight = null;

      if (result.claimed > 0) {
        logger.info(result, 'drained');
      }

      /**
       * Retention runs on its OWN clock, between drains rather than inside one.
       *
       * Not inside drainOnce, and not in its transaction: that transaction is
       * the one whose commit follows messages already handed to Kafka, and a
       * prune failure rolling it back would republish everything in the batch.
       * Delivery and housekeeping do not belong in the same commit.
       *
       * A failure here is logged and swallowed. A relay that stops delivering
       * because it could not tidy up has turned a disk-space problem into an
       * outage, which is the wrong direction by a wide margin.
       */
      if (Date.now() - lastPruneAt >= env.RELAY_PRUNE_INTERVAL_MS) {
        lastPruneAt = Date.now();
        try {
          const pruned = await pruneOnce(pool, logger, {
            olderThanDays: env.RELAY_RETENTION_DAYS,
            batchSize: env.RELAY_PRUNE_BATCH_SIZE,
            maxBatches: env.RELAY_PRUNE_MAX_BATCHES,
          });
          if (pruned.moreToDo) {
            logger.info(pruned, 'retention sweep hit its batch ceiling; more to prune next pass');
          }
        } catch (err) {
          logger.error({ err }, 'retention sweep failed; delivery is unaffected');
        }
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
  // Liveness: the process is up. It touches nothing, because /metrics asks the database,
  // and a liveness probe on a dependency restarts every relay when the database is slow.
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' }).end(JSON.stringify({ status: 'ok', service: 'relay' }));
    return;
  }
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

  // Let the batch in progress commit before pulling the broker and the pool out
  // from under it. Bounded, because a drain blocked on an unreachable broker
  // must not turn a deploy into a hang -- if the grace expires we tear down
  // anyway and those rows are simply redelivered, which the design already
  // requires every consumer to tolerate.
  if (inFlight) {
    logger.info('waiting for the in-flight drain to finish');
    await Promise.race([inFlight.catch(() => {}), sleep(SHUTDOWN_GRACE_MS)]);
  }

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
  await assertRelayIsLeastPrivilege(pool, logger, env.RELAY_FORBIDDEN_TABLE);

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
