import express from 'express';
import { createLogger } from '@eventhub/logger';
import { env } from './env.js';
import { createClickHouse } from './clickhouse.js';

const logger = createLogger('analytics-service');
const app = express();

const ch = createClickHouse({
  url: env.CLICKHOUSE_URL,
  database: env.CLICKHOUSE_DATABASE,
  username: env.CLICKHOUSE_USER,
  password: env.CLICKHOUSE_PASSWORD,
});

app.use(express.json());

/** Liveness: the process is up. Never a dependency, or a slow ClickHouse restarts every replica. */
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'analytics' });
});

/**
 * Readiness: ClickHouse answers, as the user this service actually runs as.
 *
 * A readiness probe that does not check the dependency is worse than none: it
 * keeps traffic flowing to a process that cannot answer a single query.
 */
app.get('/ready', (_req, res) => {
  ch.ping()
    .then((result) => {
      if (result.success) res.json({ ready: true });
      else res.status(503).json({ ready: false });
    })
    .catch(() => res.status(503).json({ ready: false }));
});

app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  await ch.close().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

// Listen first, as Search does: a ClickHouse that is still starting should make
// /ready say 503, not crash-loop the process.
app.listen(env.PORT, () => {
  logger.info(`analytics-service listening on port ${env.PORT}`);
});
