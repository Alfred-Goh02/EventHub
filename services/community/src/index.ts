import express from 'express';
import { createLogger } from '@eventhub/logger';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';

/**
 * The Community service: spaces, posts, threaded comments, reactions and moderation.
 *
 * This is the foundation only — the database, its isolation, and the two probes. The
 * routes arrive in the parts after it.
 */
const logger = createLogger('community-service');

const app = express();
app.use(express.json());

/** Liveness: the process is up. Never a dependency, or a slow database restarts every replica. */
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'community' });
});

/**
 * Readiness: the database answers AS THIS PROCESS, which is the app role. A probe that
 * asks a different credential than the service uses can say ready while every request
 * is refused.
 */
app.get('/ready', async (_req, res) => {
  const database = await prisma.$queryRaw`SELECT 1`.then(
    () => true,
    () => false,
  );
  res.status(database ? 200 : 503).json({ ready: database, database });
});

app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  await prisma.$disconnect().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main(): Promise<void> {
  // Refuse to start rather than run with tenant isolation silently disabled.
  await assertRuntimeRoleCannotBypassRls();

  app.listen(env.PORT, () => {
    logger.info(`community-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'community-service failed to start');
  process.exit(1);
});
