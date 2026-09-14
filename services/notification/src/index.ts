import express from 'express';
import { createLogger } from '@eventhub/logger';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';

/**
 * The Notification service: the only service that will talk to an email provider.
 *
 * This first part is its foundation — its own database, reachable only by its own
 * roles, with row-level security on the sent-log — and nothing that sends yet.
 * The consumer, the channels and the delivery path arrive in the parts that build
 * them.
 */
const logger = createLogger('notification-service');
const app = express();

app.use(express.json());

/** Liveness: the process is up. Never a dependency, or a slow database restarts every replica. */
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'notification' });
});

/**
 * Readiness: the database answers, as the role this service actually runs as.
 * A probe that does not check the dependency keeps traffic flowing to a process
 * that cannot do anything.
 */
app.get('/ready', (_req, res) => {
  prisma
    .$queryRaw`SELECT 1`
    .then(() => res.json({ ready: true }))
    .catch(() => res.status(503).json({ ready: false }));
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
    logger.info(`notification-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'notification-service failed to start');
  process.exit(1);
});
