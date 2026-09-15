import { setTimeout as sleep } from 'node:timers/promises';
import express from 'express';
import { createLogger } from '@eventhub/logger';
import { publicKeyFromEnv } from '@eventhub/auth-middleware';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';
import { createSmtpChannel } from './channels/smtp.js';
import { createSealOpener, loadNotificationPrivateKey } from './dispatch/open-seal.js';
import { createDispatcher, dispatchHealth, dispatchMetrics } from './dispatch/consumer.js';

/**
 * The Notification service: the one service that sends email.
 *
 * It consumes auth.email-requested. Each request is sealed, so it is opened with
 * this service's private key and believed only if Auth's signature verifies. The
 * delivery is claimed in the sent-log before anything is sent, and the email goes
 * out over SMTP, which is Mailpit locally.
 */
const logger = createLogger('notification-service');

const channel = createSmtpChannel({ host: env.SMTP_HOST, port: env.SMTP_PORT, from: env.EMAIL_FROM });
let dispatcher: ReturnType<typeof createDispatcher> | null = null;

const app = express();
app.use(express.json());

/** Liveness: the process is up. Never a dependency, or a slow database restarts every replica. */
app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'notification' });
});

/** Two seconds at most for anything that crosses the network on behalf of a probe. */
const bounded = <T>(work: Promise<T>): Promise<T | { error: string }> =>
  Promise.race([work, sleep(2_000).then(() => ({ error: 'timed out after 2s' }))]).catch((err: unknown) => ({
    error: err instanceof Error ? err.message : String(err),
  }));

/**
 * The counters, and consumer lag in messages. Lag takes a round trip to the
 * brokers, so it is cached for five seconds and bounded at two, as Analytics'
 * ingest does: a /metrics request must never be the thing that hangs when Kafka does.
 */
let slow: { at: number; lag: unknown } | null = null;
app.get('/metrics', async (_req, res) => {
  if (dispatcher && (slow === null || Date.now() - slow.at > 5_000)) {
    slow = { at: Date.now(), lag: await bounded(dispatcher.lag()) };
  }
  res.json({ dispatch: { ...dispatchMetrics(), lag: slow?.lag ?? null } });
});

/**
 * Ready when every dependency answers, asked the way this process uses it:
 *   - Postgres as the app role;
 *   - Kafka through the consumer's own state, which a crash or a failing message
 *     clears;
 *   - SMTP through a real handshake, cached for five seconds so a probe every few
 *     seconds does not open a connection each time.
 * A consumer that cannot send would otherwise look ready while every email
 * redelivered behind it.
 */
let smtp: { at: number; ok: boolean; error: string | null } | null = null;
app.get('/ready', async (_req, res) => {
  const database = await prisma.$queryRaw`SELECT 1`.then(
    () => true,
    () => false,
  );
  if (smtp === null || Date.now() - smtp.at > 5_000) {
    const result = await bounded(channel.verify().then(() => true as const));
    smtp = { at: Date.now(), ok: result === true, error: result === true ? null : result.error };
  }
  const { consuming, failing, stopped, lastError } = dispatchHealth();
  const ready = database && consuming && smtp.ok;
  res.status(ready ? 200 : 503).json({
    ready,
    database,
    consumer: consuming,
    smtp: smtp.ok,
    smtpError: smtp.error,
    failing,
    stopped,
    lastError,
  });
});

app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

async function startDispatcher(d: NonNullable<typeof dispatcher>): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await d.start();
      return;
    } catch (err) {
      const waitMs = Math.min(30_000, 1_000 * 2 ** attempt);
      logger.error({ err, attempt, waitMs }, 'dispatch failed to start; retrying');
      await d.stop();
      await sleep(waitMs);
    }
  }
}

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  // The consumer first, so an email being sent can record its outcome before the
  // database connection goes.
  await dispatcher?.stop();
  await prisma.$disconnect().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main(): Promise<void> {
  // Refuse to start rather than run with tenant isolation silently disabled.
  await assertRuntimeRoleCannotBypassRls();

  // Both keys before anything is consumed: a request that cannot be opened or
  // verified must not be read and skipped as if it were poison.
  const [privateKey, authPublicKey] = await Promise.all([loadNotificationPrivateKey(), publicKeyFromEnv()]);

  dispatcher = createDispatcher({
    brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    registryUrl: env.SCHEMA_REGISTRY_URL,
    groupId: env.KAFKA_CONSUMER_GROUP,
    startFrom: env.DISPATCH_START_FROM,
    logger,
    open: createSealOpener({ privateKey, authPublicKey, issuer: env.JWT_ISSUER }),
    channel,
    onFatal: (err) => {
      logger.fatal({ err }, 'the dispatch consumer stopped for good; exiting so the process is restarted');
      void prisma.$disconnect().finally(() => process.exit(1));
    },
  });

  app.listen(env.PORT, () => {
    logger.info(`notification-service listening on port ${env.PORT}`);
  });
  void startDispatcher(dispatcher);
}

main().catch((err) => {
  logger.error({ err }, 'notification-service failed to start');
  process.exit(1);
});
