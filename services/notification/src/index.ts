import { setTimeout as sleep } from 'node:timers/promises';
import express from 'express';
import { createLogger } from '@eventhub/logger';
import { createRequireAuth, publicKeyFromEnv } from '@eventhub/auth-middleware';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';
import { createSmtpChannel } from './channels/smtp.js';
import { createSealOpener, loadNotificationPrivateKey } from './dispatch/open-seal.js';
import { createDispatcher, dispatchHealth, dispatchMetrics } from './dispatch/consumer.js';
import { createSweeper } from './sweeper/index.js';
import { preferenceRoutes } from './preferences/routes.js';

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
let sweeper: ReturnType<typeof createSweeper> | null = null;

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
let slow: { at: number; lag: unknown; retryLag: unknown } | null = null;
app.get('/metrics', async (_req, res) => {
  if (dispatcher && (slow === null || Date.now() - slow.at > 5_000)) {
    const [lag, retryLag] = await Promise.all([bounded(dispatcher.lag()), bounded(dispatcher.retryLag())]);
    slow = { at: Date.now(), lag, retryLag };
  }
  res.json({
    dispatch: { ...dispatchMetrics(), lag: slow?.lag ?? null, retryLag: slow?.retryLag ?? null },
    // Straight from memory: the sweep keeps its own counts and asks the database on
    // its own clock, so a /metrics request never waits on it.
    sweep: sweeper?.metrics() ?? null,
  });
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

/**
 * Registered in main(), AFTER the routes: Express matches middleware in order, so an
 * error handler registered before a route never sees its errors. It sat up here while
 * every route was synchronous and defined above it; /preferences is mounted in main()
 * because requireAuth needs Auth's public key loaded first, which put the handler in
 * front of it. Community shipped exactly that once — a handler that threw took the
 * process down instead of answering 500.
 */
const errorHandler: express.ErrorRequestHandler = (err, _req, res, _next) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
};

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
  const started = Date.now();
  logger.info({ signal }, 'shutting down');
  // The consumers first, so an email being sent can record its outcome before the
  // database connection goes. Each step is timed: compose kills the container after
  // its grace period, and a consumer killed that way stays in its group until its
  // session times out.
  await dispatcher?.stop();
  logger.info({ ms: Date.now() - started }, 'dispatch consumers stopped');
  await sweeper?.stop();
  await prisma.$disconnect().catch(() => {});
  logger.info({ ms: Date.now() - started }, 'shut down');
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

  // A user's own preferences, and the unsubscribe link. requireAuth verifies an access
  // token — aud JWT_AUDIENCE — with the same public key, so a sealed email request,
  // which is for aud eventhub-notification, is not accepted as one.
  app.use(
    preferenceRoutes({
      requireAuth: createRequireAuth({ publicKey: authPublicKey, issuer: env.JWT_ISSUER, audience: env.JWT_AUDIENCE }),
      unsubscribeSecret: env.UNSUBSCRIBE_SECRET,
      logger,
    }),
  );
  app.use(errorHandler);

  dispatcher = createDispatcher({
    brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    registryUrl: env.SCHEMA_REGISTRY_URL,
    groupId: env.KAFKA_CONSUMER_GROUP,
    startFrom: env.DISPATCH_START_FROM,
    retryDelaysMs: env.DISPATCH_RETRY_DELAYS_MS,
    logger,
    open: createSealOpener({ privateKey, authPublicKey, issuer: env.JWT_ISSUER }),
    channel,
    onFatal: (err) => {
      logger.fatal({ err }, 'the dispatch consumer stopped for good; exiting so the process is restarted');
      void prisma.$disconnect().finally(() => process.exit(1));
    },
  });

  sweeper = createSweeper({
    databaseUrl: env.SWEEPER_DATABASE_URL,
    intervalMs: env.SWEEP_INTERVAL_MS,
    stuckAfterMs: env.SWEEP_STUCK_AFTER_MS,
    batchSize: env.SWEEP_BATCH_SIZE,
    logger,
  });

  app.listen(env.PORT, () => {
    logger.info(`notification-service listening on port ${env.PORT}`);
  });
  void startDispatcher(dispatcher);
  // Awaited, so a sweeper credential that can do more than sweep stops the service
  // here rather than being discovered by someone reading grants later.
  await sweeper.start();
  logger.info({ everyMs: env.SWEEP_INTERVAL_MS, stuckAfterMs: env.SWEEP_STUCK_AFTER_MS }, 'stuck-delivery sweep running');
}

main().catch((err) => {
  logger.error({ err }, 'notification-service failed to start');
  process.exit(1);
});
