import express from 'express';
import { createLogger } from '@eventhub/logger';
import { createRequireAuth, publicKeyFromEnv } from '@eventhub/auth-middleware';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';
import { redis } from './redis.js';
import { createRateLimiter } from './ratelimit.js';
import { spaceRoutes } from './routes/spaces.js';
import { postRoutes } from './routes/posts.js';
import { createFeedConsumer, feedMetrics } from './feed/consumer.js';

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
  const [database, cache] = await Promise.all([
    prisma.$queryRaw`SELECT 1`.then(
      () => true,
      () => false,
    ),
    redis.ping().then(
      () => true,
      () => false,
    ),
  ]);
  /**
   * REDIS IS REPORTED, NOT REQUIRED.
   *
   * Without it the membership cache falls through to Postgres and the rate limiter
   * opens: slower and less bounded, but every route still answers correctly. Refusing
   * traffic would turn a cache outage into an outage, which is the wrong trade for the
   * thing the cache is for.
   */
  res.status(database ? 200 : 503).json({ ready: database, database, cache });
});

/**
 * The fan-out consumer's counters, and its lag in offsets.
 *
 * Lag is asked for on request and bounded, so a broker that has stopped answering
 * makes this slow rather than making it hang — the same shape Notification and
 * Analytics settled on. There is deliberately no "seconds behind" derived from the
 * last message's timestamp: an idle topic and a dead consumer look identical by that
 * measure, which Ticketing shipped once and had to take back out.
 */
let lagCache: { at: number; value: unknown } | null = null;
app.get('/metrics', async (_req, res) => {
  if (feed && (lagCache === null || Date.now() - lagCache.at > 5_000)) {
    const value = await Promise.race([
      feed.lag(),
      new Promise((resolve) => setTimeout(() => resolve({ error: 'timed out reading offsets' }), 2_000)),
    ]).catch((err: unknown) => ({ error: err instanceof Error ? err.message : String(err) }));
    lagCache = { at: Date.now(), value };
  }
  res.json({ feed: { ...feedMetrics(), lag: lagCache?.value ?? null } });
});

/**
 * Registered AFTER the routes, in main(), because Express matches middleware in order:
 * an error handler mounted before the routes never sees their errors. It sat here at
 * first, and a handler that threw took the process down instead of answering 500.
 */
const errorHandler: express.ErrorRequestHandler = (err, _req, res, _next) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
};

let feed: ReturnType<typeof createFeedConsumer> | null = null;

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  // The consumer first: leaving the group deliberately, rather than letting the
  // broker time the member out, means the partitions are reassigned in seconds
  // instead of after session.timeout.ms.
  await feed?.stop().catch(() => {});
  await prisma.$disconnect().catch(() => {});
  await redis.quit().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main(): Promise<void> {
  // Refuse to start rather than run with tenant isolation silently disabled.
  await assertRuntimeRoleCannotBypassRls();

  // The PUBLIC half only: this service verifies tokens and mints none.
  const requireAuth = createRequireAuth({
    publicKey: await publicKeyFromEnv(),
    issuer: env.JWT_ISSUER,
    audience: env.JWT_AUDIENCE,
  });
  const limiter = createRateLimiter(logger);

  // Mounted behind requireAuth as a group, so a route added later cannot be reachable
  // unauthenticated by forgetting one middleware argument.
  app.use(
    requireAuth,
    spaceRoutes(logger, limiter, {
      space: env.RATE_LIMIT_SPACE_PER_MINUTE,
      join: env.RATE_LIMIT_JOIN_PER_MINUTE,
    }),
  );
  app.use(
    requireAuth,
    postRoutes(logger, limiter, {
      post: env.RATE_LIMIT_POST_PER_MINUTE,
      comment: env.RATE_LIMIT_COMMENT_PER_MINUTE,
      reaction: env.RATE_LIMIT_REACTION_PER_MINUTE,
    }),
  );

  app.use(errorHandler);

  /**
   * The consumer runs in this process, as Search's indexer does.
   *
   * It could be its own container, and if fan-out ever needs to scale separately from
   * the API that is the change to make. It does not yet: the work is one INSERT per
   * post, measured at 23 ms for a thousand members, and a second container is a second
   * thing to deploy, watch and get the credentials right for.
   */
  if (!env.FEED_CONSUMER_ENABLED) {
    logger.warn('FEED_CONSUMER_ENABLED is false: this process serves requests but no post will reach anyone\'s feed');
    app.listen(env.PORT, () => logger.info(`community-service listening on port ${env.PORT}`));
    return;
  }

  feed = createFeedConsumer({
    brokers: env.KAFKA_BROKERS.split(','),
    registryUrl: env.SCHEMA_REGISTRY_URL,
    groupId: env.FEED_CONSUMER_GROUP,
    startFrom: env.FEED_START_FROM,
    maxMembers: env.FEED_FANOUT_MAX_MEMBERS,
    logger,
  });
  /**
   * Started but NOT awaited into the listen, and RETRIED FOR EVER.
   *
   * A broker that is slow to answer must not stop this service from serving reads,
   * which do not depend on it — and `.catch(log)` on a single attempt would leave the
   * consumer permanently dead after one unlucky restart, with every route still
   * answering 200. Search's indexer settled on exactly this loop; once kafkajs is
   * running it handles its own reconnects.
   */
  void (async () => {
    for (let attempt = 1; ; attempt++) {
      try {
        await feed.start();
        return;
      } catch (err) {
        const waitMs = Math.min(30_000, 1_000 * 2 ** attempt);
        logger.error({ err, attempt, waitMs }, 'the feed consumer failed to start; retrying');
        await feed.stop().catch(() => {});
        await new Promise((r) => setTimeout(r, waitMs));
      }
    }
  })();

  app.listen(env.PORT, () => {
    logger.info(`community-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'community-service failed to start');
  process.exit(1);
});
