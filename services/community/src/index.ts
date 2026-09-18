import express from 'express';
import { createLogger } from '@eventhub/logger';
import { createRequireAuth, publicKeyFromEnv } from '@eventhub/auth-middleware';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';
import { redis } from './redis.js';
import { createRateLimiter } from './ratelimit.js';
import { spaceRoutes } from './routes/spaces.js';
import { postRoutes } from './routes/posts.js';

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
 * Registered AFTER the routes, in main(), because Express matches middleware in order:
 * an error handler mounted before the routes never sees their errors. It sat here at
 * first, and a handler that threw took the process down instead of answering 500.
 */
const errorHandler: express.ErrorRequestHandler = (err, _req, res, _next) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
};

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
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

  app.listen(env.PORT, () => {
    logger.info(`community-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'community-service failed to start');
  process.exit(1);
});
