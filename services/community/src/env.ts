import { z } from 'zod';
import { loadBaseEnv } from '@eventhub/config';

/**
 * Community's own configuration, on top of the shared base: PORT, NODE_ENV, LOG_LEVEL
 * and DATABASE_URL (the migration owner) come from there.
 *
 * Auth's public key is read by @eventhub/auth-middleware's publicKeyFromEnv, from
 * AUTH_PUBLIC_KEY_B64 or AUTH_PUBLIC_KEY_FILE, as every other verifying service reads
 * it. This service mints nothing.
 */
const communityEnvSchema = z.object({
  /** The role that owns nothing, so RLS policies actually apply to it. */
  APP_DATABASE_URL: z.string().url(),

  /** A token is acted on only if Auth issued it, for this issuer and audience. */
  JWT_ISSUER: z.string().min(1),
  JWT_AUDIENCE: z.string().min(1),

  /** Where the membership cache and the rate limiter's buckets live. */
  REDIS_URL: z.string().url(),

  /**
   * How long a membership lookup is cached.
   *
   * Space roles are mutable authorisation, so they cannot live in a 15-minute access
   * token: demoting a moderator has to take effect now. The cache is invalidated
   * explicitly whenever a role or status changes, and this is only the ceiling for a
   * change made some other way — straight in psql, say.
   */
  MEMBERSHIP_CACHE_TTL_S: z.coerce.number().int().positive().default(60),

  /**
   * Per user, per action, per minute. Community is the one service where an ordinary
   * user creates unbounded rows, and a per-IP limit at a gateway cannot express "five
   * posts but sixty reactions" because it does not know what the endpoint means.
   */
  /**
   * Creating a space was the one write here that nothing metered, in the service this
   * comment calls out for unbounded rows. It is also the heaviest: a space is a
   * container other people join, and each one costs two rows and a name in a unique
   * index. Lower than posting for that reason.
   */
  RATE_LIMIT_SPACE_PER_MINUTE: z.coerce.number().int().positive().default(3),
  RATE_LIMIT_JOIN_PER_MINUTE: z.coerce.number().int().positive().default(10),
  RATE_LIMIT_POST_PER_MINUTE: z.coerce.number().int().positive().default(5),
  RATE_LIMIT_COMMENT_PER_MINUTE: z.coerce.number().int().positive().default(30),
  RATE_LIMIT_REACTION_PER_MINUTE: z.coerce.number().int().positive().default(60),
  RATE_LIMIT_REPORT_PER_MINUTE: z.coerce.number().int().positive().default(10),

  /**
   * The most comments one thread request returns. Configurable so the verifier can set
   * it low enough to reach the boundary in a few requests: a cap that can only be hit
   * by creating five hundred rows is a cap nothing ever tests.
   */
  THREAD_PAGE_LIMIT: z.coerce.number().int().positive().default(500),
});

const base = loadBaseEnv();
const parsed = communityEnvSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid community-service environment:\n${issues}`);
}

export const env = Object.freeze({ ...base, ...parsed.data });
