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

  /**
   * Whether this process runs the fan-out consumer. On everywhere that matters.
   *
   * It exists because three verifiers spawn this service to test the HTTP side and do
   * not want a Kafka consumer as a side effect: on the shared group they would consume
   * and commit other runs' messages, and from `earliest` they would replay a week of
   * the topic to do it. Neither of them imports kafkajs, so neither can clean up a
   * throwaway group afterwards.
   *
   * Defaulting to true is the point — a deployment has to opt OUT, so nobody turns the
   * feed off by forgetting a variable.
   */
  FEED_CONSUMER_ENABLED: z
    .enum(['true', 'false'])
    .default('true')
    .transform((v) => v === 'true'),

  /** Where community.feed is read from, and the schema its messages are encoded against. */
  KAFKA_BROKERS: z.string().min(1),
  SCHEMA_REGISTRY_URL: z.string().url(),
  FEED_CONSUMER_GROUP: z.string().min(1).default('community-feed'),
  /**
   * earliest, so a consumer group created after the topic already has messages builds
   * the feed from what is there rather than from whatever arrives next.
   */
  FEED_START_FROM: z.enum(['earliest', 'latest']).default('earliest'),

  /**
   * The largest space whose posts are fanned out to every member on write. Past it a
   * space is read at query time instead.
   *
   * Not a latency limit — fan-out happens in the consumer, after the post is committed
   * and answered. Measured here, one insert covering 100 000 members takes under a
   * second. It is a write-amplification limit: at that size a hundred posts a day is
   * ten million rows a day from one space.
   */
  FEED_FANOUT_MAX_MEMBERS: z.coerce.number().int().positive().default(5000),
});

const base = loadBaseEnv();
const parsed = communityEnvSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid community-service environment:\n${issues}`);
}

export const env = Object.freeze({ ...base, ...parsed.data });
