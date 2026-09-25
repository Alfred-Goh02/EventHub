import { z } from 'zod';
import { loadBaseEnv } from '@eventhub/config';

/**
 * Notification's own configuration, on top of the shared base: PORT, NODE_ENV,
 * LOG_LEVEL and DATABASE_URL (the migration owner) come from there.
 *
 * Auth's public key is read by @eventhub/auth-middleware's publicKeyFromEnv, from
 * AUTH_PUBLIC_KEY_B64 or AUTH_PUBLIC_KEY_FILE, as every other service reads it.
 * Notification's own private key is a file beside the service (keys/private.pem),
 * as Auth's signing key is.
 */
const notificationEnvSchema = z.object({
  /** The role that owns nothing, so RLS policies actually apply to it. */
  APP_DATABASE_URL: z.string().url(),

  KAFKA_BROKERS: z.string().min(1),
  SCHEMA_REGISTRY_URL: z.string().url(),
  /** Every sealed request's iss must be this. Only Auth's requests are acted on. */
  JWT_ISSUER: z.string().min(1),
  /** One group per deployment. Verifiers run their own, so they never take its partitions. */
  KAFKA_CONSUMER_GROUP: z.string().min(1).default('notification-dispatch'),
  /**
   * The group that keeps the contact-card copy, from auth.user-contact. Its own, so a
   * backlog of cards never waits behind email requests or the reverse. It always
   * starts from the earliest offset: the topic is compacted precisely so a new group
   * can read every user's card, and there is deliberately no setting for that.
   */
  CONTACTS_CONSUMER_GROUP: z.string().min(1).default('notification-contacts'),
  /**
   * Where a group with no committed offset starts. earliest, so a request queued
   * before the service first ran is still delivered if its link has not expired.
   */
  DISPATCH_START_FROM: z.enum(['earliest', 'latest']).default('earliest'),
  /**
   * How long each retry tier waits, in milliseconds: one per tier topic, in order.
   * The defaults are spent inside the thirty minutes a reset link lives. A verifier
   * shortens them to seconds.
   */
  DISPATCH_RETRY_DELAYS_MS: z
    .string()
    .default('30000,300000,900000')
    .transform((list) => list.split(',').map((part) => Number(part.trim())))
    .pipe(z.array(z.number().int().positive()).length(3)),

  /**
   * How long a hard bounce keeps an address suppressed, in days.
   *
   * NOT FOREVER, because the reply that suppresses comes from the relay this service
   * sends through, not necessarily from the recipient's own mail server. A smarthost
   * with a stale recipient table, or a half-finished MX migration, answers 5.1.1 for
   * a mailbox that exists — and a suppression with no end would lock those users out
   * of password resets permanently. Each new hard bounce extends the window, so an
   * address that really is gone costs one refused send per window, not one per reset.
   */
  SUPPRESSION_TTL_DAYS: z.coerce.number().int().positive().default(30),

  /**
   * The sweeper's own credential: SELECT and a three-column UPDATE on the sent-log,
   * across every tenant. Separate from APP_DATABASE_URL because "which deliveries
   * never finished" has no tenant in it, and the process that sends must not gain
   * that reach. It refuses to start if this role can do more than sweep.
   */
  SWEEPER_DATABASE_URL: z.string().url(),
  /** How long a delivery may sit PENDING before it is called abandoned. */
  SWEEP_STUCK_AFTER_MS: z.coerce.number().int().positive().default(3_600_000),
  /** How often to sweep. */
  SWEEP_INTERVAL_MS: z.coerce.number().int().positive().default(60_000),
  /** Rows per pass, which bounds what one statement locks. */
  SWEEP_BATCH_SIZE: z.coerce.number().int().positive().max(10_000).default(500),

  SMTP_HOST: z.string().min(1),
  SMTP_PORT: z.coerce.number().int().positive(),
  EMAIL_FROM: z.string().email(),
});

const base = loadBaseEnv();
const parsed = notificationEnvSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid notification-service environment:\n${issues}`);
}

export const env = Object.freeze({ ...base, ...parsed.data });
