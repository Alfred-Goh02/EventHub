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
   * Where a group with no committed offset starts. earliest, so a request queued
   * before the service first ran is still delivered if its link has not expired.
   */
  DISPATCH_START_FROM: z.enum(['earliest', 'latest']).default('earliest'),

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
