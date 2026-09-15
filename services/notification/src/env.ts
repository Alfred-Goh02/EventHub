import { z } from 'zod';
import { loadBaseEnv } from '@eventhub/config';

/**
 * Notification's own configuration, on top of the shared base: PORT, NODE_ENV,
 * LOG_LEVEL and DATABASE_URL (the migration owner) come from there.
 *
 * Only what this part of the service reads. The Kafka, schema-registry, SMTP and
 * key settings arrive with the code that uses them, so no variable is required
 * here that nothing reads — the mistake that broke Search's scripts twice.
 */
const notificationEnvSchema = z.object({
  /** The role that owns nothing, so RLS policies actually apply to it. */
  APP_DATABASE_URL: z.string().url(),
});

const base = loadBaseEnv();
const parsed = notificationEnvSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid notification-service environment:\n${issues}`);
}

export const env = Object.freeze({ ...base, ...parsed.data });
