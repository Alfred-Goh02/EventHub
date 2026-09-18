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
});

const base = loadBaseEnv();
const parsed = communityEnvSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid community-service environment:\n${issues}`);
}

export const env = Object.freeze({ ...base, ...parsed.data });
