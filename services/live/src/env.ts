import { z } from 'zod';
import { loadBaseEnv } from '@eventhub/config';

/**
 * Live's own configuration, on top of the shared base.
 *
 * Fail-fast at startup is the convention here, and it earns its place in this
 * service more than in the others: a missing REDIS_URL does not break anything
 * until the SECOND client joins a room on a DIFFERENT pod, which is exactly the
 * case that never happens in a quick manual test. A service that starts and is
 * quietly incapable of cross-pod fan-out is worse than one that refuses to boot.
 */
const liveEnvSchema = z.object({
  /** The role that owns nothing, so RLS policies actually apply to it. */
  APP_DATABASE_URL: z.string().url(),

  /**
   * 127.0.0.1, not localhost, in local files.
   *
   * Node resolves localhost to ::1 before 127.0.0.1, and Docker Desktop's port
   * mapping does not always behave identically on IPv6 — which produces an
   * intermittent ECONNREFUSED that reads as a Redis fault and is not one.
   */
  REDIS_URL: z.string().url(),

  KAFKA_BROKERS: z.string().min(1),

  /**
   * WebSocket handshakes are NOT subject to CORS. The browser sends Origin and
   * enforces nothing, so checking it is ours to do — cheap, and the only thing
   * standing between a cookie-less socket and a page on another origin opening
   * one. Suffix rather than exact match so subdomains work without a list.
   */
  ALLOWED_ORIGIN_SUFFIX: z.string().min(1),
});

const base = loadBaseEnv();
const parsed = liveEnvSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid live-service environment:\n${issues}`);
}

/**
 * Auth's public key is deliberately NOT validated here.
 *
 * It arrives as one of two variables — AUTH_PUBLIC_KEY_B64 or
 * AUTH_PUBLIC_KEY_FILE — and exactly one must be set, which a zod object
 * expresses badly. `publicKeyFromEnv` in @eventhub/auth-middleware already
 * refuses both-set and neither-set with a message that says which, and it runs
 * at import time in jwt.ts, so the failure is still at startup.
 */
export const env = Object.freeze({ ...base, ...parsed.data });
