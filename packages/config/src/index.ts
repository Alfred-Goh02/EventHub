import { z } from 'zod';

// Where emailed links point, per tenant. In a module of its own; see the note there.
export * from './app-url.js';

/**
 * Exported so a service can build on it, rather than copying it.
 *
 * DATABASE_URL stays REQUIRED here, and the Search service — which owns no
 * database at all — omits it explicitly:
 *
 *   baseEnvSchema.omit({ DATABASE_URL: true }).extend({ ... })
 *
 * The obvious alternative was making it `.optional()` in the base. That would
 * have been one character and would have silently weakened Auth, Event,
 * Ticketing and Live all at once: every one of them calls loadBaseEnv() and
 * would have kept booting with the variable missing, right up until the first
 * query. Omitting is the opposite — it is loud, it is per-service, and a
 * service that omits a variable it actually needs fails immediately.
 */
export const baseEnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  PORT: z.coerce.number().int().positive(),
  DATABASE_URL: z.string().url(),
  LOG_LEVEL: z
    .enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace'])
    .default('info'),
});

export type BaseEnv = z.infer<typeof baseEnvSchema>;

export function loadBaseEnv(): BaseEnv {
  const parsed = baseEnvSchema.safeParse(process.env);

  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `  ${issue.path.join('.')}: ${issue.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }

  return parsed.data;
}

/**
 * Validates any schema against process.env, reporting every failure at once.
 *
 * Same contract as loadBaseEnv — throw at startup with a readable message, not
 * an undefined that surfaces as a connection error on the first request — but
 * for a service whose configuration is not the base shape. Reporting all issues
 * together rather than the first matters when a fresh checkout is missing four
 * variables: one round trip instead of four.
 */
export function loadEnv<S extends z.ZodType>(schema: S): z.infer<S> {
  const parsed = schema.safeParse(process.env);

  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((issue) => `  ${issue.path.join('.')}: ${issue.message}`)
      .join('\n');
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }

  return parsed.data;
}
