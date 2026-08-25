import { z } from 'zod';

/**
 * The relay's own environment schema, deliberately NOT @eventhub/config's
 * loadBaseEnv.
 *
 * loadBaseEnv requires PORT and DATABASE_URL. The relay listens on a port only
 * for metrics, and — more importantly — across this repository `DATABASE_URL`
 * means "the migration owner" and `APP_DATABASE_URL` means "the restricted
 * runtime role". The relay uses a third credential that is neither, so reusing
 * either name would be actively misleading to the next person reading it.
 */
const schema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),

  /** The eventhub_event_relay credential. Owns nothing, reads by delivery state. */
  RELAY_DATABASE_URL: z.string().url(),

  KAFKA_BROKERS: z.string().default('localhost:9092'),
  SCHEMA_REGISTRY_URL: z.string().url().default('http://localhost:8081'),

  /** Backlog and oldest-undelivered age are served from here. */
  RELAY_METRICS_PORT: z.coerce.number().int().positive().default(3010),

  /**
   * Rows claimed per drain. Every row in a batch is held under a row lock until
   * the transaction commits, so a larger batch means fewer round trips but a
   * longer lock hold — and a longer window in which a crash redelivers more.
   */
  RELAY_BATCH_SIZE: z.coerce.number().int().positive().max(1000).default(100),

  /** Idle poll interval. A drained relay sleeps; a busy one loops immediately. */
  RELAY_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(1000),

  /**
   * Attempts before a row is dead-lettered.
   *
   * Without a ceiling, one unserialisable row is re-selected on every poll,
   * fails, and is re-selected forever — and because the drain is ordered by seq,
   * nothing behind it is ever published. The only symptom is a growing backlog.
   */
  RELAY_MAX_ATTEMPTS: z.coerce.number().int().positive().default(5),
});

export type RelayEnv = z.infer<typeof schema>;

export function loadRelayEnv(): RelayEnv {
  const parsed = schema.safeParse(process.env);

  if (!parsed.success) {
    const issues = parsed.error.issues
      .map((i) => `  ${i.path.join('.')}: ${i.message}`)
      .join('\n');
    throw new Error(`Invalid relay environment:\n${issues}`);
  }

  return parsed.data;
}
