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

  /**
   * How long a DELIVERED outbox row is kept.
   *
   * Nothing pruned this table before, in either database, and the relay was
   * granted DELETE precisely so that something could -- the migration comment
   * says "DELETE is granted because nothing else can prune this table". The
   * grant existed and the code never did.
   *
   * A year is far longer than anything needs a delivered row, and that is the
   * point: one number, easy to remember, obviously safe. Dead-lettered rows
   * are exempt entirely -- see prune.ts.
   */
  RELAY_RETENTION_DAYS: z.coerce.number().int().positive().default(365),

  /** Rows per DELETE. Bounded so no statement holds locks over a huge range. */
  RELAY_PRUNE_BATCH_SIZE: z.coerce.number().int().positive().default(1000),

  /**
   * Batches per pass. Bounds the FIRST run, which on a table nobody has ever
   * pruned may have years of rows to get through -- without this the relay
   * would stop draining until it finished.
   */
  RELAY_PRUNE_MAX_BATCHES: z.coerce.number().int().positive().default(20),

  /**
   * How often to prune. Deliberately not once per drain: the drain loops as
   * fast as it can while there is a backlog, and a prune on that cadence would
   * spend the relay on maintenance exactly when it is busiest.
   */
  RELAY_PRUNE_INTERVAL_MS: z.coerce.number().int().positive().default(3_600_000),

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
  /**
   * A business table that EXISTS in this relay's database and that the relay
   * must NOT be able to read. Asserted at boot.
   *
   * Configurable because it differs per database, and because the check is
   * meaningless if the named table is absent - it would then pass on "relation
   * does not exist" without testing any privilege at all.
   */
  RELAY_FORBIDDEN_TABLE: z.string().min(1).default('Event'),

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
