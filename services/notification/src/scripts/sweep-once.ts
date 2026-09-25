import pg from 'pg';
import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { attachPoolErrorHandler } from '../pool.js';
import { assertSweeperIsLeastPrivilege, sweepOnce } from '../sweeper/sweep.js';

/**
 * One sweep, by hand, printing what it found.
 *
 *   pnpm --filter @eventhub/notification run sweep:once
 *
 * The service sweeps on its own clock; this is for a person who wants a pass now and
 * a number back — after a dead-letter incident, say, when the PENDING rows left
 * behind are known to be finished with.
 *
 * Reads SWEEPER_DATABASE_URL and the two knobs, and nothing else, for the reason
 * dlq-replay.ts gives: this needs no Kafka, no SMTP and no keys.
 */

const env = z
  .object({
    SWEEPER_DATABASE_URL: z.string().url(),
    SWEEP_STUCK_AFTER_MS: z.coerce.number().int().positive().default(3_600_000),
    SWEEP_BATCH_SIZE: z.coerce.number().int().positive().max(10_000).default(500),
  })
  .parse(process.env);

async function main(): Promise<void> {
  const logger = createLogger('notification-sweep-once');
  const pool = attachPoolErrorHandler(
    new pg.Pool({ connectionString: env.SWEEPER_DATABASE_URL, application_name: 'notification-sweep-once', max: 1 }),
    logger,
    'notification-sweep-once',
  );
  try {
    await assertSweeperIsLeastPrivilege(pool, logger);
    const result = await sweepOnce(pool, logger, { stuckAfterMs: env.SWEEP_STUCK_AFTER_MS, batchSize: env.SWEEP_BATCH_SIZE });
    console.log(
      `abandoned ${result.swept} deliver${result.swept === 1 ? 'y' : 'ies'} claimed more than ${env.SWEEP_STUCK_AFTER_MS} ms ago` +
        `${result.more ? ' (the batch filled, so run it again)' : ''}; oldest still pending: ` +
        `${result.oldestPendingMs === null ? 'none' : `${result.oldestPendingMs} ms`}`,
    );
  } finally {
    await pool.end().catch(() => {});
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
