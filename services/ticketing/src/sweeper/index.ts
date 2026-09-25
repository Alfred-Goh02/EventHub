import pg from 'pg';
import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { assertSweeperIsLeastPrivilege, sweepOnce } from './sweep.js';
import { attachPoolErrorHandler } from '../pool.js';
import { pruneOnce } from './prune.js';

/**
 * The hold sweeper: a separate process with its own database credential.
 *
 * It is not part of the HTTP service because it does something no request-scoped
 * code should be able to do — operate across every tenant. A TTL sweep selects
 * by expiry, and expiry has no tenant, so it cannot use the tenant-scoped policy
 * the app role uses. Giving it a separate role with its own role-targeted
 * policies keeps that reach explicit and bounded, rather than handing the API
 * process a credential that can see everything.
 *
 * Run once and exit:   node dist/sweeper/index.js --once
 * Run continuously:    node dist/sweeper/index.js
 */

const schema = z.object({
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  SWEEPER_DATABASE_URL: z.string().url(),
  SWEEPER_BATCH_SIZE: z.coerce.number().int().positive().max(1000).default(100),
  SWEEPER_INTERVAL_MS: z.coerce.number().int().positive().default(5000),

  /**
   * How long ProcessedMessage and IdempotencyKey rows are kept.
   *
   * Nothing pruned either table and no role could: both carry a
   * tenant_isolation policy with no TO clause, so it binds even the owner
   * under FORCE row security. A maintenance script written the obvious way
   * deleted 0 of 29 rows and exited 0.
   *
   * A year is far beyond what either needs, and that is deliberate. For
   * IdempotencyKey the window is a client retry horizon, not a size budget,
   * and the asymmetry is stark: keeping a key too long costs one small row,
   * deleting one too early costs a duplicate hold on scarce inventory.
   */
  SWEEPER_RETENTION_DAYS: z.coerce.number().int().positive().default(365),

  /** Rows per DELETE. Bounds what a single statement locks. */
  SWEEPER_PRUNE_BATCH_SIZE: z.coerce.number().int().positive().default(1000),

  /**
   * Batches per table per pass. Bounds the FIRST run, which on tables nobody
   * has ever pruned may have years to work through -- without it the sweeper
   * would stop expiring holds until it finished.
   */
  SWEEPER_PRUNE_MAX_BATCHES: z.coerce.number().int().positive().default(20),

  /**
   * How often to prune. Deliberately not once per sweep: the sweep loops
   * immediately whenever it fills a batch, and retention on that cadence would
   * spend the sweeper on housekeeping exactly when holds are expiring fastest.
   */
  SWEEPER_PRUNE_INTERVAL_MS: z.coerce.number().int().positive().default(3_600_000),
});

const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid sweeper environment:\n${issues}`);
}
const env = parsed.data;

const logger = createLogger('hold-sweeper');

const pool = attachPoolErrorHandler(
  new pg.Pool({
    connectionString: env.SWEEPER_DATABASE_URL,
    // Named so pg_stat_activity says which pool a connection belongs to. An
    // operator looking at a stuck backend should not have to guess, and the
    // resilience verifier uses it to find exactly the connection it means to
    // terminate rather than any connection on the same role.
    application_name: 'hold-sweeper',
    max: 2,
    connectionTimeoutMillis: 10_000,
    idleTimeoutMillis: 30_000,
  }),
  logger,
  'hold-sweeper',
);

const runOnce = process.argv.includes('--once');
let stopping = false;

/**
 * When retention last ran. Zero means "not yet", so the first pass happens
 * shortly after start -- on tables nobody has ever pruned, waiting an hour to
 * begin is the wrong default.
 */
let lastPruneAt = 0;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function loop(): Promise<void> {
  while (!stopping) {
    try {
      const result = await sweepOnce(pool, logger, { batchSize: env.SWEEPER_BATCH_SIZE });
      if (result.claimed > 0) logger.info(result, 'swept');

      /**
       * Retention runs on its OWN clock, between sweeps.
       *
       * A failure here is logged and swallowed. A sweeper that stops returning
       * expired seats because it could not tidy up has turned a disk-space
       * problem into unavailable inventory, which is the wrong direction --
       * the same trade the catch below is making for the sweep itself.
       */
      if (Date.now() - lastPruneAt >= env.SWEEPER_PRUNE_INTERVAL_MS) {
        lastPruneAt = Date.now();
        try {
          const pruned = await pruneOnce(pool, logger, {
            olderThanDays: env.SWEEPER_RETENTION_DAYS,
            batchSize: env.SWEEPER_PRUNE_BATCH_SIZE,
            maxBatches: env.SWEEPER_PRUNE_MAX_BATCHES,
          });
          if (pruned.moreToDo) {
            logger.info(pruned, 'retention hit its batch ceiling; more to prune next pass');
          }
        } catch (err) {
          logger.error({ err }, 'retention pass failed; hold expiry is unaffected');
        }
      }

      // A full batch means there is probably more waiting, so go straight round
      // again rather than sleeping while holds sit expired. An empty sweep waits.
      if (result.claimed < env.SWEEPER_BATCH_SIZE) {
        await sleep(env.SWEEPER_INTERVAL_MS);
      }
    } catch (err) {
      // The loop must survive. Seats stay held until it recovers, which is the
      // safe direction to fail: nobody is oversold, some inventory is briefly
      // unavailable.
      logger.error({ err }, 'sweep failed, retrying');
      await sleep(env.SWEEPER_INTERVAL_MS);
    }
  }
}

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  stopping = true;
  await pool.end().catch(() => {});
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main(): Promise<void> {
  await assertSweeperIsLeastPrivilege(pool, logger);

  if (runOnce) {
    const result = await sweepOnce(pool, logger, { batchSize: env.SWEEPER_BATCH_SIZE });
    logger.info(result, 'swept once');
    await pool.end();
    process.exit(0);
  }

  logger.info({ batchSize: env.SWEEPER_BATCH_SIZE, intervalMs: env.SWEEPER_INTERVAL_MS }, 'hold sweeper started');
  await loop();
}

main().catch((err) => {
  logger.error({ err }, 'sweeper failed to start');
  process.exit(1);
});
