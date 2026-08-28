import pg from 'pg';
import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { assertSweeperIsLeastPrivilege, sweepOnce } from './sweep.js';

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
});

const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid sweeper environment:\n${issues}`);
}
const env = parsed.data;

const logger = createLogger('hold-sweeper');

const pool = new pg.Pool({
  connectionString: env.SWEEPER_DATABASE_URL,
  max: 2,
  connectionTimeoutMillis: 10_000,
  idleTimeoutMillis: 30_000,
});

const runOnce = process.argv.includes('--once');
let stopping = false;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function loop(): Promise<void> {
  while (!stopping) {
    try {
      const result = await sweepOnce(pool, logger, { batchSize: env.SWEEPER_BATCH_SIZE });
      if (result.claimed > 0) logger.info(result, 'swept');

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
  await assertSweeperIsLeastPrivilege(pool);

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
