import pg from 'pg';
import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { FakePaymentProvider, withTimeout } from '../payments/provider.js';
import { reconcileOnce } from './reconcile.js';

/**
 * The payment reconciler: a separate process, on the sweeper's credential.
 *
 * Separate from the API for the same reason the sweeper is — it operates across
 * every tenant. "Which payments never settled" is a question with no tenant in
 * it, so it cannot use the tenant-scoped policy the app role uses, and its reach
 * is bounded by grants instead: SELECT plus column-level UPDATE, and no ability
 * to create a reservation or a payment.
 *
 * Separate from the SWEEPER, though it shares that credential, because the two
 * do opposite things to the same rows. The sweeper releases seats whose hold
 * lapsed; this confirms seats whose payment turns out to have succeeded. Running
 * them in one loop would mean a slow provider lookup delaying every expiry, and
 * it would blur the one distinction that matters here: the sweeper never talks
 * to anything off-box, and this cannot avoid it.
 *
 * Run once and exit:   node dist/reconciler/index.js --once
 * Run continuously:    node dist/reconciler/index.js
 */

const schema = z.object({
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  SWEEPER_DATABASE_URL: z.string().url(),
  RECONCILE_BATCH_SIZE: z.coerce.number().int().positive().max(500).default(50),
  RECONCILE_INTERVAL_MS: z.coerce.number().int().positive().default(30_000),
  /**
   * How long an attempt must have been unsettled before this job touches it.
   *
   * Must be comfortably longer than the pay endpoint's provider timeout. Set it
   * too low and the reconciler looks up a charge that is still in flight, is
   * told "no such charge", and releases the hold of a customer who is mid
   * checkout — a job meant to repair state would become the thing breaking it.
   */
  RECONCILE_GRACE_MS: z.coerce.number().int().positive().default(60_000),
  PAYMENT_TIMEOUT_MS: z.coerce.number().int().positive().default(8000),
  PAYMENT_FAKE_LATENCY_MS: z.coerce.number().int().nonnegative().default(0),
});

const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid reconciler environment:\n${issues}`);
}
const env = parsed.data;

/**
 * The grace period MUST outlast the provider timeout, and nothing enforced it.
 *
 * The two values live in different processes, parsed from different schemas —
 * the API reads PAYMENT_TIMEOUT_MS, this job reads both — so the relationship
 * between them existed only in a comment, and a comment cannot fail a deploy.
 *
 * Set grace below the timeout and the reconciler starts examining charges that
 * are STILL IN FLIGHT. It asks the provider, is told "no such charge" because
 * the charge has not landed yet, and releases the seats of a customer who is
 * mid-checkout. A job written to repair state becomes the thing breaking it, and
 * it does so most aggressively exactly when the provider is slow — which is
 * precisely when there are unsettled attempts for it to find.
 *
 * The 2x margin is not arithmetic for its own sake: a charge can begin just
 * before the endpoint's own timeout expires, so the real worst case is close to
 * one full timeout after startedAt, and the reconciler should not be arriving at
 * that moment either.
 */
if (env.RECONCILE_GRACE_MS <= env.PAYMENT_TIMEOUT_MS * 2) {
  throw new Error(
    `refusing to start: RECONCILE_GRACE_MS (${env.RECONCILE_GRACE_MS}ms) must be more than twice ` +
      `PAYMENT_TIMEOUT_MS (${env.PAYMENT_TIMEOUT_MS}ms). Below that, this job looks up charges that ` +
      'are still in flight, is told they do not exist, and releases the seats of customers who are ' +
      'mid-checkout.',
  );
}

const logger = createLogger('payment-reconciler');

const pool = new pg.Pool({
  connectionString: env.SWEEPER_DATABASE_URL,
  max: 2,
  connectionTimeoutMillis: 10_000,
  idleTimeoutMillis: 30_000,
});

const provider = withTimeout(
  new FakePaymentProvider(env.PAYMENT_FAKE_LATENCY_MS),
  env.PAYMENT_TIMEOUT_MS,
);

const runOnce = process.argv.includes('--once');
let stopping = false;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Refuses to start on a credential that can bypass row security.
 *
 * Asserted rather than assumed, exactly as the sweeper and the relay do. An
 * over-privileged reconciler works perfectly and is simply far more dangerous:
 * it moves money-adjacent state across every tenant in the system.
 */
async function assertLeastPrivilege(): Promise<void> {
  const client = await pool.connect();
  try {
    const { rows } = await client.query<{ rolsuper: boolean; rolbypassrls: boolean }>(
      'SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user',
    );
    const role = rows[0];
    if (!role) throw new Error('could not read current_user from pg_roles');
    if (role.rolsuper || role.rolbypassrls) {
      throw new Error(
        'refusing to start: the reconciler connects as a role that bypasses row-level security ' +
          `(rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls})`,
      );
    }

    // It must be able to READ payments and never to CREATE one. A reconciler
    // that can invent a payment row can invent a confirmed reservation.
    let couldInsert = false;
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO "Payment" (id,"tenantId","reservationId","userId","amountCents","updatedAt")
         VALUES ('reconciler_privilege_probe','probe','probe','probe',0,now())`,
      );
      couldInsert = true;
    } catch {
      // Expected: permission denied for table Payment.
    } finally {
      await client.query('ROLLBACK').catch(() => {});
    }

    if (couldInsert) {
      throw new Error(
        'refusing to start: the reconciler credential can INSERT payments. It should hold only ' +
          'SELECT plus column-level UPDATE.',
      );
    }
  } finally {
    client.release();
  }
}

async function tick(): Promise<void> {
  const result = await reconcileOnce(pool, provider, logger, {
    batchSize: env.RECONCILE_BATCH_SIZE,
    graceMs: env.RECONCILE_GRACE_MS,
  });
  if (result.examined > 0) logger.info(result, 'reconciled');
}

async function loop(): Promise<void> {
  while (!stopping) {
    try {
      await tick();
    } catch (err) {
      // The loop must survive. Unsettled payments stay unsettled until it
      // recovers, which is the safe direction: seats remain held, nobody is
      // oversold and nobody is charged for nothing.
      logger.error({ err }, 'reconcile failed, retrying');
    }
    await sleep(env.RECONCILE_INTERVAL_MS);
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
  await assertLeastPrivilege();

  if (runOnce) {
    const result = await reconcileOnce(pool, provider, logger, {
      batchSize: env.RECONCILE_BATCH_SIZE,
      graceMs: env.RECONCILE_GRACE_MS,
    });
    logger.info(result, 'reconciled once');
    await pool.end();
    process.exit(0);
  }

  logger.info(
    {
      batchSize: env.RECONCILE_BATCH_SIZE,
      intervalMs: env.RECONCILE_INTERVAL_MS,
      graceMs: env.RECONCILE_GRACE_MS,
    },
    'payment reconciler started',
  );
  await loop();
}

main().catch((err) => {
  logger.error({ err }, 'reconciler failed to start');
  process.exit(1);
});
