import pg from 'pg';
import type { Logger } from '@eventhub/logger';
import { attachPoolErrorHandler } from '../pool.js';
import { assertSweeperIsLeastPrivilege, sweepOnce } from './sweep.js';

/**
 * The sweep on its own clock, inside the service process, on its own credential.
 *
 * IN THE PROCESS, not a separate entrypoint nobody starts. The work is small and
 * continuous, and a sweeper that exists only as a script is a sweeper that runs when
 * somebody remembers. `sweep:once` is still there for a person who wants one pass and
 * a number back.
 *
 * ITS OWN POOL, so the reach of a cross-tenant sweep is never the API's credential.
 * It refuses to start on a credential that can do more than sweep.
 */
export function createSweeper(opts: {
  databaseUrl: string;
  intervalMs: number;
  stuckAfterMs: number;
  batchSize: number;
  logger: Logger;
}) {
  const pool = attachPoolErrorHandler(
    new pg.Pool({
      connectionString: opts.databaseUrl,
      // Named, so pg_stat_activity says which pool a connection belongs to rather than
      // leaving an operator to guess at a stuck backend.
      application_name: 'notification-sweeper',
      max: 2,
      connectionTimeoutMillis: 10_000,
      idleTimeoutMillis: 30_000,
    }),
    opts.logger,
    'notification-sweeper',
  );

  const counters = { abandoned: 0, passes: 0, failures: 0 };
  let last: { at: number; oldestPendingMs: number | null } | null = null;
  let timer: NodeJS.Timeout | null = null;
  let stopping = false;

  async function pass(): Promise<void> {
    try {
      const result = await sweepOnce(pool, opts.logger, { stuckAfterMs: opts.stuckAfterMs, batchSize: opts.batchSize });
      counters.passes += 1;
      counters.abandoned += result.swept;
      last = { at: Date.now(), oldestPendingMs: result.oldestPendingMs };
      // Ids are not logged: an abandoned delivery is read from the table, and the
      // count is what an operator watches.
      if (result.swept > 0) opts.logger.warn({ swept: result.swept, more: result.more }, 'deliveries abandoned: claimed but never completed');
    } catch (err) {
      // The loop must survive. A sweep that stops leaves rows PENDING, which is the
      // safe direction: nothing is sent, and the count an operator watches grows.
      counters.failures += 1;
      opts.logger.error({ err }, 'sweep failed; retrying next pass');
    }
  }

  function schedule(): void {
    if (stopping) return;
    timer = setTimeout(() => {
      void pass().finally(schedule);
    }, opts.intervalMs);
  }

  return {
    /** Checks the credential, sweeps once, then keeps sweeping. */
    async start(): Promise<void> {
      await assertSweeperIsLeastPrivilege(pool, opts.logger);
      await pass();
      schedule();
    },

    metrics(): Record<string, number | null> {
      return {
        abandonedTotal: counters.abandoned,
        passesTotal: counters.passes,
        failuresTotal: counters.failures,
        /** How old the oldest unfinished delivery was at the last pass. The number to alarm on. */
        oldestPendingMs: last?.oldestPendingMs ?? null,
        lastPassAgeMs: last ? Date.now() - last.at : null,
      };
    },

    async stop(): Promise<void> {
      stopping = true;
      if (timer) clearTimeout(timer);
      await pool.end().catch(() => {});
    },
  };
}
