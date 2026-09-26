import pg from 'pg';
import type { Logger } from '@eventhub/logger';
import { attachPoolErrorHandler } from '../pool.js';
import { assertDigestIsLeastPrivilege, flushOnce, type FlushDeps } from './flush.js';

/**
 * The hourly digest, on its own clock, inside the service process, on its own
 * credential — for the reasons the sweeper gives (see sweeper/index.ts). It refuses to
 * start on a credential that can do more than find who is waiting.
 *
 * Each pass is flushOnce (flush.ts); `digest:once` runs one by hand.
 */
export function createDigestFlusher(
  opts: FlushDeps & {
    databaseUrl: string;
    intervalMs: number;
    batchSize: number;
  },
) {
  const pool = attachPoolErrorHandler(
    new pg.Pool({
      connectionString: opts.databaseUrl,
      application_name: 'notification-digest',
      max: 2,
      connectionTimeoutMillis: 10_000,
      idleTimeoutMillis: 30_000,
    }),
    opts.logger,
    'notification-digest',
  );

  const counters = { passes: 0, failures: 0, recipientFailures: 0, digestsSent: 0, itemsSettled: 0, droppedOptedOut: 0, droppedNoAddress: 0, deferred: 0 };
  let last: { at: number; waiting: number } | null = null;
  let timer: NodeJS.Timeout | null = null;
  let stopping = false;

  async function pass(): Promise<void> {
    try {
      const r = await flushOnce(pool, opts, { batchSize: opts.batchSize, stopping: () => stopping });
      counters.digestsSent += r.digestsSent;
      counters.itemsSettled += r.itemsSettled;
      counters.droppedOptedOut += r.droppedOptedOut;
      counters.droppedNoAddress += r.droppedNoAddress;
      counters.deferred += r.deferred;
      counters.recipientFailures += r.recipientFailures;
      counters.passes += 1;
      last = { at: Date.now(), waiting: r.waiting };
    } catch (err) {
      counters.failures += 1;
      opts.logger.error({ err }, 'digest pass failed; retrying next pass');
    }
  }

  function schedule(): void {
    if (stopping) return;
    timer = setTimeout(() => {
      void pass().finally(schedule);
    }, opts.intervalMs);
  }

  return {
    /** Checks the credential, then flushes every interval. The first pass waits an interval too: held means held for the hour. */
    async start(): Promise<void> {
      await assertDigestIsLeastPrivilege(pool, opts.logger);
      schedule();
    },

    metrics(): Record<string, number | null> {
      return {
        passesTotal: counters.passes,
        failuresTotal: counters.failures,
        recipientFailuresTotal: counters.recipientFailures,
        digestsSentTotal: counters.digestsSent,
        itemsSettledTotal: counters.itemsSettled,
        droppedOptedOutTotal: counters.droppedOptedOut,
        droppedNoAddressTotal: counters.droppedNoAddress,
        deferredTotal: counters.deferred,
        /** People with replies waiting at the last pass, up to the batch size. */
        waitingAtLastPass: last?.waiting ?? null,
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
