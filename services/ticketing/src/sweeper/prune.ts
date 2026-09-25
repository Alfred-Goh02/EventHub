import type pg from 'pg';
import type { Logger } from '@eventhub/logger';
import { withClient } from '../pool.js';

/**
 * A prune that reported success while deleting nothing.
 *
 * The failure this whole module is arranged around, because it is the one that
 * looks like health. Measured on these exact tables before any pruning existed:
 * as eventhub_ticketing_owner with no app.tenant_id set, `SELECT count(*)` on
 * ProcessedMessage returned 0 of 29 real rows and a DELETE removed 0 — exit
 * code 0, no error, nothing logged.
 *
 * Row-level security is what makes it silent. A policy that does not match the
 * caller does not raise, it filters. So the job cannot ask "did I delete
 * anything" and treat zero as fine; it has to know whether there was anything
 * TO delete, and shout when the two disagree.
 */
export class PruneVisibilityError extends Error {
  constructor(table: string, eligible: number) {
    super(
      `prune of ${table} saw ${eligible} row(s) it should have deleted and deleted 0. The ` +
        'credential can SELECT these rows but not DELETE them — check the DELETE grant and the ' +
        'row-level security policy for this role. Reporting success here would hide the table ' +
        'growing without bound.',
    );
    this.name = 'PruneVisibilityError';
  }
}

/**
 * The tables this job may prune, and the column that says how old a row is.
 *
 * An allowlist of literal statements rather than an interpolated table name.
 * There is no way to parameterise an identifier in SQL, so the alternative is
 * building the statement by concatenation — and a retention job that
 * concatenates table names is one refactor away from taking one from somewhere
 * it should not.
 */
const TABLES = {
  ProcessedMessage: {
    /**
     * The consumer-side dedup ledger. Deleting a row means a redelivery of that
     * message would be APPLIED again rather than skipped.
     *
     * Safe because the projection carries its own guard: a snapshot is applied
     * only when its version is strictly higher than the stored one, so
     * re-applying an old message changes nothing. Dedup is the cheap first
     * answer; the version comparison is the one that holds.
     *
     * And retaining it forever has a failure of its own, which is worse and
     * quieter. Wipe EventProjection to rebuild it from the compacted topic
     * while keeping this table, and every replayed message is skipped as
     * "already applied" — the projection stays empty, the checkpoint advances,
     * and the service answers "no such event" for everything with complete
     * confidence.
     */
    countSql: `SELECT count(*)::text AS n FROM (
                 SELECT 1 FROM "ProcessedMessage"
                  WHERE "processedAt" < now() - ($1::int * interval '1 day')
                  LIMIT $2
               ) c`,
    deleteSql: `DELETE FROM "ProcessedMessage"
                 WHERE id IN (
                   SELECT id FROM "ProcessedMessage"
                    WHERE "processedAt" < now() - ($1::int * interval '1 day')
                    ORDER BY "processedAt"
                    LIMIT $2
                 )`,
  },
  IdempotencyKey: {
    /**
     * Deleting a COMPLETED key means a retry after the window creates a second
     * reservation instead of replaying the first.
     *
     * That is why the window is measured against a client's retry horizon and
     * not against table size. A year is far beyond any of them — a mobile app
     * flushing an offline queue, a job runner on exponential backoff, a person
     * pressing the button again after an incident — and the asymmetry is stark:
     * keeping a key too long costs one small row, deleting one too early costs
     * a duplicate hold on scarce inventory with no alarm anywhere.
     */
    countSql: `SELECT count(*)::text AS n FROM (
                 SELECT 1 FROM "IdempotencyKey"
                  WHERE "createdAt" < now() - ($1::int * interval '1 day')
                  LIMIT $2
               ) c`,
    deleteSql: `DELETE FROM "IdempotencyKey"
                 WHERE id IN (
                   SELECT id FROM "IdempotencyKey"
                    WHERE "createdAt" < now() - ($1::int * interval '1 day')
                    ORDER BY "createdAt"
                    LIMIT $2
                 )`,
  },
} as const;

export type PrunableTable = keyof typeof TABLES;

export interface PruneResult {
  deleted: Record<string, number>;
  moreToDo: boolean;
}

/**
 * Delete rows older than the retention window from the tables that grow forever.
 *
 * Runs on the sweeper's credential, which is the established identity for work
 * that crosses tenants — "what is older than a year" is a question with no
 * tenant in it, and there is no Tenant table here to loop over.
 *
 * Its reach is bounded by GRANTS rather than by policy, again as the sweeper's
 * other work is: DELETE table-wide, SELECT on only the two columns the job
 * reads. It cannot see IdempotencyKey.responseBody, which is a cached customer
 * response, or ProcessedMessage.messageId, which identifies another service's
 * traffic.
 */
export async function pruneOnce(
  pool: pg.Pool,
  logger: Logger,
  opts: { olderThanDays: number; batchSize: number; maxBatches: number },
): Promise<PruneResult> {
  const result: PruneResult = { deleted: {}, moreToDo: false };

  for (const table of Object.keys(TABLES) as PrunableTable[]) {
    const { countSql, deleteSql } = TABLES[table];
    result.deleted[table] = 0;

    // One batch, on a client withClient owns (see ../pool.ts for why). Returns
    // whether to run another: a callback cannot `break` the loop below, so the
    // two stopping conditions return false instead.
    const pruneBatch = async (client: pg.PoolClient, batch: number): Promise<boolean> => {
      try {
        await client.query('BEGIN');

        /**
         * THE CONTROL, and it has to share a transaction with the DELETE.
         *
         * Bounded by the same LIMIT rather than counting the whole table: an
         * unbounded count(*) is itself a full scan, and this role carries
         * statement_timeout=15s, so the control would become the first thing
         * to break.
         *
         * SELECT and DELETE are evaluated against DIFFERENT policy sets. A role
         * can be perfectly able to see a row and unable to delete it, and that
         * combination IS the silent no-op. Comparing the two is what catches
         * it — a control that only proved the rows were visible would pass in
         * exactly that case.
         */
        const { rows } = await client.query<{ n: string }>(countSql, [
          opts.olderThanDays,
          opts.batchSize,
        ]);
        const candidates = Number(rows[0]?.n ?? 0);

        if (candidates === 0) {
          await client.query('COMMIT');
          return false;
        }

        const deleted = await client.query(deleteSql, [opts.olderThanDays, opts.batchSize]);
        const removed = deleted.rowCount ?? 0;

        if (removed === 0) {
          await client.query('ROLLBACK');
          throw new PruneVisibilityError(table, candidates);
        }

        await client.query('COMMIT');
        result.deleted[table] += removed;

        // A short batch means the candidates ran out, not that the ceiling hit.
        if (removed < opts.batchSize) return false;
        if (batch === opts.maxBatches - 1) result.moreToDo = true;
        return true;
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        throw err;
      }
    };

    for (let batch = 0; batch < opts.maxBatches; batch += 1) {
      const again = await withClient(pool, logger, 'ticketing-sweeper', (client) =>
        pruneBatch(client, batch),
      );
      if (!again) break;
    }
  }

  const total = Object.values(result.deleted).reduce((a, b) => a + b, 0);
  if (total > 0) {
    logger.info({ ...result, olderThanDays: opts.olderThanDays }, 'pruned expired bookkeeping rows');
  }

  return result;
}
