import type pg from 'pg';
import type { Logger } from '@eventhub/logger';

export interface PruneResult {
  /** Rows removed this pass. */
  deleted: number;
  /** Batches run. Hitting the ceiling means there is more to do next pass. */
  batches: number;
  /** True when the pass stopped because it hit maxBatches rather than running dry. */
  moreToDo: boolean;
}

/**
 * A prune that reported success while deleting nothing.
 *
 * This is the failure the whole design is arranged around, because it is the
 * one that looks like health. Measured on this repository before any pruning
 * existed: as eventhub_ticketing_owner with no app.tenant_id set,
 * `SELECT count(*)` on ProcessedMessage returned 0 of 29 real rows and a DELETE
 * removed 0 — exit code 0, no error, nothing in the log. A maintenance job
 * written the obvious way would have reported "nothing to prune" forever while
 * the table grew without bound.
 *
 * Row-level security is what makes it silent: a policy that does not match the
 * caller does not raise, it filters. So the job cannot ask "did I delete
 * anything" and treat zero as fine — it has to know whether there was anything
 * TO delete, and shout when those two disagree.
 */
export class PruneVisibilityError extends Error {
  constructor(eligible: number) {
    super(
      `prune saw ${eligible} row(s) it should have deleted and deleted 0. The credential can ` +
        'SELECT these rows but not DELETE them — check the DELETE grant and the row-level ' +
        'security policy for this role. Reporting success here would hide the table growing ' +
        'without bound.',
    );
    this.name = 'PruneVisibilityError';
  }
}

/**
 * Delete outbox rows that have been delivered and are older than the window.
 *
 * WHAT IS NEVER DELETED, and it is not merely omitted from the predicate:
 *
 *   UNPUBLISHED   `publishedAt IS NOT NULL` excludes them. A row still waiting
 *                 to go to Kafka is the message itself, not a record of one.
 *   DEAD-LETTERED `deadLetteredAt IS NULL` excludes them. Those are the rows
 *                 whose payload could never encode, and nothing in this
 *                 repository clears deadLetteredAt — they are the evidence of a
 *                 failure somebody still has to look at. They are also bounded
 *                 by how many genuinely bad payloads exist, which is not a
 *                 growth problem.
 *
 * The two predicates are disjoint from the drain's, which claims only rows with
 * `publishedAt IS NULL`, so a prune can never remove a row the relay is in the
 * middle of publishing. It is not a lock question — the row simply fails the
 * prune's WHERE clause.
 *
 * BATCHED, and bounded twice. Each statement touches at most batchSize rows so
 * it cannot hold locks across a huge range, and a pass runs at most maxBatches
 * of them so it cannot monopolise the relay loop on the first run against years
 * of history. Whatever is left is picked up next pass.
 */
export async function pruneOnce(
  pool: pg.Pool,
  logger: Logger,
  opts: { olderThanDays: number; batchSize: number; maxBatches: number },
): Promise<PruneResult> {
  const result: PruneResult = { deleted: 0, batches: 0, moreToDo: false };

  for (let batch = 0; batch < opts.maxBatches; batch += 1) {
    const client = await pool.connect();

    try {
      await client.query('BEGIN');

      /**
       * THE POSITIVE CONTROL, and it has to be inside the same transaction as
       * the DELETE to mean anything.
       *
       * Bounded by the same LIMIT rather than counting the whole table: an
       * unbounded count(*) is itself a full scan, and these roles carry
       * statement_timeout=15s, so the control would become the thing that
       * breaks first.
       *
       * SELECT and DELETE are evaluated against DIFFERENT policy sets in
       * Postgres. A role can be perfectly able to see a row and unable to
       * delete it, and that combination is precisely the silent no-op: the job
       * finds candidates, removes none, and reports zero. Comparing the two
       * counts is what catches it — a control that only proved the rows were
       * VISIBLE would pass in exactly that case.
       */
      const { rows: eligible } = await client.query<{ n: string }>(
        `SELECT count(*)::text AS n
           FROM (
             SELECT 1
               FROM "OutboxMessage"
              WHERE "publishedAt" IS NOT NULL
                AND "deadLetteredAt" IS NULL
                AND "publishedAt" < now() - ($1::int * interval '1 day')
              LIMIT $2
           ) candidates`,
        [opts.olderThanDays, opts.batchSize],
      );
      const candidates = Number(eligible[0]?.n ?? 0);

      if (candidates === 0) {
        await client.query('COMMIT');
        break;
      }

      /**
       * Deleted by id, chosen by publishedAt off the partial index.
       *
       * ORDER BY matters: without it each batch restarts its scan wherever the
       * planner likes and re-reads the dead tuples the previous batches left,
       * which turns a bounded loop into a quadratic one.
       */
      const deleted = await client.query(
        `DELETE FROM "OutboxMessage"
          WHERE id IN (
            SELECT id
              FROM "OutboxMessage"
             WHERE "publishedAt" IS NOT NULL
               AND "deadLetteredAt" IS NULL
               AND "publishedAt" < now() - ($1::int * interval '1 day')
             ORDER BY "publishedAt"
             LIMIT $2
          )`,
        [opts.olderThanDays, opts.batchSize],
      );

      const removed = deleted.rowCount ?? 0;

      if (removed === 0) {
        await client.query('ROLLBACK');
        throw new PruneVisibilityError(candidates);
      }

      await client.query('COMMIT');

      result.deleted += removed;
      result.batches += 1;

      // A short batch means the candidates ran out, not that the ceiling was hit.
      if (removed < opts.batchSize) break;

      if (batch === opts.maxBatches - 1) result.moreToDo = true;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  if (result.deleted > 0) {
    logger.info(
      { ...result, olderThanDays: opts.olderThanDays },
      'pruned delivered outbox rows',
    );
  }

  return result;
}
