import type pg from 'pg';

/**
 * Ends deliveries that were claimed and never finished.
 *
 * WHAT IS ACTUALLY STUCK. A delivery is claimed PENDING before anything is sent, and
 * Kafka re-drives nearly all of them: a send that failed for a reason that may pass
 * waits in a retry tier with its row still PENDING, and comes back. The rows that
 * never move again are the ones whose message was dead-lettered, and the ones whose
 * topic retention passed while they waited.
 *
 * IT DOES NOT RESEND, AND COULD NOT. The row holds no sealed request — the seal is
 * opened in memory and never stored — and a reset link is dead within the hour
 * anyway. What it can do is make the row say what became of it, so that PENDING means
 * "in flight" and a count of PENDING rows is worth alerting on.
 *
 * ACROSS TENANTS, on the sweeper's own credential, which may write nothing but these
 * three columns and may leave a row in no state but FAILED. See the migration.
 */

export interface SweepResult {
  /** Deliveries given up on in this pass. */
  swept: number;
  /** The age of the oldest delivery still PENDING, in milliseconds; null when there is none. */
  oldestPendingMs: number | null;
  /** The pass filled its batch, so there is probably more waiting. */
  more: boolean;
}

/** Said in the row itself, because "FAILED" with no reason is the same puzzle one layer down. */
export const ABANDONED = 'abandoned: claimed but never completed';

export async function sweepOnce(pool: pg.Pool, opts: { stuckAfterMs: number; batchSize: number }): Promise<SweepResult> {
  const client = await pool.connect();
  try {
    // The subselect bounds what one statement locks, and takes the oldest first so a
    // backlog is worked through in the order the rows went stale.
    const swept = await client.query(
      `UPDATE "NotificationDelivery"
          SET status = 'FAILED', "failedAt" = now(), "lastError" = $1
        WHERE id IN (
          SELECT d.id FROM "NotificationDelivery" d
           WHERE d.status = 'PENDING'
             AND d."createdAt" < now() - ($2::double precision * interval '1 millisecond')
           ORDER BY d."createdAt"
           LIMIT $3)`,
      [ABANDONED, opts.stuckAfterMs, opts.batchSize],
    );
    const oldest = await client.query<{ ms: string | null }>(
      `SELECT extract(epoch FROM now() - min(d."createdAt")) * 1000 AS ms
         FROM "NotificationDelivery" d WHERE d.status = 'PENDING'`,
    );
    const ms = oldest.rows[0]?.ms;
    const count = swept.rowCount ?? 0;
    return {
      swept: count,
      oldestPendingMs: ms === null || ms === undefined ? null : Math.round(Number(ms)),
      more: count >= opts.batchSize,
    };
  } finally {
    client.release();
  }
}

/**
 * Refuses to sweep on a credential that can do more than sweep.
 *
 * Asserted rather than assumed, the same reasoning as the relay's boot check and
 * Ticketing's sweeper: an over-privileged sweeper works perfectly and is simply far
 * more dangerous. This role crosses every tenant, so its reach is bounded by grants
 * and by its policy, and both are worth proving at startup rather than in review.
 */
export async function assertSweeperIsLeastPrivilege(pool: pg.Pool): Promise<void> {
  const client = await pool.connect();
  try {
    const who = await client.query<{ rolsuper: boolean; rolbypassrls: boolean }>(
      'SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user',
    );
    const role = who.rows[0];
    if (!role) throw new Error('could not read current_user from pg_roles');
    if (role.rolsuper || role.rolbypassrls) {
      throw new Error(
        'refusing to sweep: the sweeper connects as a role that bypasses row-level security ' +
          `(rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls})`,
      );
    }

    /** Runs statements that must be refused, and rolls back whatever they did if they were not. */
    const refused = async (...statements: string[]): Promise<boolean> => {
      await client.query('BEGIN');
      try {
        for (const sql of statements) await client.query(sql);
        return false;
      } catch {
        return true;
      } finally {
        await client.query('ROLLBACK').catch(() => {});
      }
    };

    // WITH A TENANT SET, so this probes the GRANT and not row security. Without a
    // tenant, the app role's own policy refuses the insert too, and the probe would
    // pass for a credential it is meant to catch.
    const canCreate = !(await refused(
      `SELECT set_config('app.tenant_id', 'sweeper_privilege_probe', true)`,
      `INSERT INTO "NotificationDelivery"
         (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId")
       VALUES ('sweeper_privilege_probe', 'sweeper_privilege_probe', 'probe', 'email', 'probe', 'probe', 'probe', 'probe')`,
    ));
    if (canCreate) {
      throw new Error(
        'refusing to sweep: the sweeper credential can create deliveries. It should hold SELECT and a ' +
          'column-level UPDATE only — it crosses every tenant, so grants are the whole bound on it.',
      );
    }

    const canRead = !(await refused('SELECT 1 FROM "EmailSuppression" LIMIT 1'));
    if (canRead) {
      throw new Error(
        'refusing to sweep: the sweeper credential can read the suppression list, which is no part of ' +
          'ending an unfinished delivery.',
      );
    }

    /**
     * That it cannot mark a delivery SENT is asked of the POLICY, not of a statement.
     *
     * The obvious probe — UPDATE ... SET status = 'SENT' WHERE status = 'PENDING' —
     * is refused per row, so on a table with nothing PENDING it changes nothing,
     * raises nothing, and reports that the credential may do what it must not. It did
     * exactly that: every email verifier's Notification refused to start, because at
     * that moment no delivery was in flight. An assertion that passes on an empty
     * table is worse than none.
     *
     * scripts/verify-notification-sweeper.mjs makes the behavioural case with a row
     * in hand; this one asks whether the rule is there at all.
     */
    const policies = await client.query<{ with_check: string | null }>(
      `SELECT with_check FROM pg_policies
        WHERE schemaname = 'public' AND tablename = 'NotificationDelivery' AND current_user = ANY(roles)`,
    );
    const pinned = policies.rows.some((p) => (p.with_check ?? '').includes("'FAILED'"));
    if (!pinned) {
      throw new Error(
        'refusing to sweep: no row-security policy pins what this credential may write to FAILED ' +
          `(policies for it: ${policies.rows.length}). Without it a sweep could mark a delivery SENT and ` +
          'claim mail went out that never did.',
      );
    }
  } finally {
    client.release();
  }
}
