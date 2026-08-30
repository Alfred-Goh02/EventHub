import type pg from 'pg';
import type { Logger } from '@eventhub/logger';

export interface SweepResult {
  claimed: number;
  expired: number;
  skipped: number;
  seatsReturned: number;
}

/**
 * Releases expired seat holds and returns their inventory.
 *
 * Everything below happens in ONE transaction — claim, expire, return, emit.
 * The first design had the claim and the release as two transactions, and all
 * four reviewers independently flagged the same consequence: `FOR UPDATE SKIP
 * LOCKED` only holds its locks until its transaction ends, so claiming in one
 * transaction and releasing in another leaves the rows unlocked in between. The
 * lock becomes decorative, and two sweepers — or a sweeper and a /pay request —
 * can both act on the same reservation.
 *
 * Raw pg rather than Prisma, for the same reason as the relay: `FOR UPDATE SKIP
 * LOCKED` cannot be expressed through the query builder.
 */
export async function sweepOnce(
  pool: pg.Pool,
  logger: Logger,
  opts: { batchSize: number },
): Promise<SweepResult> {
  const client = await pool.connect();
  const result: SweepResult = { claimed: 0, expired: 0, skipped: 0, seatsReturned: 0 };

  try {
    await client.query('BEGIN');

    /**
     * `status = 'HELD'` is written as a LITERAL, deliberately.
     *
     * Reservation_sweep_idx is a partial index predicated on status = 'HELD'.
     * Postgres can only use a partial index when it can PROVE the query implies
     * the predicate — and with the value bound as a parameter it arrives as
     * ('HELD'::cstring)::"ReservationStatus", which is not a constant it can
     * fold. Measured on this database:
     *
     *   literal 'HELD'   -> Bitmap Index Scan on Reservation_sweep_idx
     *   bind parameter   -> Seq Scan
     *
     * So the index added specifically for this query would never be used by it.
     * The literal is safe here because it is a constant in this file, not input.
     * The batch size stays a parameter, since it carries no predicate.
     */
    const { rows: claimed } = await client.query<{
      id: string;
      tenantId: string;
      eventId: string;
      userId: string;
    }>(
      `SELECT id, "tenantId", "eventId", "userId"
         FROM "Reservation"
        WHERE status = 'HELD'
          AND "expiresAt" <= now()
        ORDER BY "expiresAt"
        LIMIT $1
          FOR UPDATE SKIP LOCKED`,
      [opts.batchSize],
    );

    result.claimed = claimed.length;

    /**
     * THREE PASSES, and the reason is lock ordering ACROSS the batch.
     *
     * This used to be a single loop: expire a reservation, return its inventory,
     * emit its message, then move to the next one. Its comment claimed the
     * TicketType updates were "in sorted id order", and they were — within one
     * reservation. Across the batch they were not.
     *
     * A sweep holding reservations A{tt_b} and B{tt_a} therefore locked tt_b and
     * then tt_a, while a concurrent cart {tt_a, tt_b} sorts and locks tt_a then
     * tt_b. Two writers, opposite order, one deadlock — and the sweeper was
     * exactly the actor the sorting was supposed to keep in step.
     *
     * Postgres detects it and aborts one side with 40P01, so nothing corrupts:
     * the reservation path retries, and the sweep loop catches and retries. It
     * shows up as failed checkouts and churn under load, which is the kind of
     * thing that gets blamed on the database.
     *
     *   pass 1  expire the reservations and collect what they owe back.
     *           Locks Reservation rows only — those are already held by the
     *           FOR UPDATE above, so no new lock order is introduced.
     *   pass 2  return inventory, ONE update per ticket type, sorted across the
     *           WHOLE batch. This is the only pass that touches TicketType.
     *   pass 3  emit. Touches OutboxMessage only.
     *
     * Aggregating also collapses N reservations sharing a ticket type into one
     * UPDATE instead of N, which is fewer round trips and fewer row versions.
     */
    const owed = new Map<string, number>();
    const released: Array<{ r: (typeof claimed)[number]; lines: Array<{ ticketTypeId: string; qty: string }> }> = [];

    for (const r of claimed) {
      /**
       * The guard that makes a double release impossible.
       *
       * `AND status = 'HELD'` re-checks the state as part of the write. If
       * anything else already expired or confirmed this reservation, rowCount
       * is 0 and we return no inventory.
       *
       * This matters more than it looks: a double inventory return passes BOTH
       * TicketType CHECK constraints. Returning the same seats twice keeps
       * remaining + held + sold = total true — it just moves the total in the
       * wrong direction, overselling silently. The constraint sees one row and
       * cannot notice. Only the reconciliation check would catch it after the
       * fact, so it must not be allowed to happen.
       *
       * Notably this also excludes PENDING_PAYMENT: the clock stops while a card
       * is being charged, and reclaiming those seats would take them from
       * someone who is about to pay for them.
       */
      const expired = await client.query(
        `UPDATE "Reservation"
            SET status = 'EXPIRED', version = version + 1, "updatedAt" = now()
          WHERE id = $1 AND status = 'HELD'`,
        [r.id],
      );

      if (expired.rowCount === 0) {
        result.skipped += 1;
        logger.debug({ reservationId: r.id }, 'no longer HELD, skipped');
        continue;
      }

      const { rows: lines } = await client.query<{ ticketTypeId: string; qty: string }>(
        `SELECT "ticketTypeId", sum(quantity)::text AS qty
           FROM "ReservationItem"
          WHERE "reservationId" = $1
          GROUP BY "ticketTypeId"`,
        [r.id],
      );

      for (const line of lines) {
        const qty = Number(line.qty);
        owed.set(line.ticketTypeId, (owed.get(line.ticketTypeId) ?? 0) + qty);
        result.seatsReturned += qty;
      }

      released.push({ r, lines });
      result.expired += 1;
    }

    /**
     * Pass 2 — the only statements in this transaction that lock TicketType.
     *
     * Sorted with localeCompare to match the reservation path exactly, which
     * sorts its cart the same way. What matters is not which ordering is chosen
     * but that every writer chooses the SAME one; ticket type ids are cuids, so
     * byte order and locale order coincide anyway.
     *
     * A single UPDATE ... FROM would be fewer round trips still, but Postgres
     * gives no guarantee about the order it locks the rows it joins to — which
     * would put us straight back where this started.
     */
    for (const ticketTypeId of [...owed.keys()].sort((a, b) => a.localeCompare(b))) {
      await client.query(
        `UPDATE "TicketType"
            SET remaining = remaining + $2,
                held = held - $2,
                version = version + 1,
                "updatedAt" = now()
          WHERE id = $1`,
        [ticketTypeId, owed.get(ticketTypeId)],
      );
    }

    // Pass 3 — the messages, in the same transaction as the releases they
    // describe. Without a RELEASED message, a consumer building live
    // availability decrements on RESERVED and increments on nothing, so every
    // abandoned cart permanently reduces its view — and with TTL holds that is
    // the common path, not an edge case.
    for (const { r, lines } of released) {
      await client.query(
        `INSERT INTO "OutboxMessage"
           ("id", "tenantId", topic, "eventType", "schemaVersion", "aggregateId", payload, "createdAt")
         VALUES (gen_random_uuid()::text, $1, 'tickets.reserved', 'reservation.expired', 1, $2, $3::jsonb, now())`,
        [
          r.tenantId,
          // Keyed by EVENT, matching the reservation path, so holds and releases
          // for one event land on the same partition and stay ordered relative
          // to each other. Keyed by reservation they could interleave, and a
          // consumer could see a release before the hold it releases.
          r.eventId,
          JSON.stringify({
            reservationId: r.id,
            tenantId: r.tenantId,
            eventId: r.eventId,
            userId: r.userId,
            state: 'EXPIRED',
            items: lines.map((l) => ({ ticketTypeId: l.ticketTypeId, quantity: Number(l.qty) })),
            expiresAt: null,
          }),
        ],
      );
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }

  return result;
}

/**
 * Refuses to start unless the sweeper's privileges are what they should be.
 *
 * The sweeper crosses tenants by design — it selects by expiry, which has no
 * tenant — so its blast radius must be bounded by grants rather than by policy.
 * It must be able to expire a hold and return inventory, and must NOT be able to
 * create reservations or read another service's data.
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
        'refusing to start: the sweeper connects as a role that bypasses row-level security ' +
          `(rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls})`,
      );
    }

    // It must NOT be able to create a reservation. Asserted rather than assumed,
    // because an over-privileged sweeper works perfectly and is simply far more
    // dangerous — the same reasoning as the relay's boot check.
    let couldInsert = false;
    try {
      await client.query('BEGIN');
      await client.query(
        `INSERT INTO "Reservation" (id, "tenantId", "eventId", "userId", status, "expiresAt", "updatedAt")
         VALUES ('sweeper_privilege_probe', 'probe', 'probe', 'probe', 'HELD', now(), now())`,
      );
      couldInsert = true;
    } catch {
      // Expected: permission denied for table Reservation.
    } finally {
      await client.query('ROLLBACK').catch(() => {});
    }

    if (couldInsert) {
      throw new Error(
        'refusing to start: the sweeper credential can INSERT reservations. It should hold only ' +
          'SELECT plus column-level UPDATE — it operates across every tenant, so its reach must ' +
          'be bounded by grants.',
      );
    }
  } finally {
    client.release();
  }
}
