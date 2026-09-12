/**
 * The hold sweeper's claim query, in one place because two things need the SAME
 * text: sweep.ts runs it, and scripts/verify-query-plans.mjs asserts its plan.
 *
 * Split out for the reason the relay's claim was (see
 * services/relay/src/claim-sql.ts). That one was duplicated in the plan checker
 * instead of imported, the copy drifted, and the plan checker consequently could
 * not have caught #64 -- it was explaining a query nobody runs. This query has
 * the same exposure: its whole point is a plan shape, and a plan assertion
 * against a hand copy asserts the copy.
 */

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
export const SWEEP_CLAIM_SQL =
      `SELECT id, "tenantId", "eventId", "userId"
         FROM "Reservation"
        WHERE status = 'HELD'
          AND "expiresAt" <= now()
        ORDER BY "expiresAt"
        LIMIT $1
          FOR UPDATE SKIP LOCKED`;
