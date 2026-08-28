import type pg from 'pg';
import type { Logger } from '@eventhub/logger';
import type { PaymentProvider } from '../payments/provider.js';

export interface ReconcileResult {
  examined: number;
  confirmed: number;
  released: number;
  stillUnknown: number;
}

/**
 * Resolves payment attempts that never settled.
 *
 * THE JOB THIS EXISTS FOR: a reservation sitting in PENDING_PAYMENT with a
 * Payment row that says PENDING or UNKNOWN. The provider was asked to charge a
 * card and either never answered or the process died before it could record the
 * answer. The seats are held, the customer may or may not have paid, and nothing
 * in the request path will ever revisit it — the request is long gone.
 *
 * There are exactly two wrong ways to resolve this and one right one.
 *
 *   WRONG: assume it failed and release the seats. If the money moved, the
 *          customer is charged for a seat that is now being sold to somebody
 *          else. This is the expensive mistake, and it is the one a naive
 *          "expire anything stuck for N minutes" job makes.
 *
 *   WRONG: assume it succeeded and confirm. Now inventory says sold for a seat
 *          nobody paid for, and the event under-sells by exactly that much.
 *
 *   RIGHT: ASK THE PROVIDER. They are the only party that knows.
 *
 * Structurally this is the same three phases as the pay endpoint, for the same
 * reason: phase 2 is a network call and must not happen inside a transaction.
 *
 * NO CLAIM MARKER, deliberately. Two reconcilers may examine the same row and
 * both ask the provider — lookup() is a read, so that is harmless. The guarded
 * UPDATEs in phase 3 decide which one settles it; the loser sees rowCount 0 and
 * does nothing. A claim column would add a second thing that can be left stale
 * by a crash, to protect writes that are already guarded.
 */
export async function reconcileOnce(
  pool: pg.Pool,
  provider: PaymentProvider,
  logger: Logger,
  opts: { batchSize: number; graceMs: number },
): Promise<ReconcileResult> {
  const result: ReconcileResult = { examined: 0, confirmed: 0, released: 0, stillUnknown: 0 };

  /**
   * PHASE 1 — find unsettled attempts, oldest first.
   *
   * `settledAt IS NULL` matches the partial index, so this stays cheap no matter
   * how much payment history accumulates. It covers PENDING (the process died
   * before recording anything) and UNKNOWN (the provider did not answer) with
   * one predicate, because both mean the same thing operationally: nobody knows
   * how this ended.
   *
   * The grace period stops the job racing live requests. Without it, a payment
   * started a second ago would be looked up while its own charge is still in
   * flight — the provider would answer "no such charge", and the reconciler
   * would release seats out from under a customer who is mid-checkout. It must
   * be comfortably longer than the endpoint's provider timeout.
   */
  const { rows: stuck } = await pool.query<{
    id: string;
    tenantId: string;
    reservationId: string;
    amountCents: number;
    currency: string;
  }>(
    `SELECT id, "tenantId", "reservationId", "amountCents", currency
       FROM "Payment"
      WHERE "settledAt" IS NULL
        AND "startedAt" < now() - ($2::bigint * interval '1 millisecond')
      ORDER BY "startedAt"
      LIMIT $1`,
    [opts.batchSize, opts.graceMs],
  );

  result.examined = stuck.length;

  for (const payment of stuck) {
    // PHASE 2 — ask. No transaction is open here.
    const outcome = await provider.lookup({
      idempotencyKey: payment.id,
      amountCents: payment.amountCents,
      currency: payment.currency,
    });

    if (outcome.status === 'unknown') {
      // Still no answer. Leave everything exactly as it is and ask again next
      // pass. Seats stay held, which is the safe direction: some inventory is
      // unavailable, nobody is oversold and nobody is charged for nothing.
      result.stillUnknown += 1;
      logger.warn({ paymentId: payment.id, reason: outcome.reason }, 'still unresolved');
      continue;
    }

    // PHASE 3 — write the answer down.
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      /**
       * The guard. `settledAt IS NULL` re-checks, as part of the write, that
       * nobody else resolved this between phase 1 and now — another reconciler,
       * or a late-returning request handler.
       *
       * Everything below is conditional on this rowCount, because settling twice
       * would move inventory twice. A double held->sold passes both TicketType
       * CHECK constraints: remaining + held + sold stays equal to total, the
       * numbers are just wrong. A constraint sees one row and cannot notice.
       */
      const settled = await client.query(
        `UPDATE "Payment"
            SET status = $2::"PaymentStatus",
                "providerRef" = COALESCE($3, "providerRef"),
                "failureReason" = $4,
                "settledAt" = now(),
                "updatedAt" = now()
          WHERE id = $1 AND "settledAt" IS NULL`,
        [
          payment.id,
          outcome.status === 'succeeded' ? 'SUCCEEDED' : 'FAILED',
          outcome.status === 'succeeded' ? outcome.providerRef : null,
          outcome.status === 'failed' ? outcome.reason : null,
        ],
      );

      if (settled.rowCount === 0) {
        await client.query('ROLLBACK');
        logger.debug({ paymentId: payment.id }, 'already settled by someone else');
        continue;
      }

      if (outcome.status === 'failed') {
        /**
         * The charge never happened. Return the reservation to a plain hold
         * rather than cancelling it, matching what the endpoint does on a
         * decline — and let the sweeper decide whether the hold has expired.
         *
         * Note this does NOT return inventory. The seats are still held by a
         * HELD reservation; the sweeper returns them when the clock runs out.
         * Returning them here as well would double-release.
         */
        const released = await client.query(
          `UPDATE "Reservation"
              SET status = 'HELD',
                  "paymentStartedAt" = NULL,
                  version = version + 1,
                  "updatedAt" = now()
            WHERE id = $1 AND status = 'PENDING_PAYMENT'`,
          [payment.reservationId],
        );
        if (released.rowCount && released.rowCount > 0) result.released += 1;

        await client.query('COMMIT');
        logger.info({ paymentId: payment.id, reason: outcome.reason }, 'charge never happened');
        continue;
      }

      // The money moved. Finish the sale the request handler could not.
      const confirmed = await client.query(
        `UPDATE "Reservation"
            SET status = 'CONFIRMED',
                "confirmedAt" = now(),
                "paymentStartedAt" = NULL,
                version = version + 1,
                "updatedAt" = now()
          WHERE id = $1 AND status = 'PENDING_PAYMENT'
        RETURNING "eventId", "userId", "tenantId"`,
        [payment.reservationId],
      );

      if (confirmed.rowCount === 0) {
        // The reservation is no longer awaiting payment. The Payment row is now
        // correctly marked SUCCEEDED — which is the useful part, because it says
        // a real charge exists — but this job must not touch inventory for a
        // reservation it did not transition.
        await client.query('COMMIT');
        logger.warn(
          { paymentId: payment.id, reservationId: payment.reservationId },
          'charge succeeded but the reservation was no longer PENDING_PAYMENT',
        );
        continue;
      }

      const reservation = confirmed.rows[0] as {
        eventId: string;
        userId: string;
        tenantId: string;
      };

      const { rows: lines } = await client.query<{
        ticketTypeId: string;
        qty: string;
        unitPriceCents: number;
      }>(
        `SELECT "ticketTypeId",
                sum(quantity)::text AS qty,
                max("unitPriceCents") AS "unitPriceCents"
           FROM "ReservationItem"
          WHERE "reservationId" = $1
          GROUP BY "ticketTypeId"
          ORDER BY "ticketTypeId"`,
        [payment.reservationId],
      );

      // Sorted id order, matching the reservation path, the sweeper, and the pay
      // endpoint. Every writer takes these rows in the same sequence or two of
      // them deadlock.
      for (const line of lines) {
        const qty = Number(line.qty);
        const moved = await client.query(
          `UPDATE "TicketType"
              SET held = held - $2,
                  sold = sold + $2,
                  version = version + 1,
                  "updatedAt" = now()
            WHERE id = $1 AND held >= $2`,
          [line.ticketTypeId, qty],
        );
        if (moved.rowCount === 0) {
          throw new Error(
            `cannot settle ${payment.reservationId}: ticket type ${line.ticketTypeId} does not ` +
              `hold ${qty} seats to sell`,
          );
        }
      }

      // The same two messages the pay endpoint emits, for the same reasons: seat
      // state on tickets.reserved so availability stays ordered with the hold,
      // money on tickets.purchased so revenue consumers get it without every
      // availability consumer being handed financial data.
      await client.query(
        `INSERT INTO "OutboxMessage"
           ("id","tenantId",topic,"eventType","schemaVersion","aggregateId",payload,"createdAt")
         VALUES (gen_random_uuid()::text, $1, 'tickets.reserved', 'reservation.confirmed', 1, $2, $3::jsonb, now())`,
        [
          reservation.tenantId,
          reservation.eventId,
          JSON.stringify({
            reservationId: payment.reservationId,
            tenantId: reservation.tenantId,
            eventId: reservation.eventId,
            userId: reservation.userId,
            state: 'CONFIRMED',
            items: lines.map((l) => ({ ticketTypeId: l.ticketTypeId, quantity: Number(l.qty) })),
            expiresAt: null,
          }),
        ],
      );

      await client.query(
        `INSERT INTO "OutboxMessage"
           ("id","tenantId",topic,"eventType","schemaVersion","aggregateId",payload,"createdAt")
         VALUES (gen_random_uuid()::text, $1, 'tickets.purchased', 'tickets.purchased', 1, $2, $3::jsonb, now())`,
        [
          reservation.tenantId,
          reservation.eventId,
          JSON.stringify({
            paymentId: payment.id,
            reservationId: payment.reservationId,
            tenantId: reservation.tenantId,
            eventId: reservation.eventId,
            userId: reservation.userId,
            amountCents: payment.amountCents,
            currency: payment.currency,
            items: lines.map((l) => ({
              ticketTypeId: l.ticketTypeId,
              quantity: Number(l.qty),
              unitPriceCents: l.unitPriceCents,
            })),
            providerRef: outcome.status === 'succeeded' ? outcome.providerRef : null,
          }),
        ],
      );

      await client.query('COMMIT');
      result.confirmed += 1;
      logger.info(
        { paymentId: payment.id, reservationId: payment.reservationId },
        'recovered a charge the request never recorded',
      );
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      // One bad row must not stop the batch. It stays unsettled and is picked up
      // next pass, which is the safe direction to fail.
      logger.error({ err, paymentId: payment.id }, 'failed to reconcile');
    } finally {
      client.release();
    }
  }

  return result;
}
