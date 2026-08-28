import type pg from 'pg';
import type { Logger } from '@eventhub/logger';

export interface SettleTarget {
  /** The Payment row id, which is also the provider idempotency key. */
  id: string;
  tenantId: string;
  reservationId: string;
  amountCents: number;
  currency: string;
}

export type SettleOutcome =
  | { status: 'succeeded'; providerRef: string }
  | { status: 'failed'; reason: string };

export type SettleResult = 'confirmed' | 'released' | 'noop';

/**
 * Writes down how a charge ended, and moves everything that follows from it.
 *
 * Shared by the reconciler and the provider webhook, which is the point. They
 * learn the same fact by different routes — one asks, one is told — and if they
 * settled it with two copies of this logic the copies would drift, and the drift
 * would show up as inventory that does not add up weeks later.
 *
 * The caller owns the transaction: it must have issued BEGIN and must COMMIT.
 * That is deliberate rather than convenient — a webhook does one of these and a
 * reconciliation pass does many, and only the caller knows how much work belongs
 * in one transaction.
 *
 * EVERY WRITE HERE IS GUARDED, and the guards are the only thing standing
 * between this and a double settlement. Two routes can learn the same fact at
 * the same moment: the provider's webhook can arrive while the request that
 * started the charge is still in its own phase 3. Both call this. Exactly one
 * does the work.
 *
 * The failure that makes those guards necessary is invisible to the schema. A
 * double `held -> sold` keeps `remaining + held + sold = total` perfectly true —
 * the numbers are simply wrong. Both TicketType CHECK constraints pass, because
 * a constraint sees one row and cannot notice that the row was moved twice.
 */
export async function settleAttempt(
  client: pg.PoolClient,
  payment: SettleTarget,
  outcome: SettleOutcome,
  logger: Logger,
): Promise<SettleResult> {
  /**
   * `settledAt IS NULL` re-checks, as part of the write, that nobody else has
   * resolved this attempt. Everything below is conditional on this rowCount.
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
    logger.debug({ paymentId: payment.id }, 'already settled by someone else');
    return 'noop';
  }

  if (outcome.status === 'failed') {
    /**
     * The charge never happened. Return the reservation to a plain hold rather
     * than cancelling it, matching what the pay endpoint does on a decline, and
     * let the sweeper decide whether the hold has run out.
     *
     * Note this does NOT return inventory. The seats are still held by a HELD
     * reservation, and the sweeper returns them when the clock expires.
     * Returning them here as well would be a double release.
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
    logger.info({ paymentId: payment.id, reason: outcome.reason }, 'charge never happened');
    return released.rowCount && released.rowCount > 0 ? 'released' : 'noop';
  }

  // The money moved. Finish the sale.
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
    // correctly SUCCEEDED, which is the useful part — it records that a real
    // charge exists — but inventory must not move for a transition this call
    // did not make.
    logger.warn(
      { paymentId: payment.id, reservationId: payment.reservationId },
      'charge succeeded but the reservation was no longer PENDING_PAYMENT',
    );
    return 'noop';
  }

  const reservation = confirmed.rows[0] as { eventId: string; userId: string; tenantId: string };

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

  // Sorted id order, matching the reservation path, the sweeper and the pay
  // endpoint. Every writer must take these rows in the same sequence or two of
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
        `cannot settle ${payment.reservationId}: ticket type ${line.ticketTypeId} does not hold ` +
          `${qty} seats to sell`,
      );
    }
  }

  // The same two messages the pay endpoint emits, for the same reasons: seat
  // state on tickets.reserved so availability stays ordered with the hold it
  // confirms, money on tickets.purchased so revenue consumers get it without
  // every availability consumer being handed financial data.
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
        providerRef: outcome.providerRef,
      }),
    ],
  );

  return 'confirmed';
}
