import type pg from 'pg';
import type { Logger } from '@eventhub/logger';
import type { PaymentProvider } from '../payments/provider.js';
import { settleAttempt } from '../payments/settle.js';

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
    //
    // Delegated to settleAttempt, which the provider WEBHOOK also calls. The two
    // learn the same fact by different routes -- one asks, one is told -- and
    // two copies of this logic would drift, with the drift showing up as
    // inventory that does not add up weeks later.
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const settled = await settleAttempt(client, payment, outcome, logger);
      await client.query('COMMIT');

      if (settled === 'confirmed') result.confirmed += 1;
      else if (settled === 'released') result.released += 1;
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
