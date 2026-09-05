import type pg from 'pg';
import type { Logger } from '@eventhub/logger';
import type { PaymentProvider } from '../payments/provider.js';
import { settleAttempt } from '../payments/settle.js';

export interface ReconcileResult {
  examined: number;
  confirmed: number;
  released: number;
  stillUnknown: number;
  /**
   * Rows whose provider lookup THREW.
   *
   * Counted rather than only logged, so a payment the reconciler cannot ask
   * about is visible in the same place as the ones it resolved. A job that
   * reports examined=50 confirmed=0 while quietly failing on all fifty looks
   * identical to a quiet night.
   */
  lookupFailed: number;
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
  const result: ReconcileResult = { examined: 0, confirmed: 0, released: 0, stillUnknown: 0, lookupFailed: 0 };

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
    /**
     * The lookup is inside the try, and it did not used to be.
     *
     * A timeout is not the hazard -- withTimeout() RESOLVES to `unknown`, so a
     * slow provider is already handled. The hazard is a lookup that REJECTS: a
     * DNS failure, a connection reset, a 500 the client surfaces as a throw.
     * That escaped the loop entirely and abandoned the rest of the batch, which
     * is precisely what the comment on the catch below already promised it
     * would not do.
     *
     * And it does not merely lose one pass. The query is ORDER BY startedAt
     * LIMIT n with no cursor and no per-row failure marking, so the SAME row is
     * selected first next time. A payment whose lookup fails deterministically
     * -- a malformed provider reference, a provider that 500s for one id --
     * sits at the head of every pass forever, and every payment behind it is
     * never examined at all. In the job that decides whether customers who may
     * already have been charged get their seats or their money back.
     *
     * Now one bad row costs one row per pass. It still occupies its slot, which
     * an attempt counter and a backoff column would fix properly; this change
     * buys the important half without a migration.
     */
    let outcome: Awaited<ReturnType<typeof provider.lookup>>;
    try {
      outcome = await provider.lookup({
        idempotencyKey: payment.id,
        amountCents: payment.amountCents,
        currency: payment.currency,
      });
    } catch (err) {
      result.lookupFailed += 1;
      logger.error(
        { err, paymentId: payment.id },
        'provider lookup threw; leaving this attempt unsettled and continuing the batch',
      );
      continue;
    }

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
