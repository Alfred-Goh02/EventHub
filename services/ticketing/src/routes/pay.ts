import { Router } from 'express';
import { createLogger } from '@eventhub/logger';
import { databaseNow, withTenantRetry } from '../db.js';
import { ah } from '@eventhub/http';
import { requireAuth } from '../auth.js';
import { emit } from '../outbox.js';
import { FakePaymentProvider, withTimeout, type PaymentProvider } from '../payments/provider.js';

export const pay: Router = Router();

const logger = createLogger('payment-route');

/**
 * How long to wait for the provider before calling it an unknown.
 *
 * This is the number the whole reconciliation design is sized against: it
 * bounds how long a reservation can sit in PENDING_PAYMENT because of a single
 * request, and therefore how long its seats are unavailable to anyone else.
 */
const PROVIDER_TIMEOUT_MS = (() => {
  const raw = process.env['PAYMENT_TIMEOUT_MS'];
  if (raw === undefined || raw === '') return 8000;

  const parsed = Number(raw);

  /**
   * Validated rather than coerced, because the failure mode of a bare Number()
   * here is spectacular and silent.
   *
   * `Number('8s')` is NaN, and `setTimeout(fn, NaN)` fires IMMEDIATELY. The
   * provider call would lose every race against its own timeout, so every single
   * payment would return `unknown` — seats held, nothing settled, the
   * reconciler left to resolve a queue that grows as fast as people check out.
   * Nothing crashes and no log says why.
   *
   * The reconciler already validates this same variable with zod; the request
   * path was the one place reading it raw.
   */
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(
      `PAYMENT_TIMEOUT_MS must be a positive number of milliseconds, got ${JSON.stringify(raw)}`,
    );
  }

  return parsed;
})();

/** Not modelled per ticket type yet; the Payment column defaults to the same value. */
const CURRENCY = 'SGD';

const provider: PaymentProvider = withTimeout(
  new FakePaymentProvider(Number(process.env['PAYMENT_FAKE_LATENCY_MS'] ?? 0)),
  PROVIDER_TIMEOUT_MS,
);

type Intent =
  | {
      kind: 'ready';
      paymentId: string;
      eventId: string;
      amountCents: number;
      lines: Array<{ ticketTypeId: string; quantity: number; unitPriceCents: number }>;
    }
  | { kind: 'not-found' }
  | { kind: 'already-confirmed'; paymentId: string | null; amountCents: number }
  | { kind: 'in-progress' }
  | { kind: 'not-payable'; status: string }
  | { kind: 'empty' };

/**
 * POST /reservations/:id/pay
 *
 * THE CENTRAL PROBLEM: the money and the seats live in two different systems,
 * and there is no transaction spanning both.
 *
 * Everything below follows from that. The endpoint is split into three phases
 * with a COMMIT between each, because the alternative — one transaction wrapped
 * around the provider call — is wrong in two independent ways:
 *
 *   1. It holds a row lock across a network call. Every other buyer for the same
 *      ticket type queues behind it, so throughput becomes one sale per provider
 *      round trip.
 *   2. It is a lie. The database can roll back; the charge cannot. A rollback
 *      after a successful charge leaves the customer paid and seatless, with no
 *      record on this side that it ever happened.
 *
 * So:
 *
 *   PHASE 1  commit the INTENT to charge   (transaction, no network)
 *   PHASE 2  charge                         (network, NO transaction)
 *   PHASE 3  commit the RESULT              (transaction, no network)
 *
 * A crash between any two phases leaves a state that is recoverable, because
 * phase 1 wrote down that a charge was about to happen and what it was for. That
 * is the only reason reconciliation is possible at all.
 *
 * WHY THERE IS NO IDEMPOTENCY-KEY HEADER HERE, unlike POST /reservations.
 *
 * The state machine already provides what the key would. Phase 1 is a guarded
 * transition — HELD -> PENDING_PAYMENT — and only one caller can win it. A
 * second concurrent /pay finds the reservation is no longer HELD and is told so,
 * rather than starting a second charge. An idempotency key would add a second,
 * weaker mechanism guarding the same thing, with the extra hazard that its lease
 * has to outlive the provider timeout or an expiring lease would authorise
 * exactly the double charge it exists to prevent. Where a lease can expire, a
 * status cannot.
 */
pay.post('/:id/pay', requireAuth, ah(async (req, res) => {
  const { tenantId, userId } = req.auth!;
  const reservationId = req.params['id']!;

  // -------------------------------------------------------------------------
  // PHASE 1 — commit the intent to charge, before touching the network.
  // -------------------------------------------------------------------------
  let intent: Intent;
  try {
    intent = await withTenantRetry(tenantId, async (tx): Promise<Intent> => {
      // Every stamp on this path is the database's clock, the one the reconciler
      // and the webhook compare with and stamp by. See databaseNow.
      const now = await databaseNow(tx);

      /**
       * The guarded transition. `status: 'HELD'` is the mutual exclusion.
       *
       * `userId` is in the WHERE clause and is NOT optional. Row-level security
       * scopes this transaction to a TENANT, not to a user — every policy in this
       * database reads app.tenant_id and nothing else. Without this filter any
       * authenticated member of the tenant could pay for, and thereby confirm,
       * somebody else's reservation. RLS would permit it, because as far as RLS is
       * concerned the row belongs to the right tenant.
       *
       * Note what is NOT checked: expiry. A hold whose expiresAt has passed but
       * which the sweeper has not reached yet is still HELD, its seats are still
       * held, and nobody else can have them — so charging for it is correct. And
       * it is race-free rather than merely tolerated: the sweeper's release is
       * guarded by `AND status = 'HELD'` too, so exactly one of the two wins and
       * the loser sees rowCount 0. Adding an expiry check would buy nothing and
       * make the endpoint sensitive to clock skew.
       */
      const moved = await tx.reservation.updateMany({
        where: { id: reservationId, userId, status: 'HELD' },
        data: {
          status: 'PENDING_PAYMENT',
          paymentStartedAt: now,
          version: { increment: 1 },
        },
      });

      if (moved.count === 0) {
        // Disambiguate INSIDE the transaction, on tx. Using the bare client would
        // escape tenant context and turn this into a cross-tenant existence
        // oracle. The userId filter is repeated for the same reason as above:
        // without it this read would confirm the existence of another user's
        // reservation.
        const current = await tx.reservation.findFirst({
          where: { id: reservationId, userId },
          select: { status: true },
        });

        if (!current) return { kind: 'not-found' };

        if (current.status === 'CONFIRMED') {
          // A retry of a request that already succeeded. Answer it with the
          // original result instead of an error — the caller cannot distinguish a
          // lost response from a failure, and telling them "conflict" would invite
          // them to try to pay again for something already paid for.
          const settled = await tx.payment.findFirst({
            where: { reservationId, status: 'SUCCEEDED' },
            select: { id: true, amountCents: true },
            orderBy: { settledAt: 'desc' },
          });
          return {
            kind: 'already-confirmed',
            paymentId: settled?.id ?? null,
            amountCents: settled?.amountCents ?? 0,
          };
        }

        if (current.status === 'PENDING_PAYMENT') return { kind: 'in-progress' };
        return { kind: 'not-payable', status: current.status };
      }

      const items = await tx.reservationItem.findMany({
        where: { reservationId },
        select: { ticketTypeId: true, quantity: true, unitPriceCents: true },
        // Sorted for the same reason as everywhere else in this service: phase 3
        // updates these rows, and every writer must take them in the same order or
        // two concurrent settlements deadlock.
        orderBy: { ticketTypeId: 'asc' },
      });

      if (items.length === 0) return { kind: 'empty' };

      /**
       * The amount comes from the RESERVATION, never from TicketType.
       *
       * unitPriceCents was captured when the hold was taken. Reading the live
       * price here would charge whatever the organiser last set, which may have
       * moved while the buyer was in checkout — they would be charged a number
       * they never agreed to, and the receipt would look perfectly consistent.
       */
      const amountCents = items.reduce((sum, i) => sum + i.quantity * i.unitPriceCents, 0);

      const reservation = await tx.reservation.findFirstOrThrow({
        where: { id: reservationId },
        select: { eventId: true },
      });

      /**
       * Is the event still on sale?
       *
       * POST /reservations checks this before taking a hold. /pay checked it
       * NOWHERE — a grep for eventProjection found it read in exactly one place
       * outside the consumer. So an event cancelled AFTER a hold was taken was
       * still chargeable: the organiser calls the event off, the cancellation
       * propagates to the projection, and the buyer completes checkout and is
       * charged for something that is not happening.
       *
       * Taking money is the one step where "is this still real?" has to be asked
       * again, because it is the step that is expensive to undo. A hold costs
       * nothing to abandon; a charge costs a refund and a conversation.
       *
       * THROWN, not returned, so the transaction rolls back and takes the
       * HELD -> PENDING_PAYMENT transition with it. Returning would leave the
       * reservation stuck mid-payment for an event nobody can attend, and the
       * sweeper skips PENDING_PAYMENT by design — those seats would be held until
       * a human noticed. Rolled back, it stays an ordinary hold that expires.
       */
      const projection = await tx.eventProjection.findFirst({
        where: { id: reservation.eventId },
        select: { status: true, deleted: true },
      });

      if (!projection || projection.deleted || projection.status !== 'PUBLISHED') {
        throw Object.assign(new Error('not-on-sale'), {
          kind: 'not-on-sale',
          status: projection?.status ?? 'UNKNOWN',
        });
      }

      /**
       * The Payment row, written BEFORE the provider is called.
       *
       * This is the record that makes a crash recoverable. If the process dies
       * during phase 2, this row is what says a charge was attempted, for how
       * much, and under which idempotency key — without it the reservation would
       * sit in PENDING_PAYMENT with nothing to reconcile against, and the only
       * options would be to release seats that may have been paid for or to hold
       * them forever.
       *
       * Its id is the provider idempotency key, which is why there is one row per
       * ATTEMPT rather than one per reservation.
       */
      const payment = await tx.payment.create({
        data: {
          tenantId, reservationId, userId, amountCents, currency: CURRENCY, status: 'PENDING',
          // Both default to now(), but Prisma fills that default from this process's
          // clock, and the reconciler selects on both against the database's.
          startedAt: now,
          nextLookupAt: now,
        },
        select: { id: true },
      });

      return {
        kind: 'ready',
        paymentId: payment.id,
        eventId: reservation.eventId,
        amountCents,
        lines: items,
      };
    });
  } catch (err) {
    /**
     * The on-sale gate signals by THROWING, so the whole phase-1 transaction
     * rolls back and the reservation stays an ordinary HELD hold rather than
     * being stranded in PENDING_PAYMENT for an event nobody can attend.
     * Translating it back into a response happens here, outside the
     * transaction -- the same shape POST /reservations uses for its inventory
     * guard.
     */
    if ((err as { kind?: string }).kind === 'not-on-sale') {
      return res.status(409).json({
        error: `event is not on sale (status ${(err as { status?: string }).status}) and cannot be paid for`,
        reservationId,
      });
    }
    throw err;
  }

  switch (intent.kind) {
    case 'not-found':
      return res.status(404).json({ error: 'reservation not found' });
    case 'already-confirmed':
      return res.status(200).json({
        reservationId,
        status: 'CONFIRMED',
        paymentId: intent.paymentId,
        amountCents: intent.amountCents,
        currency: CURRENCY,
        replay: true,
      });
    case 'in-progress':
      return res.status(409).json({
        error: 'a payment for this reservation is already in progress',
        reservationId,
      });
    case 'not-payable':
      return res.status(409).json({
        error: `reservation is ${intent.status} and cannot be paid for`,
        status: intent.status,
      });
    case 'empty':
      return res.status(409).json({ error: 'reservation has no items' });
    default:
      break;
  }

  // -------------------------------------------------------------------------
  // PHASE 2 — the charge. Outside every transaction, by construction: `intent`
  // above has already been committed and there is no open tx at this point.
  // -------------------------------------------------------------------------
  const outcome = await provider.charge({
    idempotencyKey: intent.paymentId,
    amountCents: intent.amountCents,
    currency: CURRENCY,
  });

  // -------------------------------------------------------------------------
  // PHASE 3 — record what happened.
  // -------------------------------------------------------------------------
  if (outcome.status === 'unknown') {
    /**
     * The case everything else is built around.
     *
     * The provider did not answer. The money may have moved. NOTHING is
     * released and NOTHING is confirmed: the reservation stays
     * PENDING_PAYMENT, so the sweeper leaves its seats alone, and the Payment
     * is marked UNKNOWN with settledAt still null, so it appears in the
     * reconciliation index.
     *
     * Marking it FAILED here would be the expensive mistake: the seats would go
     * back on sale and be resold while the customer's card had in fact been
     * charged.
     */
    const marked = await withTenantRetry(tenantId, async (tx) => {
      const settledPayment = await tx.payment.updateMany({
        where: { id: intent.paymentId, status: 'PENDING' },
        data: { status: 'UNKNOWN', failureReason: outcome.reason },
      });
      return settledPayment.count === 1;
    });

    if (!marked) {
      // Someone else settled this attempt while the provider was not answering
      // us. Nothing was written, and telling the caller their payment "is being
      // confirmed" would point them at an attempt that is already closed.
      return res.status(409).json({
        reservationId,
        paymentId: intent.paymentId,
        error: 'this payment was already resolved by another process',
      });
    }

    return res.status(202).json({
      reservationId,
      status: 'PENDING',
      paymentId: intent.paymentId,
      message: 'the payment provider did not respond; this payment is being confirmed',
      reason: outcome.reason,
    });
  }

  if (outcome.status === 'failed') {
    /**
     * A definite no. The seats go back to being a plain hold rather than being
     * released outright — a declined card is usually followed by a second card,
     * and cancelling the reservation would make the buyer re-race for seats they
     * already had.
     *
     * paymentStartedAt is cleared because it means "a charge is in flight", and
     * one is not.
     *
     * The hold clock is NOT extended. The original expiresAt still stands, so a
     * failed payment cannot be used to hold seats indefinitely by retrying.
     */
    const released = await withTenantRetry(tenantId, async (tx) => {
      const now = await databaseNow(tx);
      const settledPayment = await tx.payment.updateMany({
        where: { id: intent.paymentId, status: 'PENDING' },
        data: { status: 'FAILED', failureReason: outcome.reason, settledAt: now },
      });

      /**
       * THE GATE, and the reason it has to be here.
       *
       * The reservation UPDATE below is guarded on `status: 'PENDING_PAYMENT'`
       * and NOTHING ELSE. Nothing in the schema ties a PENDING_PAYMENT
       * reservation to the attempt that owns it — paymentStartedAt is a bare
       * timestamp. So without this gate, a superseded attempt drives another
       * attempt's state machine:
       *
       *   1. The webhook settles attempt P1 as FAILED and returns R to HELD.
       *   2. The buyer clicks pay again. P2 opens, R -> PENDING_PAYMENT.
       *   3. P1's late reply finally arrives here. The payment UPDATE matches
       *      zero rows — P1 is already FAILED — and used to be discarded.
       *   4. The reservation UPDATE then matches P2's row and flips R back to
       *      HELD while P2's charge is still live.
       *
       * The seats are now held rather than pending, so the sweeper may expire
       * and resell them under a live charge; and a third /pay can win phase 1
       * and charge the same buyer again.
       *
       * A rowCount of zero here means this request no longer owns the
       * reservation, so it must not touch it. settleAttempt already works this
       * way; this was the one copy that did not.
       */
      if (settledPayment.count === 0) return false;

      await tx.reservation.updateMany({
        where: { id: reservationId, status: 'PENDING_PAYMENT' },
        data: { status: 'HELD', paymentStartedAt: null, version: { increment: 1 } },
      });

      return true;
    });

    if (!released) {
      return res.status(409).json({
        reservationId,
        paymentId: intent.paymentId,
        error: 'this payment was already resolved by another process',
      });
    }

    return res.status(402).json({
      reservationId,
      status: 'HELD',
      paymentId: intent.paymentId,
      error: 'payment was declined',
      reason: outcome.reason,
    });
  }

  // Success.
  const settled = await withTenantRetry(tenantId, async (tx) => {
    const now = await databaseNow(tx);

    /**
     * The rowCount here is NOT decoration, and discarding it was a bug.
     *
     * Between phase 1 and here, the webhook or the reconciler may have settled
     * this same attempt — they resolve the identical fact by different routes.
     * If they did, this matches zero rows, and everything below is then
     * operating on a reservation this request did not transition.
     */
    const recorded = await tx.payment.updateMany({
      where: { id: intent.paymentId, status: 'PENDING' },
      data: { status: 'SUCCEEDED', providerRef: outcome.providerRef, settledAt: now },
    });

    if (recorded.count === 0) {
      /**
       * We no longer own this attempt, so we must not act on its outcome.
       *
       * The reservation UPDATE below is guarded on PENDING_PAYMENT alone, with
       * nothing tying it to intent.paymentId. If a retry has already opened a
       * SECOND attempt, that guard matches the second attempt's row — and this
       * request would confirm it, move held -> sold, and emit tickets.purchased
       * carrying a paymentId that settled nothing. Returning early is the only
       * safe move; the truth is read below.
       */
      const [reservation, payment] = await Promise.all([
        tx.reservation.findFirst({ where: { id: reservationId }, select: { status: true } }),
        tx.payment.findFirst({ where: { id: intent.paymentId }, select: { status: true } }),
      ]);

      return {
        alreadySettled: true,
        reservationStatus: reservation?.status ?? null,
        paymentStatus: payment?.status ?? null,
        // Our provider call said SUCCEEDED and the stored attempt says FAILED.
        // Two sources disagree about whether money moved.
        discrepancy: payment?.status === 'FAILED',
      };
    }

    /**
     * Guarded, like every other transition here. If reconciliation or another
     * process already confirmed this reservation, rowCount is 0 and the
     * inventory move below must not happen — moving held -> sold twice satisfies
     * both TicketType CHECK constraints (remaining + held + sold stays equal to
     * total) while silently corrupting the counters, exactly as a double release
     * would.
     */
    const confirmed = await tx.reservation.updateMany({
      where: { id: reservationId, status: 'PENDING_PAYMENT' },
      data: { status: 'CONFIRMED', confirmedAt: now, version: { increment: 1 } },
    });

    if (confirmed.count === 0) {
      /**
       * Somebody else resolved this attempt first. WHICH WAY they resolved it is
       * not knowable from a rowCount of zero, so read it rather than assume.
       *
       * This used to `return { alreadySettled: true }` and the handler answered
       * 200 with status CONFIRMED regardless. That is a lie in the case that
       * matters: the reconciler can conclude the charge never arrived, return
       * the reservation to HELD, and then this request's own late provider
       * response says succeeded. Both guarded updates match nothing, and the
       * buyer is told they are confirmed while holding a reservation that will
       * simply expire.
       */
      const [reservation, payment] = await Promise.all([
        tx.reservation.findFirst({ where: { id: reservationId }, select: { status: true } }),
        tx.payment.findFirst({ where: { id: intent.paymentId }, select: { status: true, providerRef: true } }),
      ]);

      return {
        alreadySettled: true,
        reservationStatus: reservation?.status ?? null,
        paymentStatus: payment?.status ?? null,
        /**
         * Reaching here means we DID record the charge as SUCCEEDED — the early
         * return above covers the other case — but the reservation was no longer
         * awaiting payment, so nobody will confirm it.
         *
         * That is money taken for seats not allocated, which is the worst state
         * this endpoint can produce and the one the CI invariant looks for:
         * no SUCCEEDED payment may point at a non-CONFIRMED reservation.
         * The transaction is not rolled back, because the charge is real and
         * deleting the record of it would make the money untraceable.
         */
        discrepancy: reservation?.status !== 'CONFIRMED',
      };
    }

    for (const line of intent.lines) {
      // `held: { gte: quantity }` is a guard, not decoration: it makes an
      // impossible state fail loudly instead of driving held negative.
      const moved = await tx.ticketType.updateMany({
        where: { id: line.ticketTypeId, held: { gte: line.quantity } },
        data: {
          held: { decrement: line.quantity },
          sold: { increment: line.quantity },
          version: { increment: 1 },
        },
      });

      if (moved.count === 0) {
        throw new Error(
          `cannot settle ${reservationId}: ticket type ${line.ticketTypeId} does not hold ` +
            `${line.quantity} seats to sell`,
        );
      }
    }

    const occurredAtItems = intent.lines.map((l) => ({
      ticketTypeId: l.ticketTypeId,
      quantity: l.quantity,
      unitPriceCents: l.unitPriceCents,
    }));

    /**
     * TWO messages, in the same transaction, on purpose.
     *
     * tickets.reserved carries the seat-state transition, keyed by event, so a
     * consumer building live availability sees CONFIRMED on the same partition
     * and in order relative to the hold it confirms.
     *
     * tickets.purchased carries the money. It is separate because seat state and
     * revenue have different audiences, and putting amounts on the seat-state
     * topic hands every availability consumer financial data it has no business
     * reading.
     *
     * They cannot be one message on one topic without choosing which of those
     * two properties to give up.
     */
    await emit(tx, {
      tenantId,
      topic: 'tickets.reserved',
      eventType: 'reservation.confirmed',
      aggregateId: intent.eventId,
      payload: {
        reservationId,
        tenantId,
        eventId: intent.eventId,
        userId,
        state: 'CONFIRMED',
        items: intent.lines.map((l) => ({ ticketTypeId: l.ticketTypeId, quantity: l.quantity })),
        expiresAt: null,
      },
    });

    await emit(tx, {
      tenantId,
      topic: 'tickets.purchased',
      eventType: 'tickets.purchased',
      aggregateId: intent.eventId,
      payload: {
        paymentId: intent.paymentId,
        reservationId,
        tenantId,
        eventId: intent.eventId,
        userId,
        amountCents: intent.amountCents,
        currency: CURRENCY,
        items: occurredAtItems,
        providerRef: outcome.providerRef,
      },
    });

    return { alreadySettled: false };
  });

  /**
   * Report what actually happened, not what this request hoped would happen.
   *
   * Only a reservation this call confirmed — or that it found already CONFIRMED
   * — may be answered 200 CONFIRMED. Anything else means another actor resolved
   * the attempt differently, and telling the buyer "CONFIRMED" would hand them a
   * receipt for seats they do not have.
   */
  if (settled.alreadySettled && settled.reservationStatus !== 'CONFIRMED') {
    if (settled.discrepancy) {
      // Our provider call returned SUCCEEDED and the stored attempt says FAILED.
      // One of the two is wrong about whether money moved, and nothing here can
      // decide which. Loud, because the resolution is a refund or a manual sale.
      logger.error(
        {
          reservationId,
          paymentId: intent.paymentId,
          providerRef: outcome.providerRef,
          paymentStatus: settled.paymentStatus,
          reservationStatus: settled.reservationStatus,
        },
        'PAYMENT DISCREPANCY: the provider answered succeeded but the reservation was not confirmed',
      );
    }

    return res.status(409).json({
      reservationId,
      status: settled.reservationStatus,
      paymentId: intent.paymentId,
      error: 'this payment was resolved by another process and did not confirm the reservation',
    });
  }

  return res.status(200).json({
    reservationId,
    status: 'CONFIRMED',
    paymentId: intent.paymentId,
    amountCents: intent.amountCents,
    currency: CURRENCY,
    providerRef: outcome.providerRef,
    replay: settled.alreadySettled,
  });
}));
