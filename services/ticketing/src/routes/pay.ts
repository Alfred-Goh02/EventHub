import { Router } from 'express';
import { withTenantRetry } from '../db.js';
import { ah } from '../async.js';
import { requireAuth } from '../auth.js';
import { emit } from '../outbox.js';
import { FakePaymentProvider, withTimeout, type PaymentProvider } from '../payments/provider.js';

export const pay: Router = Router();

/**
 * How long to wait for the provider before calling it an unknown.
 *
 * This is the number the whole reconciliation design is sized against: it
 * bounds how long a reservation can sit in PENDING_PAYMENT because of a single
 * request, and therefore how long its seats are unavailable to anyone else.
 */
const PROVIDER_TIMEOUT_MS = Number(process.env['PAYMENT_TIMEOUT_MS'] ?? 8000);

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
  const intent = await withTenantRetry(tenantId, async (tx): Promise<Intent> => {
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
        paymentStartedAt: new Date(),
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
      data: { tenantId, reservationId, userId, amountCents, currency: CURRENCY, status: 'PENDING' },
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
    await withTenantRetry(tenantId, async (tx) => {
      await tx.payment.updateMany({
        where: { id: intent.paymentId, status: 'PENDING' },
        data: { status: 'UNKNOWN', failureReason: outcome.reason },
      });
    });

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
    await withTenantRetry(tenantId, async (tx) => {
      await tx.payment.updateMany({
        where: { id: intent.paymentId, status: 'PENDING' },
        data: { status: 'FAILED', failureReason: outcome.reason, settledAt: new Date() },
      });
      await tx.reservation.updateMany({
        where: { id: reservationId, status: 'PENDING_PAYMENT' },
        data: { status: 'HELD', paymentStartedAt: null, version: { increment: 1 } },
      });
    });

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
    await tx.payment.updateMany({
      where: { id: intent.paymentId, status: 'PENDING' },
      data: { status: 'SUCCEEDED', providerRef: outcome.providerRef, settledAt: new Date() },
    });

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
      data: { status: 'CONFIRMED', confirmedAt: new Date(), version: { increment: 1 } },
    });

    if (confirmed.count === 0) return { alreadySettled: true };

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
