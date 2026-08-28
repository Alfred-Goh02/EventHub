/**
 * The payment provider boundary.
 *
 * There is no real provider here, and the shape of this interface is the whole
 * point of the chapter rather than a placeholder for one. Three things about it
 * are true of Stripe, Adyen, and every other provider, and each one forces a
 * design decision elsewhere in the service:
 *
 *   1. It is a NETWORK CALL, so it must not happen inside a transaction. A
 *      reservation holds a row lock on a TicketType until COMMIT; awaiting a
 *      provider inside that window turns a millisecond lock into a multi-second
 *      one and collapses throughput for that ticket type to one sale per round
 *      trip.
 *
 *   2. It can FAIL TO ANSWER. Not fail — fail to answer. The request may have
 *      succeeded, and the reply may have been lost. That is why `unknown` is a
 *      first-class outcome here rather than something folded into `failed`:
 *      treating "I do not know" as "no" is how a customer gets charged for a
 *      seat that was then given to someone else.
 *
 *   3. It needs an IDEMPOTENCY KEY, and the key must be unique per ATTEMPT, not
 *      per reservation. A key derived from the reservation would make every
 *      retry after a failure collide with the original charge and replay its
 *      result forever, so a buyer whose first card was declined could never pay.
 *      The key is the Payment row id, which is why that row is written before
 *      this is ever called.
 */

export type ChargeOutcome =
  | { status: 'succeeded'; providerRef: string }
  | { status: 'failed'; reason: string }
  /**
   * The provider did not tell us. A timeout, a 5xx, a dropped connection.
   *
   * The money may or may not have moved, and nothing on this side can tell the
   * difference. The only correct response is to keep the seats held, leave the
   * attempt unsettled, and let reconciliation ASK the provider later.
   */
  | { status: 'unknown'; reason: string };

export interface ChargeRequest {
  /** The Payment row id. Unique per attempt — see above. */
  idempotencyKey: string;
  amountCents: number;
  currency: string;
}

export interface PaymentProvider {
  charge(req: ChargeRequest): Promise<ChargeOutcome>;

  /**
   * Ask the provider what became of an attempt.
   *
   * This is the half of the interface that makes `unknown` recoverable rather
   * than merely recorded. Reconciliation must ASK; it must never guess. A job
   * that assumed unanswered charges had failed would release seats that had
   * genuinely been paid for, which is the same bug as overselling with an extra
   * step and a refund.
   *
   * Read-only and therefore safe to run twice, which is why reconciliation needs
   * no claim marker: two reconcilers may both ask, and the guarded UPDATE that
   * follows decides which one settles the row.
   *
   * The amount is passed because this fake derives its answer from it. A real
   * implementation looks the charge up by the idempotency key alone.
   */
  lookup(req: ChargeRequest): Promise<ChargeOutcome>;
}

/**
 * A fake provider whose behaviour is chosen by the AMOUNT.
 *
 * Deliberately not chosen by a request header or a test-only route. A backdoor
 * in the request path is a backdoor in production too, and "it is only enabled
 * in test" is a claim about configuration, not about code. Keying off the
 * amount keeps every test hook inside this file, where the real implementation
 * will simply replace it.
 *
 * The convention is borrowed from providers that do exactly this: Stripe's test
 * card numbers pick an outcome the same way.
 *
 *   ...01  ->  declined
 *   ...02  ->  no answer, and the charge DID go through
 *   ...03  ->  no answer, and the charge never arrived
 *   else   ->  succeeds
 *
 * 02 and 03 are indistinguishable at charge time — that is the entire point.
 * Both return `unknown`, and only lookup() can tell them apart. A design that
 * could tell them apart from the client side would not need reconciliation.
 */
export class FakePaymentProvider implements PaymentProvider {
  constructor(private readonly latencyMs = 0) {}

  async charge(req: ChargeRequest): Promise<ChargeOutcome> {
    if (this.latencyMs > 0) {
      await new Promise((r) => setTimeout(r, this.latencyMs));
    }

    const marker = req.amountCents % 100;

    if (marker === 1) {
      return { status: 'failed', reason: 'card_declined' };
    }
    if (marker === 2 || marker === 3) {
      return { status: 'unknown', reason: 'provider_timeout' };
    }

    // The reference is derived from the idempotency key rather than random, so
    // that charging the same attempt twice would produce the same reference and
    // a duplicate is recognisable rather than merely suspected.
    return { status: 'succeeded', providerRef: `fake_ch_${req.idempotencyKey}` };
  }

  async lookup(req: ChargeRequest): Promise<ChargeOutcome> {
    const marker = req.amountCents % 100;

    // The money moved; the reply was lost. Reconciliation must finish the sale.
    if (marker === 2) {
      return { status: 'succeeded', providerRef: `fake_ch_${req.idempotencyKey}` };
    }
    // The request never landed. Reconciliation must return the hold.
    if (marker === 3) {
      return { status: 'failed', reason: 'no_such_charge' };
    }
    if (marker === 1) {
      return { status: 'failed', reason: 'card_declined' };
    }
    return { status: 'succeeded', providerRef: `fake_ch_${req.idempotencyKey}` };
  }
}

/**
 * Wraps a provider with a hard timeout.
 *
 * Without it, a provider that never answers holds the request open forever, the
 * reservation stays PENDING_PAYMENT, and its seats are held by a request nobody
 * is waiting on any more. The timeout converts that into an explicit `unknown`,
 * which the endpoint knows how to record and reconciliation knows how to
 * resolve.
 *
 * The distinction matters: a timeout here does NOT cancel the charge. The
 * provider may still be processing it. That is precisely why the outcome is
 * `unknown` and not `failed`.
 */
export function withTimeout(provider: PaymentProvider, ms: number): PaymentProvider {
  return {
    async charge(req) {
      let timer: NodeJS.Timeout | undefined;
      const timeout = new Promise<ChargeOutcome>((resolve) => {
        timer = setTimeout(() => resolve({ status: 'unknown', reason: 'client_timeout' }), ms);
      });

      try {
        return await Promise.race([provider.charge(req), timeout]);
      } finally {
        if (timer) clearTimeout(timer);
      }
    },

    // A lookup that times out is also `unknown` — the attempt simply stays
    // unsettled and the next reconciliation pass asks again. Nothing is
    // released and nothing is confirmed on a question we could not get an
    // answer to.
    async lookup(req) {
      let timer: NodeJS.Timeout | undefined;
      const timeout = new Promise<ChargeOutcome>((resolve) => {
        timer = setTimeout(() => resolve({ status: 'unknown', reason: 'client_timeout' }), ms);
      });

      try {
        return await Promise.race([provider.lookup(req), timeout]);
      } finally {
        if (timer) clearTimeout(timer);
      }
    },
  };
}
