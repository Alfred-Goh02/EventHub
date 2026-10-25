import { api, ApiError, type CreateReservationBody, type CreatedReservation } from "@/api";

const CATCH_UP = "this service has not caught up with recent events yet; please retry";

export interface KeyState {
  sig: string;
  key: string;
}

/**
 * The body exactly as it is sent: items in ticket type order.
 *
 * The server remembers an Idempotency-Key with a hash of the JSON body it arrived with. The
 * page lists ticket types by price, so the same cart could be sent with its items in another
 * order after a price change -- the same key with a different body, answered 422, and then a
 * second hold under a new key. One order, always.
 */
export const canonicalBody = (b: CreateReservationBody): CreateReservationBody => ({
  eventId: b.eventId,
  items: [...b.items]
    .sort((a, z) => a.ticketTypeId.localeCompare(z.ticketTypeId))
    .map((i) => ({ ticketTypeId: i.ticketTypeId, quantity: i.quantity })),
});

export const cartSignature = (b: CreateReservationBody) => JSON.stringify(canonicalBody(b));

/** Reuses the key for the same cart (retry after network/5xx), new key when the cart changes. */
export function keyFor(body: CreateReservationBody, prev: KeyState | null): KeyState {
  const sig = cartSignature(body);
  return prev && prev.sig === sig ? prev : { sig, key: crypto.randomUUID() };
}

export const isRetryableTransport = (e: unknown) =>
  e instanceof ApiError && (e.isNetwork || [502, 503, 504].includes(e.status));

/** Thrown when the page that asked for the reservation has gone. Nothing to show. */
export class ReservationAbandoned extends Error {
  constructor() {
    super("reservation abandoned");
  }
}

function waitFor(ms: number, signal: AbortSignal | undefined) {
  return new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => {
      clearTimeout(t);
      reject(new ReservationAbandoned());
    });
  });
}

export async function reserve(
  body: CreateReservationBody,
  state: { current: KeyState | null },
  onWaiting?: (seconds: number, attempt: number) => void,
  signal?: AbortSignal,
): Promise<CreatedReservation> {
  const sent = canonicalBody(body);
  let current = keyFor(sent, state.current);
  state.current = current;
  let autoRetries = 0;
  let regenerated = false;
  for (;;) {
    // Leaving the page stops the automatic retries: a hold taken after that would land
    // the buyer on a checkout they had already walked away from.
    if (signal?.aborted) throw new ReservationAbandoned();
    try {
      const res = await api().createReservation(sent, current.key);
      state.current = null; // done: next checkout attempt gets a fresh key
      return res;
    } catch (e) {
      if (!(e instanceof ApiError)) throw e;
      const catchingUp =
        e.status === 503 && (e.retryAfterSeconds !== null || e.message === CATCH_UP);
      if (catchingUp && autoRetries < 3) {
        autoRetries++;
        const wait = e.retryAfterSeconds ?? 2;
        onWaiting?.(wait, autoRetries);
        await waitFor(wait * 1000, signal);
        continue;
      }
      if (e.status === 422 && !regenerated) {
        regenerated = true;
        current = { sig: current.sig, key: crypto.randomUUID() };
        state.current = current;
        continue;
      }
      if (!isRetryableTransport(e)) state.current = null;
      throw e;
    }
  }
}
