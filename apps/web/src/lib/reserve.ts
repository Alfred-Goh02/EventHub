import { api, ApiError, type CreateReservationBody, type CreatedReservation } from "@/api";

const CATCH_UP = "this service has not caught up with recent events yet; please retry";

export interface KeyState {
  sig: string;
  key: string;
}

export const cartSignature = (b: CreateReservationBody) =>
  JSON.stringify([
    b.eventId,
    [...b.items].sort((a, z) => a.ticketTypeId.localeCompare(z.ticketTypeId)),
  ]);

/** Reuses the key for the same cart (retry after network/5xx), new key when the cart changes. */
export function keyFor(body: CreateReservationBody, prev: KeyState | null): KeyState {
  const sig = cartSignature(body);
  return prev && prev.sig === sig ? prev : { sig, key: crypto.randomUUID() };
}

export const isRetryableTransport = (e: unknown) =>
  e instanceof ApiError && (e.isNetwork || [502, 503, 504].includes(e.status));

export async function reserve(
  body: CreateReservationBody,
  state: { current: KeyState | null },
  onWaiting?: (seconds: number, attempt: number) => void,
): Promise<CreatedReservation> {
  let current = keyFor(body, state.current);
  state.current = current;
  let autoRetries = 0;
  let regenerated = false;
  for (;;) {
    try {
      const res = await api().createReservation(body, current.key);
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
        await new Promise((r) => setTimeout(r, wait * 1000));
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
