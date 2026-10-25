import type { Reservation } from "@/api";

export type AwaitedPayment = "waiting" | "confirmed" | "failed" | "ended";

/**
 * What a reservation snapshot says about a payment attempt the provider has not answered
 * yet (/pay said 202, with this paymentId).
 *
 * Only a snapshot that already names THIS attempt can answer for it. The reservation the
 * page held when Pay was pressed was HELD, and is still in the cache when the 202 arrives;
 * read as an answer, it says "the payment didn't go through" about a payment that may well
 * be going through, and a buyer told that may pay again for another hold.
 */
export function settleAwaitedPayment(r: Reservation, paymentId: string): AwaitedPayment {
  if (r.payment?.id !== paymentId) return "waiting";
  if (r.status === "CONFIRMED") return "confirmed";
  if (r.status === "EXPIRED" || r.status === "CANCELLED") return "ended";
  if (r.status === "HELD") return "failed";
  return "waiting";
}
