import { describe, expect, it } from "vitest";
import { settleAwaitedPayment } from "@/lib/payment";
import type { Reservation } from "@/api";

const reservation = (over: Partial<Reservation>): Reservation => ({
  id: "r1",
  status: "HELD",
  event: {
    id: "e1",
    title: "Gig",
    slug: "gig",
    startsAt: "2026-11-01T10:00:00.000Z",
    endsAt: "2026-11-01T12:00:00.000Z",
  },
  items: [{ ticketTypeId: "t1", name: "General", quantity: 1, unitPriceCents: 2502 }],
  totalCents: 2502,
  currency: "SGD",
  createdAt: "2026-10-25T10:00:00.000Z",
  holdExpiresAt: "2026-10-25T10:10:00.000Z",
  holdSecondsLeft: 500,
  confirmedAt: null,
  payment: null,
  ...over,
});

const attempt = (status: "PENDING" | "SUCCEEDED" | "FAILED" | "UNKNOWN") => ({
  id: "pay_2",
  status,
  amountCents: 2502,
  currency: "SGD",
});

describe("a payment the provider has not answered yet (202)", () => {
  it("is not settled by the reservation cached from before Pay was pressed", () => {
    // HELD with no payment yet: exactly what the page holds when the 202 arrives.
    expect(settleAwaitedPayment(reservation({ payment: null }), "pay_2")).toBe("waiting");
  });

  it("is not settled by a snapshot that names an earlier attempt", () => {
    const earlier = { ...attempt("FAILED"), id: "pay_1" };
    expect(settleAwaitedPayment(reservation({ payment: earlier }), "pay_2")).toBe("waiting");
  });

  it("keeps waiting while the reservation is PENDING_PAYMENT", () => {
    const r = reservation({ status: "PENDING_PAYMENT", payment: attempt("UNKNOWN") });
    expect(settleAwaitedPayment(r, "pay_2")).toBe("waiting");
  });

  it("is confirmed by a snapshot of this attempt, confirmed", () => {
    const r = reservation({ status: "CONFIRMED", payment: attempt("SUCCEEDED") });
    expect(settleAwaitedPayment(r, "pay_2")).toBe("confirmed");
  });

  it("failed only when a snapshot of this attempt is back to HELD", () => {
    const r = reservation({ status: "HELD", payment: attempt("FAILED") });
    expect(settleAwaitedPayment(r, "pay_2")).toBe("failed");
  });

  it("ended when the hold lapsed", () => {
    const r = reservation({ status: "EXPIRED", payment: attempt("FAILED") });
    expect(settleAwaitedPayment(r, "pay_2")).toBe("ended");
  });
});
