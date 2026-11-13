import { describe, expect, it } from "vitest";
import { formatMoney } from "@/lib/format";
import { errorFromBody } from "@/api/errors";
import { keyFor } from "@/lib/reserve";

describe("money", () => {
  it("shows 2500 cents as S$25.00", () => expect(formatMoney(2500)).toBe("S$25.00"));
});

describe("error shapes", () => {
  it("reads string error", () =>
    expect(errorFromBody(409, { error: "event is not on sale" }, null, null).message).toBe(
      "event is not on sale",
    ));
  it("reads gateway message", () =>
    expect(errorFromBody(404, { message: "Not Found" }, "r1", null).requestId).toBe("r1"));
  it("reads field errors", () =>
    expect(
      errorFromBody(400, { error: { formErrors: [], fieldErrors: { email: ["bad"] } } }, null, null)
        .fieldErrors["email"],
    ).toEqual(["bad"]));
});

describe("idempotency key", () => {
  const body = { eventId: "e", items: [{ ticketTypeId: "a", quantity: 1 }] };
  it("reuses the key for the same cart", () => {
    const k = keyFor(body, null);
    expect(keyFor(body, k).key).toBe(k.key);
  });
  it("uses a new key when the cart changes", () => {
    const k = keyFor(body, null);
    expect(keyFor({ ...body, items: [{ ticketTypeId: "a", quantity: 2 }] }, k).key).not.toBe(k.key);
  });
});
