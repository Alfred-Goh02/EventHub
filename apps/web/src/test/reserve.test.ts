import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, type CreateReservationBody } from "@/api";
import { mockApi } from "@/api/mock";
import { reserve, ReservationAbandoned, type KeyState } from "@/lib/reserve";

const priceOrder: CreateReservationBody = {
  eventId: "e1",
  items: [
    { ticketTypeId: "tt_b", quantity: 1 },
    { ticketTypeId: "tt_a", quantity: 2 },
  ],
};
const otherOrder: CreateReservationBody = { eventId: "e1", items: [...priceOrder.items].reverse() };

afterEach(() => vi.restoreAllMocks());

describe("reserving", () => {
  it("sends the same cart as the same body under the same key, whatever order the page lists it in", async () => {
    const sent: { body: CreateReservationBody; key: string }[] = [];
    vi.spyOn(mockApi, "createReservation").mockImplementation(async (body, key) => {
      sent.push({ body, key });
      // The first answer is lost on the way back; the retry must replay it exactly.
      if (sent.length === 1) throw new ApiError({ status: 0, message: "network" });
      return {
        id: "r1",
        eventId: "e1",
        status: "HELD",
        expiresAt: null as never,
        items: body.items,
      };
    });
    const state: { current: KeyState | null } = { current: null };
    await expect(reserve(priceOrder, state)).rejects.toBeInstanceOf(ApiError);
    await reserve(otherOrder, state);

    expect(sent).toHaveLength(2);
    expect(sent[1]?.key).toBe(sent[0]?.key);
    expect(JSON.stringify(sent[1]?.body)).toBe(JSON.stringify(sent[0]?.body));
    expect(sent[0]?.body.items.map((i) => i.ticketTypeId)).toEqual(["tt_a", "tt_b"]);
  });

  it("stops retrying once the page that asked has gone", async () => {
    vi.useFakeTimers();
    const calls = vi
      .spyOn(mockApi, "createReservation")
      .mockRejectedValue(
        new ApiError({ status: 503, message: "please retry", retryAfterSeconds: 2 }),
      );
    const leaving = new AbortController();
    const pending = reserve(priceOrder, { current: null }, undefined, leaving.signal);
    const settled = expect(pending).rejects.toBeInstanceOf(ReservationAbandoned);
    await vi.advanceTimersByTimeAsync(10);
    leaving.abort();
    await vi.advanceTimersByTimeAsync(10_000);
    await settled;
    expect(calls).toHaveBeenCalledTimes(1);
    vi.useRealTimers();
  });
});
