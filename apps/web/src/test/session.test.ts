import { describe, expect, it } from "vitest";
import { refreshDueAt } from "@/api/tokens";
import { isMockHostname } from "@/api/env";
import type { AccessClaims } from "@/api";

/** A token as Auth issues it: 15 minutes, both times on the SERVER's clock. */
const issued = (iatSeconds: number): AccessClaims => ({
  sub: "u1",
  tenantSlug: "acme",
  role: "ATTENDEE",
  iat: iatSeconds,
  exp: iatSeconds + 15 * 60,
});

describe("refresh schedule", () => {
  it("counts the token's lifetime from when it arrived, whatever the browser's clock says", () => {
    const serverNow = 1_800_000_000; // seconds
    const claims = issued(serverNow);
    // The browser runs 20 minutes FAST: when the token arrives, its clock already reads
    // past the token's exp. Comparing exp with that clock would refresh at once, forever.
    const receivedAt = (serverNow + 20 * 60) * 1000;
    const due = refreshDueAt(claims, receivedAt);
    expect(due - receivedAt).toBe(14 * 60 * 1000);
  });

  it("works the same with the browser's clock slow", () => {
    const serverNow = 1_800_000_000;
    const receivedAt = (serverNow - 30 * 60) * 1000;
    expect(refreshDueAt(issued(serverNow), receivedAt) - receivedAt).toBe(14 * 60 * 1000);
  });

  it("never schedules a refresh sooner than 30 s after a token arrives", () => {
    const claims: AccessClaims = { ...issued(1_800_000_000), exp: 1_800_000_010 };
    expect(refreshDueAt(claims, 5_000) - 5_000).toBe(30_000);
  });
});

describe("mock mode", () => {
  it("is off on every real host, a fully qualified one with its trailing dot included", () => {
    expect(isMockHostname("acme.eventhub.com")).toBe(false);
    expect(isMockHostname("acme.eventhub.com.")).toBe(false);
    expect(isMockHostname("ACME.EventHub.com")).toBe(false);
    expect(isMockHostname("acme.eventhub.localhost")).toBe(false);
  });

  it("is on anywhere else", () => {
    expect(isMockHostname("id-preview--1234.lovable.app")).toBe(true);
    expect(isMockHostname("localhost")).toBe(true);
    expect(isMockHostname("eventhub.com.evil.example")).toBe(true);
  });
});
