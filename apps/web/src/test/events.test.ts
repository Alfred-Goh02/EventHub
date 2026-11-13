import { afterEach, describe, expect, it, vi } from "vitest";
import { httpApi } from "@/api/http";
import { clearTokens, writeTokens } from "@/api/tokens";

const b64url = (o: object) =>
  btoa(JSON.stringify(o)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

/** A signed-in session whose token is nowhere near its refresh. */
function signIn() {
  const now = Math.floor(Date.now() / 1000);
  const claims = { sub: "u1", tenantSlug: "acme", role: "ATTENDEE", iat: now, exp: now + 900 };
  writeTokens({
    accessToken: `${b64url({ alg: "RS256" })}.${b64url(claims)}.sig`,
    refreshToken: "r1",
  });
}

/** The query string of every request the page sends. */
function captureRequests() {
  const urls: URL[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      urls.push(new URL(String(input)));
      return new Response(JSON.stringify({ data: [], nextCursor: null }), { status: 200 });
    }),
  );
  return urls;
}

afterEach(() => {
  vi.unstubAllGlobals();
  clearTokens();
});

describe("the events list", () => {
  it("asks only for events that have not ended, on the first page and on every page after it", async () => {
    signIn();
    const urls = captureRequests();
    await httpApi.listEvents(null);
    await httpApi.listEvents("cursor-2");

    expect(urls).toHaveLength(2);
    expect(urls.map((u) => u.pathname)).toEqual(["/events", "/events"]);
    // The server's cursor does not carry the filter: a page without it would start
    // listing events that are over.
    expect(urls.map((u) => u.searchParams.get("upcoming"))).toEqual(["true", "true"]);
    expect(urls[1]?.searchParams.get("cursor")).toBe("cursor-2");
  });
});
