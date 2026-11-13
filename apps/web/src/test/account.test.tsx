import { QueryClient } from "@tanstack/react-query";
import { createRouter, rootRouteId } from "@tanstack/react-router";
import { renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { httpApi } from "@/api/http";
import { clearTokens, writeTokens } from "@/api/tokens";
import { linkSearch, passwordProblem, useLinkToken } from "@/lib/account";
import { routeTree } from "@/routeTree.gen";

const b64url = (o: object) =>
  btoa(JSON.stringify(o)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

afterEach(() => {
  vi.unstubAllGlobals();
  clearTokens();
});

describe("a link's token", () => {
  it("is kept by the page after the address bar loses it", () => {
    const clear = vi.fn();
    const { result, rerender } = renderHook(({ fromUrl }) => useLinkToken(fromUrl, clear), {
      initialProps: { fromUrl: "tok_123" as string | undefined },
    });
    expect(result.current).toBe("tok_123");
    // The page asked for the URL without it...
    expect(clear).toHaveBeenCalledTimes(1);
    // ...and once the URL has none, the page still holds it, and asks for nothing more.
    rerender({ fromUrl: undefined });
    expect(result.current).toBe("tok_123");
    expect(clear).toHaveBeenCalledTimes(1);
  });

  it("is absent when the page was opened without one, and nothing is cleared", () => {
    const clear = vi.fn();
    const { result } = renderHook(() => useLinkToken(undefined, clear));
    expect(result.current).toBeNull();
    expect(clear).not.toHaveBeenCalled();
  });

  it("is read from the query string only as a non-empty string", () => {
    expect(linkSearch({ token: "abc" })).toEqual({ token: "abc" });
    expect(linkSearch({ token: "" })).toEqual({});
    expect(linkSearch({ token: ["a", "b"] })).toEqual({});
    expect(linkSearch({ token: 5, other: "x" })).toEqual({});
  });
});

describe("a new password", () => {
  it("needs Auth's eight characters and a matching confirmation", () => {
    expect(passwordProblem("short", "short")).toMatch(/at least 8/);
    expect(passwordProblem("longenough", "longenougH")).toMatch(/don't match/);
    expect(passwordProblem("longenough", "longenough")).toBeNull();
  });
});

describe("the account calls", () => {
  it("prove themselves by the link's token alone, never by the session in this browser", async () => {
    // Signed in as someone, as a person who opens an invitation in a shared browser may be.
    const now = Math.floor(Date.now() / 1000);
    const claims = { sub: "u1", tenantSlug: "acme", role: "ADMIN", iat: now, exp: now + 900 };
    writeTokens({
      accessToken: `${b64url({ alg: "RS256" })}.${b64url(claims)}.sig`,
      refreshToken: "r1",
    });
    const sent: { path: string; auth: string | null; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const headers = new Headers(init?.headers);
        sent.push({
          path: new URL(String(input)).pathname,
          auth: headers.get("Authorization"),
          body: JSON.parse(String(init?.body)),
        });
        const reply = sent.length === 3 ? { id: "u2", email: "b@x.com", role: "ATTENDEE" } : {};
        return new Response(JSON.stringify(reply), { status: sent.length === 3 ? 201 : 202 });
      }),
    );

    await httpApi.forgotPassword("a@x.com");
    await httpApi.resetPassword("reset_tok", "newpassword");
    const created = await httpApi.register("invite_tok", "newpassword");

    expect(sent).toEqual([
      { path: "/auth/forgot-password", auth: null, body: { email: "a@x.com" } },
      {
        path: "/auth/reset-password",
        auth: null,
        body: { token: "reset_tok", password: "newpassword" },
      },
      {
        path: "/auth/register",
        auth: null,
        body: { token: "invite_tok", password: "newpassword" },
      },
    ]);
    expect(created.email).toBe("b@x.com");
  });
});

describe("the account pages", () => {
  it.each(["/forgot-password", "/reset-password?token=t", "/accept-invite?token=t"])(
    "%s is a page, not the not-found fallback",
    (href) => {
      const router = createRouter({ routeTree, context: { queryClient: new QueryClient() } });
      const path = href.split("?")[0] ?? href;
      const matches = router.matchRoutes(path);
      expect(matches.at(-1)?.routeId).not.toBe(rootRouteId);
      expect(matches.at(-1)?.routeId).toBe(path);
    },
  );
});
