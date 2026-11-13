import { QueryClient } from "@tanstack/react-query";
import { createRouter } from "@tanstack/react-router";
import { afterEach, describe, expect, it, vi } from "vitest";

import { httpApi } from "@/api/http";
import { clearTokens, writeTokens } from "@/api/tokens";
import { authorLabel, commentError } from "@/lib/thread";
import { routeTree } from "@/routeTree.gen";

const b64url = (o: object) =>
  btoa(JSON.stringify(o)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

afterEach(() => {
  vi.unstubAllGlobals();
  clearTokens();
});

describe("the page an emailed reply link opens", () => {
  it("is a page for /posts/<id>, inside the signed-in layout", () => {
    const router = createRouter({ routeTree, context: { queryClient: new QueryClient() } });
    const ids = router.matchRoutes("/posts/3f1c").map((m) => m.routeId);
    expect(ids.at(-1)).toBe("/_app/posts/$postId");
    // Under _app, so a signed-out reader is sent to sign in and brought back.
    expect(ids).toContain("/_app");
  });

  it("reads the post and its thread, and replies to the post or to one comment", async () => {
    const now = Math.floor(Date.now() / 1000);
    const claims = { sub: "u1", tenantSlug: "acme", role: "ATTENDEE", iat: now, exp: now + 900 };
    writeTokens({
      accessToken: `${b64url({ alg: "RS256" })}.${b64url(claims)}.sig`,
      refreshToken: "r1",
    });
    const sent: { method: string; path: string; body: unknown }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        sent.push({
          method: init?.method ?? "GET",
          path: new URL(String(input)).pathname,
          body: init?.body ? JSON.parse(String(init.body)) : null,
        });
        return new Response(JSON.stringify({ id: "c9" }), { status: 200 });
      }),
    );

    await httpApi.getPost("p 1");
    await httpApi.getThread("p 1");
    await httpApi.addComment("p 1", "hello");
    await httpApi.addComment("p 1", "and you", "c1");

    expect(sent).toEqual([
      { method: "GET", path: "/posts/p%201", body: null },
      { method: "GET", path: "/posts/p%201/comments", body: null },
      { method: "POST", path: "/posts/p%201/comments", body: { body: "hello" } },
      { method: "POST", path: "/posts/p%201/comments", body: { body: "and you", parentId: "c1" } },
    ]);
  });
});

describe("the thread's words", () => {
  it("calls the reader's own posts theirs, and nobody else's", () => {
    expect(authorLabel("u1", "u1")).toBe("You");
    expect(authorLabel("u2", "u1")).toBe("A member");
    // A deleted comment has no author; it is never "You", even with no session.
    expect(authorLabel(null, null)).toBe("A member");
  });

  it("explains a refused reply", () => {
    expect(commentError(429, "Try again in 5 s.")).toMatch(
      /faster than this space allows\. Try again in 5 s\./,
    );
    expect(commentError(422, null)).toMatch(/as deep as it goes/);
    expect(commentError(500, null)).toBeUndefined();
  });
});
