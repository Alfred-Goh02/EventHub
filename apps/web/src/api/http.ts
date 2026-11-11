import { ApiError, errorFromBody, parseRetryAfter } from "./errors";
import { getApiOrigin } from "./env";
import {
  forceSignOut,
  readTokens,
  storedRefreshDueAt,
  subscribeTokens,
  writeTokens,
} from "./tokens";
import type {
  CreateReservationBody,
  CreatedReservation,
  Event,
  EventDetail,
  EventHubApi,
  Page,
  PayResult,
  Reservation,
  TicketType,
  TokenPair,
} from "./types";

interface RawResult {
  status: number;
  body: unknown;
  headers: Headers;
}

async function raw(
  path: string,
  init: { method?: string; body?: unknown; headers?: Record<string, string> } = {},
): Promise<RawResult> {
  const headers: Record<string, string> = { ...init.headers };
  if (init.body !== undefined) headers["Content-Type"] = "application/json";
  let res: Response;
  try {
    res = await fetch(getApiOrigin() + path, {
      method: init.method ?? "GET",
      headers,
      body: init.body !== undefined ? JSON.stringify(init.body) : null,
    });
  } catch {
    throw new ApiError({
      status: 0,
      message: "Can't reach EventHub. Check your connection and try again.",
    });
  }
  let body: unknown = null;
  const text = await res.text();
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { message: text };
    }
  }
  return { status: res.status, body, headers: res.headers };
}

function toError(r: RawResult) {
  return errorFromBody(
    r.status,
    r.body,
    r.headers.get("X-Request-ID"),
    parseRetryAfter(r.headers.get("Retry-After")),
  );
}

// ---- single-flight refresh (per tab via promise, across tabs via Web Locks) ----
let inflight: Promise<TokenPair> | null = null;

async function doRefresh(heldRefreshToken: string): Promise<TokenPair> {
  const current = readTokens();
  if (!current) {
    forceSignOut();
    throw new ApiError({ status: 401, message: "You've been signed out." });
  }
  // Another tab already rotated: use its tokens.
  if (current.refreshToken !== heldRefreshToken) return current;
  const r = await raw("/auth/refresh", {
    method: "POST",
    body: { refreshToken: current.refreshToken },
  });
  if (r.status === 401) {
    forceSignOut();
    throw toError(r);
  }
  if (r.status < 200 || r.status >= 300) throw toError(r);
  const pair = r.body as TokenPair;
  // Signed out, or signed in as someone else, while this refresh was in flight: writing the
  // new pair would sign that session back in. Revoke it instead, so nothing can use it.
  if (readTokens()?.refreshToken !== current.refreshToken) {
    raw("/auth/logout", { method: "POST", body: { refreshToken: pair.refreshToken } }).catch(
      () => {},
    );
    throw new ApiError({ status: 401, message: "You've been signed out." });
  }
  writeTokens(pair);
  return pair;
}

export function refreshTokens(heldRefreshToken: string): Promise<TokenPair> {
  if (inflight) return inflight;
  const run = () => doRefresh(heldRefreshToken);
  const locks = typeof navigator !== "undefined" ? navigator.locks : undefined;
  // The lock resolves with the callback's settled value; `.then` says so to the type checker.
  const refreshed: Promise<TokenPair> = locks
    ? locks.request("eventhub-refresh", run).then((pair) => pair)
    : run();
  const pending = refreshed.finally(() => {
    inflight = null;
  });
  inflight = pending;
  return pending;
}

// ---- proactive refresh a minute before the token's lifetime runs out (see refreshDueAt) ----
let timer: ReturnType<typeof setTimeout> | null = null;
let lifecycleStarted = false;
function schedule() {
  if (timer) clearTimeout(timer);
  const t = readTokens();
  if (!t) return;
  const due = storedRefreshDueAt(t);
  if (due === null) return;
  const ms = Math.max(0, due - Date.now());
  timer = setTimeout(() => {
    const now = readTokens();
    if (now) refreshTokens(now.refreshToken).catch(() => {});
  }, ms);
}
export function startTokenLifecycle() {
  if (lifecycleStarted) return;
  lifecycleStarted = true;
  subscribeTokens(schedule);
  schedule();
}

async function authed(
  path: string,
  init: { method?: string; body?: unknown; headers?: Record<string, string> } = {},
): Promise<RawResult> {
  let tokens = readTokens();
  if (!tokens) {
    forceSignOut();
    throw new ApiError({ status: 401, message: "Please sign in." });
  }
  const due = storedRefreshDueAt(tokens);
  if (due !== null && Date.now() >= due) {
    try {
      tokens = await refreshTokens(tokens.refreshToken);
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) throw e;
    }
  }
  const send = (t: TokenPair) =>
    raw(path, { ...init, headers: { ...init.headers, Authorization: `Bearer ${t.accessToken}` } });
  let r = await send(tokens);
  if (r.status === 401) {
    tokens = await refreshTokens(tokens.refreshToken);
    r = await send(tokens);
  }
  if (r.status < 200 || r.status >= 300) throw toError(r);
  return r;
}

const qs = (p: Record<string, string | number | null | undefined>) => {
  const s = new URLSearchParams();
  Object.entries(p).forEach(
    ([k, v]) => v !== undefined && v !== null && v !== "" && s.set(k, String(v)),
  );
  return s.toString();
};

export const httpApi: EventHubApi = {
  async login(email, password) {
    const r = await raw("/auth/login", { method: "POST", body: { email, password } });
    if (r.status < 200 || r.status >= 300) throw toError(r);
    return r.body as TokenPair;
  },
  async logout(refreshToken) {
    try {
      await raw("/auth/logout", { method: "POST", body: { refreshToken } });
    } catch {
      /* the tokens are already gone from this browser */
    }
  },
  async listEvents(cursor) {
    // upcoming: events that have not ended, by the server's clock. The cursor does not
    // carry it, so it goes with every page.
    const query = qs({ status: "PUBLISHED", upcoming: "true", limit: 20, cursor });
    return (await authed(`/events?${query}`)).body as Page<Event>;
  },
  async getEvent(id) {
    return (await authed(`/events/${encodeURIComponent(id)}`)).body as EventDetail;
  },
  async listTicketTypes(eventId) {
    return (await authed(`/ticket-types?${qs({ eventId })}`)).body as { data: TicketType[] };
  },
  async createReservation(body: CreateReservationBody, key: string) {
    return (
      await authed("/reservations", { method: "POST", body, headers: { "Idempotency-Key": key } })
    ).body as CreatedReservation;
  },
  async getReservation(id) {
    return (await authed(`/reservations/${encodeURIComponent(id)}`)).body as Reservation;
  },
  async payReservation(id) {
    const r = await authed(`/reservations/${encodeURIComponent(id)}/pay`, { method: "POST" });
    return { httpStatus: r.status === 202 ? 202 : 200, body: r.body } as PayResult;
  },
  async listReservations({ status, cursor }) {
    return (await authed(`/reservations?${qs({ limit: 20, status, cursor })}`))
      .body as Page<Reservation>;
  },
};
