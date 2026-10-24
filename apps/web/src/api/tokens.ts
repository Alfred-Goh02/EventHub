import type { AccessClaims, TokenPair } from "./types";
import { getTenantSlug } from "./env";

const ACCESS = "eventhub.accessToken";
const REFRESH = "eventhub.refreshToken";

type Listener = () => void;
const listeners = new Set<Listener>();
let signedOutHandler: (() => void) | null = null;

export function readTokens(): TokenPair | null {
  const accessToken = localStorage.getItem(ACCESS);
  const refreshToken = localStorage.getItem(REFRESH);
  return accessToken && refreshToken ? { accessToken, refreshToken } : null;
}

export function writeTokens(t: TokenPair) {
  localStorage.setItem(ACCESS, t.accessToken);
  localStorage.setItem(REFRESH, t.refreshToken);
  emit();
}

export function clearTokens() {
  localStorage.removeItem(ACCESS);
  localStorage.removeItem(REFRESH);
  emit();
}

function emit() {
  listeners.forEach((l) => l());
}

export function subscribeTokens(l: Listener) {
  listeners.add(l);
  return () => listeners.delete(l);
}

/** Called when a refresh is rejected: the app sends the user to sign in. */
export function onForcedSignOut(h: () => void) {
  signedOutHandler = h;
}
export function forceSignOut() {
  clearTokens();
  signedOutHandler?.();
}

export function decodeClaims(token: string): AccessClaims | null {
  try {
    const part = token.split(".")[1];
    if (!part) return null;
    const b64 = part.replace(/-/g, "+").replace(/_/g, "/");
    const json = decodeURIComponent(
      atob(b64.padEnd(b64.length + ((4 - (b64.length % 4)) % 4), "="))
        .split("")
        .map((c) => "%" + c.charCodeAt(0).toString(16).padStart(2, "0"))
        .join(""),
    );
    const c = JSON.parse(json);
    if (!c.sub || !c.tenantSlug || !c.role || typeof c.exp !== "number") return null;
    return c as AccessClaims;
  } catch {
    return null;
  }
}

/** Current session, or null when signed out or the token belongs to another tenant. */
export function getSession(): { claims: AccessClaims; tokens: TokenPair } | null {
  if (typeof window === "undefined") return null;
  const tokens = readTokens();
  if (!tokens) return null;
  const claims = decodeClaims(tokens.accessToken);
  if (!claims || claims.tenantSlug !== getTenantSlug()) return null;
  return { claims, tokens };
}

// Other tabs write tokens: re-broadcast so every tab picks them up.
if (typeof window !== "undefined") {
  window.addEventListener("storage", (e) => {
    if (e.key === ACCESS || e.key === REFRESH || e.key === null) emit();
  });
}
