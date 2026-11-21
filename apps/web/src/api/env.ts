/**
 * The host as a name, without the trailing dot a fully qualified one may carry:
 * `acme.example.org.` is the same site as `acme.example.org`, and must not fall through
 * to the sample data.
 */
const hostName = (h: string) => h.toLowerCase().replace(/\.$/, "");

declare global {
  interface Window {
    /** Set by /config.js, which the app's own server writes (src/server.ts) before the app loads. */
    __EVENTHUB__?: { domain?: string | null };
  }
}

/**
 * The deployment's own domain, as its server announced it, or null. A laptop needs none:
 * eventhub.localhost is always real. Anywhere that serves no /config.js -- a preview host,
 * a static copy -- has none either, and gets the sample data.
 */
export function publicDomain(): string | null {
  if (typeof window === "undefined") return null;
  const d = window.__EVENTHUB__?.domain;
  return typeof d === "string" && d.length > 0 ? d : null;
}

/**
 * True when `hostname` is not a real EventHub host; the mock API is used then. Real is a
 * tenant's subdomain of eventhub.localhost, or of the deployment's own domain: a whole label
 * of it, so evilexample.org is not example.org's.
 */
export function isMockHostname(hostname: string, domain: string | null = publicDomain()): boolean {
  const h = hostName(hostname);
  if (h.endsWith(".eventhub.localhost")) return false;
  return !(domain !== null && h.endsWith(`.${domain}`));
}

/** True when the page is not on a real EventHub host; the mock API is used then. */
export function isMockHost(): boolean {
  if (typeof window === "undefined") return true;
  return isMockHostname(window.location.hostname);
}

export function getTenantSlug(): string {
  if (typeof window === "undefined") return "";
  if (isMockHost()) return "acme";
  return hostName(window.location.hostname).split(".")[0] ?? "";
}

export function getTenantName(): string {
  const s = getTenantSlug();
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : "EventHub";
}

export function getApiOrigin(): string {
  const fromEnv = import.meta.env.VITE_API_ORIGIN as string | undefined;
  if (fromEnv) return fromEnv.replace(/\/$/, "");
  return `${window.location.protocol}//${window.location.hostname}:8000`;
}
