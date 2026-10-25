/**
 * The host as a name, without the trailing dot a fully qualified one may carry:
 * `acme.eventhub.com.` is the same site as `acme.eventhub.com`, and must not fall through
 * to the sample data.
 */
const hostName = (h: string) => h.toLowerCase().replace(/\.$/, "");

/** True when `hostname` is not a real EventHub host; the mock API is used then. */
export function isMockHostname(hostname: string): boolean {
  const h = hostName(hostname);
  return !(h.endsWith(".eventhub.localhost") || h.endsWith(".eventhub.com"));
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
