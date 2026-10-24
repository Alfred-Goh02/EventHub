/** True when the page is not on a real EventHub host; the mock API is used then. */
export function isMockHost(): boolean {
  if (typeof window === "undefined") return true;
  const h = window.location.hostname;
  return !(h.endsWith(".eventhub.localhost") || h.endsWith(".eventhub.com"));
}

export function getTenantSlug(): string {
  if (typeof window === "undefined") return "";
  if (isMockHost()) return "acme";
  return window.location.hostname.split(".")[0] ?? "";
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
