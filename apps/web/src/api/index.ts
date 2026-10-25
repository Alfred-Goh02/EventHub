import { isMockHost } from "./env";
import { httpApi, startTokenLifecycle } from "./http";
import { mockApi } from "./mock";
import type { EventHubApi } from "./types";

export * from "./types";
export { ApiError } from "./errors";
export { getTenantName, getTenantSlug, isMockHost } from "./env";
export { getSession, writeTokens, clearTokens, subscribeTokens, onForcedSignOut } from "./tokens";

/** The single API client. Mock off EventHub hosts, real HTTP everywhere else. */
export function api(): EventHubApi {
  return isMockHost() ? mockApi : httpApi;
}

export function startSessionLifecycle() {
  if (!isMockHost()) startTokenLifecycle();
}
