/**
 * Which compose hosts an environment names, and on which ports.
 *
 * ONE DEFINITION, TWO READERS. infra/local/bridge.mjs derives from it every host that
 * infra/local/up.mjs bridges into the kind cluster, and scripts/verify-store-chart.mjs
 * requires infra/stores to serve every one of them on EKS. Kept in two places, the two would drift apart, and the chart could leave a
 * port out that the bridge serves.
 */

/** host:port pairs a value names, for hosts that are compose services. */
export function hostsIn(value, composeNames) {
  const found = [];
  for (const m of String(value).matchAll(/(?:^|[/@,\s])([a-z][a-z0-9-]*):(\d{2,5})\b/g)) {
    if (composeNames.has(m[1])) found.push({ host: m[1], port: Number(m[2]) });
  }
  return found;
}

/**
 * Every compose host an environment names: host:port inside a value, and a bare
 * `X_HOST=<service>` with its `X_PORT` beside it -- how Notification names mailpit.
 * A bare value alone is not a host: CLICKHOUSE_DATABASE=analytics and
 * KAFKA_CONSUMER_GROUP=analytics-ingest both equal a compose service's name.
 */
export function hostsOf(env, composeNames) {
  const found = [];
  for (const [key, value] of Object.entries(env)) {
    found.push(...hostsIn(value, composeNames));
    const prefix = /^(.*)_HOST$/.exec(key)?.[1];
    if (prefix && composeNames.has(String(value)) && /^\d{2,5}$/.test(String(env[`${prefix}_PORT`] ?? ''))) {
      found.push({ host: String(value), port: Number(env[`${prefix}_PORT`]) });
    }
  }
  return found;
}
