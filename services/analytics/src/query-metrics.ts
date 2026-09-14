/**
 * What the read API has been doing, for /metrics. The same shape as Search's
 * search-metrics.ts: counts by outcome, and durations as percentiles over a
 * rolling window rather than a histogram, because nothing scrapes this yet.
 *
 * PER ROUTE, because the routes do different work: a per-event route reads one
 * key prefix, while the overview reads every event in a tenant's range. One
 * blended percentile would say "analytics is slow" without saying which.
 *
 * Only served queries are timed. An unavailable store fails fast or slowly
 * depending on how it is down, and mixing that in would make an outage look like
 * a latency trend.
 */
export type RouteName = 'funnel' | 'engagement' | 'overview';
export type Outcome = 'served' | 'rejected' | 'unavailable' | 'failed';

const WINDOW = 1024;

interface Series {
  counts: Record<Outcome, number>;
  durations: number[];
  nextSlot: number;
}

const series = new Map<RouteName, Series>();

function seriesFor(route: RouteName): Series {
  let s = series.get(route);
  if (!s) {
    s = { counts: { served: 0, rejected: 0, unavailable: 0, failed: 0 }, durations: [], nextSlot: 0 };
    series.set(route, s);
  }
  return s;
}

export function recordQuery(route: RouteName, outcome: Outcome, ms: number): void {
  const s = seriesFor(route);
  s.counts[outcome] += 1;
  if (outcome !== 'served') return;
  if (s.durations.length < WINDOW) {
    s.durations.push(ms);
  } else {
    s.durations[s.nextSlot] = ms;
    s.nextSlot = (s.nextSlot + 1) % WINDOW;
  }
}

function percentile(sorted: number[], p: number): number | null {
  if (sorted.length === 0) return null;
  return Math.round(sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]! * 10) / 10;
}

export function queryMetrics(): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const route of ['funnel', 'engagement', 'overview'] as const) {
    const s = seriesFor(route);
    const sorted = [...s.durations].sort((a, b) => a - b);
    out[route] = {
      servedTotal: s.counts.served,
      rejectedTotal: s.counts.rejected,
      unavailableTotal: s.counts.unavailable,
      failedTotal: s.counts.failed,
      durationMs: {
        samples: sorted.length,
        p50: percentile(sorted, 0.5),
        p95: percentile(sorted, 0.95),
        p99: percentile(sorted, 0.99),
      },
    };
  }
  return out;
}
