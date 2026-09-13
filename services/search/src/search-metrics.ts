/**
 * What GET /search has been doing, for /metrics.
 *
 * COUNTS BY OUTCOME, because the three mean different things to an operator:
 * served is normal; degraded means OpenSearch is down and Postgres is carrying
 * search, which is an incident even though every response was a 200; failed
 * means neither could answer.
 *
 * DURATIONS AS PERCENTILES OVER THE LAST 1024 SEARCHES, not a histogram. Nothing
 * scrapes this service yet, and a histogram's buckets are a decision about what a
 * scraper will aggregate. A rolling window answers the question a person reading
 * /metrics has — "how slow is search right now" — and costs one sort per read.
 * Only served searches are timed: a degraded search's time is Postgres's, and
 * mixing the two would make a fast fallback look like a fast cluster.
 */
const WINDOW = 1024;
const durations: number[] = [];
let nextSlot = 0;
const counts = { served: 0, degraded: 0, failed: 0 };

export function recordSearch(outcome: 'served' | 'degraded' | 'failed', ms: number): void {
  counts[outcome] += 1;
  if (outcome !== 'served') return;
  if (durations.length < WINDOW) {
    durations.push(ms);
  } else {
    durations[nextSlot] = ms;
    nextSlot = (nextSlot + 1) % WINDOW;
  }
}

function percentile(sorted: number[], p: number): number | null {
  if (sorted.length === 0) return null;
  return Math.round(sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))]! * 10) / 10;
}

export function searchMetrics(): Record<string, unknown> {
  const sorted = [...durations].sort((a, b) => a - b);
  return {
    servedTotal: counts.served,
    degradedTotal: counts.degraded,
    failedTotal: counts.failed,
    durationMs: {
      samples: sorted.length,
      p50: percentile(sorted, 0.5),
      p95: percentile(sorted, 0.95),
      p99: percentile(sorted, 0.99),
    },
  };
}
