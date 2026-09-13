import { env } from './env.js';

export interface DegradedParams {
  q?: string;
  includePast: boolean;
  limit: number;
}

/**
 * Search without OpenSearch: the Event service's capped title match.
 *
 * FORWARDS THE CALLER'S TOKEN. Search holds no credential for the Event service
 * and does not need one: Event verifies the same token, takes the tenant from
 * the same signed claim, and RLS scopes the query. A fallback that authenticated
 * as Search itself would need a way to say which tenant it meant, and that is a
 * value someone could get wrong.
 *
 * WHAT IS LOST, said plainly in the response rather than hidden: no relevance
 * ranking, no typo tolerance, no facets, no category or city filters, at most
 * 50 rows. `degraded: true` and the X-Search-Degraded header let a client hide
 * the facet sidebar rather than showing an empty one that looks like "no
 * results".
 *
 * Two seconds, because a fallback that hangs is no fallback: the page would wait
 * on OpenSearch's timeout AND this one.
 */
export async function degradedSearch(
  authorization: string,
  params: DegradedParams,
): Promise<Record<string, unknown>> {
  const url = new URL('/events/search', env.EVENT_SERVICE_URL);
  if (params.q) url.searchParams.set('q', params.q);
  if (params.includePast) url.searchParams.set('includePast', 'true');

  const res = await fetch(url, {
    headers: { Authorization: authorization },
    signal: AbortSignal.timeout(2_000),
  });
  if (!res.ok) throw new Error(`the Event service answered ${res.status}`);

  const { data, cap } = (await res.json()) as {
    data: Array<{ id: string; slug: string; title: string; startsAt: string; endsAt: string }>;
    cap: number;
  };

  return {
    hits: data.slice(0, params.limit).map((e) => ({
      eventId: e.id,
      slug: e.slug,
      title: e.title,
      startsAt: e.startsAt,
      endsAt: e.endsAt,
      highlight: {},
    })),
    total: data.length,
    // At the cap, "total" is a floor, not a count.
    capped: data.length >= cap,
    facets: {},
    facetsExact: false,
    tookMs: null,
    degraded: true,
  };
}
