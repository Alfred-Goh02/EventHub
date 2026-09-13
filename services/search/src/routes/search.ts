import { Router } from 'express';
import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { getAuth } from '@eventhub/auth-middleware';
import { requireAuth } from '../auth.js';
import { CursorError, decodeCursor, encodeCursor, queryHash } from '../cursor.js';
import { degradedSearch } from '../degraded.js';
import { recordSearch } from '../search-metrics.js';
import {
  closePit,
  openPitForTenant,
  searchForTenant,
  type EventSearchRequest,
} from '../tenant-alias.js';

const logger = createLogger('search-route');

// Annotated for the same reason as Event's router: the inferred type lives in a
// package pnpm does not hoist, and declaration emit cannot name it.
export const search: Router = Router();

const oneOrMany = z
  .union([z.string().min(1).max(100), z.array(z.string().min(1).max(100)).max(20)])
  .optional()
  .transform((v) => (v === undefined ? undefined : Array.isArray(v) ? v : [v]));

/**
 * The query string, validated and COERCED — every value arrives as a string.
 *
 * `includePast` is an enum, not z.coerce.boolean(). coerce.boolean is
 * Boolean(value), and Boolean("false") is true: ?includePast=false would include
 * the past. scripts/verify-search-api.mjs sends exactly that string.
 *
 * `.strict()`, so ?tenantId=... is a 400 rather than an ignored parameter. It
 * would be ignored either way — the tenant comes from the token — but a client
 * that believes it can choose a tenant should be told it cannot.
 */
export const searchParams = z
  .object({
    q: z.preprocess(
      (v) => (typeof v === 'string' && v.trim() === '' ? undefined : v),
      z.string().trim().max(200).optional(),
    ),
    category: oneOrMany,
    city: oneOrMany,
    includePast: z.enum(['true', 'false']).default('false').transform((v) => v === 'true'),
    limit: z.coerce.number().int().min(1).max(100).default(20),
    cursor: z.string().max(4096).optional(),
  })
  .strict();

interface Hit {
  _source: Record<string, unknown>;
  highlight?: Record<string, string[]>;
  sort?: Array<string | number | null>;
}
interface Bucket {
  key: string | number;
  key_as_string?: string;
  doc_count: number;
}
interface TermsAgg {
  buckets: Bucket[];
  doc_count_error_upper_bound?: number;
  sum_other_doc_count?: number;
}

function toResponse(body: Record<string, unknown>, nextCursor: string | null): Record<string, unknown> {
  const hits = body['hits'] as { hits: Hit[]; total: { value: number } };
  const aggs = (body['aggregations'] ?? {}) as Record<string, TermsAgg>;
  const terms = (name: string) =>
    (aggs[name]?.buckets ?? []).map((b) => ({ value: b.key, count: b.doc_count }));

  return {
    hits: hits.hits.map((h) => ({
      eventId: h._source['eventId'],
      slug: h._source['slug'],
      title: h._source['title'],
      startsAt: h._source['startsAt'],
      endsAt: h._source['endsAt'],
      city: h._source['city'],
      category: h._source['category'],
      highlight: h.highlight ?? {},
    })),
    total: hits.total.value,
    facets: {
      category: terms('category'),
      city: terms('city'),
      week: (aggs['week']?.buckets ?? []).map((b) => ({ start: b.key_as_string, count: b.doc_count })),
    },
    /**
     * Whether the facet counts are counts or estimates. A terms aggregation on a
     * multi-shard index merges each shard's top buckets and can undercount; with
     * routing putting a tenant on one shard there is nothing to merge, so this
     * should always be true. Reported rather than assumed, so the property is
     * seen to disappear if routing ever does.
     */
    facetsExact: ['category', 'city'].every(
      (n) => (aggs[n]?.doc_count_error_upper_bound ?? 0) === 0 && (aggs[n]?.sum_other_doc_count ?? 0) === 0,
    ),
    nextCursor,
    tookMs: body['took'],
    degraded: false,
  };
}

/**
 * Whether a failure means the CLUSTER cannot serve, which is the only case that
 * falls back. Anything else is a bug, and a fallback that swallowed bugs would
 * turn every one of them into quietly worse results.
 */
function clusterUnavailable(err: unknown): boolean {
  const e = err as { name?: string; meta?: { statusCode?: number; body?: { error?: { type?: string } } } };
  if (['ConnectionError', 'TimeoutError', 'NoLivingConnectionsError'].includes(e.name ?? '')) return true;
  const status = e.meta?.statusCode ?? 0;
  return (
    e.name === 'ResponseError' &&
    (status >= 500 || status === 429 || e.meta?.body?.error?.type === 'index_not_found_exception')
  );
}

/**
 * Whether a failure is a PIT that no longer exists — expired, or released.
 *
 * Matched on what the cluster actually returns, measured by releasing a PIT and
 * searching with it: a search_phase_execution_exception whose every root cause
 * is search_context_missing_exception, "No search context found for id [347]".
 */
function pitGone(err: unknown): boolean {
  const text = JSON.stringify((err as { meta?: { body?: unknown } })?.meta?.body ?? '');
  return /search_context_missing_exception/.test(text);
}

search.get('/search', requireAuth, async (req, res, next) => {
  const parsed = searchParams.safeParse(req.query);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.flatten() });
    return;
  }

  // The signed claim. X-Tenant-ID is never read on this path.
  const { tenantId } = getAuth(req);
  const p = parsed.data;
  const started = performance.now();
  const elapsed = () => performance.now() - started;

  /**
   * What PUBLIC search shows: published, upcoming unless asked otherwise. A
   * policy of this route, not of the engine — the index holds drafts and
   * deleted events too, because the read model mirrors the source and a future
   * organiser view will want them.
   */
  const request: EventSearchRequest = {
    ...(p.q ? { text: p.q } : {}),
    filters: {
      status: ['published'],
      ...(p.category ? { category: p.category } : {}),
      ...(p.city ? { city: p.city } : {}),
    },
    upcomingOnly: !p.includePast,
    facets: ['category', 'city', 'week'],
    size: p.limit,
  };

  // Every parameter but the cursor itself, in a fixed order, so the same query
  // always hashes the same.
  const hash = queryHash({
    q: p.q ?? null,
    category: p.category ?? null,
    city: p.city ?? null,
    includePast: p.includePast,
    limit: p.limit,
  });

  let after: EventSearchRequest['after'];
  let pitId: string | undefined;
  if (p.cursor) {
    try {
      const cursor = decodeCursor(p.cursor, tenantId, hash);
      after = cursor.a as EventSearchRequest['after'];
      pitId = cursor.p;
    } catch (err) {
      if (!(err instanceof CursorError)) throw err;
      logger.warn({ tenantId, reason: err.message }, 'refused a cursor');
      res.status(400).json({ error: 'the cursor is not valid for this search' });
      return;
    }
  }

  try {
    /**
     * DEEP PAGINATION: search_after inside a Point-in-Time.
     *
     * from/size is refused past max_result_window, and raising that limit trades
     * an error for heap: every shard sorts from + size hits and throws all but
     * size away. search_after seeks from the last sort values instead, at a
     * constant cost per page — which is why the sort ends in a unique eventId.
     *
     * The PIT is what stops pages shifting while the indexer writes: without a
     * snapshot a reader sees duplicates and skips. It is opened on the SECOND
     * page, not the first. Opening one for every first page would open one for
     * nearly every search, and most are never paged; the price is that page one
     * and page two are not guaranteed one snapshot, while everything after is.
     */
    if (p.cursor && !pitId) pitId = await openPitForTenant(tenantId);

    const body = await searchForTenant(
      tenantId,
      { ...request, ...(after ? { after } : {}) },
      pitId ? { pitId } : undefined,
    );

    const hits = (body['hits'] as { hits: Hit[] }).hits;
    // A search inside a PIT may hand back a new id; the latest is the one to keep.
    const livePit = (body['pit_id'] as string | undefined) ?? pitId;
    const last = hits[hits.length - 1];
    const more = hits.length === p.limit && Array.isArray(last?.sort);

    const nextCursor = more
      ? encodeCursor({ t: tenantId, h: hash, a: last!.sort!, ...(livePit ? { p: livePit } : {}) })
      : null;

    // The traversal is over: release the snapshot now rather than in a minute.
    // Awaited, so the last page is answered with nothing left open behind it.
    if (!more && livePit) await closePit(livePit);

    const response = toResponse(body, nextCursor);

    // The query's SHAPE, never its text: search terms are user content and can
    // carry personal data, so they do not belong in an info-level log line.
    logger.info(
      {
        tenantId,
        tookMs: body['took'],
        total: response['total'],
        hasQuery: Boolean(p.q),
        qLength: p.q?.length ?? 0,
        paged: Boolean(p.cursor),
        filters: [p.category ? 'category' : null, p.city ? 'city' : null].filter(Boolean),
      },
      'search served',
    );
    recordSearch('served', elapsed());
    res.json(response);
  } catch (err) {
    if (pitGone(err)) {
      res.status(410).json({ error: 'this page has expired; start the search again' });
      return;
    }
    if (!clusterUnavailable(err)) {
      recordSearch('failed', elapsed());
      next(err);
      return;
    }
    if (p.cursor) {
      // The degraded path has no pages to continue. Returning its first page to
      // a request for page four would look like results and be wrong.
      recordSearch('failed', elapsed());
      res.status(503).json({ error: 'search is degraded and cannot continue this page' });
      return;
    }

    logger.warn({ err, tenantId }, 'opensearch unavailable; serving the degraded path');
    try {
      const fallback = await degradedSearch(req.header('Authorization') ?? '', {
        ...(p.q ? { q: p.q } : {}),
        includePast: p.includePast,
        limit: p.limit,
      });
      recordSearch('degraded', elapsed());
      res.setHeader('X-Search-Degraded', 'true');
      res.json({ ...fallback, nextCursor: null });
    } catch (fallbackErr) {
      logger.error({ err: fallbackErr }, 'the degraded path failed as well');
      recordSearch('failed', elapsed());
      res.status(503).json({ error: 'search is unavailable' });
    }
  }
});
