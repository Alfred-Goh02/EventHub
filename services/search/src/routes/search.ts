import { Router } from 'express';
import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { getAuth } from '@eventhub/auth-middleware';
import { requireAuth } from '../auth.js';
import { degradedSearch } from '../degraded.js';
import { searchForTenant, type EventSearchRequest } from '../tenant-alias.js';

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
  })
  .strict();

interface Hit {
  _source: Record<string, unknown>;
  highlight?: Record<string, string[]>;
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

function toResponse(body: Record<string, unknown>): Record<string, unknown> {
  const hits = (body['hits'] as { hits: Hit[]; total: { value: number } });
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

search.get('/search', requireAuth, async (req, res, next) => {
  const parsed = searchParams.safeParse(req.query);
  if (!parsed.success) {
    res.status(400).json({ error: parsed.error.flatten() });
    return;
  }

  // The signed claim. X-Tenant-ID is never read on this path.
  const { tenantId } = getAuth(req);
  const p = parsed.data;

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

  try {
    const body = await searchForTenant(tenantId, request);
    const response = toResponse(body);

    // The query's SHAPE, never its text: search terms are user content and can
    // carry personal data, so they do not belong in an info-level log line.
    logger.info(
      {
        tenantId,
        tookMs: body['took'],
        total: response['total'],
        hasQuery: Boolean(p.q),
        qLength: p.q?.length ?? 0,
        filters: [p.category ? 'category' : null, p.city ? 'city' : null].filter(Boolean),
      },
      'search served',
    );
    res.json(response);
  } catch (err) {
    if (!clusterUnavailable(err)) {
      next(err);
      return;
    }

    logger.warn({ err, tenantId }, 'opensearch unavailable; serving the degraded path');
    try {
      const fallback = await degradedSearch(req.header('Authorization') ?? '', {
        ...(p.q ? { q: p.q } : {}),
        includePast: p.includePast,
        limit: p.limit,
      });
      res.setHeader('X-Search-Degraded', 'true');
      res.json(fallback);
    } catch (fallbackErr) {
      logger.error({ err: fallbackErr }, 'the degraded path failed as well');
      res.status(503).json({ error: 'search is unavailable' });
    }
  }
});
