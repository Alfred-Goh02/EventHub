import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { os } from './opensearch.js';
import { READ_ALIAS, tenantAlias } from './index-definition.js';

const log = createLogger('search-tenant-alias');

/**
 * Tenant isolation for search, in TWO LAYERS — and the review of this code
 * proved the second one is not optional.
 *
 * LAYER ONE: WHICH DOCUMENTS CAN MATCH. Each tenant gets a filtered alias,
 * `tenant-{id}-events`, carrying `filter: { term: { tenantId } }`. OpenSearch
 * ANDs that filter onto every query sent through the alias, in the engine: a
 * query naming another tenant explicitly still returns nothing, because the
 * result is A AND B. Hits, and aggregations computed over hits, are confined.
 *
 * LAYER TWO: WHICH REQUESTS CAN BE SENT. The alias filter does NOT confine what
 * the engine reads off the shard itself, and a shard holds every tenant routed
 * to it. Through tenant A's own, correctly filtered alias — reproduced against
 * this module while it still accepted a raw body:
 *
 *   - a terms aggregation with min_doc_count: 0 listed tenant B's titles, slugs
 *     and tenant id, each with doc_count 0. Those buckets come from the shard's
 *     term dictionary, not from the documents that matched.
 *   - term and phrase suggesters offered words that exist only in B's titles
 *   - explain and significant_terms reported document counts that include B's
 *
 * So there is no raw body. A caller sends an EventSearchRequest — a closed shape,
 * validated at runtime, with unknown keys REJECTED rather than stripped — and the
 * OpenSearch body is built here, with min_doc_count pinned to 1 on every facet.
 * scripts/verify-search-tenant-isolation.mjs sends the raw leak through the alias
 * as a control, then asserts this module cannot express it.
 *
 * Why an alias per tenant rather than an index per tenant: every shard is a full
 * Lucene index carrying tens of megabytes of heap even when almost empty, so a
 * long tail of small tenants produces thousands of shards holding very little,
 * and the cluster dies on heap long before disk.
 *
 * `routing: tenantId` sends a tenant's whole corpus to ONE shard. Queries hit one
 * shard instead of fanning out, and terms aggregations become exact: with every
 * bucket on one shard there are no per-shard top lists to merge, so a facet count
 * is a count rather than an estimate.
 *
 * ROUTING IS NOT THE ISOLATION, although it sits on the same alias. Removing the
 * filter and keeping the routing was run against the verifier, and tenant A
 * immediately read tenant B's documents. Routing picks which shard is searched;
 * every document on that shard is then a candidate. That is also why the
 * verifier picks tenants that share a shard: on different shards, a missing
 * filter would look exactly like isolation.
 *
 * THE CEILING, because a design whose limits you cannot name is not understood:
 * aliases live in cluster state, which every node holds and which is republished
 * on change. This runs out somewhere in the low thousands of tenants, at which
 * point enforcement has to move to document-level security.
 */

/**
 * Aliases confirmed correct, and until when that confirmation is trusted.
 *
 * An alias is VERIFIED, never trusted by its name. The first version asked only
 * whether an alias called tenant-{id}-events existed, and cached that answer for
 * the life of the process — so an alias created by hand without the filter, or
 * left behind by an older definition, was used as isolation while confining
 * nothing. The review reproduced exactly that: a pre-existing unfiltered alias,
 * and tenant A's search returned both tenants.
 *
 * The expiry exists for migrations. Tenant aliases follow whichever index the
 * read alias resolves to, so when a reindex swaps events-read onto a new index,
 * each pod notices within this window and re-points its tenant aliases instead
 * of serving the old index until it restarts. The swap itself should still move
 * tenant aliases in the same atomic call; this is the backstop, not the plan.
 */
const VERIFIED_FOR_MS = 30_000;
const verified = new Map<string, number>();

interface AliasDefinition {
  filter?: unknown;
  index_routing?: string;
  search_routing?: string;
}

/** Every index holding `alias`, each with the alias's definition there. */
async function holdersOf(alias: string): Promise<Map<string, AliasDefinition>> {
  const response = await os.indices.getAlias({ name: alias }, { ignore: [404] });
  const holders = new Map<string, AliasDefinition>();
  if (response.statusCode === 404) return holders;

  const body = response.body as Record<string, { aliases?: Record<string, AliasDefinition> }>;
  for (const [index, entry] of Object.entries(body)) {
    const definition = entry.aliases?.[alias];
    if (definition) holders.set(index, definition);
  }
  return holders;
}

/**
 * Whether a definition confines to exactly this tenant.
 *
 * Compared against the shape the cluster returns — measured with GET _alias on
 * this cluster: `{"filter":{"term":{"tenantId":"x"}},"index_routing":"x",
 * "search_routing":"x"}` — rather than against what was sent, which is the
 * question that actually matters.
 */
function confines(definition: AliasDefinition | undefined, tenantId: string): boolean {
  return (
    definition !== undefined &&
    definition.index_routing === tenantId &&
    definition.search_routing === tenantId &&
    JSON.stringify(definition.filter) === JSON.stringify({ term: { tenantId } })
  );
}

export async function ensureTenantAlias(tenantId: string): Promise<string> {
  // Validates the id. Nothing below runs for an id that could be an expression.
  const alias = tenantAlias(tenantId);

  const until = verified.get(alias);
  if (until !== undefined && until > Date.now()) return alias;

  const readHolders = [...(await holdersOf(READ_ALIAS)).keys()];
  if (readHolders.length !== 1) {
    throw new Error(
      `${READ_ALIAS} resolves to ${readHolders.length} indices [${readHolders.join(', ')}]; ` +
        'a tenant alias has to follow exactly one. Run bootstrap:index.',
    );
  }
  const index = readHolders[0]!;

  for (let attempt = 1; ; attempt++) {
    const holders = await holdersOf(alias);
    if (holders.size === 1 && confines(holders.get(index), tenantId)) break;

    if (holders.size > 0) {
      log.warn(
        { alias, index, holders: Object.fromEntries(holders) },
        'tenant alias did not confine as required; replacing it',
      );
    }

    try {
      await os.indices.updateAliases({
        body: {
          actions: [
            // Off every other index in the SAME atomic call as the add, so there
            // is no moment at which the alias spans two.
            ...[...holders.keys()]
              .filter((i) => i !== index)
              .map((i) => ({ remove: { index: i, alias } })),
            // `add` on an index that already holds the alias REPLACES its
            // definition, which is what repairs a missing or wrong filter.
            { add: { index, alias, filter: { term: { tenantId } }, routing: tenantId } },
          ],
        },
      });
      break;
    } catch (err) {
      // Two pods repairing the same alias at once can make a remove fail. Look
      // at the cluster again once, rather than failing a search over it.
      if (attempt >= 2) throw err;
    }
  }

  verified.set(alias, Date.now() + VERIFIED_FOR_MS);
  return alias;
}

/** The only fields a facet can be asked for. Each is a keyword field. */
export const FACET_FIELDS = ['category', 'city', 'country', 'tags'] as const;

const keywordValues = z.array(z.string().min(1).max(200)).max(50);

/**
 * Everything a caller can ask of search. `.strict()` is load-bearing: without it
 * zod STRIPS unknown keys, so `{ suggest: ... }` would be silently accepted as
 * `{}` — harmless today, and indistinguishable from a request that was honoured.
 * Rejecting it says so.
 */
export const eventSearchRequest = z
  .object({
    text: z.string().min(1).max(200).optional(),
    filters: z
      .object({
        category: keywordValues.optional(),
        city: keywordValues.optional(),
        country: keywordValues.optional(),
        tags: keywordValues.optional(),
        status: keywordValues.optional(),
      })
      .strict()
      .optional(),
    startsAfter: z.string().datetime().optional(),
    startsBefore: z.string().datetime().optional(),
    facets: z.array(z.enum(FACET_FIELDS)).max(FACET_FIELDS.length).optional(),
    from: z.number().int().min(0).max(10_000).optional(),
    size: z.number().int().min(0).max(100).optional(),
  })
  .strict();

export type EventSearchRequest = z.infer<typeof eventSearchRequest>;

/**
 * The OpenSearch body for a validated request. Every key in it is written here.
 *
 * Query context for the text, filter context for everything else: a filter does
 * not affect the score, which is right for "city is Berlin", and its result is
 * cacheable across requests, which a scored clause is not.
 */
export function buildSearchBody(request: EventSearchRequest): Record<string, unknown> {
  const filter: unknown[] = [];
  for (const [field, values] of Object.entries(request.filters ?? {})) {
    if (values && values.length > 0) filter.push({ terms: { [field]: values } });
  }
  if (request.startsAfter || request.startsBefore) {
    filter.push({
      range: {
        startsAt: {
          ...(request.startsAfter ? { gte: request.startsAfter } : {}),
          ...(request.startsBefore ? { lt: request.startsBefore } : {}),
        },
      },
    });
  }

  const must = request.text
    ? [
        {
          multi_match: {
            query: request.text,
            fields: ['title^3', 'description', 'venueName', 'organizerName'],
          },
        },
      ]
    : [{ match_all: {} }];

  /**
   * min_doc_count: 1, ALWAYS. At 0 a terms aggregation fills its buckets from
   * the shard's term dictionary, which holds every tenant on the shard — the
   * first leak in the header above.
   *
   * And not only the tenants that exist. Setting this to 0 was run against the
   * verifier, and tenant A's facets listed tenant B's values from the PREVIOUS
   * run, whose documents had already been deleted: a delete only marks a
   * document, and its terms stay in the segment until a merge rewrites it. So
   * this leaks data that no longer exists anywhere a delete could reach.
   */
  const aggs = Object.fromEntries(
    (request.facets ?? []).map((field) => [
      field,
      { terms: { field, size: 50, min_doc_count: 1 } },
    ]),
  );

  return {
    from: request.from ?? 0,
    size: request.size ?? 20,
    track_total_hits: true,
    query: { bool: { must, filter } },
    ...(Object.keys(aggs).length > 0 ? { aggs } : {}),
  };
}

/**
 * THE ONLY WAY TO SEARCH, and deliberately the only one exported.
 *
 * `request` is typed `unknown` on purpose: this is where an HTTP body will
 * arrive, and a TypeScript type says nothing about what a client actually sent.
 * The runtime parse is the guarantee.
 */
export async function searchForTenant(
  tenantId: string,
  request: unknown,
): Promise<Record<string, unknown>> {
  const parsed = eventSearchRequest.parse(request);
  const alias = await ensureTenantAlias(tenantId);

  try {
    const { body } = await os.search({
      index: alias,
      body: buildSearchBody(parsed),
      // Belt and braces with the id check: the target is one literal name, and
      // if it ever were not, nothing would expand it.
      expand_wildcards: 'none',
      allow_no_indices: false,
      ignore_unavailable: false,
    });
    return body as Record<string, unknown>;
  } catch (err) {
    // Whatever went wrong, stop trusting the cached verification: an alias that
    // vanished under a migration must be looked at again, not retried blind.
    verified.delete(alias);
    throw err;
  }
}

/** Test seam: forget verified aliases so a fresh run re-checks the cluster. */
export const forgetTenantAliases = (): void => verified.clear();
