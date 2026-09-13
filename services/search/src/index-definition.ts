/**
 * THIS FILE IS THE SCHEMA.
 *
 * Nothing is created by hand in Dashboards, because a mapping created by hand is
 * a mapping nobody can review, reproduce, or diff — the same reason the Kafka
 * topics are a checked-in script and the database is a migration.
 */

/**
 * Bumped when the mapping or the analysis chain changes, which is a migration
 * and not an edit.
 *
 * Analysis happens at INDEX time: existing documents already carry the tokens
 * the old analyzer produced, so changing the analyzer does not change them. A
 * new stemmer applies to nothing that is already indexed. The only honest way to
 * ship one is to build events-v{n+1}, reindex into it, and swap the aliases —
 * which is exactly why application code addresses aliases and never this name.
 */
export const INDEX_VERSION = 1;
export const INDEX_NAME = `events-v${INDEX_VERSION}`;

export const READ_ALIAS = 'events-read';
export const WRITE_ALIAS = 'events-write';

/**
 * What a tenant id may look like before it goes anywhere near an index name.
 *
 * To OpenSearch an alias name in a request is not a string, it is an
 * EXPRESSION: `*` expands to every alias it matches, and a comma adds more
 * targets. Unchecked, a tenant id of `*` searched every tenant's alias at once,
 * and `<own>-events,events-read,tenant-<own>` reached the unfiltered read alias
 * with no tenant filter at all — both reproduced against the real module by the
 * review of this code. Real tenant ids are cuids or seeded values such as
 * tenant_acme, so this admits all of them and nothing carrying an operator.
 */
export const TENANT_ID_PATTERN = /^[a-z0-9][a-z0-9_]{0,63}$/;

/**
 * The per-tenant filtered alias. See tenant-alias.ts for what it buys.
 *
 * Validates rather than trusting the caller, because this is the one place
 * every alias name is built: a caller that never checked cannot get an
 * expression past it.
 */
export function tenantAlias(tenantId: string): string {
  if (!TENANT_ID_PATTERN.test(tenantId)) {
    throw new Error(
      `refusing to build a tenant alias from ${JSON.stringify(tenantId)}: ` +
        `tenant ids must match ${TENANT_ID_PATTERN}`,
    );
  }
  return `tenant-${tenantId}-events`;
}

export const indexBody = {
  settings: {
    index: {
      /**
       * THREE, locally as in production — and IMMUTABLE.
       *
       * Routing is hash(routing_value) % number_of_shards, so changing the
       * divisor changes where every existing document should live and the old
       * ones are all in the wrong place. That is why growing an index means
       * reindexing into a new one rather than editing this number, and why
       * bootstrap-index.ts refuses an existing index whose count differs.
       *
       * Three rather than one locally, because one shard makes routing
       * untestable. Every routing value lands on shard 0, so an indexer that
       * forgets `routing: tenantId` still finds every document, every local
       * check passes, and in production each tenant silently loses the events
       * that hashed elsewhere. scripts/verify-search-tenant-isolation.mjs indexes
       * a document with the wrong routing and asserts its tenant cannot see it —
       * an assertion that cannot fail on one shard, so it refuses to run on one.
       */
      number_of_shards: 3,

      /**
       * Zero locally. A replica has nowhere to go on a single node, so asking
       * for one leaves the cluster permanently yellow and makes "is it healthy"
       * unanswerable. Production wants 1.
       */
      number_of_replicas: 0,

      /**
       * The reason this is "near real-time" rather than real-time: a document is
       * searchable after the next refresh, not when the write returns. Tests
       * that write then immediately search must use refresh: 'wait_for' on the
       * write — never refresh: true, which forces a segment per document and
       * buries the shard in merge pressure.
       */
      refresh_interval: '1s',

      'search.slowlog.threshold.query.warn': '2s',
    },

    analysis: {
      filter: {
        english_stop: { type: 'stop', stopwords: '_english_' },

        /**
         * light_english, not the aggressive Porter stemmer.
         *
         * Event titles are short marketing copy, where over-stemming costs more
         * precision than it buys recall. Measured with _analyze on this cluster:
         * the aggressive `english` (Porter) stemmer reduces both "organizer" and
         * "organic" to `organ`, so a search for one returns the other.
         * light_english keeps them apart (`organize`, `organic`). The British
         * "organiser" happens to escape — Porter gives `organis` — which is
         * exactly why the example has to be run rather than remembered.
         */
        english_stemmer: { type: 'stemmer', language: 'light_english' },
        english_possessive: { type: 'stemmer', language: 'possessive_english' },
      },

      analyzer: {
        /**
         * Defined explicitly rather than left as `standard`, so the pipeline is
         * a reviewable decision. asciifolding is what makes "café" findable by
         * typing "cafe".
         */
        event_text: {
          type: 'custom',
          tokenizer: 'standard',
          filter: [
            'lowercase',
            'asciifolding',
            'english_possessive',
            'english_stop',
            'english_stemmer',
          ],
        },
      },

      normalizer: {
        // Keywords are not tokenised, so "Berlin" and "berlin" would otherwise
        // be two separate facet buckets.
        lowercase_keyword: { type: 'custom', filter: ['lowercase', 'asciifolding'] },
      },
    },
  },

  mappings: {
    /**
     * An unexpected field from an upstream producer is a loud 400, not silent
     * schema rot. Affordable precisely because this index is a read model: it is
     * rebuildable, and the indexer has somewhere to put what it cannot handle.
     */
    dynamic: 'strict',

    properties: {
      /**
       * keyword, and NEVER text. Tenant isolation depends on it.
       *
       * A `term` filter compares an unanalysed query value against the index's
       * terms. On an analysed field those terms have been lowercased and
       * stemmed, so the filter can silently match nothing — or, worse, match
       * differently than intended. A mapping typo here is a tenant-isolation bug.
       */
      tenantId: { type: 'keyword' },
      eventId: { type: 'keyword' },
      slug: { type: 'keyword' },

      title: {
        type: 'text',
        analyzer: 'event_text',
        fields: {
          // A multi-field: the same value indexed twice under two names. `title`
          // matches, `title.keyword` sorts and aggregates — analysed text cannot
          // do either, because those need column-oriented doc_values.
          keyword: { type: 'keyword', ignore_above: 256 },
        },
      },

      description: { type: 'text', analyzer: 'event_text' },
      venueName: {
        type: 'text',
        analyzer: 'event_text',
        fields: { keyword: { type: 'keyword', ignore_above: 256 } },
      },
      organizerName: {
        type: 'text',
        analyzer: 'event_text',
        fields: { keyword: { type: 'keyword', ignore_above: 256 } },
      },

      category: { type: 'keyword', normalizer: 'lowercase_keyword' },
      tags: { type: 'keyword', normalizer: 'lowercase_keyword' },
      city: { type: 'keyword', normalizer: 'lowercase_keyword' },
      country: { type: 'keyword' },

      status: { type: 'keyword' },
      visibility: { type: 'keyword' },

      startsAt: { type: 'date' },
      endsAt: { type: 'date' },

      capacity: { type: 'integer' },

      updatedAt: { type: 'date' },

      /**
       * The source version, carried so the indexer can use version_type:
       * external. Not indexed: nothing queries or sorts on it, and an unindexed
       * field still round-trips in _source.
       */
      docVersion: { type: 'long', index: false },
    },
  },
} as const;
