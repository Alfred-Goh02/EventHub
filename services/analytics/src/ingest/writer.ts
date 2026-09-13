import { createHash } from 'node:crypto';
import type { ClickHouseClient } from '@clickhouse/client';
import type { ActivityRow } from './mappers.js';

export interface WriteOutcome {
  /** Rows sent to activity_events on this call. */
  inserted: number;
  /** Rows dropped because the same tenant, message_id and line was already stored, or appeared twice in the batch. */
  duplicates: number;
}

/** A row's identity. Tenant ids cannot contain a space, so the key cannot be forged across fields. */
const rowKey = (r: { tenant_id: string; message_id: string; line: number }): string =>
  `${r.tenant_id} ${r.message_id}#${r.line}`;

/**
 * Which of a batch's rows are already stored.
 *
 * TWO IN LISTS, NOT ONE IN OVER (tenant_id, event_id) PAIRS. The pair form reads
 * as more exact, and EXPLAIN indexes = 1 on ClickHouse 25.8 shows it using only
 * event_id from the primary key — both as a parameter and as literals — which is
 * the search scripts/verify-analytics-schema.mjs measured reading every granule.
 * Two lists use tenant_id and event_id both. Their cross product can name pairs the
 * batch does not contain, which costs nothing: the match is made in code on
 * (tenant_id, message_id, line), so it stays exact.
 *
 * Exported so scripts/verify-analytics-ingest.mjs can EXPLAIN this exact text.
 */
export const EXISTING_ROWS_QUERY = `SELECT tenant_id, message_id, line FROM activity_events
 WHERE tenant_id IN {tenants:Array(String)}
   AND event_id IN {events:Array(String)}
   AND occurred_at BETWEEN {from:DateTime64(3, 'UTC')} AND {to:DateTime64(3, 'UTC')}
   AND message_id IN {ids:Array(String)}`;

/**
 * Writes rows to activity_events so that writing the same message twice counts it once.
 *
 * WHY THIS IS NEEDED. Kafka delivers at least once, and the consumer commits only
 * after ClickHouse acknowledges, so any failure between the two redelivers. A
 * duplicate row here is not cosmetic: the materialized views fire on insert, so a
 * duplicated purchase is a duplicated sale in ticket_funnel_daily, and a rollup
 * cannot be deduplicated after the fact.
 *
 * WHY A TOKEN FROM THE BATCH'S OFFSETS WAS NOT ENOUGH. The first version used
 * insert_deduplication_token = topic-partition-firstOffset-lastOffset. A refetch
 * after a crash starts at the same offset but ends wherever the log ends by then,
 * so the redelivered batch has a different last offset, a different token, and
 * every row in it lands a second time. And a relay that republishes a message puts
 * it at a NEW offset, which no offset-derived token can recognise — the Avro
 * contract says in so many words that consumers must dedupe on messageId.
 *
 * SO IDENTITY COMES FROM THE MESSAGE, in two layers:
 *
 *   1. Rows whose (tenant_id, message_id, line) is already stored are dropped
 *      before the insert. This catches both cases above, whatever the batch
 *      boundaries. The tenant is part of the identity because Kafka is not
 *      tenant-scoped and an id is only as unique as its producer makes it:
 *      keyed on the id alone, one tenant's message silently suppressed another
 *      tenant's, both inside one batch and against a row already stored, which
 *      scripts/verify-analytics-ingest.mjs measured. A duplicate carries the same
 *      occurred_at as its original, so the time bounds prune monthly partitions
 *      as well.
 *
 *   2. insertRows derives its deduplication token from the rows' identities. The
 *      lookup and the insert are two statements, so an insert that timed out on the
 *      client while still landing on the server can race its own retry past step 1.
 *
 * WHAT THIS STILL DOES NOT COVER, said here rather than discovered: two consumers
 * writing the same message at the same moment inside DIFFERENT sets of rows (a
 * rebalance zombie with other neighbours in its batch) both pass step 1 with
 * different tokens. The window is one insert long, and money stays reconcilable
 * against Ticketing's Postgres, which is the system of record.
 */
export async function writeActivity(ch: ClickHouseClient, rows: ActivityRow[]): Promise<WriteOutcome> {
  if (rows.length === 0) return { inserted: 0, duplicates: 0 };

  const unique = new Map<string, ActivityRow>();
  for (const row of rows) if (!unique.has(rowKey(row))) unique.set(rowKey(row), row);
  const candidates = [...unique.values()];

  // This time format sorts lexicographically, so the string min and max are the time min and max.
  const times = candidates.map((r) => r.occurred_at).sort();

  const existing = await ch.query({
    query: EXISTING_ROWS_QUERY,
    query_params: {
      tenants: [...new Set(candidates.map((r) => r.tenant_id))],
      events: [...new Set(candidates.map((r) => r.event_id))],
      ids: [...new Set(candidates.map((r) => r.message_id))],
      from: times[0],
      to: times[times.length - 1],
    },
    format: 'JSONEachRow',
  });
  const stored = new Set((await existing.json<{ tenant_id: string; message_id: string; line: number }>()).map(rowKey));
  const fresh = candidates.filter((r) => !stored.has(rowKey(r)));

  if (fresh.length > 0) await insertRows(ch, fresh);
  return { inserted: fresh.length, duplicates: rows.length - fresh.length };
}

/**
 * The insert, with a token that is a hash of the rows' identities — so the same
 * token always means the same rows, and the token can never discard a row it has
 * not already seen. deduplicate_blocks_in_dependent_materialized_views carries the
 * deduplication into every rollup; without it the raw table deduplicates and the
 * rollups double-count, which scripts/verify-analytics-schema.mjs measured.
 *
 * THE TENANT IS IN THE TOKEN for the same reason it is in the lookup. Hashed from
 * message ids alone, a second tenant's rows under a colliding id produced the same
 * token as the first tenant's, and ClickHouse discarded them as a duplicate block
 * while the caller counted them inserted: scripts/verify-analytics-ingest.mjs found
 * 0 rows stored for a tenant the writer reported writing.
 *
 * Exported so a verifier can play the retry that got past the lookup.
 */
export async function insertRows(ch: ClickHouseClient, rows: ActivityRow[]): Promise<void> {
  const token = createHash('sha256').update(rows.map(rowKey).sort().join('\n')).digest('hex');
  await ch.insert({
    table: 'activity_events',
    values: rows,
    format: 'JSONEachRow',
    clickhouse_settings: {
      insert_deduplication_token: token,
      deduplicate_blocks_in_dependent_materialized_views: 1,
    },
  });
}
