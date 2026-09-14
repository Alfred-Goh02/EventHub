import { createHash } from 'node:crypto';
import { ClickHouseError, type ClickHouseClient } from '@clickhouse/client';
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
 * How much of one lookup parameter a chunk may use.
 *
 * Each query parameter travels as one HTTP form field, and ClickHouse 25.8 refuses
 * a field longer than http_max_field_value_size, 128 KiB by default. Measured, not
 * found by any test: 10000 ids in one lookup failed with "Field value too long".
 * That is not a refusal of the rows, so the batch was rethrown and would have been
 * redelivered forever — a consumer catching up on a backlog would never get past
 * its first large batch. Half the limit, with every character costed as if it
 * needed escaping.
 */
const LOOKUP_PARAMETER_BYTES = 64 * 1024;
const lookupCost = (value: string): number => 2 * Buffer.byteLength(value) + 4;

/** Candidates split so that no lookup parameter — tenants, events or ids — outgrows LOOKUP_PARAMETER_BYTES. */
function lookupChunks(candidates: ActivityRow[]): ActivityRow[][] {
  const chunks: ActivityRow[][] = [];
  let chunk: ActivityRow[] = [];
  let seen = { tenants: new Set<string>(), events: new Set<string>(), ids: new Set<string>() };
  let used = { tenants: 0, events: 0, ids: 0 };

  for (const row of candidates) {
    const costs = () => ({
      tenants: seen.tenants.has(row.tenant_id) ? 0 : lookupCost(row.tenant_id),
      events: seen.events.has(row.event_id) ? 0 : lookupCost(row.event_id),
      ids: seen.ids.has(row.message_id) ? 0 : lookupCost(row.message_id),
    });
    let add = costs();
    const tooBig =
      used.tenants + add.tenants > LOOKUP_PARAMETER_BYTES ||
      used.events + add.events > LOOKUP_PARAMETER_BYTES ||
      used.ids + add.ids > LOOKUP_PARAMETER_BYTES;
    if (tooBig && chunk.length > 0) {
      chunks.push(chunk);
      chunk = [];
      seen = { tenants: new Set(), events: new Set(), ids: new Set() };
      used = { tenants: 0, events: 0, ids: 0 };
      add = costs();
    }
    chunk.push(row);
    seen.tenants.add(row.tenant_id);
    seen.events.add(row.event_id);
    seen.ids.add(row.message_id);
    used = { tenants: used.tenants + add.tenants, events: used.events + add.events, ids: used.ids + add.ids };
  }
  if (chunk.length > 0) chunks.push(chunk);
  return chunks;
}

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

  const stored = new Set<string>();
  for (const chunk of lookupChunks(candidates)) {
    // This time format sorts lexicographically, so the string min and max are the time min and max.
    const times = chunk.map((r) => r.occurred_at).sort();
    const existing = await ch.query({
      query: EXISTING_ROWS_QUERY,
      query_params: {
        tenants: [...new Set(chunk.map((r) => r.tenant_id))],
        events: [...new Set(chunk.map((r) => r.event_id))],
        ids: [...new Set(chunk.map((r) => r.message_id))],
        from: times[0],
        to: times[times.length - 1],
      },
      format: 'JSONEachRow',
    });
    for (const r of await existing.json<{ tenant_id: string; message_id: string; line: number }>()) stored.add(rowKey(r));
  }
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

/** One Kafka message's rows, with what the dead-letter table needs if ClickHouse refuses them. */
export interface MessageRows {
  offset: string;
  payload: Buffer | null;
  rows: ActivityRow[];
}

export interface Refused {
  offset: string;
  payload: Buffer | null;
  error: string;
}

export interface BatchOutcome extends WriteOutcome {
  /** Messages whose rows ClickHouse refused. Everything else in the batch was written. */
  refused: Refused[];
}

/**
 * Whether ClickHouse refused the ROWS, as opposed to being unable to take them.
 *
 * Only the CANNOT_PARSE_ family, because that is what bad rows were measured to
 * produce on 25.8: a lone surrogate (CANNOT_PARSE_ESCAPE_SEQUENCE), a negative
 * number in an unsigned column (CANNOT_PARSE_NUMBER), a fraction in an integer
 * column (CANNOT_PARSE_INPUT_ASSERTION_FAILED). A missing table, refused access,
 * too many parts, a timeout — none of those is the message's fault, and treating
 * one as a refusal would dead-letter every message in the batch.
 *
 * NOT A SUBSTITUTE FOR THE MAPPER. The same measurement found ClickHouse storing
 * many bad values without complaint — 70000 in a UInt16 as 4464, 2^64 cents as
 * 384, an impossible date as a real one. Nothing downstream can see those; only
 * refusing them in mappers.ts keeps them out.
 */
export function refusedByClickHouse(err: unknown): err is ClickHouseError {
  return err instanceof ClickHouseError && /^CANNOT_PARSE_/.test(err.type ?? '');
}

/**
 * Writes a batch; if ClickHouse refuses it, finds which messages it refused.
 *
 * An insert is all or nothing, so one bad row refuses every row beside it. The
 * first version rethrew that refusal like an outage: nothing was committed, the
 * same batch came back, and was refused again — a partition stalled for good by
 * one message, with every good message behind it (found by review, then measured
 * in scripts/verify-analytics-ingest.mjs).
 *
 * On a refusal, each message is written on its own. That costs a data part per
 * message, which is what batching exists to avoid, but only for a batch that has
 * already failed. Deduplication still holds, because each write is writeActivity.
 * A failure that is not a refusal, at either step, is rethrown: that is an outage,
 * and redelivery is the right answer to it.
 */
export async function writeMessages(ch: ClickHouseClient, messages: MessageRows[]): Promise<BatchOutcome> {
  try {
    const written = await writeActivity(ch, messages.flatMap((m) => m.rows));
    return { ...written, refused: [] };
  } catch (err) {
    if (!refusedByClickHouse(err)) throw err;
  }

  const outcome: BatchOutcome = { inserted: 0, duplicates: 0, refused: [] };
  for (const message of messages) {
    try {
      const written = await writeActivity(ch, message.rows);
      outcome.inserted += written.inserted;
      outcome.duplicates += written.duplicates;
    } catch (err) {
      if (!refusedByClickHouse(err)) throw err;
      outcome.refused.push({
        offset: message.offset,
        payload: message.payload,
        error: `refused by ClickHouse (${err.type}): ${err.message}`.slice(0, 2_000),
      });
    }
  }
  return outcome;
}
