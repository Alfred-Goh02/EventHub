import type { ClickHouseClient } from '@clickhouse/client';

export interface TableParts {
  table: string;
  activeParts: number;
  /** The partition holding the most active parts, which is the one the thresholds apply to. */
  worstPartition: string | null;
  worstPartitionParts: number;
}

export interface PartCounts {
  tables: TableParts[];
  /** Inserts into a partition with this many active parts are slowed down. */
  delayInsertAt: number | null;
  /** And at this many they are refused. */
  throwInsertAt: number | null;
}

/**
 * Active data parts per table: the number that says ingest is ABOUT to break,
 * where consumer lag says it already has.
 *
 * Every insert creates a part and background merges shrink the count. Inserting
 * faster than merges keep up — one row per message is the classic way — and
 * ClickHouse first slows inserts into a partition at parts_to_delay_insert active
 * parts, then refuses them at parts_to_throw_insert (1000 and 3000 on 25.8,
 * measured). Both apply per partition, so what matters is each table's WORST
 * partition, reported beside the thresholds read from the server itself rather
 * than copied here.
 *
 * Needs the column grants on system.parts and system.merge_tree_settings in
 * db/clickhouse-init/01-users.sql; without them both queries are ACCESS_DENIED.
 */
export async function partCounts(ch: ClickHouseClient, database: string): Promise<PartCounts> {
  const [parts, settings] = await Promise.all([
    ch
      .query({
        query: `SELECT table, partition, count() AS parts FROM system.parts
                 WHERE database = {db:String} AND active GROUP BY table, partition`,
        query_params: { db: database },
        format: 'JSONEachRow',
      })
      .then((r) => r.json<{ table: string; partition: string; parts: number | string }>()),
    ch
      .query({
        query: `SELECT name, value FROM system.merge_tree_settings
                 WHERE name IN ('parts_to_delay_insert', 'parts_to_throw_insert')`,
        format: 'JSONEachRow',
      })
      .then((r) => r.json<{ name: string; value: string }>()),
  ]);

  const byTable = new Map<string, TableParts>();
  for (const p of parts) {
    const n = Number(p.parts);
    const t = byTable.get(p.table) ?? { table: p.table, activeParts: 0, worstPartition: null, worstPartitionParts: 0 };
    t.activeParts += n;
    if (n > t.worstPartitionParts) {
      t.worstPartitionParts = n;
      t.worstPartition = p.partition;
    }
    byTable.set(p.table, t);
  }

  const setting = (name: string): number | null => {
    const s = settings.find((x) => x.name === name);
    return s ? Number(s.value) : null;
  };

  return {
    tables: [...byTable.values()].sort((a, b) => a.table.localeCompare(b.table)),
    delayInsertAt: setting('parts_to_delay_insert'),
    throwInsertAt: setting('parts_to_throw_insert'),
  };
}
