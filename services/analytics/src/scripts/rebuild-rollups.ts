import { createLogger } from '@eventhub/logger';
import { createClickHouse } from '../clickhouse.js';
import { migrateEnv } from '../migrate-env.js';

/**
 * Rebuilds every rollup from the raw table: the drill that proves ClickHouse holds
 * nothing irreplaceable. The rollups are rebuildable from activity_events, and
 * activity_events from Kafka, within the topics' retention.
 *
 * AS THE MIGRATOR. TRUNCATE is refused to analytics_app, measured, and a process
 * that answers requests must never be able to empty a table.
 *
 * DERIVED FROM CLICKHOUSE, NOT COPIED. Each materialized view's SELECT comes from
 * system.tables.as_select and its target from its own CREATE statement, so a rollup
 * a later migration adds is rebuilt without this file changing, and the rebuild
 * query can never drift from the view that fills the table day to day. Measured on
 * 25.8: as_select is fully qualified with the database, and TRUNCATE followed by
 * INSERT ... as_select restored identical sums and distinct counts.
 *
 * NEVER WITH INGEST RUNNING. A row inserted between the TRUNCATE and the INSERT ...
 * SELECT reaches the rollup twice: once through the view as it lands, and again
 * through the rebuild's read of the raw table. Stop the ingest first. This script
 * cannot see whether it is stopped, and says so rather than pretending to check.
 */
const logger = createLogger('analytics-rebuild-rollups');

const ch = createClickHouse({
  url: migrateEnv.CLICKHOUSE_URL,
  database: migrateEnv.CLICKHOUSE_DATABASE,
  username: migrateEnv.CLICKHOUSE_MIGRATOR_USER,
  password: migrateEnv.CLICKHOUSE_MIGRATOR_PASSWORD,
});

interface View {
  name: string;
  as_select: string;
  create_table_query: string;
}

async function main(): Promise<void> {
  const views = await (
    await ch.query({
      query: `SELECT name, as_select, create_table_query FROM system.tables
               WHERE database = {db:String} AND engine = 'MaterializedView' ORDER BY name`,
      query_params: { db: migrateEnv.CLICKHOUSE_DATABASE },
      format: 'JSONEachRow',
    })
  ).json<View>();

  if (views.length === 0) {
    throw new Error(`no materialized views in ${migrateEnv.CLICKHOUSE_DATABASE}: nothing to rebuild, which is not a success`);
  }

  for (const view of views) {
    const target = view.create_table_query.match(/\bTO\s+([\w.`]+)/)?.[1];
    if (!target) throw new Error(`${view.name}: its CREATE statement names no TO target, so there is no table to rebuild`);

    const started = Date.now();
    await ch.command({ query: `TRUNCATE TABLE ${target}` });
    await ch.command({ query: `INSERT INTO ${target} ${view.as_select}` });
    logger.info({ view: view.name, target, ms: Date.now() - started }, 'rollup rebuilt from the raw table');
  }
  logger.info({ rebuilt: views.length }, 'rollups rebuilt');
}

main()
  .then(() => ch.close())
  .catch(async (err: unknown) => {
    logger.error({ err }, 'rebuild failed');
    await ch.close().catch(() => {});
    process.exit(1);
  });
