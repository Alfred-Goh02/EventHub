import { createHash } from 'node:crypto';
import { readdirSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createLogger } from '@eventhub/logger';
import { createClickHouse } from '../clickhouse.js';
import { migrateEnv as env } from '../migrate-env.js';

const log = createLogger('analytics-migrate');

/** services/analytics/migrations, from dist/scripts. */
const MIGRATIONS_DIR = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'migrations');

/**
 * Statements are separated by a marker line, never by splitting on semicolons,
 * which breaks the first time a statement contains one inside a string literal.
 */
const SEPARATOR = /^-- >>>[ \t]*$/m;

/** A chunk that is nothing but comments and whitespace is not a statement. */
const isOnlyComments = (chunk: string): boolean =>
  chunk
    .split(/\r?\n/)
    .map((l) => l.trim())
    .every((l) => l === '' || l.startsWith('--'));

/**
 * Applies services/analytics/migrations/*.sql in filename order, as
 * analytics_migrator.
 *
 * NO TRANSACTIONS, SO NOTHING HERE IS ATOMIC. ClickHouse has no multi-statement
 * DDL transaction, so a migration that fails halfway leaves its earlier
 * statements applied. The response is to make every statement idempotent — IF NOT
 * EXISTS everywhere — so re-running a half-applied migration finishes it rather
 * than failing on what already exists.
 *
 * A MIGRATION EDITED AFTER IT WAS APPLIED IS REFUSED. Each file's checksum is
 * recorded. A changed file would otherwise be skipped as "already applied" and
 * the database would silently differ from the file that claims to describe it;
 * the fix for a mistake in an applied migration is a new migration.
 *
 * Why not a migration tool: ClickHouse has none built in, the third-party ones
 * each bring a toolchain and a dialect, and this is short enough to read in full.
 */
async function main(): Promise<void> {
  const ch = createClickHouse({
    url: env.CLICKHOUSE_URL,
    database: env.CLICKHOUSE_DATABASE,
    username: env.CLICKHOUSE_MIGRATOR_USER,
    password: env.CLICKHOUSE_MIGRATOR_PASSWORD,
  });

  try {
    await ch.command({
      query: `CREATE TABLE IF NOT EXISTS schema_migrations
              (version String, checksum String, applied_at DateTime64(3, 'UTC') DEFAULT now64(3))
              ENGINE = MergeTree ORDER BY version`,
    });

    const applied = new Map(
      (
        await (await ch.query({ query: 'SELECT version, checksum FROM schema_migrations', format: 'JSONEachRow' })).json<{
          version: string;
          checksum: string;
        }>()
      ).map((r) => [r.version, r.checksum]),
    );

    const files = readdirSync(MIGRATIONS_DIR).filter((f) => f.endsWith('.sql')).sort();
    if (files.length === 0) throw new Error(`no migrations found in ${MIGRATIONS_DIR}`);

    let ran = 0;
    for (const file of files) {
      const text = readFileSync(join(MIGRATIONS_DIR, file), 'utf8');
      const checksum = createHash('sha256').update(text.replace(/\r\n/g, '\n')).digest('hex');
      const previous = applied.get(file);

      if (previous !== undefined) {
        if (previous !== checksum) {
          throw new Error(
            `${file} was changed after it was applied (recorded ${previous.slice(0, 12)}, file is ` +
              `${checksum.slice(0, 12)}). An applied migration is history; write a new one.`,
          );
        }
        continue;
      }

      const statements = text
        .split(SEPARATOR)
        .map((s) => s.trim().replace(/;\s*$/, ''))
        .filter((s) => s !== '' && !isOnlyComments(s));

      for (const statement of statements) await ch.command({ query: statement });

      await ch.insert({ table: 'schema_migrations', values: [{ version: file, checksum }], format: 'JSONEachRow' });
      log.info({ migration: file, statements: statements.length }, 'applied');
      ran += 1;
    }

    log.info({ database: env.CLICKHOUSE_DATABASE, applied: ran, total: files.length }, 'migrations complete');
  } finally {
    await ch.close();
  }
}

main().then(
  () => process.exit(0),
  (err: unknown) => {
    log.error({ err }, 'migration failed');
    process.exit(1);
  },
);
