import { createLogger } from '@eventhub/logger';
import { INDEX_NAME, READ_ALIAS, WRITE_ALIAS, indexBody } from '../index-definition.js';
import { migrateIndex, type MigrateOptions } from '../migrate.js';

/**
 * Ships a mapping or analyzer change: bump INDEX_VERSION in index-definition.ts,
 * deploy, and run this. It moves search from whatever events-read resolves to
 * onto events-v{INDEX_VERSION}. See migrate.ts for the order and why.
 *
 * Rolling back is the same command after reverting INDEX_VERSION: the old index
 * is still there, and the copy runs the other way.
 */
const log = createLogger('search-reindex');

migrateIndex({
  target: INDEX_NAME,
  readAlias: READ_ALIAS,
  writeAlias: WRITE_ALIAS,
  body: indexBody as unknown as MigrateOptions['body'],
}).then(
  (result) => {
    log.info(result, 'migration complete');
    process.exit(0);
  },
  (err: unknown) => {
    log.error({ err }, 'migration failed');
    process.exit(1);
  },
);
