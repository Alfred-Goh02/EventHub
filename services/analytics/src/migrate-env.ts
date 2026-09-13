import { z } from 'zod';
import { loadEnv } from '@eventhub/config';
import { DATABASE_NAME } from './identifiers.js';

/**
 * The migration runner's configuration, and nothing the service needs.
 *
 * Separate from env.ts for two reasons. The migrator credential must not be in
 * the service's environment at all: least privilege is a property of what a
 * process HOLDS, not of which variable it happens to read. And the runner must
 * not require what only the service needs — Chapter 6 learned that twice, when a
 * variable added for the HTTP side broke every script that merely created an
 * index.
 */
const migrateEnvSchema = z.object({
  CLICKHOUSE_URL: z.string().url(),
  CLICKHOUSE_DATABASE: z.string().regex(DATABASE_NAME).default('analytics'),
  CLICKHOUSE_MIGRATOR_USER: z.string().min(1),
  CLICKHOUSE_MIGRATOR_PASSWORD: z.string().min(1),
});

export const migrateEnv = loadEnv(migrateEnvSchema);
