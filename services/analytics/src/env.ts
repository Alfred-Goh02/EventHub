import { z } from 'zod';
import { baseEnvSchema, loadEnv } from '@eventhub/config';
import { DATABASE_NAME } from './identifiers.js';

/**
 * The Analytics SERVICE's configuration.
 *
 * DATABASE_URL IS OMITTED, as in Search: this service owns no Postgres database.
 * Its store is ClickHouse, derived from Kafka and rebuildable by replaying the
 * topics, and it never reads Postgres.
 *
 * ONLY THE APP CREDENTIAL. The migrator's user and password live in
 * migrate-env.ts, read by the migration runner alone, so the process answering
 * requests never holds a credential that can DROP TABLE.
 */
const analyticsEnvSchema = baseEnvSchema.omit({ DATABASE_URL: true }).extend({
  CLICKHOUSE_URL: z.string().url(),
  CLICKHOUSE_DATABASE: z.string().regex(DATABASE_NAME).default('analytics'),
  CLICKHOUSE_USER: z.string().min(1),
  CLICKHOUSE_PASSWORD: z.string().min(1),
});

export const env = loadEnv(analyticsEnvSchema);
export type AnalyticsEnv = typeof env;
