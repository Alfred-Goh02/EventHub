import { z } from 'zod';
import { loadEnv } from '@eventhub/config';
import { DATABASE_NAME } from '../identifiers.js';

/**
 * The ingest process's configuration: Kafka in, ClickHouse out.
 *
 * Its own schema, not the API's. The consumer is a separate process from the read
 * API — separate Deployment, separate scaling driver — so it must not require
 * what only the API needs, and the API must not require Kafka. Chapter 6 paid for
 * learning that twice.
 *
 * It connects to ClickHouse as analytics_app, the same user as the API: INSERT is
 * all ingest needs, and nothing that consumes a topic should hold a credential
 * that can change the schema.
 */
const ingestEnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),

  /** A small HTTP listener for probes and /metrics. A process with no server cannot be probed. */
  INGEST_PORT: z.coerce.number().int().positive().default(3016),

  CLICKHOUSE_URL: z.string().url(),
  CLICKHOUSE_DATABASE: z.string().regex(DATABASE_NAME).default('analytics'),
  CLICKHOUSE_USER: z.string().min(1),
  CLICKHOUSE_PASSWORD: z.string().min(1),

  KAFKA_BROKERS: z.string().min(1),
  SCHEMA_REGISTRY_URL: z.string().url(),
  KAFKA_CONSUMER_GROUP: z.string().min(1).default('analytics-ingest'),

  /**
   * Where a group with no committed offsets begins. 'earliest' in production: the
   * store is derived, and replaying retained history is how it fills.
   * 'latest' LOSES MESSAGES on any partition the group has never committed on,
   * every time the consumer starts — measured in Chapter 6 — so it is for
   * verifiers, which run under a fresh group each time.
   */
  INGEST_START_FROM: z.enum(['earliest', 'latest']).default('earliest'),
});

export const ingestEnv = loadEnv(ingestEnvSchema);
