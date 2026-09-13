import { z } from 'zod';
import { baseEnvSchema, loadEnv } from '@eventhub/config';

/**
 * Search's configuration.
 *
 * DATABASE_URL IS OMITTED, AND THAT ABSENCE IS THE POINT. This service owns no
 * database — it owns a derived read model in OpenSearch, fed from Kafka, which
 * is rebuildable by definition. Postgres remains the write model and Search
 * never talks to it.
 *
 * Omitting rather than making it optional in the shared base: optional would
 * have silently weakened Auth, Event, Ticketing and Live, every one of which
 * calls loadBaseEnv() and would have kept booting with the variable missing
 * right up until its first query. This way the absence is declared here, by the
 * one service it is true of.
 */
const searchEnvSchema = baseEnvSchema.omit({ DATABASE_URL: true }).extend({
  OPENSEARCH_NODE: z.string().url(),

  /*
   * There is deliberately no variable for the index or alias names. They were
   * here once, declared and documented, and nothing read them: the code used the
   * constants in index-definition.ts, so setting one changed nothing and said
   * nothing. The names are fixed on purpose — a migration repoints what an alias
   * RESOLVES TO, never what the application calls it.
   */

  /**
   * `none` locally — the security plugin is off, so there is nothing to sign.
   * `aws-sigv4` against a managed domain. Branching here rather than at each
   * call site means moving to AWS is an environment change, not a code change.
   */
  OPENSEARCH_AUTH_MODE: z.enum(['none', 'aws-sigv4']).default('none'),
  AWS_REGION: z.string().optional(),

  KAFKA_BROKERS: z.string().min(1),

  /**
   * One group, so partitions are divided across replicas rather than every
   * replica indexing every message. This is the case where Kafka's
   * load-balancing IS what you want — the opposite of Live's fan-out problem.
   */
  KAFKA_CONSUMER_GROUP: z.string().default('search-indexer'),
});

export const env = loadEnv(searchEnvSchema);
export type SearchEnv = typeof env;
