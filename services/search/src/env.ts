import { z } from 'zod';
import { baseEnvSchema, loadEnv } from '@eventhub/config';
import { clusterEnvSchema } from './cluster-env.js';

/**
 * The Search SERVICE's configuration: the cluster, plus what the HTTP API and the
 * indexer need. Anything that only talks to the cluster imports cluster-env.ts
 * instead — see there for why the two are separate.
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
 *
 * There is deliberately no variable for the index or alias names. They were
 * here once, declared and documented, and nothing read them: the code used the
 * constants in index-definition.ts, so setting one changed nothing and said
 * nothing. The names are fixed on purpose — a migration repoints what an alias
 * RESOLVES TO, never what the application calls it.
 */
const searchEnvSchema = baseEnvSchema
  .omit({ DATABASE_URL: true })
  .merge(clusterEnvSchema)
  .extend({
    KAFKA_BROKERS: z.string().min(1),

    /**
     * One group, so partitions are divided across replicas rather than every
     * replica indexing every message. This is the case where Kafka's
     * load-balancing IS what you want — the opposite of Live's fan-out problem.
     */
    KAFKA_CONSUMER_GROUP: z.string().default('search-indexer'),

    /** Where the indexer resolves Avro schema ids. The relay registers them. */
    SCHEMA_REGISTRY_URL: z.string().url(),

    /**
     * Where a consumer group with NO committed offsets begins.
     *
     * 'earliest' is the real setting: the topic is compacted precisely so that a
     * new deployment, or a wiped index, rebuilds by replaying from the start.
     * 'latest' exists for the verifiers, which run under a fresh group each time
     * — replaying the topic's whole history there would index every earlier
     * run's fixtures into the shared index.
     *
     * 'latest' LOSES MESSAGES, which is why it is for verifiers only. A partition
     * the group has never committed on starts at the end EVERY time the consumer
     * starts, so anything produced there while the indexer was down is skipped
     * for good. scripts/verify-search-observability.mjs found it: a restart under
     * 'latest' never indexed the seven events produced while it was stopped.
     */
    SEARCH_INDEXER_START_FROM: z.enum(['earliest', 'latest']).default('earliest'),

    /**
     * Rejected unless a token's `iss` and `aud` match exactly. Auth's public key
     * arrives separately, as AUTH_PUBLIC_KEY_B64 or AUTH_PUBLIC_KEY_FILE, read by
     * publicKeyFromEnv in @eventhub/auth-middleware.
     */
    JWT_ISSUER: z.string().min(1),
    JWT_AUDIENCE: z.string().min(1),

    /**
     * Where the degraded path goes when OpenSearch cannot serve. Only ever called
     * with the caller's own token; Search holds no credential for it.
     */
    EVENT_SERVICE_URL: z.string().url(),

    /**
     * Signs pagination cursors so a client cannot edit one — see cursor.ts. At
     * least 32 characters, because an HMAC key is only as strong as its entropy.
     * Rotating it invalidates every cursor in flight, which costs a reader one
     * "start again" and nothing more.
     */
    SEARCH_CURSOR_SECRET: z.string().min(32),
  });

export const env = loadEnv(searchEnvSchema);
export type SearchEnv = typeof env;
