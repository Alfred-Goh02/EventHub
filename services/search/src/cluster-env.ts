import { z } from 'zod';
import { loadEnv } from '@eventhub/config';

/**
 * What it takes to reach the cluster, and nothing else.
 *
 * SPLIT FROM env.ts BECAUSE THE SAME BREAK HAPPENED TWICE. opensearch.ts used to
 * load the whole service schema, and everything that talks to OpenSearch imports
 * opensearch.ts: bootstrap-index, and all three search verifiers. So each
 * variable the HTTP or Kafka side added became a variable a script that only
 * creates an index had to be given. Part 2 added SCHEMA_REGISTRY_URL and the
 * isolation verifier and CI's bootstrap step both failed on it; part 3 added
 * JWT_ISSUER, JWT_AUDIENCE and EVENT_SERVICE_URL and all of them failed again.
 * Supplying defaults script by script would have been a list maintained by hand,
 * which is the trap this repository keeps writing checks for.
 *
 * A script that creates an index has no business knowing how tokens are verified.
 */
export const clusterEnvSchema = z.object({
  OPENSEARCH_NODE: z.string().url(),

  /**
   * `none` locally — the security plugin is off, so there is nothing to sign.
   * `aws-sigv4` against a managed domain. Branching here rather than at each
   * call site means moving to AWS is an environment change, not a code change.
   */
  OPENSEARCH_AUTH_MODE: z.enum(['none', 'aws-sigv4']).default('none'),
  AWS_REGION: z.string().optional(),
});

export const clusterEnv = loadEnv(clusterEnvSchema);
