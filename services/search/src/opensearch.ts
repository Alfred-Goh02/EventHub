import { Client } from '@opensearch-project/opensearch';
// The cluster's settings only — not the service's. See cluster-env.ts.
import { clusterEnv as env } from './cluster-env.js';

/**
 * The one place that knows how to reach the cluster.
 *
 * Everything else imports `os` and never constructs a client, so moving from a
 * local container to a managed domain is an environment change rather than a
 * code change.
 */
function build(): Client {
  const common = {
    node: env.OPENSEARCH_NODE,

    /**
     * A bounded wait, because degrading depends on failing fast.
     *
     * Search is the one service allowed to be degraded rather than broken: its
     * store is a derived read model, so losing it should cost the search box and
     * nothing else. Today that means /ready answers 503 while /health stays 200
     * (see index.ts). The user-facing half — an honest empty state instead of an
     * error — is not built yet. Both only work if a dead cluster fails FAST:
     * without a timeout the request hangs, readiness never flips, and the page
     * never loads.
     */
    requestTimeout: 5_000,
    maxRetries: 2,

    /**
     * MUST stay false against Amazon OpenSearch Service.
     *
     * Sniffing asks the cluster for its node addresses and then talks to them
     * directly — but a managed domain's nodes are not reachable from outside
     * its endpoint, so sniffing discovers addresses that cannot be dialled and
     * every request after the first fails. It looks like an intermittent
     * network fault.
     */
    sniffOnStart: false,
    compression: 'gzip' as const,
  };

  if (env.OPENSEARCH_AUTH_MODE === 'aws-sigv4') {
    throw new Error(
      'OPENSEARCH_AUTH_MODE=aws-sigv4 is declared but not wired yet. It needs ' +
        'AwsSigv4Signer from @opensearch-project/opensearch/aws plus ' +
        '@aws-sdk/credential-provider-node, and the service name differs between a ' +
        "managed domain ('es') and a Serverless collection ('aoss'). Refusing to start " +
        'rather than silently connecting unsigned.',
    );
  }

  return new Client(common);
}

export const os = build();

/**
 * A note that will save someone twenty minutes: the v2 client WRAPS responses.
 *
 *   const { body } = await os.search({ index, body: {...} });
 *   body.hits.hits
 *
 * not `response.hits.hits`. Modern elasticsearch-js unwrapped this, so every
 * recent example you will find online is written the other way round.
 */
export async function clusterInfo(): Promise<{ distribution: string; version: string }> {
  const { body } = await os.info();
  return { distribution: body.version.distribution, version: body.version.number };
}
