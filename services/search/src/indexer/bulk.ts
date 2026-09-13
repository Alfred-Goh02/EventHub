import { os } from '../opensearch.js';
import { WRITE_ALIAS } from '../index-definition.js';
import type { IndexAction } from './to-document.js';

/**
 * What became of one document in a bulk request.
 *
 *   indexed    written
 *   stale      refused with a version conflict — an older or duplicate snapshot.
 *              EXPECTED under at-least-once delivery, counted, never an error.
 *   rejected   refused for something about the document itself. It will be
 *              refused again however often it is sent, so it is dead-lettered.
 *   retry      refused for something about the CLUSTER — overloaded, read-only,
 *              index missing. Nothing is wrong with the document, and
 *              dead-lettering it would lose a good event to a bad afternoon.
 */
export type ItemOutcome =
  | { kind: 'indexed' }
  | { kind: 'stale' }
  | { kind: 'rejected'; status: number; reason: string }
  | { kind: 'retry'; status: number; reason: string };

/**
 * Error types that describe the cluster rather than the document, whatever their
 * status code. index_not_found arrives as a 404 and a read-only block as a 403,
 * and classifying by status alone would dead-letter every event written while
 * the index was missing or the disk was full.
 */
const CLUSTER_ERRORS = new Set([
  'index_not_found_exception',
  'cluster_block_exception',
  'es_rejected_execution_exception',
  'circuit_breaking_exception',
]);

interface BulkItemResult {
  status: number;
  error?: { type: string; reason: string };
}

/**
 * Sends one bulk request and classifies every item.
 *
 * Throws only when the REQUEST failed — connection refused, timeout — because
 * then nothing is known about any item and the caller has to treat the whole
 * batch as unprocessed.
 *
 * The response's items are in the same order as the request's actions, which
 * is what lets outcome i be matched to action i.
 */
export async function bulkIndex(actions: IndexAction[]): Promise<ItemOutcome[]> {
  if (actions.length === 0) return [];

  const body = actions.flatMap((action) => [
    {
      index: {
        _index: WRITE_ALIAS,
        _id: action.id,
        // MUST match the tenant alias's routing, or the document lands on a
        // shard that tenant's searches never visit.
        routing: action.routing,
        // Strictly greater wins. Equal or lower is a 409, which is `stale`.
        version: action.version,
        version_type: 'external',
      },
    },
    action.document,
  ]);

  // require_alias: a write through a name that is not an alias fails rather
  // than inventing an index with a guessed mapping.
  const { body: response } = await os.bulk({ body, require_alias: true });
  const items = response.items as Array<{ index?: BulkItemResult }>;

  return items.map((item): ItemOutcome => {
    const result = item.index;
    if (!result) return { kind: 'rejected', status: 0, reason: 'bulk response item carried no index result' };
    if (!result.error) return { kind: 'indexed' };

    const reason = `${result.error.type}: ${result.error.reason}`;
    if (result.error.type === 'version_conflict_engine_exception') return { kind: 'stale' };
    if (result.status === 429 || result.status >= 500 || CLUSTER_ERRORS.has(result.error.type)) {
      return { kind: 'retry', status: result.status, reason };
    }
    return { kind: 'rejected', status: result.status, reason };
  });
}
