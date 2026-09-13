import type { EventSnapshot } from './contract.js';

export interface IndexAction {
  id: string;
  routing: string;
  version: number;
  document: Record<string, unknown>;
}

/**
 * One snapshot becomes one whole document.
 *
 * events.activity carries FULL snapshots, never deltas — the topic is compacted,
 * so a replay holds only the last message per event. That is what lets every
 * write be a complete `index` with external versioning. A partial `_update` would
 * not do: it does not accept version_type at all, so it could not refuse a stale
 * message.
 *
 * A SOFT DELETE BECOMES status "deleted", NOT A DELETE REQUEST. A delete leaves a
 * tombstone, and the version check on a later write compares against that
 * tombstone. Tombstones are garbage-collected after index.gc_deletes, 60 seconds
 * by default, and a stale snapshot arriving after that finds nothing to compare
 * with and RESURRECTS the event. A document that still exists keeps its version
 * for ever, so a stale snapshot is refused however late it arrives. The query
 * side excludes the status; scripts/verify-search-indexer.mjs replays a stale
 * snapshot over a deleted event to prove it stays deleted.
 *
 * ONLY FIELDS THE SNAPSHOT CARRIES. The mapping also has venueName, city, tags
 * and others that nothing produces yet, and inventing values for them here would
 * be worse than leaving them empty. organiserId is left out: it is a user id from
 * another service's database, nothing searches by it, and `dynamic: strict` would
 * reject it anyway.
 */
export function toIndexAction(snapshot: EventSnapshot): IndexAction {
  return {
    id: snapshot.eventId,
    routing: snapshot.tenantId,
    version: snapshot.version,
    document: {
      tenantId: snapshot.tenantId,
      eventId: snapshot.eventId,
      slug: snapshot.slug,
      title: snapshot.title,
      ...(snapshot.description !== null ? { description: snapshot.description } : {}),
      status: snapshot.deleted ? 'deleted' : snapshot.status.toLowerCase(),
      startsAt: snapshot.startsAt,
      endsAt: snapshot.endsAt,
      capacity: snapshot.capacity,
      updatedAt: snapshot.occurredAt,
      docVersion: snapshot.version,
    },
  };
}
