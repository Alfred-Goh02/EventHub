import type { Prisma } from './generated/prisma/client.js';

/**
 * The single place this service writes to its outbox.
 *
 * It exists because of a defect found by review: only the publish handler
 * emitted a message. PATCH and DELETE changed Event state and emitted nothing,
 * so a downstream projection would receive one snapshot at publish time and be
 * wrong forever after — an organiser publishes at capacity 500, patches it to
 * 200, and Ticketing keeps selling 500. There is no foreign key anywhere to
 * catch that, because the data lives in another database.
 *
 * Routing every state change through one function makes "changed the row but
 * forgot to emit" a thing you have to do deliberately rather than by omission.
 */

export const EVENTS_ACTIVITY = 'events.activity';

/**
 * Why events.activity carries ONE record type rather than several.
 *
 * A Schema Registry subject holds one schema, and under the default
 * TopicNameStrategy the subject is `<topic>-value` — one schema for the whole
 * topic. Registering a second record NAME against it is rejected:
 *
 *   409 NAME_MISMATCH — The name of the schema has changed (path '/name')
 *
 * (Verified against this project's running registry.) So `event.published` and
 * `event.cancelled` cannot be separate record types on one topic. Instead every
 * message is a full SNAPSHOT of the event, with `changeType` saying what
 * happened. One record type, no top-level union.
 *
 * That also solves a second problem for free. events.activity is a COMPACTED
 * topic keyed by event id, so Kafka retains the latest snapshot per event
 * forever. A consumer can rebuild its entire projection by replaying from the
 * beginning — no HTTP bootstrap path, no cold-start hole. A partial "what
 * changed" message could not do that, because replaying deltas requires every
 * delta, and compaction throws away all but the last.
 *
 * The cost, stated plainly: you lose the change history on this topic. That is
 * why tickets.reserved and tickets.purchased stay cleanup.policy=delete — for
 * those, the sequence of events IS the information.
 */
export type ChangeType = 'CREATED' | 'UPDATED' | 'PUBLISHED' | 'CANCELLED' | 'DELETED';

/** The fields a snapshot needs. Any select feeding emitEventSnapshot must cover these. */
export interface EventRow {
  id: string;
  tenantId: string;
  title: string;
  slug: string;
  description: string | null;
  status: string;
  capacity: number;
  startsAt: Date;
  endsAt: Date;
  organiserId: string;
  version: number;
  deletedAt: Date | null;
}

export interface EventSnapshot {
  eventId: string;
  tenantId: string;
  title: string;
  slug: string;
  description: string | null;
  status: string;
  capacity: number;
  startsAt: string;
  endsAt: string;
  organiserId: string;
  /**
   * The row version this snapshot describes.
   *
   * A consumer keeps the highest version it has applied and discards anything
   * lower. That is what makes the projection safe under at-least-once delivery
   * AND under out-of-order arrival — neither a redelivery nor a late message can
   * move the projection backwards.
   */
  version: number;
  changeType: ChangeType;
  /** Soft-deleted events are still published, so consumers learn to stop selling. */
  deleted: boolean;
}

/**
 * Writes one outbox row. MUST be called with the same `tx` as the state change
 * it describes — that is the entire point of the pattern.
 */
export async function emit(
  tx: Prisma.TransactionClient,
  params: {
    tenantId: string;
    topic: string;
    eventType: string;
    aggregateId: string;
    payload: unknown;
    schemaVersion?: number;
  },
): Promise<void> {
  await tx.outboxMessage.create({
    data: {
      tenantId: params.tenantId,
      topic: params.topic,
      eventType: params.eventType,
      schemaVersion: params.schemaVersion ?? 1,
      aggregateId: params.aggregateId,
      payload: params.payload as Prisma.InputJsonValue,
    },
  });
}

/**
 * Emits a full snapshot of an event to events.activity.
 *
 * `aggregateId` is the event id, so every message about one event shares a Kafka
 * key and therefore a partition — which is what preserves their relative order.
 * Keying by tenantId instead would put one tenant's entire traffic on a single
 * partition, and adding brokers would do nothing for the busiest customer.
 */
export async function emitEventSnapshot(
  tx: Prisma.TransactionClient,
  event: EventRow,
  changeType: ChangeType,
): Promise<void> {
  const snapshot: EventSnapshot = {
    eventId: event.id,
    tenantId: event.tenantId,
    title: event.title,
    slug: event.slug,
    description: event.description,
    status: event.status,
    capacity: event.capacity,
    // ISO-8601 strings, matching the Avro schema's chosen representation. The
    // alternative, timestamp-millis, rejects a string and would also reject a
    // Date unless a logicalTypes factory is registered — a poison message that
    // blocks the queue rather than failing one row.
    startsAt: event.startsAt.toISOString(),
    endsAt: event.endsAt.toISOString(),
    organiserId: event.organiserId,
    version: event.version,
    changeType,
    deleted: event.deletedAt !== null,
  };

  await emit(tx, {
    tenantId: event.tenantId,
    topic: EVENTS_ACTIVITY,
    eventType: 'event.snapshot',
    aggregateId: event.id,
    payload: snapshot,
  });
}

/** Everything emitEventSnapshot needs, for reuse in every handler's select. */
export const SNAPSHOT_SELECT = {
  id: true,
  tenantId: true,
  title: true,
  slug: true,
  description: true,
  status: true,
  capacity: true,
  startsAt: true,
  endsAt: true,
  organiserId: true,
  version: true,
  deletedAt: true,
} as const;
