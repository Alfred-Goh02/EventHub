// GENERATED FILE — do not edit by hand.
// Source: contracts/avro/events.activity-value.avsc
// Regenerate: node scripts/generate-avro-types.mjs

export type EventStatus =
  | 'DRAFT'
  | 'PUBLISHED'
  | 'CANCELLED'
  | 'COMPLETED';

export type ChangeType =
  | 'CREATED'
  | 'UPDATED'
  | 'PUBLISHED'
  | 'CANCELLED'
  | 'DELETED';

/**
 * A complete snapshot of an Event's state at one point in time.
 * events.activity is a COMPACTED topic keyed by eventId, so Kafka retains the
 * latest snapshot per event forever and a consumer can rebuild its entire
 * projection by replaying from the beginning. That is why this is a full
 * snapshot rather than a delta: replaying deltas needs every delta, and
 * compaction keeps only the last.
 */
export interface EventSnapshot {
  /**
   * The OutboxMessage id. Stable across relay retries, so a consumer can
   * recognise a redelivery — (topic, partition, offset) cannot, because a
   * redelivered message gets a new offset. Delivery is at-least-once, so every
   * consumer must dedupe on this.
   */
  messageId: string;
  /**
   * ISO-8601. When the state change was committed, taken from
   * OutboxMessage.createdAt — NOT when it was published, which can be much later
   * after a backlog.
   */
  occurredAt: string;
  /**
   * Also the Kafka message key, so every message about one event shares a
   * partition and stays ordered relative to the others.
   */
  eventId: string;
  /**
   * Required, and deliberately in the payload as well as being derivable
   * elsewhere. A Kafka topic is NOT tenant-scoped: Postgres enforces isolation
   * with FORCE RLS and role-targeted policies, but Kafka enforces nothing, and
   * anything holding the consumer group's credentials reads every tenant. The
   * boundary moves from 'the engine enforces it' to 'the consumer re-establishes
   * it on write' — which is why a consumer must pass this to withTenant rather
   * than trusting a wide-open role.
   */
  tenantId: string;
  title: string;
  slug: string;
  /**
   * Nullable union with a default, so a consumer built against a later schema
   * can still read this.
   */
  description: null | string;
  /**
   * Ticketing only sells for PUBLISHED. CANCELLED is the signal to stop selling
   * and start refunding — there is no other way for it to find out, since it has
   * no foreign key to the Event table and no credential for that database.
   */
  status: EventStatus;
  /**
   * The organiser's declared ceiling. Advisory to Ticketing, which owns the real
   * numbers: capacity is immutable once an event is published precisely because
   * this service cannot see how many tickets were sold.
   */
  capacity: number;
  /**
   * ISO-8601 string, NOT timestamp-millis. Deliberate: a logicalType would
   * reject the string this payload actually contains, and would also reject a JS
   * Date unless a logicalTypes factory is registered. That failure is a poison
   * message — a row that can never be serialised, re-selected on every drain,
   * blocking everything behind it. A string cannot fail that way.
   */
  startsAt: string;
  /**
   * ISO-8601. Same reasoning as startsAt.
   */
  endsAt: string;
  /**
   * A User id from the Auth service's database. There is no foreign key: that
   * table is in another database this service cannot open a connection to.
   * Referential validity is guaranteed upstream by the signed token, never here.
   */
  organiserId: string;
  /**
   * The row version this snapshot describes. A consumer keeps the highest
   * version it has applied and discards anything lower — which is what makes a
   * projection safe under BOTH at-least-once redelivery and out-of-order
   * arrival. Neither can move the projection backwards.
   */
  version: number;
  /**
   * What happened. A discriminator inside ONE record type, not separate record
   * types: a Schema Registry subject holds one schema, so registering a second
   * record name against events.activity-value is rejected with 409
   * NAME_MISMATCH.
   */
  changeType: ChangeType;
  /**
   * Soft-deleted events are still published, so consumers learn to stop selling.
   * A hard delete would instead be a null-value tombstone, which compaction
   * eventually removes entirely.
   */
  deleted: boolean;
}
