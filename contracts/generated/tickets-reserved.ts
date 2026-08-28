// GENERATED FILE — do not edit by hand.
// Source: contracts/avro/tickets.reserved-value.avsc
// Regenerate: node scripts/generate-avro-types.mjs

export type ReservationState =
  | 'RESERVED'
  | 'RELEASED'
  | 'EXPIRED'
  | 'CANCELLED'
  | 'CONFIRMED';

/**
 * A seat hold was taken, or released. Unlike events.activity this topic is NOT
 * compacted: for reservations the SEQUENCE is the information — a consumer
 * building live availability needs every hold and every release, and
 * compaction would keep only the last per key and silently destroy the count.
 */
export interface ReservationStateChanged {
  /**
   * The OutboxMessage id, added by the relay. Stable across relay retries, so a
   * consumer can recognise a redelivery — (topic, partition, offset) cannot,
   * because a redelivery gets a new offset. Delivery is at-least-once; every
   * consumer must dedupe on this.
   */
  messageId: string;
  /**
   * ISO-8601. When the state change COMMITTED, from OutboxMessage.createdAt —
   * not when it was published, which after a backlog can be much later.
   */
  occurredAt: string;
  /**
   * The reservation this message is about.
   */
  reservationId: string;
  /**
   * Required, and deliberately in the payload rather than only implied. A Kafka
   * topic is NOT tenant-scoped: Postgres enforces isolation with FORCE RLS and
   * role-targeted policies, Kafka enforces nothing. A consumer must pass this to
   * withTenant to re-establish the boundary on write.
   */
  tenantId: string;
  /**
   * Also the Kafka message key, so every reservation message for one event
   * shares a partition and stays ordered. Keyed by event rather than by
   * reservation because availability for an event is only meaningful if its
   * holds and releases arrive in order relative to each other.
   */
  eventId: string;
  /**
   * A User id from Auth's database. No foreign key exists — that table is in
   * another database this service cannot connect to.
   */
  userId: string;
  /**
   * What happened to the hold. A REQUIRED discriminator carried on the message
   * rather than split across topics, for two reasons: one Schema Registry
   * subject holds one record type, so separate types on this topic are rejected
   * with 409 NAME_MISMATCH; and without a release state, any consumer building
   * live availability decrements on RESERVED and increments on nothing, so every
   * abandoned cart permanently reduces its view. TTL holds make that the common
   * path, not an edge case.
   */
  state: ReservationState;
  /**
   * What was held or returned. Present on releases too, so a consumer can add
   * the quantities back without having remembered the original hold.
   */
  items: Array<ReservationLine>;
  /**
   * ISO-8601, when the hold lapses. Null once the reservation has reached a
   * terminal state. Nullable with a default so a consumer built against a later
   * schema can still read older messages.
   */
  expiresAt: null | string;
}
