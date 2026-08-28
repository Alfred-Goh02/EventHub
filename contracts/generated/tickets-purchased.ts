// GENERATED FILE — do not edit by hand.
// Source: contracts/avro/tickets.purchased-value.avsc
// Regenerate: node scripts/generate-avro-types.mjs

export interface PurchaseLine {
  ticketTypeId: string;
  quantity: number;
  /**
   * The price captured when the hold was taken, NOT the ticket type's current
   * price. Those differ whenever an organiser edits a price, and a receipt must
   * show what the buyer agreed to.
   */
  unitPriceCents: number;
}

/**
 * Money changed hands for a reservation. This topic carries ONLY successful
 * purchases — a failed charge is not a purchase, and a consumer of this topic
 * should never have to filter for that. Deliberately separate from
 * tickets.reserved even though ReservationState already has a CONFIRMED
 * symbol: seat state and money have different audiences, and putting amounts
 * on the seat-state topic hands every availability consumer financial data it
 * has no business reading. The CONFIRMED transition is still emitted on
 * tickets.reserved in the same transaction, so an availability consumer sees
 * the hold and its confirmation in order on one partition and never has to
 * join across topics — which it could not do safely, since Kafka guarantees no
 * ordering between them.
 */
export interface TicketsPurchased {
  /**
   * The OutboxMessage id, added by the relay. Stable across relay retries, so a
   * consumer can recognise a redelivery — (topic, partition, offset) cannot,
   * because a redelivery gets a new offset. Delivery is at-least-once, and on
   * THIS topic a duplicate means a second receipt or a double-counted day of
   * revenue, so deduping on this is not optional.
   */
  messageId: string;
  /**
   * ISO-8601. When the purchase COMMITTED, from OutboxMessage.createdAt — not
   * when it was published. Financial reporting must bucket by when the money
   * moved, and after a relay backlog those two can differ by hours.
   */
  occurredAt: string;
  /**
   * The Payment row for the attempt that succeeded. One row per attempt, so this
   * identifies WHICH attempt paid — a reservation may have several failed ones
   * behind it, and a receipt must point at the charge that actually went
   * through.
   */
  paymentId: string;
  /**
   * The reservation that was paid for.
   */
  reservationId: string;
  /**
   * Required, and in the payload rather than only implied. A Kafka topic is NOT
   * tenant-scoped: Postgres enforces isolation with FORCE RLS and role-targeted
   * policies, Kafka enforces nothing. A consumer must pass this to withTenant to
   * re-establish the boundary on write — and on this topic a leak is one
   * tenant's revenue appearing in another's books.
   */
  tenantId: string;
  /**
   * Also the Kafka message key, matching tickets.reserved so that a given
   * event's messages stay on one partition on both topics. Keyed by event rather
   * than by payment because per-event revenue is the question this topic is
   * actually asked.
   */
  eventId: string;
  /**
   * A User id from Auth's database. No foreign key exists — that table is in
   * another database this service cannot connect to.
   */
  userId: string;
  /**
   * The total charged, in the minor unit. A long rather than an int: the
   * Postgres column is INTEGER, which caps a single payment at about 21 million
   * dollars and is fine for a ticket, but a consumer aggregating this field
   * across a tenant's whole history would overflow an int32 long before that is
   * an unreasonable number. Widening a field later is a schema change; starting
   * wide costs nothing.
   */
  amountCents: number;
  /**
   * ISO-4217. Carried explicitly rather than assumed, because an amount without
   * a currency is not an amount — a consumer summing mixed currencies into one
   * figure produces a number that looks right and means nothing.
   */
  currency: string;
  /**
   * The lines that were paid for, with prices, so a receipt is buildable from
   * this message alone. A consumer that had to read TicketType to render a
   * receipt would render a different receipt each time the price changed.
   */
  items: Array<PurchaseLine>;
  /**
   * The payment provider's own charge identifier, for reconciling against their
   * reports and for refunds. Nullable with a default because a purchase
   * confirmed by reconciliation rather than by a live response may not have one,
   * and because a nullable-with-default field can be read by consumers built
   * against either schema version.
   */
  providerRef: null | string;
}
