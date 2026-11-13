// GENERATED FILE — do not edit by hand.
// Source: contracts/avro/auth.user-contact-value.avsc
// Regenerate: node scripts/generate-avro-types.mjs

/**
 * A user's current address, sealed for Notification: its contact card.
 * COMPACTED, keyed by `tenantId:userId`, so the topic holds the latest card
 * for every user and a new consumer reads them all from the start. A tombstone
 * (a null value under a key) means the user's card is withdrawn and a consumer
 * must delete its copy. The address is inside `sealed`, encrypted to
 * Notification's public key, so Kafka, Auth's outbox table and any log hold
 * only ciphertext.
 */
export interface AuthUserContact {
  /**
   * The OutboxMessage id, added by the relay. It is also the jti inside the
   * seal, so a sealed card copied into another message does not match it.
   */
  messageId: string;
  /**
   * ISO-8601, UTC, milliseconds. When the card was queued, stamped by Auth from
   * the database clock in the transaction that queued it, and repeated inside
   * the seal. It is the ordering key: a consumer keeps the card with the latest
   * occurredAt, so an older card replayed after a newer one cannot replace it,
   * and because it is signed, a replayed card cannot claim to be newer than it
   * is.
   */
  occurredAt: string;
  /**
   * Required, and in the payload rather than implied: a Kafka topic is not
   * tenant-scoped. Repeated inside the seal, which is the copy a consumer
   * trusts.
   */
  tenantId: string;
  /**
   * The user the card is for. With tenantId it forms the Kafka key; the user id
   * alone would let one tenant's card compact away another's under a colliding
   * id.
   */
  userId: string;
  /**
   * A compact JWE (alg RSA-OAEP-256, enc A256GCM) to Notification's public key,
   * named by kid. Its plaintext is a compact JWS signed by Auth (RS256, typ
   * auth-contact+jwt, aud eventhub-notification, no exp) whose claims carry the
   * tenant, the address and, as tenantSlug, the tenant's subdomain, which a
   * reply email's links open on. tenantSlug is absent on a card made before it
   * was added, and for a tenant whose slug the gateway would not route; a
   * consumer then builds no link rather than one to another host. It is inside
   * the seal and not a field of this record because this record is plaintext a
   * producer can edit, and the slug decides which host a link opens. The typ is
   * what stops a card being accepted as an email request, which is sealed and
   * signed by the same keys.
   */
  sealed: string;
}
