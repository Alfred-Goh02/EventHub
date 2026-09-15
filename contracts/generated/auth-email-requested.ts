// GENERATED FILE — do not edit by hand.
// Source: contracts/avro/auth.email-requested-value.avsc
// Regenerate: node scripts/generate-avro-types.mjs

export type AuthEmailKind =
  | 'PASSWORD_RESET';

/**
 * Auth asks Notification to send an email that carries a credential.
 * Everything that could take over an account, or say whose account it is, is
 * inside `sealed`: encrypted to Notification's public key, so Kafka, Auth's
 * outbox table and any log hold only ciphertext. Not compacted: every request
 * is its own event, and Auth revokes a user's earlier link when it issues a
 * newer one.
 */
export interface AuthEmailRequested {
  /**
   * The OutboxMessage id, added by the relay. Stable across relay retries, so a
   * consumer can recognise a redelivery. It is also the jti inside the seal, so
   * a sealed payload copied into a different message does not match it.
   */
  messageId: string;
  /**
   * ISO-8601. When the request COMMITTED, from OutboxMessage.createdAt, not when
   * it was published.
   */
  occurredAt: string;
  /**
   * Required, and in the payload rather than implied: a Kafka topic is not
   * tenant-scoped. Repeated inside the seal, which is the copy a consumer
   * trusts.
   */
  tenantId: string;
  /**
   * The User the email is for. Also the Kafka message key, so one user's
   * requests stay in order and a newer request arrives after the one it
   * supersedes.
   */
  userId: string;
  /**
   * Which email. One symbol today. Adding a symbol later is BACKWARD-compatible.
   */
  kind: AuthEmailKind;
  /**
   * ISO-8601. When the link inside stops working, in plaintext so a consumer can
   * drop a stale request without decrypting it. The exp claim inside the seal is
   * the same instant, and that is the copy to trust.
   */
  expiresAt: string;
  /**
   * A compact JWE (alg RSA-OAEP-256, enc A256GCM) to Notification's public key,
   * named by kid. Its plaintext is a compact JWS signed by Auth (RS256, typ
   * auth-email+jwt, aud eventhub-notification) whose claims carry the address
   * and the link. Encryption keeps the credential off the topic; the signature
   * proves Auth wrote it, since anyone can encrypt to a public key.
   */
  sealed: string;
}
