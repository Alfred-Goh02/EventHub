// GENERATED FILE — do not edit by hand.
// Source: contracts/avro/community.feed-value.avsc
// Regenerate: node scripts/generate-avro-types.mjs

export type ActivityKind =
  | 'POST_CREATED'
  | 'COMMENT_CREATED'
  | 'REACTION_ADDED'
  | 'REACTION_REMOVED';

/**
 * Something a person did in a space: a post, a comment, a reaction. ONE RECORD
 * WITH A DISCRIMINATOR rather than a record per kind, because a Schema
 * Registry subject holds one schema — registering a second record name against
 * community.feed-value is rejected with 409 NAME_MISMATCH.
 * events.activity-value carries the same shape for the same reason.
 */
export interface CommunityActivity {
  /**
   * The OutboxMessage id. Stable across relay retries, so a consumer can
   * recognise a redelivery — (topic, partition, offset) cannot, because a
   * redelivered message gets a new offset. Delivery is at-least-once, so every
   * consumer dedupes on this.
   */
  messageId: string;
  /**
   * ISO-8601. When the change was committed, taken from the outbox row, NOT when
   * it was published — which can be much later after a backlog.
   */
  occurredAt: string;
  /**
   * Required, and deliberately in the payload as well as derivable elsewhere. A
   * Kafka topic is NOT tenant-scoped: Postgres enforces isolation with FORCE
   * RLS, Kafka enforces nothing, and anything holding the consumer group's
   * credentials reads every tenant. A consumer must pass this to its own tenant
   * scoping rather than trusting a wide-open role.
   */
  tenantId: string;
  /**
   * Also the Kafka message key, so everything about one space shares a partition
   * and stays ordered: a comment can never be read before the post it answers.
   */
  spaceId: string;
  /**
   * What happened. Adding a symbol later is backward-compatible for readers that
   * have it; a reader on the older schema rejects the unknown symbol, which is
   * why consumers are written to skip kinds they do not know.
   */
  kind: ActivityKind;
  /**
   * Who did it. A User id from Auth's database; there is no foreign key, because
   * that table is in another database this service cannot open.
   */
  actorId: string;
  /**
   * The post this is about. A comment and a reaction both carry the post they
   * belong to, so a consumer counting a post's activity never has to look one
   * up.
   */
  postId: string;
  /**
   * Set for COMMENT_CREATED, and for a reaction to a comment.
   */
  commentId: null | string;
  /**
   * The comment being replied to, when there is one. What a notification
   * consumer needs to tell someone their comment was answered.
   */
  parentCommentId: null | string;
  /**
   * The text, for consumers that index or render it. Nullable because a reaction
   * has none.
   */
  body: null | string;
}
