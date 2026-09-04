/**
 * The consumer checkpoint write, guarded so it cannot move BACKWARDS.
 *
 * It used to be an unconditional `SET "lastOffset" = EXCLUDED."lastOffset"`,
 * which is a problem because this statement runs on EVERY message — including
 * ones the dedup check just discarded. The consumer subscribes
 * `fromBeginning: true` against a COMPACTED topic, so a replay is not an
 * unusual event: it happens on a cold start against a wiped database, on a
 * consumer-group reset, and on its own if Kafka expires the group's committed
 * offsets after a period of inactivity. Every one of those rewound the recorded
 * offset to 0 and then climbed back, so anybody reading the table during a
 * replay saw a checkpoint that was confidently wrong.
 *
 * Same shape as the projection guard in projection-upsert.ts: the comparison
 * goes in the WHERE clause of the DO UPDATE, so Postgres evaluates it under the
 * row lock ON CONFLICT already takes, and the columns move together or not at
 * all. lastMessageOccurred deliberately follows the offset rather than being
 * maxed independently — it describes the message AT that offset, and a
 * checkpoint whose timestamp came from a different message than its offset
 * would be worse than no checkpoint.
 *
 * WHAT THIS TABLE IS FOR, since its own schema comment was wrong about it.
 *
 * It says the table disambiguates a missing projection row — 404 if the lag is
 * near zero, 503 if it is high. That WAS the intent, and it is no longer how
 * the decision is made. projectionLagSeconds() measured wall-clock since the
 * last checkpoint WRITE and called it lag, which on a healthy idle system reads
 * as hours; it was replaced by projectionIsCurrent(), which reads two in-memory
 * signals from the consumer (a recent heartbeat, and a recent EMPTY fetch) and
 * never touches this table.
 *
 * So this is an OPERATIONAL RECORD, not a control: it tells an operator how far
 * each partition has been consumed and when the last applied message occurred.
 * That is worth having, and worth having correct — but nothing reads it in
 * request handling, and a comment claiming otherwise is how a future reader
 * ends up trusting a number no code depends on. This repository has already
 * been bitten once by exactly that: a mechanism that was documented, exported,
 * and called by nothing.
 */

/**
 * Parameters, in order:
 *
 *   $1 consumerGroup   $2 topic   $3 partition
 *   $4 lastOffset      $5 lastMessageOccurred (nullable)
 *
 * Returns a row count: 1 if the checkpoint advanced, 0 if this offset was at or
 * behind what is already recorded. Zero is not an error — it is a replay being
 * correctly ignored.
 */
export const CHECKPOINT_UPSERT_SQL = `
INSERT INTO "ConsumerCheckpoint"
  ("id", "consumerGroup", "topic", "partition", "lastOffset", "lastMessageOccurred", "updatedAt")
VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5, now())
ON CONFLICT ("consumerGroup", "topic", "partition") DO UPDATE
  SET "lastOffset"          = EXCLUDED."lastOffset",
      "lastMessageOccurred" = EXCLUDED."lastMessageOccurred",
      "updatedAt"           = now()
  WHERE "ConsumerCheckpoint"."lastOffset" < EXCLUDED."lastOffset"
`;
