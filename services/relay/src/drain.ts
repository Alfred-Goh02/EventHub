import type pg from 'pg';
import type { Logger } from '@eventhub/logger';
import { CLAIM_SQL } from './claim-sql.js';
import { withClient } from './db.js';
import { PermanentPublishError, type Publisher } from './publisher.js';

export interface OutboxRow {
  id: string;
  seq: string; // BIGINT arrives as a string from pg; never coerce it to Number
  tenantId: string;
  topic: string;
  eventType: string;
  schemaVersion: number;
  aggregateId: string;
  payload: Record<string, unknown>;
  attempts: number;
  /**
   * createdAt as ISO-8601 UTC text, rendered by Postgres in the claim. A string
   * on purpose: pg parses the timestamp-without-time-zone column as LOCAL time,
   * so a Date here is off by the process's UTC offset. See claim-sql.ts.
   */
  occurredAt: string;
}

export interface DrainResult {
  claimed: number;
  published: number;
  failed: number;
  deadLettered: number;
}

/**
 * Claims a batch of undelivered rows, publishes them, and marks them done —
 * all inside ONE transaction held across the Kafka round-trip.
 *
 * `FOR UPDATE SKIP LOCKED` is what makes this safe to run more than once.
 * Without it, two relays — or, far more likely, one forgotten `tsx watch`
 * process — both read seq 1..100, both publish, and both mark. Duplicates are
 * survivable; the worse failure is ordering, because relay A can publish seq 5
 * while relay B is still mid-flight on seq 4, so the same Kafka key arrives out
 * of order. That is the exact property scripts/kafka-smoke.mjs exists to assert.
 *
 * SKIP LOCKED prevents duplicate WORK. It does not prevent duplicate DELIVERY:
 * a crash after the produce but before the commit will redeliver on restart.
 * That is inherent to the outbox pattern, and it is why every consumer must
 * dedupe on messageId. At-least-once is the guarantee, not exactly-once.
 *
 * The drain is `WHERE publishedAt IS NULL ORDER BY seq`, NOT a `seq > cursor`
 * cursor. Sequence values are allocated BEFORE commit, so a row with seq=5 can
 * commit after seq=6 — and a cursor-based relay would skip it permanently. A
 * silently lost message is the precise failure an outbox exists to prevent.
 *
 * PER-AGGREGATE ORDER SURVIVES A FAILED PUBLISH, which it did not.
 *
 * A transient failure sets nextAttemptAt to a backoff and leaves the row
 * unpublished. The claim then skipped it — `nextAttemptAt <= now()` — and
 * nothing stopped the SAME aggregate’s later rows going out ahead of it. CI
 * caught the result on a fresh broker: v3 at offset 0, then v1 at offset 8,
 * for one event. Inside a single drain the same thing happened one level down,
 * because the publish loop caught the failure on seq 5 and carried on to 6.
 *
 * On a COMPACTED topic that is worse than untidy. Compaction keeps the last
 * message per key, so a stale version arriving last becomes the surviving one,
 * and events.activity exists precisely so a consumer can rebuild by replaying
 * from the beginning. The live projection stays correct — its monotonic
 * version guard discards the late arrival — so nothing looks wrong until a
 * rebuild produces a projection made of stale snapshots.
 *
 * Two guards, because there are two ways to overtake: NOT EXISTS in the claim
 * stops a later DRAIN passing a row that is backing off, and `blocked` below
 * stops the loop passing one inside the batch it already holds.
 *
 * THE TWO-RELAY CASE IS CLOSED TOO, and it was reachable. SKIP LOCKED hides a
 * row another relay holds -- from the ORDER BY as well. Relay A claims seq 1
 * and stalls inside the Kafka round-trip this transaction is held across; seq 2
 * and 3 commit meanwhile; relay B drains, cannot see seq 1, publishes 2 and 3
 * ahead of it. Measured against this schema: [2,3,1].
 *
 * A per-aggregate advisory lock, taken in the claim and released at COMMIT,
 * makes B get nothing for that aggregate and pick the whole stream up on its
 * next pass. Running one relay is still the sensible configuration -- but the
 * forgotten `tsx watch` this comment has warned about since it was
 * written now costs throughput rather than a projection that rebuilds to stale
 * state.
 *
 * WHY THE CHECKOUT GOES THROUGH withClient. Holding a transaction across the
 * Kafka round-trip is the whole design above — and it is also the longest
 * stretch in this repository during which a client is checked out with no query
 * in flight. That is the shape `idle_in_transaction_session_timeout = '30s'`,
 * which this role carries, exists to kill. pg emits that FATAL as an 'error' on
 * the Client, which has no listener while it is checked out, so before
 * withClient it was an unhandled 'error' event and the relay exited 1 mid-drain.
 */
export async function drainOnce(
  pool: pg.Pool,
  publisher: Publisher,
  logger: Logger,
  opts: { batchSize: number; maxAttempts: number },
): Promise<DrainResult> {
  const result: DrainResult = { claimed: 0, published: 0, failed: 0, deadLettered: 0 };

  await withClient(pool, logger, 'eventhub-relay', (client) =>
    drainWithClient(client, result, publisher, logger, opts),
  );

  return result;
}

/**
 * The drain itself, on a client someone else owns.
 *
 * Split out so drainOnce is the checkout and this is the work. The transaction
 * is still entirely here: whoever hands this a client gets it back in the same
 * state it was lent, committed or rolled back.
 */
async function drainWithClient(
  client: pg.PoolClient,
  result: DrainResult,
  publisher: Publisher,
  logger: Logger,
  opts: { batchSize: number; maxAttempts: number },
): Promise<void> {
  try {
    await client.query('BEGIN');

    /**
     * TWO STAGES, and the second one is why.
     *
     * The advisory lock cannot sit in the first WHERE. It is volatile, so the
     * planner stops using OutboxMessage_undelivered_idx to supply the ordering
     * and switches to a bitmap scan with a Sort on top -- the exact plan shape
     * the partial index was created to avoid, and one that degrades as the
     * backlog grows. Worse, the function would then be evaluated for every
     * pending row BEFORE the LIMIT, so a drain would take an advisory lock per
     * aggregate in the whole backlog and hold them all to COMMIT. That is
     * unbounded, and max_locks_per_transaction is not.
     *
     * The CTE bounds the work to batchSize rows using the index, and the outer
     * filter takes at most batchSize locks. Rows the CTE locked and the outer
     * filter then rejects stay row-locked until COMMIT, which is moments away.
     */
    const { rows } = await client.query<OutboxRow>(CLAIM_SQL, [opts.batchSize]);

    result.claimed = rows.length;

    if (rows.length === 0) {
      await client.query('COMMIT');
      return;
    }

    const succeeded: string[] = [];

    /**
     * Aggregates that have already failed IN THIS BATCH.
     *
     * The claim above stops a later drain overtaking a row that is backing
     * off. This stops it happening inside a single drain, where the query has
     * already handed us seq 5, 6 and 7 for one aggregate and 5 then fails.
     * Without it the loop carries straight on and publishes 6 and 7, and the
     * aggregate's stream is reordered before the next drain gets a say.
     */
    const blocked = new Set<string>();

    for (const row of rows) {
      const stream = `${row.tenantId}:${row.topic}:${row.aggregateId}`;

      if (blocked.has(stream)) {
        // Left unclaimed rather than failed: no attempt was made, so nothing
        // about this row has gone wrong and its attempts count must not move.
        // The next drain picks it up once the older row has published.
        logger.debug(
          { messageId: row.id, seq: row.seq, aggregateId: row.aggregateId },
          'held back: an older message for this aggregate has not published yet',
        );
        continue;
      }

      try {
        await publisher.publish(row);
        succeeded.push(row.id);
      } catch (err) {
        blocked.add(stream);
        const attempts = row.attempts + 1;
        const message = err instanceof Error ? err.message : String(err);

        /**
         * WHAT gets dead-lettered, not HOW MANY TIMES it has failed.
         *
         * This used to be `attempts >= maxAttempts` alone, which treated a
         * payload that can never encode and a broker that is briefly down as the
         * same thing. A broker outage spanning a few drains therefore walked the
         * whole backlog to the ceiling and abandoned it — permanently, since
         * nothing clears deadLetteredAt, and silently, since a message lost to a
         * retry ceiling looks identical to one lost to a bad payload.
         *
         * Now only a PermanentPublishError does it: the payload does not match
         * its registered schema, so no number of retries will help and holding
         * the row helps nobody. Anything else is transient and keeps retrying.
         */
        const permanent = err instanceof PermanentPublishError;

        if (permanent) {
          // Dead-letter immediately rather than after maxAttempts. A poison row
          // is poison on the first attempt; spending the ceiling on it only
          // delays the operator finding out.
          await client.query(
            `UPDATE "OutboxMessage"
                SET attempts = $2, "lastError" = $3, "deadLetteredAt" = now()
              WHERE id = $1`,
            [row.id, attempts, message.slice(0, 2000)],
          );
          result.deadLettered += 1;
          logger.error(
            { messageId: row.id, seq: row.seq, topic: row.topic, attempts, err: message },
            'outbox row dead-lettered: its payload does not match the registered schema, ' +
              'so it will never publish and will not be retried',
          );
        } else {
          // Exponential backoff with a ceiling. A broker that is down should not
          // be hammered, and a transient failure should not become permanent.
          const delaySeconds = Math.min(2 ** attempts, 300);
          await client.query(
            `UPDATE "OutboxMessage"
                SET attempts = $2, "lastError" = $3,
                    "nextAttemptAt" = now() + ($4 || ' seconds')::interval
              WHERE id = $1`,
            [row.id, attempts, message.slice(0, 2000), String(delaySeconds)],
          );
          result.failed += 1;

          /**
           * Past the old ceiling this gets LOUD rather than lossy.
           *
           * The row keeps retrying, because a transient failure that has lasted
           * a long time is still transient — the broker being down for an hour
           * is not a reason to throw the messages away. But an operator needs to
           * know the queue is stuck, and a warn line among many is not that.
           *
           * `maxAttempts` therefore no longer decides an outcome; it decides
           * when to start shouting.
           */
          if (attempts >= opts.maxAttempts) {
            logger.error(
              {
                messageId: row.id,
                seq: row.seq,
                topic: row.topic,
                attempts,
                retryInSeconds: delaySeconds,
                err: message,
              },
              'outbox row STUCK: still failing after the retry ceiling and still being retried — ' +
                'the broker or schema registry is likely unreachable',
            );
          } else {
            logger.warn(
              { messageId: row.id, seq: row.seq, attempts, retryInSeconds: delaySeconds, err: message },
              'outbox publish failed, will retry',
            );
          }
        }
      }
    }

    if (succeeded.length > 0) {
      // One statement for the whole batch. Marking published in the SAME
      // transaction that claimed the rows is what makes the lock meaningful:
      // the rows stay claimed from SELECT to COMMIT, so no concurrent relay can
      // touch them in between.
      await client.query(
        `UPDATE "OutboxMessage" SET "publishedAt" = now() WHERE id = ANY($1::text[])`,
        [succeeded],
      );
      result.published = succeeded.length;
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

export interface RelayMetrics {
  backlog: number;
  deadLettered: number;
  oldestUndeliveredSeconds: number | null;
}

/**
 * Alert on AGE, not count.
 *
 * 500 rows draining quickly is healthy. Three rows stuck for an hour is an
 * outage. A count threshold gets both of those wrong.
 */
export async function readMetrics(pool: pg.Pool): Promise<RelayMetrics> {
  const { rows } = await pool.query<{
    backlog: string;
    dead_lettered: string;
    oldest_seconds: string | null;
  }>(
    `SELECT count(*) FILTER (WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL) AS backlog,
            count(*) FILTER (WHERE "deadLetteredAt" IS NOT NULL)                       AS dead_lettered,
            extract(epoch FROM now() - min("createdAt")
              FILTER (WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL))        AS oldest_seconds
       FROM "OutboxMessage"`,
  );

  const r = rows[0]!;

  // count(*) comes back as a JS bigint through some drivers and a string here.
  // Either way it must be converted at this boundary: JSON.stringify throws
  // "Do not know how to serialize a BigInt", and in Express that surfaces as a
  // 500 from inside res.json rather than anywhere near the query.
  return {
    backlog: Number(r.backlog),
    deadLettered: Number(r.dead_lettered),
    oldestUndeliveredSeconds: r.oldest_seconds === null ? null : Math.round(Number(r.oldest_seconds)),
  };
}
