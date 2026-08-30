import type pg from 'pg';
import type { Logger } from '@eventhub/logger';
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
  createdAt: Date;
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
 */
export async function drainOnce(
  pool: pg.Pool,
  publisher: Publisher,
  logger: Logger,
  opts: { batchSize: number; maxAttempts: number },
): Promise<DrainResult> {
  const client = await pool.connect();
  const result: DrainResult = { claimed: 0, published: 0, failed: 0, deadLettered: 0 };

  try {
    await client.query('BEGIN');

    const { rows } = await client.query<OutboxRow>(
      `SELECT id, seq::text AS "seq", "tenantId", topic, "eventType", "schemaVersion",
              "aggregateId", payload, attempts, "createdAt"
         FROM "OutboxMessage"
        WHERE "publishedAt" IS NULL
          AND "deadLetteredAt" IS NULL
          AND ("nextAttemptAt" IS NULL OR "nextAttemptAt" <= now())
        ORDER BY seq
        LIMIT $1
          FOR UPDATE SKIP LOCKED`,
      [opts.batchSize],
    );

    result.claimed = rows.length;

    if (rows.length === 0) {
      await client.query('COMMIT');
      return result;
    }

    const succeeded: string[] = [];

    for (const row of rows) {
      try {
        await publisher.publish(row);
        succeeded.push(row.id);
      } catch (err) {
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
  } finally {
    client.release();
  }

  return result;
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
