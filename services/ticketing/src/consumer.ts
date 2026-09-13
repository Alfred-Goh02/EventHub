import { Kafka, logLevel, type Consumer, type EachMessagePayload } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { Logger } from '@eventhub/logger';
import { withTenant, prisma } from './db.js';
import { PROJECTION_UPSERT_SQL } from './projection-upsert.js';
import { CHECKPOINT_UPSERT_SQL } from './checkpoint-upsert.js';

const TOPIC = 'events.activity';
const DLQ = 'events.activity.dlq';
const GROUP = 'ticketing-event-projection';

/**
 * When the projection consumer last heartbeat to its group.
 *
 * Module-level so a request handler can read it without a handle on the
 * consumer. Written by the HEARTBEAT event below; read by
 * projectionIsCurrent().
 */
let lastHeartbeatAt: number | null = null;

/**
 * When a fetch last came back EMPTY, i.e. the broker had nothing more to give.
 *
 * This is the half that actually means "caught up". A heartbeat only proves the
 * consumer holds an assignment, and kafkajs starts heartbeating as soon as the
 * group is joined -- which is BEFORE the backlog is replayed. On a cold start
 * against a compacted topic that window can be long, and it is exactly the
 * window in which this service must not claim an event does not exist.
 *
 * An empty fetch is the cheapest honest answer to "is there more": the consumer
 * asked the broker and was told no.
 */
let caughtUpAt: number | null = null;

/**
 * How long without a heartbeat before this consumer is assumed to be behind.
 *
 * kafkajs heartbeats every heartbeatInterval (3s by default), so 15s is several
 * missed beats rather than one unlucky pause. Erring long matters: a false
 * "behind" costs a retry, while a false "current" tells a customer an event
 * does not exist when it does.
 */
const HEARTBEAT_STALE_MS = 15_000;

/**
 * Keeps EventProjection in step with the Event service.
 *
 * This service CANNOT join to eventhub_event — CONNECT is revoked and it holds
 * no credential for that database. So the join it would otherwise write becomes
 * a subscription, and this consumer is what maintains the local answer to "does
 * this event exist, is it on sale, and what is its capacity?".
 *
 * Three correctness properties, each defending against a different failure:
 *
 *   DEDUPLICATION — delivery is at-least-once. The relay can crash after
 *   producing and before committing its transaction, and republishes on
 *   restart. ProcessedMessage records what has been applied, keyed on the
 *   outbox row id, which is stable across those retries. (topic, partition,
 *   offset) cannot serve: a redelivery gets a new offset.
 *
 *   MONOTONIC VERSION — apply only when the message's version is HIGHER than
 *   the stored sourceVersion. This is what makes the projection safe under
 *   out-of-order arrival as well as redelivery. Neither can move it backwards.
 *
 *   COMMIT ORDER — the Kafka offset is committed only AFTER the database
 *   transaction commits. Committing the offset first would mean a crash in
 *   between silently skips the message forever; this way a crash merely
 *   redelivers, which deduplication already handles.
 */
export function createProjectionConsumer(opts: {
  brokers: string[];
  registryUrl: string;
  logger: Logger;
}) {
  // A TimeoutNegativeWarning in this service's logs is an upstream kafkajs bug,
  // not this code: its request queue schedules a timer with `-1 - Date.now()`
  // whenever a request is fulfilled with an empty pending queue. One needless
  // 1ms timer, nothing dropped. scripts/create-topics.mjs explains it in full.
  const kafka = new Kafka({
    clientId: 'ticketing-projection',
    brokers: opts.brokers,
    logLevel: logLevel.ERROR,
  });

  const registry = new SchemaRegistry({ host: opts.registryUrl });

  // autoCommit is off: the offset must follow the database transaction, not run
  // on a timer independent of it.
  const consumer: Consumer = kafka.consumer({ groupId: GROUP });
  const producer = kafka.producer();

  /**
   * Park a message that can never be processed, and move on.
   *
   * Two things reach here, and only one of them used to. An UNDECODABLE
   * message was dead-lettered; a message that decoded but carried no
   * messageId, tenantId or eventId was logged and silently DROPPED, with the
   * offset committed underneath it.
   *
   * Those two are the same situation. Both are permanently unprocessable,
   * both would block the partition forever if retried, and neither is the
   * consumer's fault. The difference was only in how visible the loss was:
   * one left a record an operator could inspect and replay by hand, the other
   * left an ERROR line in a log nobody greps and no artefact at all.
   *
   * A message with no tenantId is the sharper case, because it cannot be
   * recorded in ProcessedMessage either -- every table in this database is
   * FORCE RLS and tenant-scoped, so there is nowhere in Postgres to write
   * down that it happened. The dead-letter topic is the only place it can go.
   */
  async function deadLetter(
    { topic, partition, message }: EachMessagePayload,
    reason: string,
  ): Promise<void> {
    await producer.send({
      topic: DLQ,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'dlq-reason': reason,
            // The DLQ is shared with Search's indexer, which parks its own copy of
            // the same poison message. This says whose failure this copy records.
            'dlq-consumer-group': GROUP,
            'dlq-source-topic': topic,
            'dlq-source-partition': String(partition),
            'dlq-source-offset': message.offset,
          },
        },
      ],
    });
  }
  async function handle({ topic, partition, message }: EachMessagePayload): Promise<void> {
    let decoded: Record<string, unknown>;

    try {
      decoded = (await registry.decode(message.value!)) as Record<string, unknown>;
    } catch (err) {
      /**
       * A message that cannot be decoded will never decode. Retrying it blocks
       * this partition forever — every message behind it waits on something
       * that can only fail. So it goes to a dead-letter topic and the consumer
       * moves on, which is the consumer-side twin of the relay's dead-lettering.
       */
      opts.logger.error(
        { topic, partition, offset: message.offset, err },
        'undecodable message sent to the dead-letter topic',
      );
      await deadLetter(
        { topic, partition, message } as EachMessagePayload,
        err instanceof Error ? err.message : String(err),
      );
      return;
    }

    const messageId = String(decoded['messageId'] ?? '');
    const tenantId = String(decoded['tenantId'] ?? '');
    const eventId = String(decoded['eventId'] ?? '');
    const version = Number(decoded['version'] ?? 0);

    if (!messageId || !tenantId || !eventId) {
      /**
       * Decoded cleanly and is still unusable. It used to be dropped here with
       * nothing but a log line, while the caller committed the offset -- so the
       * message was gone, and the only evidence was an ERROR nobody greps.
       *
       * It cannot be recorded in ProcessedMessage: that table is tenant-scoped
       * under FORCE RLS, and the whole problem is that this message has no
       * tenant. The dead-letter topic is the only place it can be kept.
       */
      const missing = [
        !messageId ? 'messageId' : null,
        !tenantId ? 'tenantId' : null,
        !eventId ? 'eventId' : null,
      ]
        .filter(Boolean)
        .join(', ');

      opts.logger.error(
        { topic, partition, offset: message.offset, missing },
        'message missing required identity fields, sent to the dead-letter topic',
      );
      await deadLetter(
        { topic, partition, message } as EachMessagePayload,
        `missing required identity fields: ${missing}`,
      );
      return;
    }

    // One transaction: dedupe, project, checkpoint. Split them and a crash
    // between leaves the checkpoint claiming progress the projection never made.
    await withTenant(tenantId, async (tx) => {
      // Dedupe by INSERT, not by SELECT-then-INSERT. The unique constraint is
      // the arbiter; a read-then-write would race two consumers in the same
      // group during a rebalance.
      const inserted = await tx.$executeRaw`
        INSERT INTO "ProcessedMessage" ("id", "tenantId", "consumerGroup", "messageId", "processedAt")
        VALUES (gen_random_uuid()::text, ${tenantId}, ${GROUP}, ${messageId}, now())
        ON CONFLICT ("tenantId", "consumerGroup", "messageId") DO NOTHING
      `;

      if (inserted === 0) {
        opts.logger.debug({ messageId }, 'already applied, skipping');
      } else {
        /**
         * The monotonic guard, evaluated by Postgres rather than by this
         * process. See projection-upsert.ts for why that distinction matters:
         * the read-then-write it replaces was safe only because of a
         * deployment property (one partition per aggregate) that is invisible
         * from here and stops holding during a rebalance.
         *
         * $executeRawUnsafe, but the SQL is a module constant and every value
         * is a bound parameter -- "unsafe" refers to the caller supplying the
         * statement text, not to the arguments, which are still parameterised.
         * $executeRaw cannot be used because it demands a tagged template, and
         * a tagged template cannot be shared with the verifier that runs this
         * exact statement.
         */
        const applied = await tx.$executeRawUnsafe(
          PROJECTION_UPSERT_SQL,
          eventId,
          tenantId,
          String(decoded['title'] ?? ''),
          String(decoded['slug'] ?? ''),
          String(decoded['status'] ?? ''),
          Number(decoded['capacity'] ?? 0),
          new Date(String(decoded['startsAt'])),
          new Date(String(decoded['endsAt'])),
          String(decoded['organiserId'] ?? ''),
          Boolean(decoded['deleted']),
          version,
        );

        // 0 is not an error: it is the WHERE clause discarding a snapshot
        // older than what is already stored.
        if (applied === 0) {
          opts.logger.debug({ eventId, incoming: version }, 'stale snapshot ignored');
        }
      }

      /**
       * Written in the SAME transaction, so it cannot claim progress the
       * projection did not make.
       *
       * It runs on every message, INCLUDING ones the dedup check above just
       * discarded — consuming a redelivery really does mean this partition has
       * been read that far. That is why the statement has to refuse to move
       * backwards: this consumer subscribes fromBeginning against a COMPACTED
       * topic, so replays are routine, and an unguarded write rewound the
       * recorded offset to 0 on every one of them.
       *
       * NOT the 404-vs-503 mechanism, whatever the schema comment used to say.
       * That decision is made by projectionIsCurrent(), from two in-memory
       * signals, and never reads this table. See checkpoint-upsert.ts.
       */
      await tx.$executeRawUnsafe(
        CHECKPOINT_UPSERT_SQL,
        GROUP,
        topic,
        partition,
        BigInt(message.offset),
        decoded['occurredAt'] ? new Date(String(decoded['occurredAt'])) : null,
      );
    });
  }

  return {
    async start(): Promise<void> {
      await producer.connect();
      await consumer.connect();
      // fromBeginning matters: events.activity is COMPACTED, so replaying from
      // the start rebuilds the whole projection. A new deployment or a wiped
      // database needs no bootstrap endpoint.
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

      // A heartbeat proves the consumer holds an assignment and is being fed.
      // It is the one cheap signal that separates "idle and caught up" from
      // "disconnected and arbitrarily behind" -- see projectionIsCurrent().
      consumer.on(consumer.events.HEARTBEAT, () => {
        lastHeartbeatAt = Date.now();
      });

      // A fetch that returned no batches means the broker had nothing more.
      // That, and not the heartbeat, is what "caught up" means here.
      consumer.on(consumer.events.FETCH, ({ payload }) => {
        if (payload.numberOfBatches === 0) caughtUpAt = Date.now();
      });


      await consumer.run({
        autoCommit: false,
        eachMessage: async (payload) => {
          lastHeartbeatAt = Date.now();
          await handle(payload);
          // Only now. A crash before this redelivers, which dedupe absorbs;
          // committing first would skip the message permanently.
          await consumer.commitOffsets([
            {
              topic: payload.topic,
              partition: payload.partition,
              offset: (BigInt(payload.message.offset) + 1n).toString(),
            },
          ]);
        },
      });

      opts.logger.info({ topic: TOPIC, group: GROUP }, 'projection consumer running');
    },

    async stop(): Promise<void> {
      await consumer.disconnect().catch(() => {});
      await producer.disconnect().catch(() => {});
    },
  };
}

/**
 * Whether this service is in a position to say an event does not exist.
 *
 * REPLACES projectionLagSeconds(), which measured the wrong thing and was never
 * called — a combination that hid the mistake. It returned wall-clock seconds
 * since the last checkpoint WRITE and called that "lag", but a checkpoint is
 * only written when a message is applied. On a healthy, fully caught-up system
 * with no new events it therefore reported enormous numbers: measured 12329
 * seconds on a stack that was perfectly up to date. Wiring it into the 404/503
 * decision as written turned every legitimate 404 into a 503 on any quiet
 * system, which is worse than the bug it was meant to fix.
 *
 * The honest distinction is not "how long since I saw a message" — an idle topic
 * and a broken consumer look identical by that measure. It takes TWO signals,
 * and dropping either one reintroduces a different version of the same bug:
 *
 *   LIVENESS      the consumer still heartbeats, so it holds an assignment and
 *                 is being fed. Without this, a crashed or disconnected consumer
 *                 keeps answering 404 while falling arbitrarily behind.
 *   COMPLETENESS  a fetch came back EMPTY, so the broker has confirmed there is
 *                 nothing more right now. Without this, a COLD START looks
 *                 current: kafkajs heartbeats as soon as the group is joined,
 *                 which is before a compacted topic has been replayed — exactly
 *                 the window this decision exists for.
 *
 * True consumer lag — comparing committed offsets to the topic high-water mark —
 * would be strictly better and is not available here: it needs eachBatch's
 * offsetLag() or an admin call, and this consumer uses eachMessage precisely so
 * that the offset commit can follow each database transaction. An empty fetch is
 * the cheapest honest substitute: the consumer asked and was told no.
 *
 * The residual gap is inherent to eventual consistency: an event published
 * milliseconds ago may not have arrived yet, and no signal inside this process
 * can know that. That window is the reason the reservation route treats a
 * missing row as retryable rather than final whenever it has any doubt.
 */
export function projectionIsCurrent(): boolean {
  // Liveness: the consumer still holds its assignment.
  if (lastHeartbeatAt === null || Date.now() - lastHeartbeatAt >= HEARTBEAT_STALE_MS) return false;

  // Completeness: the broker has told us, recently, that there is nothing more.
  // Without this a consumer still replaying its backlog would look current,
  // which is the very case this decision exists for.
  if (caughtUpAt === null || Date.now() - caughtUpAt >= HEARTBEAT_STALE_MS) return false;

  return true;
}

/** Seconds since the consumer last heartbeat, or null if it never has. */
export function secondsSinceHeartbeat(): number | null {
  if (lastHeartbeatAt === null) return null;
  return Math.round((Date.now() - lastHeartbeatAt) / 1000);
}
