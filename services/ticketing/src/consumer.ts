import { Kafka, logLevel, type Consumer, type EachMessagePayload } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { Logger } from '@eventhub/logger';
import { withTenant, prisma } from './db.js';

const TOPIC = 'events.activity';
const DLQ = 'events.activity.dlq';
const GROUP = 'ticketing-event-projection';

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
      await producer.send({
        topic: DLQ,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: {
              ...message.headers,
              'dlq-reason': err instanceof Error ? err.message : String(err),
              'dlq-source-topic': topic,
              'dlq-source-partition': String(partition),
              'dlq-source-offset': message.offset,
            },
          },
        ],
      });
      return;
    }

    const messageId = String(decoded['messageId'] ?? '');
    const tenantId = String(decoded['tenantId'] ?? '');
    const eventId = String(decoded['eventId'] ?? '');
    const version = Number(decoded['version'] ?? 0);

    if (!messageId || !tenantId || !eventId) {
      opts.logger.error({ topic, partition, offset: message.offset }, 'message missing required identity fields');
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
        const existing = await tx.eventProjection.findFirst({
          where: { id: eventId },
          select: { sourceVersion: true },
        });

        // The monotonic guard. A redelivered or late message carrying an older
        // version is discarded rather than silently reverting the projection.
        if (existing && existing.sourceVersion >= version) {
          opts.logger.debug(
            { eventId, incoming: version, stored: existing.sourceVersion },
            'stale snapshot ignored',
          );
        } else {
          const data = {
            tenantId,
            title: String(decoded['title'] ?? ''),
            slug: String(decoded['slug'] ?? ''),
            status: String(decoded['status'] ?? ''),
            capacity: Number(decoded['capacity'] ?? 0),
            startsAt: new Date(String(decoded['startsAt'])),
            endsAt: new Date(String(decoded['endsAt'])),
            organiserId: String(decoded['organiserId'] ?? ''),
            deleted: Boolean(decoded['deleted']),
            sourceVersion: version,
          };

          await tx.eventProjection.upsert({
            where: { id: eventId },
            create: { id: eventId, ...data },
            update: data,
          });
        }
      }

      // Written in the SAME transaction, so it cannot claim to be caught up on
      // data it did not apply. This is what lets a request distinguish "no such
      // event" (404) from "this service is behind" (503).
      await tx.$executeRaw`
        INSERT INTO "ConsumerCheckpoint"
          ("id", "consumerGroup", "topic", "partition", "lastOffset", "lastMessageOccurred", "updatedAt")
        VALUES (gen_random_uuid()::text, ${GROUP}, ${topic}, ${partition}, ${BigInt(message.offset)},
                ${decoded['occurredAt'] ? new Date(String(decoded['occurredAt'])) : null}, now())
        ON CONFLICT ("consumerGroup", "topic", "partition") DO UPDATE
          SET "lastOffset" = EXCLUDED."lastOffset",
              "lastMessageOccurred" = EXCLUDED."lastMessageOccurred",
              "updatedAt" = now()
      `;
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

      await consumer.run({
        autoCommit: false,
        eachMessage: async (payload) => {
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
 * How far behind the projection is, for the 404-versus-503 decision.
 *
 * A missing projection row means one of two very different things: the event
 * genuinely does not exist, or this service has not caught up yet. Telling a
 * customer an event does not exist when it does is the worse error, so lag is
 * measured rather than assumed.
 */
export async function projectionLagSeconds(): Promise<number | null> {
  const rows = await prisma.$queryRaw<Array<{ lag: number | null }>>`
    SELECT extract(epoch FROM now() - max("updatedAt"))::float8 AS lag
      FROM "ConsumerCheckpoint"
     WHERE "consumerGroup" = ${GROUP}
  `;
  const lag = rows[0]?.lag;
  return lag === null || lag === undefined ? null : Math.round(lag);
}
