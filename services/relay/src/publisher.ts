import { Kafka, type Producer, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { OutboxRow } from './drain.js';

export interface Publisher {
  publish(row: OutboxRow): Promise<void>;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
}

/**
 * A failure that will happen again no matter how many times it is retried.
 *
 * The relay used to treat every publish failure the same way and dead-letter on
 * attempt count alone. That conflates two opposite situations:
 *
 *   PERMANENT   the payload does not match its schema. It never will. Retrying
 *               wastes attempts and delays the row leaving the queue.
 *   TRANSIENT   the broker or the registry is unreachable. The message is
 *               perfectly fine and will publish as soon as they return.
 *
 * Under the old rule a broker outage spanning a handful of drains walked the
 * ENTIRE BACKLOG to maxAttempts and dead-lettered all of it — permanently,
 * because nothing clears deadLetteredAt, and silently, because losing a message
 * to a retry ceiling looks exactly like losing it to a bad payload.
 *
 * Only this error dead-letters. Everything else backs off forever and gets
 * louder, which is the right failure direction for delivery: a stuck queue is
 * recoverable, an abandoned one is not.
 */
export class PermanentPublishError extends Error {
  override readonly cause?: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = 'PermanentPublishError';
    this.cause = cause;
  }
}

/**
 * Publishes outbox rows to Kafka, Avro-encoded against the Schema Registry.
 *
 * Why the registry rather than encoding with a local .avsc: the Confluent wire
 * format is a magic byte (0x00), then a 4-byte big-endian schema id, then the
 * Avro body. A consumer reads that id to fetch the WRITER's schema and resolve
 * it against its own. A plain `avsc` encode produces the body without the
 * prefix, so it is not interchangeable — every registry-aware consumer would
 * fail to decode it, and the failure looks like data corruption rather than a
 * format mismatch.
 */
export function createPublisher(opts: {
  brokers: string[];
  registryUrl: string;
}): Publisher {
  const kafka = new Kafka({
    clientId: 'eventhub-relay',
    brokers: opts.brokers,
    logLevel: logLevel.ERROR,
  });

  /**
   * idempotent + maxInFlightRequests: 1.
   *
   * Neither is the default, and without them the per-key ordering this whole
   * design rests on is not actually delivered. kafkajs defaults to
   * idempotent: false with unbounded in-flight requests, so a transient failure
   * on batch 1 can be retried AFTER batch 2 was already accepted on the same
   * partition — and the same key lands out of order.
   *
   * `idempotent` also makes the broker deduplicate producer retries, so a retry
   * after a timeout does not append the message twice.
   */
  const producer: Producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
  });

  const registry = new SchemaRegistry({ host: opts.registryUrl });

  // Subject ids are cached: the registry is consulted once per subject, not once
  // per message. A lookup per message would put an HTTP round-trip inside a
  // transaction that is already holding row locks.
  const subjectIds = new Map<string, number>();

  async function schemaIdFor(topic: string): Promise<number> {
    const cached = subjectIds.get(topic);
    if (cached !== undefined) return cached;

    // TopicNameStrategy: the subject for a topic's values is `<topic>-value`.
    const id = await registry.getLatestSchemaId(`${topic}-value`);
    subjectIds.set(topic, id);
    return id;
  }

  return {
    async connect() {
      await producer.connect();
    },

    async disconnect() {
      await producer.disconnect();
    },

    async publish(row: OutboxRow) {
      const id = await schemaIdFor(row.topic);

      /**
       * The envelope is built HERE, from columns the row already has, rather
       * than being stored in the payload.
       *
       * messageId is the outbox row id — stable across relay retries, which is
       * what lets a consumer recognise a redelivery. (topic, partition, offset)
       * cannot serve as a dedup key, because a redelivered message gets a new
       * offset.
       *
       * occurredAt is createdAt, i.e. when the state change COMMITTED — not when
       * it was published, which after a backlog can be much later. A consumer
       * ordering by publish time would see history in the wrong order.
       */
      const message = {
        messageId: row.id,
        occurredAt: row.createdAt.toISOString(),
        ...row.payload,
      };

      /**
       * Encoding is the one failure here that is PERMANENT.
       *
       * A payload that does not match its registered schema will not match it on
       * the next attempt either — retrying is pure waste, and worse, it delays
       * the row's own dead-lettering while every retry writes another log line.
       *
       * Everything else in this method is transient by contrast: the Schema
       * Registry can be briefly unreachable, and the broker certainly can. Those
       * must NOT be dead-lettered, because the message is fine and the world
       * will recover. See drain.ts for what the distinction buys.
       */
      let value: Buffer;
      try {
        value = await registry.encode(id, message);
      } catch (err) {
        throw new PermanentPublishError(
          `payload does not match the registered schema for ${row.topic}: ` +
            (err instanceof Error ? err.message : String(err)),
          err,
        );
      }

      await producer.send({
        topic: row.topic,
        messages: [
          {
            // The partition key. Every message about one aggregate shares a
            // partition and therefore stays ordered relative to the others.
            key: row.aggregateId,
            value,
            headers: {
              // In a header too, so a consumer can dedupe without paying to
              // deserialise a message it is about to discard.
              'message-id': row.id,
              'event-type': row.eventType,
              'schema-version': String(row.schemaVersion),
              'tenant-id': row.tenantId,
            },
          },
        ],
      });
    },
  };
}
