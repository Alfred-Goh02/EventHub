import { Kafka, type Producer, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { OutboxRow } from './drain.js';

export interface Publisher {
  publish(row: OutboxRow): Promise<void>;
  connect(): Promise<void>;
  disconnect(): Promise<void>;
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

      const value = await registry.encode(id, message);

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
