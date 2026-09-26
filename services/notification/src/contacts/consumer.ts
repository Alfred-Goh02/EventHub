import { Kafka, logLevel, type EachBatchPayload, type KafkaMessage } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { consumerLag, type ConsumerLag } from '@eventhub/kafka';
import type { Logger } from '@eventhub/logger';
import { z, ZodError } from 'zod';
import { withTenant } from '../db.js';
import { PoisonMessage, type ContactCardEnvelope, type OpenedContactCard } from '../dispatch/open-seal.js';

export const CONTACTS_TOPIC = 'auth.user-contact';
export const CONTACTS_DLQ = 'auth.user-contact.dlq';

/**
 * Keeps Notification's copy of every user's contact card, from auth.user-contact.
 *
 * THE CARD IS VERIFIED HERE AND STORED SEALED. Opening it proves Auth wrote it for
 * this user and tenant; the address inside is then dropped, and the ciphertext is what
 * the table keeps. A forged card — one anyone could have encrypted to this service's
 * public key — never reaches the table.
 *
 * ALWAYS FROM THE EARLIEST OFFSET, and not configurable. The topic is compacted so a
 * new consumer can read every user's card from the start; a group starting at
 * `latest` would silently know nobody who registered before it.
 *
 * THE LATEST CARD WINS, judged by occurredAt in the database. A redelivery, or a
 * replay from the start, can bring an older card after a newer one.
 *
 * A TOMBSTONE — a null value — withdraws the card, and the copy is deleted. A
 * tombstone carries no seal, so anyone who can produce to the topic can send one;
 * the worst it does is stop that user's non-credential emails until their next card.
 * Password resets do not use this table.
 *
 * THE COMMIT RULE, as everywhere here: an offset is resolved only once its message
 * has a final outcome — stored, ignored as older, withdrawn, or dead-lettered. An
 * outage throws, nothing past it is committed, and the batch is redelivered.
 */

const envelopeSchema = z.object({
  messageId: z.string().min(1),
  occurredAt: z.string().min(1),
  tenantId: z.string().min(1),
  userId: z.string().min(1),
  sealed: z.string().min(1),
});

const counters = { stored: 0, olderIgnored: 0, withdrawn: 0, deadLettered: 0 };
let running = false;

export function contactsMetrics(): Record<string, unknown> {
  return {
    cardsStoredTotal: counters.stored,
    olderCardsIgnoredTotal: counters.olderIgnored,
    cardsWithdrawnTotal: counters.withdrawn,
    deadLetteredTotal: counters.deadLettered,
    running,
  };
}

function isTransient(err: unknown): boolean {
  const text = err instanceof Error ? `${err.name} ${err.message}` : String(err);
  const status = (err as { status?: number })?.status;
  return (
    (typeof status === 'number' && status >= 500) ||
    /ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|timed? ?out/i.test(text)
  );
}

/** Well-formed, because part of it can be the message's own bytes. */
const describe = (err: unknown): string => (err instanceof Error ? err.message : String(err)).slice(0, 500).toWellFormed();

/**
 * The key is `tenantId:userId`. A tenant id with a colon in it would make this
 * ambiguous, so the split is on the FIRST colon and a key without one is poison.
 */
function parseKey(key: Buffer | null): { tenantId: string; userId: string } | null {
  const text = key?.toString('utf8') ?? '';
  const at = text.indexOf(':');
  if (at <= 0 || at === text.length - 1) return null;
  return { tenantId: text.slice(0, at), userId: text.slice(at + 1) };
}

export function createContactsConsumer(opts: {
  brokers: string[];
  registryUrl: string;
  groupId: string;
  logger: Logger;
  openCard: (envelope: ContactCardEnvelope) => Promise<OpenedContactCard>;
}) {
  const kafka = new Kafka({ clientId: 'notification-contacts', brokers: opts.brokers, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: opts.registryUrl });
  const consumer = kafka.consumer({ groupId: opts.groupId });
  const producer = kafka.producer();
  const admin = kafka.admin();

  async function deadLetter(partition: number, message: KafkaMessage, reason: string): Promise<void> {
    await producer.send({
      topic: CONTACTS_DLQ,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'dlq-reason': reason,
            'dlq-consumer-group': opts.groupId,
            'dlq-source-topic': CONTACTS_TOPIC,
            'dlq-source-partition': String(partition),
            'dlq-source-offset': message.offset,
          },
        },
      ],
    });
    counters.deadLettered += 1;
    opts.logger.error({ partition, offset: message.offset, reason }, 'contact card sent to the dead-letter topic');
  }

  /** A final outcome for one message, or a throw for an outage. */
  async function apply(message: KafkaMessage): Promise<{ poison: string } | null> {
    if (message.value === null) {
      const key = parseKey(message.key);
      if (!key) return { poison: 'a tombstone whose key is not tenantId:userId' };
      await withTenant(key.tenantId, (tx) =>
        tx.$executeRaw`DELETE FROM "ContactCard" WHERE "tenantId" = ${key.tenantId} AND "userId" = ${key.userId}`,
      );
      counters.withdrawn += 1;
      return null;
    }

    let envelope: z.infer<typeof envelopeSchema>;
    try {
      envelope = envelopeSchema.parse(await registry.decode(message.value));
    } catch (err) {
      if (isTransient(err)) throw err;
      return { poison: err instanceof ZodError ? 'the envelope does not match the contract' : `undecodable: ${describe(err)}` };
    }

    // The key is what compaction keeps. A card filed under another user's key would
    // replace THAT user's card on the topic, so it is refused even if the seal is good.
    const key = parseKey(message.key);
    if (!key || key.tenantId !== envelope.tenantId || key.userId !== envelope.userId) {
      return { poison: 'the message key does not name the tenant and user in the envelope' };
    }

    try {
      await opts.openCard(envelope);
    } catch (err) {
      if (err instanceof PoisonMessage) return { poison: err.message };
      throw err;
    }

    /**
     * occurredAt goes to Postgres as TEXT and is cast there, never through a JS Date:
     * node-postgres writes a Date into `timestamp without time zone` as LOCAL wall
     * time, which on a host eight hours from UTC stores the wrong instant.
     */
    const written = await withTenant(envelope.tenantId, (tx) =>
      tx.$executeRaw`
        INSERT INTO "ContactCard" ("tenantId", "userId", sealed, "messageId", "occurredAt", "updatedAt")
        VALUES (${envelope.tenantId}, ${envelope.userId}, ${envelope.sealed}, ${envelope.messageId},
                (${envelope.occurredAt}::timestamptz AT TIME ZONE 'UTC'), now())
        ON CONFLICT ("tenantId", "userId") DO UPDATE
           SET sealed = EXCLUDED.sealed, "messageId" = EXCLUDED."messageId",
               "occurredAt" = EXCLUDED."occurredAt", "updatedAt" = now()
         WHERE "ContactCard"."occurredAt" <= EXCLUDED."occurredAt"`,
    );
    if (written === 1) counters.stored += 1;
    else counters.olderIgnored += 1;
    return null;
  }

  async function eachBatch({ batch, resolveOffset, heartbeat, isRunning, isStale }: EachBatchPayload): Promise<void> {
    for (const message of batch.messages) {
      if (!isRunning() || isStale()) return;
      // An outage throws out of here on purpose: the offset is not resolved, so this
      // message and everything after it are redelivered.
      const outcome = await apply(message);
      if (outcome) await deadLetter(batch.partition, message, outcome.poison);
      resolveOffset(message.offset);
      await heartbeat();
    }
  }

  return {
    async start(): Promise<void> {
      await Promise.all([consumer.connect(), producer.connect(), admin.connect()]);
      await consumer.subscribe({ topic: CONTACTS_TOPIC, fromBeginning: true });
      consumer.on(consumer.events.CRASH, () => {
        running = false;
      });
      // eachBatchAutoResolve: false, or offsets the loop never reached would be
      // committed when it returns early. See Community's feed consumer.
      await consumer.run({ eachBatchAutoResolve: false, eachBatch });
      running = true;
      opts.logger.info({ topic: CONTACTS_TOPIC, groupId: opts.groupId }, 'contact card consumer running');
    },
    async stop(): Promise<void> {
      running = false;
      await Promise.allSettled([consumer.disconnect(), producer.disconnect(), admin.disconnect()]);
    },
    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, [CONTACTS_TOPIC]);
    },
  };
}
