import { Kafka, type Producer } from 'kafkajs';
import { createLogger } from '@eventhub/logger';
import { env } from './env.js';

const logger = createLogger('live-kafka');

// A TimeoutNegativeWarning in these logs is an upstream kafkajs bug, not this
// code: its request queue schedules a timer with `-1 - Date.now()` whenever a
// request is fulfilled with an empty pending queue. One needless 1ms timer,
// nothing dropped. scripts/create-topics.mjs explains it in full.
const kafka = new Kafka({
  clientId: 'live-service',
  brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
  logLevel: 1,
});

const producer: Producer = kafka.producer();
let connected = false;

export async function connectProducer(): Promise<void> {
  await producer.connect();
  connected = true;
}

export async function disconnectProducer(): Promise<void> {
  if (!connected) return;
  connected = false;
  await producer.disconnect();
}

export interface LiveInteraction {
  kind: 'question.created' | 'question.upvoted';
  tenantId: string;
  eventId: string;
  userId: string;
  questionId: string;
  occurredAt: string;
}

/**
 * FIRE AND FORGET, AND NOT THROUGH A TRANSACTIONAL OUTBOX — deliberately, and
 * this is the place in the repository where that pattern is NOT applied.
 *
 * Ticketing publishes tickets.purchased through an outbox because losing one, or
 * emitting one for a transaction that rolled back, is a financial incident worth
 * a table, a poller and ordering machinery.
 *
 * live.interactions feeds analytics rollups, and a busy keynote produces
 * thousands of upvotes. A lost upvote in an aggregate is a rounding error. Paying
 * outbox cost per upvote would be over-engineering, and being able to say where
 * you deliberately did not apply a pattern is worth more than applying it
 * everywhere.
 *
 * The consequences, which belong in the open rather than in a footnote:
 *   - a process death between the Postgres commit and this call loses the event
 *   - so Analytics must treat this topic as AT-MOST-ONCE, and must never use it
 *     for anything involving money or entitlement
 *   - a failure counter is required, or the loss is silent
 *
 * Awaiting the broker ack here would put broker latency and broker availability
 * directly on the interactive path: a partition leader election would make the
 * Q&A feel broken, in exchange for durability nobody needs at that moment.
 */
export function emitInteraction(evt: LiveInteraction): void {
  if (!connected) {
    produceFailures += 1;
    return;
  }

  producer
    .send({
      topic: 'live.interactions',
      // Keyed by eventId so every interaction for one event lands on one
      // partition and keeps its order. Kafka guarantees ordering only within a
      // partition, which is exactly why the key is not the userId.
      messages: [{ key: evt.eventId, value: JSON.stringify(evt) }],
    })
    .catch((err: unknown) => {
      produceFailures += 1;
      logger.error({ err, kind: evt.kind }, 'live.interactions produce failed');
    });
}

/**
 * Exported so /metrics can report it. Without a counter the loss above is
 * invisible, which would turn a deliberate trade-off into an accident.
 */
let produceFailures = 0;
export const liveKafkaProduceFailuresTotal = (): number => produceFailures;
