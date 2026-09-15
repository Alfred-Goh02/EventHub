import { Kafka, logLevel, type Consumer, type KafkaMessage } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { z, ZodError } from 'zod';
import { consumerLag, type ConsumerLag } from '@eventhub/kafka';
import type { Logger } from '@eventhub/logger';
import { PermanentSendError, type EmailChannel } from '../channels/types.js';
import { ExpiredRequest, PoisonMessage, type EmailRequestEnvelope, type OpenedEmailRequest } from './open-seal.js';
import { DLQ_HEADERS, DLQ_TOPIC, RETRY_HEADERS, header, jittered, originalHeaders, retryTiers } from './retry.js';
import { claimDelivery, markFailed, markSent, recordAttempt, type DeliveryKey } from './sent-log.js';
import { renderPasswordReset } from './templates.js';

export const DISPATCH_TOPIC = 'auth.email-requested';

const counters = {
  received: 0,
  sent: 0,
  duplicates: 0,
  expired: 0,
  poison: 0,
  rejected: 0,
  transientFailures: 0,
  retried: 0,
  deadLettered: 0,
  crashes: 0,
};

/**
 * Whether the dispatcher is doing its job, for /ready. Cleared on failure and on a
 * crash, not only set once at start: a probe that latched ready would report a
 * consumer that has stopped consuming as healthy.
 */
const health = { subscribed: false, failing: false, stopped: false, lastError: null as string | null };

export function dispatchMetrics(): Record<string, number> {
  return {
    receivedTotal: counters.received,
    sentTotal: counters.sent,
    duplicatesSkippedTotal: counters.duplicates,
    expiredSkippedTotal: counters.expired,
    poisonSkippedTotal: counters.poison,
    rejectedTotal: counters.rejected,
    transientFailuresTotal: counters.transientFailures,
    /** Moved to a delayed retry tier. */
    retriedTotal: counters.retried,
    /** Sent to notification.dlq. Its depth is the number an operator should be paged on. */
    deadLetteredTotal: counters.deadLettered,
    crashesTotal: counters.crashes,
  };
}

export function dispatchHealth(): { consuming: boolean; failing: boolean; stopped: boolean; lastError: string | null } {
  return {
    consuming: health.subscribed && !health.failing && !health.stopped,
    failing: health.failing,
    stopped: health.stopped,
    lastError: health.lastError,
  };
}

/** Network and registry failures that are not the message's fault. The same rule as Analytics' ingest. */
function isTransient(err: unknown): boolean {
  const text = err instanceof Error ? `${err.name} ${err.message}` : String(err);
  const status = (err as { status?: number })?.status;
  return (
    (typeof status === 'number' && status >= 500) ||
    /ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|timed? ?out/i.test(text)
  );
}

/** One line for an operator; well-formed, because part of it can be the message's own bytes. */
const describe = (err: unknown): string => (err instanceof Error ? err.message : String(err)).slice(0, 500).toWellFormed();

const envelopeSchema = z.object({
  messageId: z.string().min(1),
  tenantId: z.string().min(1),
  userId: z.string().min(1),
  kind: z.string().min(1),
  expiresAt: z.string().min(1),
  sealed: z.string().min(1),
});

export type Outcome =
  | { kind: 'sent' }
  | { kind: 'duplicate' }
  | { kind: 'expired' }
  | { kind: 'rejected' }
  /** Will never succeed. Straight to the dead-letter topic. */
  | { kind: 'poison'; reason: string }
  /** May succeed later. To the next retry tier. */
  | { kind: 'transient'; error: string };

export interface MessageHandlerOptions {
  decode: (value: Buffer | null) => Promise<unknown>;
  open: (envelope: EmailRequestEnvelope) => Promise<OpenedEmailRequest>;
  channel: EmailChannel;
  logger: Logger;
}

/**
 * One message, start to finish, returning what became of it. It does not throw
 * for anything the message or its delivery did; the caller decides where a failure
 * goes and commits only once it is there.
 *
 * THE ORDER IS THE DESIGN:
 *   1. Decode and open the seal. A message that can never open is poison. It is not
 *      written to the sent-log, because nothing in it is trustworthy, including the
 *      tenant it claims.
 *   2. Expired, by the plaintext envelope or by the signed exp: skipped. A link that
 *      no longer works is worse than no email.
 *   3. CLAIM the delivery, with one INSERT ... ON CONFLICT DO NOTHING. A final status
 *      means this message was already handled, so it is a duplicate and nothing is
 *      sent. PENDING means an earlier attempt never finished, so it is tried again,
 *      which is how a retry tier resumes a delivery the main topic started.
 *   4. Send, then record SENT. A permanent refusal is recorded as FAILED. Anything
 *      else is transient, and the row stays PENDING with its attempts counted.
 *
 * AT LEAST ONCE, NOT EXACTLY ONCE. If the process dies after the provider accepted
 * the mail but before SENT is recorded, the row stays PENDING and the next attempt
 * sends again. SMTP has no idempotency key to close that gap. What the claim does
 * guarantee is that a message already recorded SENT is never sent again.
 *
 * Nothing logged here carries the address or the link: ids only.
 */
export function createMessageHandler(opts: MessageHandlerOptions): (value: Buffer | null) => Promise<Outcome> {
  const transient = (err: unknown): Outcome => {
    counters.transientFailures += 1;
    return { kind: 'transient', error: describe(err) };
  };

  return async function handle(value) {
    counters.received += 1;

    let envelope: EmailRequestEnvelope;
    try {
      envelope = envelopeSchema.parse(await opts.decode(value));
    } catch (err) {
      if (isTransient(err)) return transient(err);
      counters.poison += 1;
      const reason = err instanceof ZodError ? 'the envelope does not match the contract' : describe(err);
      opts.logger.warn({ reason }, 'undecodable email request refused');
      return { kind: 'poison', reason };
    }
    const ids = { messageId: envelope.messageId, tenantId: envelope.tenantId, userId: envelope.userId };

    if (!(Date.parse(envelope.expiresAt) > Date.now())) {
      counters.expired += 1;
      opts.logger.info(ids, 'email request expired before delivery; skipped');
      return { kind: 'expired' };
    }

    let opened: OpenedEmailRequest;
    try {
      opened = await opts.open(envelope);
    } catch (err) {
      if (err instanceof ExpiredRequest) {
        counters.expired += 1;
        opts.logger.info(ids, 'email request expired before delivery; skipped');
        return { kind: 'expired' };
      }
      if (err instanceof PoisonMessage) {
        counters.poison += 1;
        opts.logger.warn({ ...ids, reason: err.message }, 'email request refused');
        return { kind: 'poison', reason: err.message };
      }
      return transient(err);
    }

    const key: DeliveryKey = {
      tenantId: opened.tenantId,
      sourceMessageId: envelope.messageId,
      recipientId: opened.userId,
      channel: 'email',
    };

    let claim;
    try {
      claim = await claimDelivery({ ...key, category: 'account', templateKey: 'password-reset' });
    } catch (err) {
      return transient(err);
    }
    if (!claim.claimed && claim.status !== 'PENDING') {
      counters.duplicates += 1;
      opts.logger.info({ ...ids, status: claim.status }, 'already delivered; skipped');
      return { kind: 'duplicate' };
    }

    const email = renderPasswordReset({ link: opened.link, expiresAt: opened.expiresAt });
    try {
      const result = await opts.channel.send({
        to: opened.email,
        ...email,
        headers: { 'X-EventHub-Message-Id': envelope.messageId },
      });
      await markSent(key, result.providerMessageId);
      counters.sent += 1;
      opts.logger.info(ids, 'password reset email sent');
      return { kind: 'sent' };
    } catch (err) {
      if (err instanceof PermanentSendError) {
        try {
          await markFailed(key, describe(err));
        } catch (recordErr) {
          return transient(recordErr);
        }
        counters.rejected += 1;
        opts.logger.warn({ ...ids, reason: describe(err) }, 'the provider refused the email; recorded as FAILED');
        return { kind: 'rejected' };
      }
      await recordAttempt(key, describe(err)).catch(() => {});
      return transient(err);
    }
  };
}

export function createDispatcher(opts: {
  brokers: string[];
  registryUrl: string;
  groupId: string;
  startFrom: 'earliest' | 'latest';
  /** One delay per retry tier topic, in milliseconds. */
  retryDelaysMs: readonly number[];
  logger: Logger;
  open: MessageHandlerOptions['open'];
  channel: EmailChannel;
  /** Called when kafkajs stops a consumer for good. The caller exits. */
  onFatal: (err: unknown) => void;
}) {
  const tiers = retryTiers(opts.retryDelaysMs);
  const retryGroupId = `${opts.groupId}-retry`;

  const kafka = new Kafka({ clientId: 'notification-dispatch', brokers: opts.brokers, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: opts.registryUrl });
  const main = kafka.consumer({ groupId: opts.groupId, sessionTimeout: 30_000 });
  const retry = kafka.consumer({ groupId: retryGroupId, sessionTimeout: 30_000 });
  // Idempotent, which also means acks from every in-sync replica: a message handed
  // on to a retry tier or the DLQ must be durable before its source offset commits.
  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  const admin = kafka.admin();
  const resumeTimers = new Set<NodeJS.Timeout>();

  const handle = createMessageHandler({
    decode: async (value) => {
      if (value === null) throw new Error('null value (a tombstone)');
      return registry.decode(value);
    },
    open: opts.open,
    channel: opts.channel,
    logger: opts.logger,
  });

  /**
   * Hands a failed message on: to the next retry tier, or to the dead-letter topic
   * once it is poison or the tiers are spent. Resolves only once the broker has it.
   * If this throws, the caller must not commit, so the message is read again rather
   * than lost between two topics.
   */
  async function forward(
    source: { topic: string; partition: number; message: KafkaMessage },
    outcome: Extract<Outcome, { kind: 'poison' | 'transient' }>,
    consumerGroup: string,
  ): Promise<void> {
    const { message } = source;
    const originalTopic = header(message.headers, RETRY_HEADERS.originalTopic) ?? source.topic;
    const failedAttempts = Number(header(message.headers, RETRY_HEADERS.attempt) ?? '0') + 1;
    const firstFailedAt = header(message.headers, RETRY_HEADERS.firstFailedAt) ?? new Date().toISOString();
    const base = originalHeaders(message.headers);
    const messageId = base['message-id'];

    if (outcome.kind === 'transient' && failedAttempts <= tiers.length) {
      const tier = tiers[failedAttempts - 1]!;
      const notBefore = Date.now() + jittered(tier.delayMs);
      await producer.send({
        topic: tier.topic,
        messages: [
          {
            key: message.key,
            value: message.value,
            headers: {
              ...base,
              [RETRY_HEADERS.originalTopic]: originalTopic,
              [RETRY_HEADERS.attempt]: String(failedAttempts),
              [RETRY_HEADERS.notBefore]: String(notBefore),
              [RETRY_HEADERS.firstFailedAt]: firstFailedAt,
              [RETRY_HEADERS.lastError]: outcome.error,
            },
          },
        ],
      });
      counters.retried += 1;
      opts.logger.warn({ messageId, attempt: failedAttempts, tier: tier.topic, notBefore, reason: outcome.error }, 'delivery failed; retrying later');
      return;
    }

    const reason =
      outcome.kind === 'poison' ? outcome.reason : `retries exhausted after ${failedAttempts} attempts: ${outcome.error}`;
    await producer.send({
      topic: DLQ_TOPIC,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...base,
            [DLQ_HEADERS.reason]: reason,
            [DLQ_HEADERS.consumerGroup]: consumerGroup,
            [DLQ_HEADERS.originalTopic]: originalTopic,
            [DLQ_HEADERS.sourceTopic]: source.topic,
            [DLQ_HEADERS.sourcePartition]: String(source.partition),
            [DLQ_HEADERS.sourceOffset]: message.offset,
            [DLQ_HEADERS.attempts]: String(failedAttempts),
            [DLQ_HEADERS.firstFailedAt]: firstFailedAt,
          },
        },
      ],
    });
    counters.deadLettered += 1;
    opts.logger.error({ messageId, reason }, 'sent to notification.dlq');
  }

  /** Handle, forward if it failed, and only then commit. A throw leaves the offset where it was. */
  async function process(consumer: Consumer, consumerGroup: string, topic: string, partition: number, message: KafkaMessage) {
    try {
      const outcome = await handle(message.value);
      if (outcome.kind === 'poison' || outcome.kind === 'transient') {
        await forward({ topic, partition, message }, outcome, consumerGroup);
      }
      // A committed offset is the NEXT one to read, hence the + 1. Spelled out,
      // because a commit that waits for an interval never happens with autoCommit off.
      await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
      health.failing = false;
    } catch (err) {
      health.failing = true;
      health.lastError = describe(err);
      throw err;
    }
  }

  function watch(consumer: Consumer, name: string) {
    consumer.on(consumer.events.GROUP_JOIN, () => {
      health.failing = false;
    });
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      counters.crashes += 1;
      if (payload.restart) {
        opts.logger.error({ err: payload.error, consumer: name }, 'consumer crashed; kafkajs is restarting it');
        return;
      }
      health.stopped = true;
      health.lastError = describe(payload.error);
      opts.onFatal(payload.error);
    });
  }

  return {
    async start(): Promise<void> {
      await Promise.all([admin.connect(), producer.connect(), main.connect(), retry.connect()]);
      watch(main, 'dispatch');
      watch(retry, 'retry');
      await main.subscribe({ topic: DISPATCH_TOPIC, fromBeginning: opts.startFrom === 'earliest' });
      await retry.subscribe({ topics: tiers.map((t) => t.topic), fromBeginning: opts.startFrom === 'earliest' });
      health.subscribed = true;

      await main.run({
        autoCommit: false,
        eachMessage: ({ topic, partition, message }) => process(main, opts.groupId, topic, partition, message),
      });

      await retry.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          const waitMs = Number(header(message.headers, RETRY_HEADERS.notBefore) ?? '0') - Date.now();
          if (waitMs > 0) {
            /**
             * NOT DUE YET, AND NO SLEEPING HERE. Sleeping in the handler would stop the
             * poll loop, heartbeats with it, and get the member thrown out of its group.
             * Instead: pause this partition, seek back to this message, and resume when
             * it is due. kafkajs marks the message done when this returns, but the seek
             * moves the fetch position back before the next fetch, and nothing is
             * committed, so it is read again rather than skipped.
             */
            retry.pause([{ topic, partitions: [partition] }]);
            retry.seek({ topic, partition, offset: message.offset });
            const timer = setTimeout(() => {
              resumeTimers.delete(timer);
              try {
                retry.resume([{ topic, partitions: [partition] }]);
              } catch (err) {
                // A rebalance may have moved the partition elsewhere while it waited.
                opts.logger.warn({ err, topic, partition }, 'could not resume a retry partition');
              }
            }, waitMs);
            resumeTimers.add(timer);
            return;
          }
          await process(retry, retryGroupId, topic, partition, message);
        },
      });

      opts.logger.info(
        { group: opts.groupId, retryGroup: retryGroupId, tiers, startFrom: opts.startFrom },
        'dispatch running',
      );
    },

    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, [DISPATCH_TOPIC]);
    },

    retryLag(): Promise<ConsumerLag> {
      return consumerLag(admin, retryGroupId, tiers.map((t) => t.topic));
    },

    async stop(): Promise<void> {
      health.subscribed = false;
      for (const timer of resumeTimers) clearTimeout(timer);
      resumeTimers.clear();
      await Promise.all([main.disconnect(), retry.disconnect()].map((p) => p.catch(() => {})));
      await producer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
    },
  };
}
