import { Kafka, logLevel, type Consumer, type KafkaMessage } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { z, ZodError } from 'zod';
import { consumerLag, type ConsumerLag } from '@eventhub/kafka';
import type { Logger } from '@eventhub/logger';
import { PermanentSendError, type EmailChannel } from '../channels/types.js';
import { ExpiredRequest, PoisonMessage, type EmailRequestEnvelope, type OpenedEmailRequest } from './open-seal.js';
import { DLQ_HEADERS, DLQ_TOPIC, RETRY_HEADERS, header, jittered, originalHeaders, retryTiers } from './retry.js';
import { claimDelivery, markFailed, markSent, recordAttempt, suppressIfListed, type DeliveryKey } from './sent-log.js';
import { renderPasswordReset, type RenderedEmail } from './templates.js';

export const DISPATCH_TOPIC = 'auth.email-requested';

const counters = {
  received: 0,
  sent: 0,
  duplicates: 0,
  expired: 0,
  poison: 0,
  rejected: 0,
  suppressed: 0,
  suppressionsAdded: 0,
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
    /** Not sent, because the address is on the suppression list. */
    suppressedTotal: counters.suppressed,
    /** Addresses newly suppressed, after a refusal that proved they do not exist. */
    suppressionsAddedTotal: counters.suppressionsAdded,
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
export function isTransient(err: unknown): boolean {
  const text = err instanceof Error ? `${err.name} ${err.message}` : String(err);
  const status = (err as { status?: number })?.status;
  return (
    (typeof status === 'number' && status >= 500) ||
    /ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|timed? ?out/i.test(text)
  );
}

/** One line for an operator; well-formed, because part of it can be the message's own bytes. */
export const describe = (err: unknown): string => (err instanceof Error ? err.message : String(err)).slice(0, 500).toWellFormed();

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
  /** The address is on the suppression list. Recorded, and nothing sent. */
  | { kind: 'suppressed' }
  /**
   * Nothing to send, by design: a message on a shared topic that is not for this
   * service, or a reply to someone who asked not to be told. Final, and committed.
   */
  | { kind: 'skipped'; why: string }
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

/** One message, decoded to the point of knowing whom to send what. See deliver(). */
export interface PreparedDelivery {
  key: DeliveryKey;
  /** For the sent-log. */
  category: string;
  templateKey: string;
  to: string;
  email: RenderedEmail;
  headers: Record<string, string>;
  /** Identifiers only — never an address or a link — for every log line about it. */
  ids: Record<string, string>;
  /** What the log says once it is sent. */
  sentMessage: string;
}

/**
 * Claim, check the suppression list, send, record: the half of every email that does
 * not depend on what kind of email it is. One copy, so a reply and a password reset
 * cannot drift apart on the part that decides whether mail goes out twice.
 *
 * THE ORDER IS THE DESIGN, and createMessageHandler explains it; this is steps 3 to 5.
 */
export async function deliver(p: PreparedDelivery, opts: { channel: EmailChannel; logger: Logger }): Promise<Outcome> {
  const transient = (err: unknown): Outcome => {
    counters.transientFailures += 1;
    return { kind: 'transient', error: describe(err) };
  };

  let claim;
  try {
    claim = await claimDelivery({ ...p.key, category: p.category, templateKey: p.templateKey });
  } catch (err) {
    return transient(err);
  }
  if (!claim.claimed && claim.status !== 'PENDING') {
    counters.duplicates += 1;
    opts.logger.info({ ...p.ids, status: claim.status }, 'already delivered; skipped');
    return { kind: 'duplicate' };
  }

  let suppressed: boolean;
  try {
    suppressed = await suppressIfListed(p.key, p.to);
  } catch (err) {
    return transient(err);
  }
  if (suppressed) {
    counters.suppressed += 1;
    opts.logger.warn(p.ids, 'the address is on the suppression list; recorded SUPPRESSED and not sent');
    return { kind: 'suppressed' };
  }

  try {
    const result = await opts.channel.send({ to: p.to, ...p.email, headers: p.headers });
    await markSent(p.key, result.providerMessageId);
    counters.sent += 1;
    opts.logger.info(p.ids, p.sentMessage);
    return { kind: 'sent' };
  } catch (err) {
    if (err instanceof PermanentSendError) {
      let recorded: { suppressionAdded: boolean };
      try {
        recorded = await markFailed(
          p.key,
          describe(err),
          err.addressDoesNotExist && err.status ? { address: p.to, status: err.status } : undefined,
        );
      } catch (recordErr) {
        return transient(recordErr);
      }
      counters.rejected += 1;
      if (recorded.suppressionAdded) counters.suppressionsAdded += 1;
      opts.logger.warn(
        { ...p.ids, reason: describe(err), suppressionAdded: recorded.suppressionAdded },
        'the provider refused the email; recorded as FAILED',
      );
      return { kind: 'rejected' };
    }
    await recordAttempt(p.key, describe(err)).catch(() => {});
    return transient(err);
  }
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
 *   4. An address on the tenant's suppression list is recorded SUPPRESSED, and nothing
 *      is sent.
 *   5. Send, then record SENT. A permanent refusal is recorded as FAILED, and when it
 *      proves the address does not exist, the address is suppressed in the same
 *      transaction. Anything else is transient, and the row stays PENDING with its
 *      attempts counted.
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

    return deliver(
      {
        key: { tenantId: opened.tenantId, sourceMessageId: envelope.messageId, recipientId: opened.userId, channel: 'email' },
        category: 'account',
        templateKey: 'password-reset',
        to: opened.email,
        email: renderPasswordReset({ link: opened.link, expiresAt: opened.expiresAt }),
        headers: { 'X-EventHub-Message-Id': envelope.messageId },
        ids,
        sentMessage: 'password reset email sent',
      },
      opts,
    );
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
  /**
   * Other topics this dispatcher sends email for, each with its own handler and its
   * own starting point. They share the sent-log, the retry tiers and the dead-letter
   * topic with password resets — one pipeline, so they cannot drift apart on whether
   * mail goes out twice.
   */
  routes?: Array<{
    topic: string;
    startFrom: 'earliest' | 'latest';
    make: (decode: MessageHandlerOptions['decode']) => (value: Buffer | null) => Promise<Outcome>;
  }>;
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

  const decode: MessageHandlerOptions['decode'] = async (value) => {
    if (value === null) throw new Error('null value (a tombstone)');
    return registry.decode(value);
  };

  /**
   * One handler per topic this dispatcher reads. A message is handled by the handler
   * for the topic it ORIGINALLY came from — the retry tiers carry messages from every
   * route, and a reply retried from a tier must not be decoded as a password reset.
   */
  const routes = [
    { topic: DISPATCH_TOPIC, startFrom: opts.startFrom, handle: createMessageHandler({ decode, open: opts.open, channel: opts.channel, logger: opts.logger }) },
    ...(opts.routes ?? []).map((r) => ({ topic: r.topic, startFrom: r.startFrom, handle: r.make(decode) })),
  ];
  const handlers = new Map(routes.map((r) => [r.topic, r.handle]));
  async function handle(originalTopic: string, value: Buffer | null): Promise<Outcome> {
    const handler = handlers.get(originalTopic);
    if (!handler) {
      counters.poison += 1;
      return { kind: 'poison', reason: `no handler for messages from ${originalTopic}` };
    }
    return handler(value);
  }

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
      /**
       * Where the message first came from. forward() always stamps it on a message it
       * moves to a tier; one on a tier WITHOUT it predates routes, when everything on
       * the tiers was a password reset, and is handled as one — the retry and
       * suppression verifiers' canaries are exactly that, and failed their control
       * until this fallback existed.
       */
      const originalTopic =
        header(message.headers, RETRY_HEADERS.originalTopic) ?? (tiers.some((t) => t.topic === topic) ? DISPATCH_TOPIC : topic);
      const outcome = await handle(originalTopic, message.value);
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
      // One subscribe per route, because each chooses where a new group starts: a reset
      // queued before the service first ran is still worth sending, a week of old
      // replies on first deploy is not.
      for (const r of routes) await main.subscribe({ topic: r.topic, fromBeginning: r.startFrom === 'earliest' });
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
        { group: opts.groupId, retryGroup: retryGroupId, tiers, routes: routes.map((r) => `${r.topic} (${r.startFrom})`) },
        'dispatch running',
      );
    },

    /** Across every topic the main group reads: it is one group, so one backlog. */
    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, routes.map((r) => r.topic));
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
