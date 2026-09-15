import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { z, ZodError } from 'zod';
import { consumerLag, type ConsumerLag } from '@eventhub/kafka';
import type { Logger } from '@eventhub/logger';
import { PermanentSendError, type EmailChannel } from '../channels/types.js';
import { ExpiredRequest, PoisonMessage, type EmailRequestEnvelope, type OpenedEmailRequest } from './open-seal.js';
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
const describe = (err: unknown): string => (err instanceof Error ? err.message : String(err)).slice(0, 1000).toWellFormed();

const envelopeSchema = z.object({
  messageId: z.string().min(1),
  tenantId: z.string().min(1),
  userId: z.string().min(1),
  kind: z.string().min(1),
  expiresAt: z.string().min(1),
  sealed: z.string().min(1),
});

export type Outcome = 'sent' | 'duplicate' | 'expired' | 'poison' | 'rejected';

export interface MessageHandlerOptions {
  decode: (value: Buffer | null) => Promise<unknown>;
  open: (envelope: EmailRequestEnvelope) => Promise<OpenedEmailRequest>;
  channel: EmailChannel;
  logger: Logger;
}

/**
 * One message, start to finish. Returning means its offset may be committed;
 * throwing means it must come back.
 *
 * THE ORDER IS THE DESIGN:
 *   1. decode and open the seal. A message that can never open is poison: counted,
 *      logged by id, and skipped. It is not written to the sent-log, because
 *      nothing in it is trustworthy, including the tenant it claims.
 *   2. expired, by the plaintext envelope or by the signed exp: skipped. A link that
 *      no longer works is worse than no email.
 *   3. CLAIM the delivery, one INSERT ... ON CONFLICT DO NOTHING. A final status
 *      means this message was already handled, so it is a duplicate and nothing is
 *      sent. PENDING means an earlier attempt never finished, so it is tried again.
 *   4. send, then record SENT. A permanent refusal is recorded as FAILED and the
 *      partition moves on. Anything else rethrows, uncommitted.
 *
 * AT LEAST ONCE, NOT EXACTLY ONCE. A crash after the provider accepted the mail but
 * before SENT is recorded leaves the row PENDING, and the redelivery sends again.
 * Nothing can close that gap with SMTP, which has no idempotency key. What the
 * claim does guarantee is that a redelivery of a message already recorded SENT
 * sends nothing.
 *
 * Nothing logged here carries the address or the link: ids only.
 */
export function createMessageHandler(opts: MessageHandlerOptions): (value: Buffer | null) => Promise<Outcome> {
  return async function handle(value) {
    counters.received += 1;

    let envelope: EmailRequestEnvelope;
    try {
      envelope = envelopeSchema.parse(await opts.decode(value));
    } catch (err) {
      if (isTransient(err)) throw err;
      counters.poison += 1;
      const reason = err instanceof ZodError ? 'the envelope does not match the contract' : describe(err);
      opts.logger.warn({ reason }, 'undecodable email request skipped');
      return 'poison';
    }
    const ids = { messageId: envelope.messageId, tenantId: envelope.tenantId, userId: envelope.userId };

    if (!(Date.parse(envelope.expiresAt) > Date.now())) {
      counters.expired += 1;
      opts.logger.info(ids, 'email request expired before delivery; skipped');
      return 'expired';
    }

    let opened: OpenedEmailRequest;
    try {
      opened = await opts.open(envelope);
    } catch (err) {
      if (err instanceof ExpiredRequest) {
        counters.expired += 1;
        opts.logger.info(ids, 'email request expired before delivery; skipped');
        return 'expired';
      }
      if (err instanceof PoisonMessage) {
        counters.poison += 1;
        opts.logger.warn({ ...ids, reason: err.message }, 'email request refused; skipped');
        return 'poison';
      }
      throw err;
    }

    const key: DeliveryKey = {
      tenantId: opened.tenantId,
      sourceMessageId: envelope.messageId,
      recipientId: opened.userId,
      channel: 'email',
    };
    const claim = await claimDelivery({ ...key, category: 'account', templateKey: 'password-reset' });
    if (!claim.claimed && claim.status !== 'PENDING') {
      counters.duplicates += 1;
      opts.logger.info({ ...ids, status: claim.status }, 'already delivered; skipped');
      return 'duplicate';
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
      return 'sent';
    } catch (err) {
      if (err instanceof PermanentSendError) {
        await markFailed(key, describe(err));
        counters.rejected += 1;
        opts.logger.warn({ ...ids, reason: describe(err) }, 'the provider refused the email; recorded as FAILED');
        return 'rejected';
      }
      counters.transientFailures += 1;
      await recordAttempt(key, describe(err)).catch(() => {});
      throw err;
    }
  };
}

export function createDispatcher(opts: {
  brokers: string[];
  registryUrl: string;
  groupId: string;
  startFrom: 'earliest' | 'latest';
  logger: Logger;
  open: MessageHandlerOptions['open'];
  channel: EmailChannel;
  /** Called when kafkajs stops the consumer for good. The caller exits. */
  onFatal: (err: unknown) => void;
}) {
  const kafka = new Kafka({ clientId: 'notification-dispatch', brokers: opts.brokers, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: opts.registryUrl });
  const consumer = kafka.consumer({ groupId: opts.groupId, sessionTimeout: 30_000 });
  const admin = kafka.admin();

  const handle = createMessageHandler({
    decode: async (value) => {
      if (value === null) throw new Error('null value (a tombstone)');
      return registry.decode(value);
    },
    open: opts.open,
    channel: opts.channel,
    logger: opts.logger,
  });

  return {
    async start(): Promise<void> {
      await admin.connect();
      await consumer.connect();
      consumer.on(consumer.events.GROUP_JOIN, () => {
        health.failing = false;
      });
      consumer.on(consumer.events.CRASH, ({ payload }) => {
        counters.crashes += 1;
        if (payload.restart) {
          opts.logger.error({ err: payload.error }, 'dispatch consumer crashed; kafkajs is restarting it');
          return;
        }
        health.stopped = true;
        health.lastError = describe(payload.error);
        opts.onFatal(payload.error);
      });
      await consumer.subscribe({ topic: DISPATCH_TOPIC, fromBeginning: opts.startFrom === 'earliest' });
      health.subscribed = true;
      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          try {
            await handle(message.value);
          } catch (err) {
            health.failing = true;
            health.lastError = describe(err);
            throw err;
          }
          // A committed offset is the NEXT one to read, hence the + 1. Spelled out,
          // because a commit that waits for an interval never happens with
          // autoCommit off (Analytics' ingest found that the hard way).
          await consumer.commitOffsets([{ topic, partition, offset: (BigInt(message.offset) + 1n).toString() }]);
          health.failing = false;
        },
      });
      opts.logger.info({ group: opts.groupId, topic: DISPATCH_TOPIC, startFrom: opts.startFrom }, 'dispatch running');
    },

    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, [DISPATCH_TOPIC]);
    },

    async stop(): Promise<void> {
      health.subscribed = false;
      await consumer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
    },
  };
}
