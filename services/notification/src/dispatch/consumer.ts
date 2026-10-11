import { setTimeout as sleep } from 'node:timers/promises';
import { Kafka, logLevel, type Consumer, type EachMessagePayload, type KafkaMessage } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { z, ZodError } from 'zod';
import { consumerLag, type ConsumerLag } from '@eventhub/kafka';
import type { Logger } from '@eventhub/logger';
import { PermanentSendError, type EmailChannel } from '../channels/types.js';
import { isOptional, type Category } from '../preferences/categories.js';
import { ExpiredRequest, PoisonMessage, type EmailRequestEnvelope, type OpenedEmailRequest, type SealedKind } from './open-seal.js';
import { DLQ_HEADERS, DLQ_TOPIC, RETRY_HEADERS, header, jittered, originalHeaders, retryTiers } from './retry.js';
import { claimDelivery, markFailed, markSent, recordAttempt, suppressIfListed, type DeliveryKey } from './sent-log.js';
import { renderInvitation, renderPasswordReset, type RenderedEmail } from './templates.js';

export const DISPATCH_TOPIC = 'auth.email-requested';

/**
 * What each sealed kind is sent as.
 *
 * templateKey  part of the sent-log's idempotency key. A reset's key has none, for the
 *              reason sent-log.ts gives; every other kind's does, so one kind's message
 *              id can never claim another's delivery.
 * category     the sent-log's grouping, for an operator.
 * preference   its category in preferences/categories.ts, which must be TRANSACTIONAL:
 *              see below.
 */
const SEALED: {
  [K in SealedKind]: {
    templateKey: string;
    category: string;
    preference: Category;
    sentMessage: string;
    render: (opened: Extract<OpenedEmailRequest, { kind: K }>) => RenderedEmail;
  };
} = {
  PASSWORD_RESET: {
    templateKey: 'password-reset',
    category: 'account',
    preference: 'PASSWORD_RESET',
    sentMessage: 'password reset email sent',
    render: (o) => renderPasswordReset({ link: o.link, expiresAt: o.expiresAt }),
  },
  INVITATION: {
    templateKey: 'invitation',
    category: 'invitation',
    preference: 'INVITATION',
    sentMessage: 'invitation email sent',
    render: (o) => renderInvitation({ link: o.link, role: o.role, expiresAt: o.expiresAt }),
  },
};

/**
 * THE SEALED PATH ASKS NO PREFERENCE, SO IT MAY SEND ONLY WHAT CANNOT BE TURNED OFF.
 * A reset's reader may be locked out, and an invitation's has no account to hold a
 * choice. A kind added here under an OPTIONAL category would be sent to people who had
 * turned it off, so the service refuses to start instead.
 */
for (const [kind, s] of Object.entries(SEALED)) {
  if (isOptional(s.preference)) {
    throw new Error(`refusing to start: sealed ${kind} emails are sent without asking a preference, but ${s.preference} can be turned off`);
  }
}

/** One message's handling, by the kind Auth signed. */
function prepareSealed(opened: OpenedEmailRequest): { templateKey: string; category: string; sentMessage: string; email: RenderedEmail } {
  if (opened.kind === 'INVITATION') {
    const s = SEALED.INVITATION;
    return { templateKey: s.templateKey, category: s.category, sentMessage: s.sentMessage, email: s.render(opened) };
  }
  const s = SEALED.PASSWORD_RESET;
  return { templateKey: s.templateKey, category: s.category, sentMessage: s.sentMessage, email: s.render(opened) };
}

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

/** What the dispatcher knows about a message besides its bytes. */
export interface HandlerContext {
  /** Read from a retry tier: an earlier attempt failed, and may have claimed the delivery. */
  retried: boolean;
  /** The message's event-type header, which the relay stamps from the outbox row. */
  eventType: string | undefined;
}

export type MessageHandler = (value: Buffer | null, ctx: HandlerContext) => Promise<Outcome>;

/** One message, decoded to the point of knowing whom to send what. See deliver(). */
export interface PreparedDelivery {
  key: DeliveryKey;
  /** For the sent-log. */
  category: string;
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
    claim = await claimDelivery({ ...p.key, category: p.category });
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
export function createMessageHandler(opts: MessageHandlerOptions): MessageHandler {
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
    // Named for what the subject is: an invitation's is an Invite, never a user.
    const ids: Record<string, string> =
      envelope.kind === 'INVITATION'
        ? { messageId: envelope.messageId, tenantId: envelope.tenantId, kind: envelope.kind, inviteId: envelope.userId }
        : { messageId: envelope.messageId, tenantId: envelope.tenantId, userId: envelope.userId };

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

    /**
     * A message whose email cannot be rendered is the message's fault, and poison. Thrown
     * from here it would reach kafkajs as a TypeError, which crashes the consumer for
     * good: measured, with the opener's role check removed, an invitation naming a role
     * the template has no words for took the whole process down, and a restart would
     * have read the same message and gone down again.
     */
    let sealed: ReturnType<typeof prepareSealed>;
    try {
      sealed = prepareSealed(opened);
    } catch (err) {
      counters.poison += 1;
      // The error's name only: its message could quote the values being rendered.
      const reason = `the email could not be rendered (${err instanceof Error ? err.name : 'unknown'})`;
      opts.logger.warn({ ...ids, reason }, 'email request refused');
      return { kind: 'poison', reason };
    }

    // Steps 3 to 5 are the same for every kind: the claim, the suppression list, the
    // send. Only the template differs, and no kind on this path asks a preference or
    // reads a contact card -- the address is in the seal.
    return deliver(
      {
        key: {
          tenantId: opened.tenantId,
          sourceMessageId: envelope.messageId,
          recipientId: opened.userId,
          channel: 'email',
          templateKey: sealed.templateKey,
        },
        category: sealed.category,
        to: opened.email,
        email: sealed.email,
        // No List-Unsubscribe on any kind here: each is transactional.
        headers: { 'X-EventHub-Message-Id': envelope.messageId },
        ids,
        sentMessage: sealed.sentMessage,
      },
      opts,
    );
  };
}

/**
 * A topic other than password resets that this dispatcher sends email for.
 *
 * EACH IS READ BY A CONSUMER AND A GROUP OF ITS OWN, `${groupId}-${name}`. One consumer
 * for everything handles one message at a time, and a fetch waits for the last one to
 * finish: a backlog of replies after an outage — each a few database round trips, an
 * unseal and an SMTP connection — would sit in front of every password reset published
 * behind it, long enough for the reset's thirty-minute link to die unsent. Before
 * replies, a reset only ever queued behind other resets, and that is kept.
 *
 * ITS RETRIES ARE ITS OWN TOO. The retry tiers are shared topics, but each tier is read
 * by the reset path's retry group AND by one retry group per route,
 * `${groupId}-${name}-retry`, and each acts only on its own messages and steps over the
 * rest. With one retry consumer for everything, replies that failed in an outage would
 * be retried in front of the resets that failed with them.
 *
 * They still share the sent-log, the tiers and the dead-letter topic with password
 * resets — one pipeline for whether mail goes out twice — and each route is started
 * after the reset path is running, retrying on its own with a growing wait, so a route
 * that cannot start or keeps crashing never stops a password reset from being read.
 */
export interface Route {
  /** Names its consumer groups, `${groupId}-${name}` and `${groupId}-${name}-retry`, and its log lines. */
  name: string;
  topic: string;
  /** Where its group starts when it has no committed offset. */
  startFrom: 'earliest' | 'latest';
  make: (decode: MessageHandlerOptions['decode']) => MessageHandler;
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
  routes?: Route[];
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
  const restartTimers = new Set<NodeJS.Timeout>();
  /** Bumped by every start and stop, so a route still starting from an earlier start gives up. */
  let generation = 0;

  const decode: MessageHandlerOptions['decode'] = async (value) => {
    if (value === null) throw new Error('null value (a tombstone)');
    return registry.decode(value);
  };

  const routes = (opts.routes ?? []).map((r) => ({ ...r, handle: r.make(decode) }));
  const routeTopics = new Set(routes.map((r) => r.topic));

  /**
   * One handler per topic. A message is handled by the handler for the topic it
   * ORIGINALLY came from — the retry tiers carry messages from every route, and a reply
   * retried from a tier must not be decoded as a password reset.
   */
  const handlers = new Map<string, MessageHandler>([
    [DISPATCH_TOPIC, createMessageHandler({ decode, open: opts.open, channel: opts.channel, logger: opts.logger })],
    ...routes.map((r): [string, MessageHandler] => [r.topic, r.handle]),
  ]);
  async function handle(originalTopic: string, value: Buffer | null, ctx: HandlerContext): Promise<Outcome> {
    const handler = handlers.get(originalTopic);
    if (!handler) {
      counters.poison += 1;
      return { kind: 'poison', reason: `no handler for messages from ${originalTopic}` };
    }
    return handler(value, ctx);
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
    /** As process() decided it, so the handler that ran and the header written agree. */
    originalTopic: string,
  ): Promise<void> {
    const { message } = source;
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
    /**
     * THE VALUE IS KEPT ONLY WHERE IT IS SEALED. A password reset's value, and an
     * invitation's, is ciphertext only this service can open, around a link that stops
     * working when its reset or invitation does. Anything else — a reply, carrying the
     * reply's words and who wrote them — would outlive, in a topic kept forever, the
     * author's delete and a moderator's hide, which change only Community's database; and
     * replaying it would
     * put those words back on the topic it came from, for every consumer of that topic.
     * So the dead-letter copy keeps the headers — message id, reason, where it came
     * from — and not the value: the count an operator is paged on still moves, and the
     * words go when their own topic's retention takes them.
     */
    const keepValue = originalTopic === DISPATCH_TOPIC;
    await producer.send({
      topic: DLQ_TOPIC,
      messages: [
        {
          key: message.key,
          value: keepValue ? message.value : null,
          headers: {
            ...base,
            ...(keepValue ? {} : { [DLQ_HEADERS.valueWithheld]: 'true' }),
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

  const isTier = (topic: string) => tiers.some((t) => t.topic === topic);

  /**
   * Where a message first came from. forward() always stamps it on a message it moves
   * to a tier; one on a tier WITHOUT it predates routes, when everything on the tiers
   * was a password reset, and is handled as one — the retry and suppression verifiers'
   * canaries are exactly that, and failed their control until this fallback existed.
   * One copy, read by process() and by the tier consumers deciding whose it is.
   */
  const originalTopicOf = (topic: string, message: KafkaMessage): string =>
    header(message.headers, RETRY_HEADERS.originalTopic) ?? (isTier(topic) ? DISPATCH_TOPIC : topic);

  /** A committed offset is the NEXT one to read, hence the + 1. */
  const next = (message: KafkaMessage) => (BigInt(message.offset) + 1n).toString();

  /**
   * Handle, forward if it failed, and only then commit. A throw leaves the offset where
   * it was. `state` is whose health a failure marks: a route's own, so a route in
   * trouble does not report the password-reset path as failing.
   */
  async function process(
    consumer: Consumer,
    consumerGroup: string,
    topic: string,
    partition: number,
    message: KafkaMessage,
    state: { failing: boolean; lastError: string | null } = health,
  ) {
    try {
      // Decided once, here, and handed to forward(), so a message that fails again is
      // stamped with the topic it was handled as.
      const originalTopic = originalTopicOf(topic, message);
      const outcome = await handle(originalTopic, message.value, { retried: isTier(topic), eventType: header(message.headers, 'event-type') });
      if (outcome.kind === 'poison' || outcome.kind === 'transient') {
        await forward({ topic, partition, message }, outcome, consumerGroup, originalTopic);
      }
      // Spelled out, because a commit that waits for an interval never happens with
      // autoCommit off.
      await consumer.commitOffsets([{ topic, partition, offset: next(message) }]);
      state.failing = false;
    } catch (err) {
      state.failing = true;
      state.lastError = describe(err);
      throw err;
    }
  }

  /**
   * What a retry consumer runs for each tier message. Every tier message is read by the
   * reset path's retry group and by each route's; `owns` says which are this consumer's,
   * and the rest are stepped over — committed, not handled — so each consumer waits only
   * on its own messages, and not on another's that are not due yet.
   */
  function onTier(
    consumer: Consumer,
    consumerGroup: string,
    owns: (originalTopic: string) => boolean,
    state: { failing: boolean; lastError: string | null } = health,
  ) {
    return async ({ topic, partition, message }: EachMessagePayload): Promise<void> => {
      if (!owns(originalTopicOf(topic, message))) {
        await consumer.commitOffsets([{ topic, partition, offset: next(message) }]);
        return;
      }
      const waitMs = Number(header(message.headers, RETRY_HEADERS.notBefore) ?? '0') - Date.now();
      if (waitMs > 0) {
        /**
         * NOT DUE YET, AND NO SLEEPING HERE. Sleeping in the handler would stop the poll
         * loop, heartbeats with it, and get the member thrown out of its group. Instead:
         * pause this partition, seek back to this message, and resume when it is due.
         * kafkajs marks the message done when this returns, but the seek moves the fetch
         * position back before the next fetch, and nothing is committed, so it is read
         * again rather than skipped.
         */
        consumer.pause([{ topic, partitions: [partition] }]);
        consumer.seek({ topic, partition, offset: message.offset });
        const timer = setTimeout(() => {
          resumeTimers.delete(timer);
          try {
            consumer.resume([{ topic, partitions: [partition] }]);
          } catch (err) {
            // A rebalance may have moved the partition elsewhere while it waited.
            opts.logger.warn({ err, topic, partition }, 'could not resume a retry partition');
          }
        }, waitMs);
        resumeTimers.add(timer);
        return;
      }
      await process(consumer, consumerGroup, topic, partition, message, state);
    };
  }

  /**
   * A consumer a route runs: one on the route's topic, one on its share of the tiers.
   *
   * RUNNING MEANS JOINED. kafkajs's run() resolves even when joining the group fails —
   * it reports that as a crash instead — so "run() returned" is no evidence of anything.
   * `running` is set by GROUP_JOIN and cleared by a crash or a stop.
   *
   * A FRESH CONSUMER PER START. A loop still starting from an earlier start, or a crash
   * restart already in flight when stop() ran, then acts on a consumer nothing else
   * uses, and one that was started under an older generation gives up rather than
   * joining after the dispatcher stopped.
   */
  interface Runner {
    name: string;
    groupId: string;
    topics: string[];
    fromBeginning: boolean;
    eachMessage: (consumer: Consumer, state: Runner['state']) => (payload: EachMessagePayload) => Promise<void>;
    consumer: Consumer | null;
    /** The generation it was last started under. */
    gen: number;
    state: { running: boolean; failing: boolean; lastError: string | null; crashes: number };
  }
  const newState = (): Runner['state'] => ({ running: false, failing: false, lastError: null, crashes: 0 });
  const runners: Runner[] = routes.flatMap((r): Runner[] => [
    {
      name: r.name,
      groupId: `${opts.groupId}-${r.name}`,
      topics: [r.topic],
      fromBeginning: r.startFrom === 'earliest',
      eachMessage: (consumer, state) => ({ topic, partition, message }) =>
        process(consumer, `${opts.groupId}-${r.name}`, topic, partition, message, state),
      consumer: null,
      gen: 0,
      state: newState(),
    },
    {
      name: `${r.name}-retry`,
      groupId: `${opts.groupId}-${r.name}-retry`,
      topics: tiers.map((t) => t.topic),
      // Where the route itself starts: on first deploy the tiers hold no message of this
      // route's, and after that the group has offsets of its own.
      fromBeginning: r.startFrom === 'earliest',
      eachMessage: (consumer, state) => onTier(consumer, `${opts.groupId}-${r.name}-retry`, (origin) => origin === r.topic, state),
      consumer: null,
      gen: 0,
      state: newState(),
    },
  ]);

  /** Retried, with a growing wait, until it runs or the dispatcher is stopped or restarted. Never awaited by start(). */
  async function startRunner(x: Runner, gen: number): Promise<void> {
    const current = () => gen === generation;
    let mine: Consumer | null = null;
    for (let attempt = 1; current(); attempt++) {
      const consumer = kafka.consumer({ groupId: x.groupId, sessionTimeout: 30_000 });
      mine = consumer;
      x.consumer = consumer;
      x.gen = gen;
      consumer.on(consumer.events.GROUP_JOIN, () => {
        if (x.consumer !== consumer) return;
        x.state.running = true;
        x.state.crashes = 0;
        opts.logger.info({ group: x.groupId, topics: x.topics }, `${x.name} running`);
      });
      consumer.on(consumer.events.CRASH, ({ payload }) => {
        if (x.consumer !== consumer) return;
        counters.crashes += 1;
        x.state.running = false;
        x.state.lastError = describe(payload.error);
        if (payload.restart) {
          opts.logger.error({ err: payload.error, consumer: x.name }, 'consumer crashed; kafkajs is restarting it');
          return;
        }
        // kafkajs has given up on it. Start it again — never at once: a crash that
        // recurs, a group the broker refuses or a message that throws every time, would
        // otherwise be a loop with no wait in it.
        if (x.gen !== generation) return;
        x.state.crashes += 1;
        const waitMs = Math.min(30_000, 1_000 * 2 ** x.state.crashes);
        opts.logger.error({ err: payload.error, consumer: x.name, waitMs }, `${x.name} stopped; password resets are unaffected; starting it again`);
        const timer = setTimeout(() => {
          restartTimers.delete(timer);
          if (x.gen === generation) void startRunner(x, x.gen);
        }, waitMs);
        restartTimers.add(timer);
      });
      try {
        await consumer.connect();
        if (!current()) break;
        await consumer.subscribe({ topics: x.topics, fromBeginning: x.fromBeginning });
        if (!current()) break;
        await consumer.run({ autoCommit: false, eachMessage: x.eachMessage(consumer, x.state) });
        if (!current()) break;
        return;
      } catch (err) {
        x.state.lastError = describe(err);
        const waitMs = Math.min(30_000, 1_000 * 2 ** attempt);
        opts.logger.error({ err, consumer: x.name, attempt, waitMs }, `${x.name} failed to start; password resets are unaffected; retrying`);
        await consumer.disconnect().catch(() => {});
        await sleep(waitMs);
      }
    }
    // Stopped or restarted while starting: leave nothing of this start connected. Its
    // own consumer, which a newer start has already replaced in `x` if there is one.
    if (mine) await mine.disconnect().catch(() => {});
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
      const gen = ++generation;
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

      // Password resets' retries, and anything on a tier from a topic no route claims,
      // which is handled here and dead-lettered as having no handler.
      await retry.run({ autoCommit: false, eachMessage: onTier(retry, retryGroupId, (origin) => !routeTopics.has(origin)) });

      opts.logger.info({ group: opts.groupId, retryGroup: retryGroupId, tiers }, 'dispatch running');

      // Last, and not awaited: password resets are flowing before any route is asked for.
      for (const x of runners) void startRunner(x, gen);
    },

    /** Password resets' backlog: the main group reads nothing else. */
    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, [DISPATCH_TOPIC]);
    },

    retryLag(): Promise<ConsumerLag> {
      return consumerLag(admin, retryGroupId, tiers.map((t) => t.topic));
    },

    /** One route consumer's backlog, in its own group: `replies`, or `replies-retry` across the tiers. */
    routeLag(name: string): Promise<ConsumerLag> {
      const x = runners.find((r) => r.name === name);
      if (!x) return Promise.reject(new Error(`no route consumer named ${name}`));
      return consumerLag(admin, x.groupId, x.topics);
    },

    /** Reported by /ready, never required by it: a route in trouble must not stop password resets being served. */
    routeHealth(): Array<{ name: string; running: boolean; failing: boolean; lastError: string | null }> {
      return runners.map((x) => ({ name: x.name, running: x.state.running, failing: x.state.failing, lastError: x.state.lastError }));
    },

    async stop(): Promise<void> {
      generation += 1;
      health.subscribed = false;
      for (const timer of [...resumeTimers, ...restartTimers]) clearTimeout(timer);
      resumeTimers.clear();
      restartTimers.clear();
      await Promise.all(
        [main.disconnect(), retry.disconnect(), ...runners.map((x) => x.consumer?.disconnect())].map((p) => Promise.resolve(p).catch(() => {})),
      );
      for (const x of runners) x.state.running = false;
      await producer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
    },
  };
}
