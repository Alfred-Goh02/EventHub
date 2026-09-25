import { Kafka, logLevel, type EachBatchPayload, type KafkaMessage } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { consumerLag, type ConsumerLag } from '@eventhub/kafka';
import type { Logger } from '@eventhub/logger';
import { z } from 'zod';
import { withTenant } from '../db.js';
import { fanOutPost } from './fanout.js';

const TOPIC = 'community.feed';
const DLQ = 'community.feed.dlq';

/**
 * The contract this consumer requires, checked rather than assumed.
 *
 * The generated CommunityActivity type says what the SCHEMA declares; this says what
 * this consumer cannot work without. They are not the same statement, and the
 * generated type disappears at runtime.
 */
const activity = z.object({
  messageId: z.string().min(1),
  occurredAt: z.string().min(1),
  tenantId: z.string().min(1),
  spaceId: z.string().min(1),
  /**
   * A STRING, NOT AN ENUM, SO AN UNKNOWN KIND IS SKIPPED RATHER THAN DEAD-LETTERED.
   *
   * community.feed-value says it outright: "Adding a symbol later is
   * backward-compatible for readers that have it; a reader on the older schema rejects
   * the unknown symbol, which is why consumers are written to skip kinds they do not
   * know." A z.enum here broke that promise — a fifth symbol would have failed the
   * contract check and parked every message carrying it in the dead-letter topic, on a
   * change the schema calls compatible.
   */
  kind: z.string().min(1),
  actorId: z.string().min(1),
  postId: z.string().min(1),
});

const counters = { fannedOut: 0, rowsWritten: 0, tooLarge: 0, missingSpace: 0, ignored: 0, skipped: 0, deadLettered: 0 };
let assignedPartitions = 0;

export function feedMetrics(): Record<string, unknown> {
  return {
    postsFannedOutTotal: counters.fannedOut,
    feedRowsWrittenTotal: counters.rowsWritten,
    spacesTooLargeToFanOutTotal: counters.tooLarge,
    missingSpaceTotal: counters.missingSpace,
    activityIgnoredTotal: counters.ignored,
    unknownKindsSkippedTotal: counters.skipped,
    deadLetteredTotal: counters.deadLettered,
    assignedPartitions,
  };
}

/**
 * A registry or broker that is briefly unreachable fails the same call a poison
 * message does, and dead-lettering on an outage throws away good events. Copied in
 * shape from Search's indexer, which learned it the same way.
 */
function isTransient(err: unknown): boolean {
  const text = err instanceof Error ? `${err.name} ${err.message}` : String(err);
  const status = (err as { status?: number })?.status;
  return (
    (typeof status === 'number' && status >= 500) ||
    /ECONNREFUSED|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|socket hang up|timed? ?out/i.test(text)
  );
}

type Decoded = { ok: true; value: z.infer<typeof activity> } | { ok: false; reason: string };

/**
 * Keeps FeedEntry in step with community.feed.
 *
 * THE COMMIT RULE, as everywhere else here: an offset is resolved only when every
 * message up to it has a FINAL outcome — applied, skipped, or dead-lettered. A
 * database that would not take the write right now is none of those, so the batch
 * throws, nothing past it commits, and Kafka redelivers. Lag grows while Postgres is
 * down, which is correct.
 *
 * THE DEAD-LETTER RULE: only what can never succeed. Undecodable bytes, a payload the
 * contract rejects. Never an outage.
 *
 * EVERY WRITE IS SCOPED TO THE TENANT IN THE MESSAGE. Kafka is not tenant-scoped and
 * this consumer's credentials read every partition, so the tenant comes from the
 * payload and goes through withTenant, where row security applies it. A consumer that
 * trusted its own connection would write one tenant's posts into another's feed.
 */
export function createFeedConsumer(opts: {
  brokers: string[];
  registryUrl: string;
  groupId: string;
  startFrom: 'earliest' | 'latest';
  maxMembers: number;
  logger: Logger;
}) {
  const kafka = new Kafka({ clientId: 'community-feed', brokers: opts.brokers, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: opts.registryUrl });
  const consumer = kafka.consumer({ groupId: opts.groupId });
  const producer = kafka.producer();
  const admin = kafka.admin();

  async function deadLetter(partition: number, message: KafkaMessage, reason: string): Promise<void> {
    await producer.send({
      topic: DLQ,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            'dlq-reason': reason,
            'dlq-consumer-group': opts.groupId,
            'dlq-source-topic': TOPIC,
            'dlq-source-partition': String(partition),
            'dlq-source-offset': message.offset,
          },
        },
      ],
    });
    counters.deadLettered += 1;
    opts.logger.error({ partition, offset: message.offset, reason }, 'message sent to the dead-letter topic');
  }

  async function decode(message: KafkaMessage): Promise<Decoded> {
    if (!message.value) return { ok: false, reason: 'null value; community.feed is not compacted and has no tombstones' };
    let raw: unknown;
    try {
      raw = await registry.decode(message.value);
    } catch (err) {
      if (isTransient(err)) throw err;
      return { ok: false, reason: `undecodable: ${err instanceof Error ? err.message : String(err)}` };
    }
    const parsed = activity.safeParse(raw);
    if (!parsed.success) {
      return { ok: false, reason: `contract violation: ${parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ')}` };
    }
    return { ok: true, value: parsed.data };
  }

  /**
   * Kinds this consumer knows and has nothing to do for. A comment or a reaction moves
   * the post's score, and the feed reads that score from the post at query time, so no
   * fanned-out row needs touching. They used to rewrite a copy of the score on every
   * row; see migration 20260925120000.
   *
   * Counted apart from UNKNOWN kinds, which mean a producer shipped something this
   * consumer was never taught. Folding the two together would bury that signal under
   * every comment and reaction.
   */
  const IGNORED = new Set(['COMMENT_CREATED', 'REACTION_ADDED', 'REACTION_REMOVED']);

  async function apply(value: z.infer<typeof activity>): Promise<void> {
    if (IGNORED.has(value.kind)) {
      counters.ignored += 1;
      return;
    }
    if (value.kind !== 'POST_CREATED') {
      // Counted, not silent: a kind nobody handles climbing steadily is how you find
      // out a producer shipped something this consumer was never taught.
      counters.skipped += 1;
      opts.logger.info({ kind: value.kind, postId: value.postId }, 'kind not handled by the feed consumer; skipped');
      return;
    }

    const result = await withTenant(value.tenantId, (tx) => fanOutPost(tx, value, opts.maxMembers));
    if (result.outcome === 'fanned-out') {
      counters.fannedOut += 1;
      counters.rowsWritten += result.rows;
    } else if (result.outcome === 'too-large') {
      counters.tooLarge += 1;
      opts.logger.info(
        { spaceId: value.spaceId, postId: value.postId, memberCount: result.memberCount, maxMembers: opts.maxMembers },
        'space is past the fan-out threshold; its posts are read at query time',
      );
    } else {
      /**
       * Not an error, and NOT the same as too large. The topic keeps a week, so a
       * replay reaches posts whose space has since been deleted. Counted separately
       * so a rising number here means something real — a producer naming a space
       * that never existed — rather than being lost inside the threshold count.
       */
      counters.missingSpace += 1;
      opts.logger.warn({ spaceId: value.spaceId, postId: value.postId }, 'no such space for this tenant; nothing to fan out to');
    }
  }

  async function eachBatch({ batch, resolveOffset, heartbeat, isRunning, isStale }: EachBatchPayload): Promise<void> {
    for (const message of batch.messages) {
      if (!isRunning() || isStale()) return;

      const decoded = await decode(message);
      if (!decoded.ok) {
        await deadLetter(batch.partition, message, decoded.reason);
        resolveOffset(message.offset);
        await heartbeat();
        continue;
      }

      // A database failure throws out of here on purpose: the offset is not resolved,
      // so this message and everything after it are redelivered.
      await apply(decoded.value);
      resolveOffset(message.offset);
      await heartbeat();
    }
  }

  return {
    async start(): Promise<void> {
      await Promise.all([consumer.connect(), producer.connect(), admin.connect()]);
      await consumer.subscribe({ topic: TOPIC, fromBeginning: opts.startFrom === 'earliest' });
      consumer.on(consumer.events.GROUP_JOIN, (e) => {
        assignedPartitions = Object.values(e.payload.memberAssignment).reduce((n, list) => n + list.length, 0);
      });
      /**
       * eachBatchAutoResolve: false, and it is not optional.
       *
       * It DEFAULTS TO TRUE, which resolves every offset in the batch as soon as
       * eachBatch returns — including messages the loop never reached. The loop below
       * returns early when the consumer is stopping or the batch is stale, so with the
       * default those unprocessed messages would be committed and never redelivered:
       * silent loss, visible only as a gap. Search's indexer sets it for the same
       * reason. Offsets are resolved one at a time, by the loop, after each message has
       * a final outcome.
       */
      await consumer.run({ eachBatchAutoResolve: false, eachBatch });
      opts.logger.info({ topic: TOPIC, groupId: opts.groupId }, 'feed consumer running');
    },
    async stop(): Promise<void> {
      await Promise.allSettled([consumer.disconnect(), producer.disconnect(), admin.disconnect()]);
    },
    lag(): Promise<ConsumerLag> {
      return consumerLag(admin, opts.groupId, [TOPIC]);
    },
  };
}
