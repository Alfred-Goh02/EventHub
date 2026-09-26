import { z, ZodError } from 'zod';
import type { Logger } from '@eventhub/logger';
import type { EmailChannel } from '../channels/types.js';
import { prisma, withTenant } from '../db.js';
import { isEnabled } from '../preferences/store.js';
import { signUnsubscribe } from '../preferences/unsubscribe-token.js';
import { deliver, describe, isTransient, type HandlerContext, type MessageHandler, type MessageHandlerOptions, type Outcome } from './consumer.js';
import { PoisonMessage, type ContactCardEnvelope, type OpenedContactCard } from './open-seal.js';
import { closePending, type DeliveryKey } from './sent-log.js';
import { renderCommunityReply } from './templates.js';

export const COMMUNITY_TOPIC = 'community.feed';

/** The event-type header the relay stamps on a comment, from the outbox row. */
const COMMENT_CREATED_EVENT = 'community.comment_created';

/**
 * Tells someone their comment or post was answered, from community.feed.
 *
 * WHO IS TOLD WAS DECIDED BY COMMUNITY. recipientId is set only for someone who can
 * still read the space, and never for the commenter themselves; Community is the only
 * service that can see who may read what, so this takes the answer rather than
 * second-guessing it. The one check repeated here is the self check, because it costs
 * nothing and an email to yourself about yourself is never right.
 *
 * THEN, IN ORDER, each a reason to send nothing that is recorded as a count:
 *   - the reply is older than REPLY_MAX_AGE_MS. Community decided the recipient when the
 *     comment was written, and nothing re-asks it: a reply caught up on hours later —
 *     after an outage, or a dead-letter replay — may have been hidden since, or its
 *     recipient banned. A notification that late is also not news. So there is an age
 *     past which it is not sent at all, measured by Postgres against the instant
 *     Community's database stamped, never by this process's clock;
 *   - the recipient turned replies off (the preferences table, 9c);
 *   - there is no contact card for them (9a), so no address to send to. Final: a card
 *     that arrives later does not bring the reply back. Once 9a's backfill has run,
 *     every user has one, so a missing card is an erased user or a contact consumer that
 *     has fallen behind, which its lag in /metrics shows.
 * None writes to the sent-log on a first attempt: nothing was attempted. On a retry an
 * earlier attempt may have claimed the delivery, so its row is closed with the reason.
 *
 * Then the shared half — claim, suppression list, send, record — exactly as a
 * password reset takes it, so a redelivered reply is not sent twice.
 *
 * EVERY REPLY CARRIES ITS OWN WAY OUT: List-Unsubscribe with a signed one-click link,
 * and the same link in the body. Decided 2026-09-26: replies are on by default, and a
 * default that is on must be easy to turn off.
 *
 * WHAT THIS TRUSTS. community.feed is not signed, as auth.email-requested is, so a
 * process that can produce to it can have a reply email carry any words to anyone with a
 * card. That is accepted, and bounded: the words are what any member can already send an
 * author by replying, the email says only that someone replied, and nothing in it is a
 * credential. What an unsigned message must never do is touch a credential email, which
 * is why the sent-log key includes the template (sent-log.ts). Deciding who may produce
 * at all is the broker's job — ACLs — and arrives with the deployment chapters.
 *
 * Nothing logged here carries an address or the reply's words: ids only.
 */

const envelopeSchema = z.object({
  messageId: z.string().min(1),
  /** When Community committed the comment, from its database. What the age limit measures. */
  occurredAt: z.string().datetime({ offset: true }),
  tenantId: z.string().min(1),
  kind: z.string().min(1),
  actorId: z.string().min(1),
  postId: z.string().min(1),
  commentId: z.string().nullable().optional(),
  parentCommentId: z.string().nullable().optional(),
  body: z.string().nullable().optional(),
  recipientId: z.string().nullable().optional(),
});

const counters = { received: 0, notAReply: 0, nobodyToTell: 0, toSelf: 0, tooOld: 0, optedOut: 0, noAddress: 0, poison: 0, handedOn: 0 };

export function replyMetrics(): Record<string, number> {
  return {
    receivedTotal: counters.received,
    /** Posts and reactions: on the same topic, and not for this service. */
    notAReplyTotal: counters.notAReply,
    /** Community named nobody: the commenter answered themselves, or the one answered can no longer read the space. */
    nobodyToTellTotal: counters.nobodyToTell,
    toSelfTotal: counters.toSelf,
    /** Older than REPLY_MAX_AGE_MS when read. */
    tooOldTotal: counters.tooOld,
    /** The recipient turned replies off. */
    optedOutTotal: counters.optedOut,
    /** No contact card for the recipient, or one that no longer opens. */
    noAddressTotal: counters.noAddress,
    poisonTotal: counters.poison,
    /** Went on to claim, suppression and send; see dispatch's counters for what became of them. */
    handedOnTotal: counters.handedOn,
  };
}

/** The stored card, as the envelope openCard checks it against. Rendered by Postgres, never through a JS Date. */
async function loadCard(tenantId: string, userId: string): Promise<ContactCardEnvelope | null> {
  const rows = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ sealed: string; messageId: string; occurredAt: string }>>`
      SELECT c.sealed, c."messageId", to_char(c."occurredAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "occurredAt"
        FROM "ContactCard" c
       WHERE c."tenantId" = ${tenantId} AND c."userId" = ${userId}`,
  );
  const row = rows[0];
  return row ? { tenantId, userId, sealed: row.sealed, messageId: row.messageId, occurredAt: row.occurredAt } : null;
}

/** Whether an instant Community's database stamped is further back than maxAgeMs, by this database's clock. */
async function olderThan(occurredAt: string, maxAgeMs: number): Promise<boolean> {
  const rows = await prisma.$queryRaw<Array<{ old: boolean }>>`
    SELECT ${occurredAt}::timestamptz < now() - make_interval(secs => ${maxAgeMs}::double precision / 1000) AS old`;
  return rows[0]?.old === true;
}

export function createReplyHandler(opts: {
  decode: MessageHandlerOptions['decode'];
  openCard: (envelope: ContactCardEnvelope) => Promise<OpenedContactCard>;
  channel: EmailChannel;
  logger: Logger;
  unsubscribeSecret: string;
  /** Where this service answers /unsubscribe, as a reader's mail client reaches it. */
  publicUrl: string;
  /** Where the web app shows a post. */
  appUrl: string;
  /** Past this age a reply is not sent. See the note at the top of this file. */
  maxAgeMs: number;
}): MessageHandler {
  const skipped = (why: string): Outcome => ({ kind: 'skipped', why });
  const transient = (err: unknown): Outcome => ({ kind: 'transient', error: describe(err) });

  return async function handle(value: Buffer | null, ctx: HandlerContext) {
    counters.received += 1;

    // Posts and reactions share the topic. The header says which this is without
    // decoding it, so a schema-registry outage holds up only the messages that could
    // become an email. One without the header is decoded and judged by its kind.
    if (ctx.eventType !== undefined && ctx.eventType !== COMMENT_CREATED_EVENT) {
      counters.notAReply += 1;
      return skipped('not a reply');
    }

    let e: z.infer<typeof envelopeSchema>;
    try {
      e = envelopeSchema.parse(await opts.decode(value));
    } catch (err) {
      if (isTransient(err)) return transient(err);
      counters.poison += 1;
      const reason = err instanceof ZodError ? 'the community message does not match the contract' : describe(err);
      opts.logger.warn({ reason }, 'undecodable community message refused');
      return { kind: 'poison', reason };
    }

    if (e.kind !== 'COMMENT_CREATED') {
      counters.notAReply += 1;
      return skipped('not a reply');
    }
    if (!e.recipientId) {
      counters.nobodyToTell += 1;
      return skipped('nobody to tell');
    }
    if (e.recipientId === e.actorId) {
      counters.toSelf += 1;
      return skipped('a reply to yourself');
    }
    if (!e.commentId) {
      counters.poison += 1;
      return { kind: 'poison', reason: 'a COMMENT_CREATED with no commentId' };
    }
    const ids = { messageId: e.messageId, tenantId: e.tenantId, recipientId: e.recipientId };
    const key: DeliveryKey = {
      tenantId: e.tenantId,
      sourceMessageId: e.messageId,
      recipientId: e.recipientId,
      channel: 'email',
      templateKey: 'community-reply',
    };

    /**
     * A reason to send nothing. On a RETRY an earlier attempt may have claimed the
     * delivery before it failed, and its row would sit PENDING until the sweeper called
     * it abandoned — a correctly declined email alarmed on as a stuck one. So it is
     * closed, with the reason. A first attempt claimed nothing, so there is nothing to
     * close, and no statement is spent on it.
     */
    const decline = async (why: string): Promise<Outcome> => {
      if (ctx.retried) {
        try {
          await closePending(key, `not sent: ${why}`);
        } catch (err) {
          return transient(err);
        }
      }
      return skipped(why);
    };

    try {
      if (await olderThan(e.occurredAt, opts.maxAgeMs)) {
        counters.tooOld += 1;
        opts.logger.info(ids, 'the reply is too old to be news; nothing sent');
        return decline('too old');
      }
    } catch (err) {
      return transient(err);
    }

    try {
      if (!(await isEnabled(e.tenantId, e.recipientId, 'COMMUNITY_REPLY', 'EMAIL'))) {
        counters.optedOut += 1;
        opts.logger.info(ids, 'replies are off for this recipient; nothing sent');
        return decline('opted out');
      }
    } catch (err) {
      return transient(err);
    }

    let card: ContactCardEnvelope | null;
    try {
      card = await loadCard(e.tenantId, e.recipientId);
    } catch (err) {
      return transient(err);
    }
    if (!card) {
      counters.noAddress += 1;
      opts.logger.info(ids, 'no contact card for this recipient; nothing sent');
      return decline('no address');
    }

    let opened: OpenedContactCard;
    try {
      opened = await opts.openCard(card);
    } catch (err) {
      /**
       * A card was verified when it was stored, so one that no longer opens means the
       * keys changed under it. That is not the REPLY's fault, and dead-lettering the
       * reply would not fix it: the recipient simply cannot be written to until their
       * card is republished. Counted with the missing ones, and logged loudly.
       */
      if (err instanceof PoisonMessage) {
        counters.noAddress += 1;
        opts.logger.error({ ...ids, reason: err.message }, 'the stored contact card does not open; nothing sent');
        return decline('address unreadable');
      }
      return transient(err);
    }

    const unsubscribeUrl = `${opts.publicUrl}/unsubscribe?token=${encodeURIComponent(
      signUnsubscribe(opts.unsubscribeSecret, { tenantId: e.tenantId, userId: e.recipientId, category: 'COMMUNITY_REPLY' }),
    )}`;
    counters.handedOn += 1;
    return deliver(
      {
        key,
        category: 'community',
        to: opened.email,
        email: renderCommunityReply({
          toComment: Boolean(e.parentCommentId),
          body: e.body ?? '',
          postUrl: `${opts.appUrl}/posts/${encodeURIComponent(e.postId)}`,
          unsubscribeUrl,
        }),
        headers: {
          'X-EventHub-Message-Id': e.messageId,
          // RFC 2369 and RFC 8058: the angle brackets are part of the syntax, and the
          // Post header is what makes a mail client's button a one-click POST.
          'List-Unsubscribe': `<${unsubscribeUrl}>`,
          'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
        },
        ids,
        sentMessage: 'reply email sent',
      },
      opts,
    );
  };
}
