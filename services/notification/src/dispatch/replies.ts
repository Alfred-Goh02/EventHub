import { z, ZodError } from 'zod';
import type { Logger } from '@eventhub/logger';
import type { EmailChannel } from '../channels/types.js';
import { withTenant } from '../db.js';
import { isEnabled } from '../preferences/store.js';
import { signUnsubscribe } from '../preferences/unsubscribe-token.js';
import { deliver, describe, isTransient, type MessageHandlerOptions, type Outcome } from './consumer.js';
import { PoisonMessage, type ContactCardEnvelope, type OpenedContactCard } from './open-seal.js';
import { renderCommunityReply } from './templates.js';

export const COMMUNITY_TOPIC = 'community.feed';

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
 *   - the recipient turned replies off (the preferences table, 9c);
 *   - there is no contact card for them yet (9a), so no address to send to.
 * Neither writes to the sent-log: nothing was attempted.
 *
 * Then the shared half — claim, suppression list, send, record — exactly as a
 * password reset takes it, so a redelivered reply is not sent twice.
 *
 * EVERY REPLY CARRIES ITS OWN WAY OUT: List-Unsubscribe with a signed one-click link,
 * and the same link in the body. Decided 2026-09-26: replies are on by default, and a
 * default that is on must be easy to turn off.
 *
 * Nothing logged here carries an address or the reply's words: ids only.
 */

const envelopeSchema = z.object({
  messageId: z.string().min(1),
  tenantId: z.string().min(1),
  kind: z.string().min(1),
  actorId: z.string().min(1),
  postId: z.string().min(1),
  commentId: z.string().nullable().optional(),
  parentCommentId: z.string().nullable().optional(),
  body: z.string().nullable().optional(),
  recipientId: z.string().nullable().optional(),
});

const counters = { received: 0, notAReply: 0, nobodyToTell: 0, toSelf: 0, optedOut: 0, noAddress: 0, poison: 0, handedOn: 0 };

export function replyMetrics(): Record<string, number> {
  return {
    receivedTotal: counters.received,
    /** Posts and reactions: on the same topic, and not for this service. */
    notAReplyTotal: counters.notAReply,
    /** Community named nobody: the commenter answered themselves, or the one answered can no longer read the space. */
    nobodyToTellTotal: counters.nobodyToTell,
    toSelfTotal: counters.toSelf,
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
}): (value: Buffer | null) => Promise<Outcome> {
  const skipped = (why: string): Outcome => ({ kind: 'skipped', why });
  const transient = (err: unknown): Outcome => ({ kind: 'transient', error: describe(err) });

  return async function handle(value) {
    counters.received += 1;

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

    try {
      if (!(await isEnabled(e.tenantId, e.recipientId, 'COMMUNITY_REPLY', 'EMAIL'))) {
        counters.optedOut += 1;
        opts.logger.info(ids, 'replies are off for this recipient; nothing sent');
        return skipped('opted out');
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
      opts.logger.info(ids, 'no contact card for this recipient yet; nothing sent');
      return skipped('no address');
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
        return skipped('address unreadable');
      }
      return transient(err);
    }

    const unsubscribeUrl = `${opts.publicUrl}/unsubscribe?token=${encodeURIComponent(
      signUnsubscribe(opts.unsubscribeSecret, { tenantId: e.tenantId, userId: e.recipientId, category: 'COMMUNITY_REPLY' }),
    )}`;
    counters.handedOn += 1;
    return deliver(
      {
        key: { tenantId: e.tenantId, sourceMessageId: e.messageId, recipientId: e.recipientId, channel: 'email' },
        category: 'community',
        templateKey: 'community-reply',
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
