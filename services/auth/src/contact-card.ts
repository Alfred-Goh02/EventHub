import { randomUUID } from 'node:crypto';
import type { Prisma } from './generated/prisma/client.js';
import { sealToNotification } from './email-request.js';
import { signContactCard } from './jwt.js';
import { emit } from './outbox.js';

/**
 * A user's address, published to Notification as a sealed contact card.
 *
 * DECIDED 2026-09-26: Notification learns addresses from this compacted topic rather
 * than asking Auth when it sends, so an email that is not a credential — a reply
 * notification — never waits on Auth being up. The address travels the way a
 * password-reset link does: signed by Auth, then encrypted to Notification's key, so
 * the topic, this service's outbox table and any log hold only ciphertext, and
 * Notification can tell a card Auth wrote from one anyone could have encrypted.
 *
 * COMPACTED, AND KEYED BY TENANT AND USER. Compaction keeps the LAST record per key,
 * so the key decides which card survives. A key of the user id alone would let one
 * tenant's card erase another's under a colliding id — Auth mints ids per tenant, and
 * CLAUDE.md records that failure for a dedupe key and a cache key already.
 */
export const AUTH_CONTACT_TOPIC = 'auth.user-contact';

/** The Kafka key, which is the relay's aggregateId. See the note above. */
export const contactKey = (tenantId: string, userId: string) => `${tenantId}:${userId}`;

/**
 * Queues a contact card, and stamps the user as published, in the caller's
 * transaction. The card and the marker commit with the change that caused them or not
 * at all, so a user can never be marked published with no card on its way.
 *
 * contactPublishedAt IS the current card's occurredAt: one value, stamped once.
 */
export async function queueContactCard(
  tx: Prisma.TransactionClient,
  user: { tenantId: string; userId: string; email: string },
): Promise<void> {
  // Chosen here, because the seal carries it as its jti and the seal is made before
  // the outbox row exists.
  const messageId = randomUUID();

  /**
   * occurredAt IS STAMPED BY THE STATEMENT THAT LOCKS THE USER, AND SIGNED.
   *
   * Notification keeps the card with the latest occurredAt, so this instant decides
   * which address a user has, and three sources were wrong:
   *   - the relay's own stamp, the outbox row's createdAt read through node-postgres
   *     as LOCAL time: eight hours early from a relay on this machine's host, so a
   *     newer card published there would lose to an older one published in a
   *     container;
   *   - now(), which is when the TRANSACTION started. Two transactions queueing a card
   *     for one user serialise on this row, and the one that waited, and so committed
   *     the newer card, can have started first — its card would lose;
   *   - this process's clock, for every reason CLAUDE.md gives.
   * clock_timestamp() inside the UPDATE is read once this transaction holds the user's
   * row lock, so card order follows lock order, which is commit order. It is rendered
   * in UTC by Postgres, and the relay leaves a payload's own occurredAt in place.
   *
   * It is inside the seal as well, because it is the ordering key: a producer could
   * otherwise re-send an old genuine card with a far-future occurredAt in its
   * plaintext envelope, and pin that user to an old address for good.
   */
  const stamped = await tx.$queryRaw<Array<{ at: string }>>`
    UPDATE "User" SET "contactPublishedAt" = clock_timestamp() AT TIME ZONE 'UTC'
     WHERE id = ${user.userId}
    RETURNING to_char("contactPublishedAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS at`;
  const at = stamped[0]?.at;
  if (!at) throw new Error(`no user ${user.userId} to queue a contact card for`);

  const signed = await signContactCard({
    subject: user.userId,
    jti: messageId,
    claims: { tenantId: user.tenantId, email: user.email, occurredAt: at },
  });

  await emit(tx, {
    id: messageId,
    tenantId: user.tenantId,
    topic: AUTH_CONTACT_TOPIC,
    eventType: 'UserContactPublished',
    aggregateId: contactKey(user.tenantId, user.userId),
    // Only identifiers, the ordering instant and ciphertext: the address is inside `sealed`.
    payload: { occurredAt: at, tenantId: user.tenantId, userId: user.userId, sealed: await sealToNotification(signed) },
  });
}
