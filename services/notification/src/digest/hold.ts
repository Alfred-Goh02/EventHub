import { withTenant } from '../db.js';
import { idempotencyKey, insertClaim, type DeliveryKey } from '../dispatch/sent-log.js';

/**
 * WHETHER A REPLY MAY BE EMAILED NOW, OR IS HELD FOR THE HOURLY DIGEST.
 *
 * Decided 2026-09-27: at most five reply emails per person per hour. A busy thread
 * would otherwise send one email per reply, and replies are on by default. Past the
 * limit, a reply is held and goes out in one digest an hour, as a count and a link.
 *
 * THE COUNT IS THE SENT-LOG'S, over the last hour: reply deliveries to this person that
 * went out or are going out (SENT or PENDING). One that was suppressed or refused
 * reached nobody, and does not use up the hour.
 *
 * ONE RECIPIENT AT A TIME. Replies to one person arrive from any partition, on the
 * replies consumer and its retry consumer at once, and across replicas. Two of them
 * counting in parallel would both see four and both send the fifth. So the count and
 * what follows from it — the claim, or the hold — happen in one transaction, under an
 * advisory lock for this tenant's recipient. The claim is the delivery's own sent-log
 * row, so deliver() then finds it PENDING and carries on, exactly as a retry resumes a
 * delivery an earlier attempt claimed.
 *
 * A MESSAGE ALREADY DECIDED STAYS DECIDED. One that already has a delivery row is
 * deliver()'s to judge (a duplicate, or a retry of a claim), and one already held
 * stays held; neither is counted again.
 *
 * UNTIL ITS DIGEST GOES. A held item is deleted once its digest is settled, and nothing
 * remembers that the message was held. So a copy redelivered after that — the offset
 * commit that followed the hold failed, and Kafka sends the message again — is judged
 * afresh, and emailed on its own if the person's hour has room. It takes a failed
 * commit, a digest pass, and a redelivery inside REPLY_MAX_AGE_MS, and costs one email
 * for a reply the digest already counted. Accepted: remembering every held message
 * past its digest would keep, for every reply, the record this table exists to drop.
 */

/** The advisory-lock namespace for this, so no other lock in the database can collide with it. */
const REPLY_LIMIT_LOCK = 9_401;

export type Admission = 'send' | 'held';

export async function sendOrHold(
  key: DeliveryKey,
  opts: {
    /** The sent-log category the claim is written with. */
    category: string;
    /** The preference category, which is also the digest's. */
    digestCategory: string;
    postId: string;
    toComment: boolean;
    perHour: number;
  },
): Promise<Admission> {
  return withTenant(key.tenantId, async (tx) => {
    await tx.$executeRaw`
      SELECT pg_advisory_xact_lock(${REPLY_LIMIT_LOCK}::int, hashtext(${key.tenantId} || chr(31) || ${key.recipientId}))`;

    const decided = await tx.$queryRaw<Array<{ one: number }>>`
      SELECT 1 AS one FROM "NotificationDelivery" d
       WHERE d."tenantId" = ${key.tenantId} AND d."idempotencyKey" = ${idempotencyKey(key)}`;
    if (decided.length > 0) return 'send';

    const held = await tx.$queryRaw<Array<{ one: number }>>`
      SELECT 1 AS one FROM "DigestItem" i
       WHERE i."tenantId" = ${key.tenantId} AND i."userId" = ${key.recipientId}
         AND i.category = ${opts.digestCategory} AND i."sourceMessageId" = ${key.sourceMessageId}`;
    if (held.length > 0) return 'held';

    const [sent] = await tx.$queryRaw<Array<{ n: number }>>`
      SELECT count(*)::int AS n FROM "NotificationDelivery" d
       WHERE d."tenantId" = ${key.tenantId} AND d."recipientId" = ${key.recipientId}
         AND d."templateKey" = ${key.templateKey} AND d.status IN ('SENT', 'PENDING')
         AND d."createdAt" > now() - interval '1 hour'`;
    if ((sent?.n ?? 0) < opts.perHour) {
      await insertClaim(tx, { ...key, category: opts.category });
      return 'send';
    }

    await tx.$executeRaw`
      INSERT INTO "DigestItem" ("tenantId", "userId", category, "sourceMessageId", "postId", "toComment")
      VALUES (${key.tenantId}, ${key.recipientId}, ${opts.digestCategory}, ${key.sourceMessageId}, ${opts.postId}, ${opts.toComment})
      ON CONFLICT DO NOTHING`;
    return 'held';
  });
}
