import { randomUUID } from 'node:crypto';
import { withTenant } from '../db.js';
import type { Prisma } from '../generated/prisma/client.js';
import { env } from '../env.js';
import { ABANDONED } from '../sweeper/sweep.js';
import { addressHash } from './suppression.js';

/**
 * The sent-log, from the consumer's side: claim a delivery before sending, then
 * record what happened.
 *
 * THE CLAIM IS ONE STATEMENT. INSERT ... ON CONFLICT DO NOTHING on (tenantId,
 * idempotencyKey). A read followed by an insert would let two deliveries of one
 * message both pass the read and both send. scripts/verify-notification-sentlog.mjs
 * races eight claims and requires exactly one to win.
 *
 * The key is built from the message id, the channel, the recipient and — for every
 * email but a password reset — the template; idempotencyKey says why. The message id
 * is the outbox row's id, which a relay retry reuses; a Kafka offset would change.
 */

export type DeliveryStatus = 'PENDING' | 'SENT' | 'FAILED' | 'SUPPRESSED';

export type Claim =
  /** Ours to send. */
  | { claimed: true }
  /**
   * Someone claimed it first. PENDING means an earlier attempt claimed it and never
   * recorded an outcome — it crashed, or the send failed transiently — so it is
   * still ours to try. Any other status is final, with one exception: `abandoned`.
   */
  | {
      claimed: false;
      status: DeliveryStatus;
      /**
       * FAILED by the sweeper, not by an outcome: claimed, never completed, and ended
       * an hour later so that PENDING keeps meaning "in flight". Nothing was refused
       * and nothing was sent, so it is not settled. See reopenAbandoned.
       */
      abandoned: boolean;
    };

export interface DeliveryKey {
  tenantId: string;
  sourceMessageId: string;
  recipientId: string;
  channel: 'email';
  templateKey: string;
}

/**
 * WHAT KIND OF EMAIL IS PART OF ITS IDENTITY. A message id is only as unique as its
 * producer makes it, and two topics have two producers: community.feed is not signed,
 * so a message on it can carry any id, including a pending password reset's, which
 * travels in plaintext. Keyed on the id alone, such a reply would find the reset's row
 * PENDING, send, and mark it SENT — and the reset's own retry would then read SENT and
 * skip. An unsigned producer must never be able to stop a credential email.
 *
 * A password reset's key has no template in it because it predates the others: its
 * format is unchanged, so a reset in flight across the deploy is not sent twice.
 */
export const idempotencyKey = (k: DeliveryKey): string =>
  k.templateKey === 'password-reset'
    ? `${k.sourceMessageId}:${k.channel}:${k.recipientId}`
    : `${k.templateKey}:${k.sourceMessageId}:${k.channel}:${k.recipientId}`;

/**
 * The claim's one statement, inside a transaction the caller already holds in the
 * tenant. Resolves whether THIS statement made the row. claimDelivery is the usual way
 * in; the reply limit uses this directly, so a reply it lets through is claimed under
 * the same lock that counted it.
 */
export async function insertClaim(tx: Prisma.TransactionClient, k: DeliveryKey & { category: string }): Promise<boolean> {
  const inserted = await tx.$executeRaw`
    INSERT INTO "NotificationDelivery"
      (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId")
    VALUES (${randomUUID()}, ${k.tenantId}, ${k.recipientId}, ${k.channel}, ${k.category}, ${k.templateKey}, ${idempotencyKey(k)}, ${k.sourceMessageId})
    ON CONFLICT DO NOTHING`;
  return inserted === 1;
}

export async function claimDelivery(k: DeliveryKey & { category: string }): Promise<Claim> {
  const key = idempotencyKey(k);
  return withTenant(k.tenantId, async (tx) => {
    if (await insertClaim(tx, k)) return { claimed: true };

    const rows = await tx.$queryRaw<Array<{ status: DeliveryStatus; lastError: string | null }>>`
      SELECT d.status::text AS status, d."lastError" FROM "NotificationDelivery" d
       WHERE d."tenantId" = ${k.tenantId} AND d."idempotencyKey" = ${key}`;
    const row = rows[0];
    if (!row) throw new Error(`claim conflicted but no delivery row is visible for ${key}`);
    return { claimed: false, status: row.status, abandoned: row.status === 'FAILED' && row.lastError === ABANDONED };
  });
}

/**
 * Takes an abandoned claim back, so this attempt may finish what the first one did not.
 *
 * AN ENDED CLAIM IS NOT A SENT ONE. The sweeper ends a claim left PENDING for an hour,
 * and until invitations nothing could arrive after that: a reset's seal is dead in
 * thirty minutes. An invitation's lives for days, so `dlq:replay` of one that was
 * dead-lettered more than an hour ago meets its own claim FAILED as abandoned. Read as
 * final, that replay sent nothing and the script still printed "replayed".
 *
 * Only a row the SWEEPER ended moves, and only back to PENDING, in one statement, so
 * of two attempts that read it abandoned at once only one gets it back; the other
 * then finds it PENDING, which is the usual resume. A FAILED row with any other
 * reason — a refusal, a decline — stays final.
 */
export async function reopenAbandoned(k: DeliveryKey): Promise<boolean> {
  const key = idempotencyKey(k);
  const moved = await withTenant(k.tenantId, (tx) => tx.$executeRaw`
    UPDATE "NotificationDelivery" SET status = 'PENDING', "failedAt" = NULL, "lastError" = NULL
     WHERE "tenantId" = ${k.tenantId} AND "idempotencyKey" = ${key}
       AND status = 'FAILED' AND "lastError" = ${ABANDONED}`);
  return moved === 1;
}

/** Only a PENDING row moves, so a late duplicate can never overwrite a final outcome. */
export async function markSent(k: DeliveryKey, providerMessageId: string | null): Promise<void> {
  const key = idempotencyKey(k);
  await withTenant(k.tenantId, (tx) => tx.$executeRaw`
    UPDATE "NotificationDelivery" SET status = 'SENT', "sentAt" = now(), "providerMessageId" = ${providerMessageId},
           attempts = attempts + 1, "lastError" = NULL
     WHERE "tenantId" = ${k.tenantId} AND "idempotencyKey" = ${key} AND status = 'PENDING'`);
}

/**
 * A permanent refusal. When it proved the address does not exist, the address is
 * suppressed IN THE SAME TRANSACTION, so there is never a FAILED row whose suppression
 * was lost between two statements.
 *
 * A LATER BOUNCE EXTENDS THE WINDOW rather than being ignored. The suppression ends
 * after SUPPRESSION_TTL_DAYS, because the refusal came from the relay this service
 * sends through and a relay can be wrong about a live mailbox; an address that really
 * is gone bounces again when the window ends, and the window moves out again. The row
 * is kept either way, so the count of bounces and the first time stay readable.
 */
export async function markFailed(
  k: DeliveryKey,
  error: string,
  suppress?: { address: string; status: string },
): Promise<{ suppressionAdded: boolean }> {
  const key = idempotencyKey(k);
  return withTenant(k.tenantId, async (tx) => {
    await tx.$executeRaw`
      UPDATE "NotificationDelivery" SET status = 'FAILED', "failedAt" = now(), "lastError" = ${error.slice(0, 1000)},
             attempts = attempts + 1
       WHERE "tenantId" = ${k.tenantId} AND "idempotencyKey" = ${key} AND status = 'PENDING'`;
    if (!suppress) return { suppressionAdded: false };
    const recorded = await tx.$executeRaw`
      INSERT INTO "EmailSuppression" (id, "tenantId", "addressHash", reason, "smtpStatus", "sourceDeliveryId", "expiresAt")
      VALUES (
        ${randomUUID()}, ${k.tenantId}, ${addressHash(suppress.address)}, 'HARD_BOUNCE', ${suppress.status},
        (SELECT d.id FROM "NotificationDelivery" d WHERE d."tenantId" = ${k.tenantId} AND d."idempotencyKey" = ${key}),
        now() + ${env.SUPPRESSION_TTL_DAYS} * interval '1 day'
      )
      ON CONFLICT ("tenantId", "addressHash") DO UPDATE
         SET "expiresAt" = EXCLUDED."expiresAt",
             "smtpStatus" = EXCLUDED."smtpStatus",
             "sourceDeliveryId" = EXCLUDED."sourceDeliveryId",
             bounces = "EmailSuppression".bounces + 1`;
    return { suppressionAdded: recorded === 1 };
  });
}

/**
 * Records the delivery SUPPRESSED if its address is on this tenant's suppression list,
 * in one statement, and resolves whether it was. Only a PENDING row moves, as with
 * every other outcome.
 *
 * An expired suppression does not count: the window is what makes a relay's mistake
 * recoverable without anyone noticing it in time.
 */
export async function suppressIfListed(k: DeliveryKey, address: string): Promise<boolean> {
  const key = idempotencyKey(k);
  const hash = addressHash(address);
  const moved = await withTenant(k.tenantId, (tx) => tx.$executeRaw`
    UPDATE "NotificationDelivery" SET status = 'SUPPRESSED', "lastError" = 'the address is on the suppression list'
     WHERE "tenantId" = ${k.tenantId} AND "idempotencyKey" = ${key} AND status = 'PENDING'
       AND EXISTS (
         SELECT 1 FROM "EmailSuppression" s
          WHERE s."tenantId" = ${k.tenantId} AND s."addressHash" = ${hash} AND s."expiresAt" > now())`);
  return moved === 1;
}

/**
 * A retry that decided, after an earlier attempt claimed the delivery, to send nothing:
 * the recipient turned replies off in between, or their card went. Without this the row
 * would sit PENDING until the sweeper called it abandoned — a correctly declined email
 * counted, and alarmed on, as a stuck one. FAILED with the reason, and only from PENDING,
 * so a delivery that did go out is never rewritten.
 */
export async function closePending(k: DeliveryKey, reason: string): Promise<void> {
  const key = idempotencyKey(k);
  await withTenant(k.tenantId, (tx) => tx.$executeRaw`
    UPDATE "NotificationDelivery" SET status = 'FAILED', "failedAt" = now(), "lastError" = ${reason.slice(0, 1000)}
     WHERE "tenantId" = ${k.tenantId} AND "idempotencyKey" = ${key} AND status = 'PENDING'`);
}

/** A transient failure: the row stays PENDING, so the redelivery tries again. */
export async function recordAttempt(k: DeliveryKey, error: string): Promise<void> {
  const key = idempotencyKey(k);
  await withTenant(k.tenantId, (tx) => tx.$executeRaw`
    UPDATE "NotificationDelivery" SET attempts = attempts + 1, "lastError" = ${error.slice(0, 1000)}
     WHERE "tenantId" = ${k.tenantId} AND "idempotencyKey" = ${key} AND status = 'PENDING'`);
}
