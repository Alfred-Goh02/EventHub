import { randomUUID } from 'node:crypto';
import { withTenant } from '../db.js';
import { env } from '../env.js';
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
 * The key is built from the message id, the channel and the recipient. The message
 * id is the outbox row's id, which a relay retry reuses; a Kafka offset would change.
 */

export type DeliveryStatus = 'PENDING' | 'SENT' | 'FAILED' | 'SUPPRESSED';

export type Claim =
  /** Ours to send. */
  | { claimed: true }
  /**
   * Someone claimed it first. PENDING means an earlier attempt claimed it and never
   * recorded an outcome — it crashed, or the send failed transiently — so it is
   * still ours to try. Any other status is final.
   */
  | { claimed: false; status: DeliveryStatus };

export interface DeliveryKey {
  tenantId: string;
  sourceMessageId: string;
  recipientId: string;
  channel: 'email';
}

export const idempotencyKey = (k: DeliveryKey): string => `${k.sourceMessageId}:${k.channel}:${k.recipientId}`;

export async function claimDelivery(k: DeliveryKey & { category: string; templateKey: string }): Promise<Claim> {
  const key = idempotencyKey(k);
  return withTenant(k.tenantId, async (tx) => {
    const inserted = await tx.$executeRaw`
      INSERT INTO "NotificationDelivery"
        (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId")
      VALUES (${randomUUID()}, ${k.tenantId}, ${k.recipientId}, ${k.channel}, ${k.category}, ${k.templateKey}, ${key}, ${k.sourceMessageId})
      ON CONFLICT DO NOTHING`;
    if (inserted === 1) return { claimed: true };

    const rows = await tx.$queryRaw<Array<{ status: DeliveryStatus }>>`
      SELECT d.status::text AS status FROM "NotificationDelivery" d
       WHERE d."tenantId" = ${k.tenantId} AND d."idempotencyKey" = ${key}`;
    const status = rows[0]?.status;
    if (!status) throw new Error(`claim conflicted but no delivery row is visible for ${key}`);
    return { claimed: false, status };
  });
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

/** A transient failure: the row stays PENDING, so the redelivery tries again. */
export async function recordAttempt(k: DeliveryKey, error: string): Promise<void> {
  const key = idempotencyKey(k);
  await withTenant(k.tenantId, (tx) => tx.$executeRaw`
    UPDATE "NotificationDelivery" SET attempts = attempts + 1, "lastError" = ${error.slice(0, 1000)}
     WHERE "tenantId" = ${k.tenantId} AND "idempotencyKey" = ${key} AND status = 'PENDING'`);
}
