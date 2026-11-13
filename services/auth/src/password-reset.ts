import { databaseNow, prisma } from './db.js';
import type { Prisma } from './generated/prisma/client.js';
import { hashSecretToken, newSecretToken } from './secret-token.js';

/**
 * Thirty minutes, not the invite's seven days.
 *
 * An invite waits on a human who may not be at their desk. A reset is requested
 * by someone sitting in front of the screen right now, and every extra minute is
 * extra life for a token that may already have escaped into a mailbox somebody
 * else can read.
 */
const RESET_TTL_MS = 30 * 60 * 1000;

/**
 * Revokes every live reset token a user has, in the caller's transaction, on the
 * database's clock.
 *
 * Nothing compares a revokedAt with a time -- every reader asks only whether it is
 * set -- so a stamp from this process would do no harm today. It is the database's
 * anyway, in the statement that sets it, so that every time on this row is on one
 * clock and the next reader of it does not have to know which columns are safe.
 *
 * Called when a reset is issued, so at most one is ever live, and when one is
 * completed, so a link harvested before the user noticed anything dies with their
 * own recovery.
 */
export async function revokeLiveResets(tx: Prisma.TransactionClient, userId: string): Promise<number> {
  return tx.$executeRaw`
    UPDATE "PasswordResetToken" SET "revokedAt" = now() AT TIME ZONE 'UTC'
     WHERE "userId" = ${userId} AND "usedAt" IS NULL AND "revokedAt" IS NULL`;
}

/**
 * Issues a reset token and revokes the user's earlier ones, in the caller's
 * transaction.
 *
 * Takes a `tx` rather than using the module client, because the revoke and the
 * create MUST commit together. Two statements on the module client could be
 * interrupted between them, leaving either two live tokens or none.
 *
 * The revoke is what makes "at most one live reset per user" true, and that
 * matters more than it looks. Without it, an attacker who harvested a link from
 * a mailbox at 10:00 still holds a working credential at 10:20, after the victim
 * has noticed the intrusion and completed their own reset. The user does the one
 * thing they are told to do and it buys them nothing.
 *
 * THE EXPIRY IS THE DATABASE'S, and so is the expiresAt returned, which the email's
 * seal and its envelope carry. It was `new Date(Date.now() + RESET_TTL_MS)`, this
 * process's clock, while every judgement of it is made against now(): measured with
 * Auth an hour fast, a thirty-minute link was stored to expire 5400 s after the
 * database's now(), and told Notification the same.
 *
 * Returns the raw token EXACTLY ONCE. Only the hash is stored, so it cannot be
 * recovered afterwards — a lost link must be reissued, not looked up.
 */
export async function issueResetToken(
  tx: Prisma.TransactionClient,
  params: { userId: string; tenantId: string },
): Promise<{ token: string; expiresAt: Date }> {
  await revokeLiveResets(tx, params.userId);

  const token = newSecretToken();
  const expiresAt = await databaseNow(tx, RESET_TTL_MS);

  await tx.passwordResetToken.create({
    data: {
      userId: params.userId,
      tenantId: params.tenantId,
      tokenHash: hashSecretToken(token),
      expiresAt,
    },
  });

  return { token, expiresAt };
}

export type ResetLookup =
  | { ok: true; id: string; userId: string; tenantId: string }
  | { ok: false; reason: 'invalid' | 'used' | 'revoked' | 'expired' };

/**
 * Resolves a raw reset token to the user and tenant it belongs to.
 *
 * There is no tenantId parameter, and there cannot be one: this runs before any
 * tenant context exists. That is exactly why PasswordResetToken carries no RLS
 * policy — a tenant policy would filter out the row that tells us which tenant
 * to use. See the migration for the full justification.
 *
 * It runs on the module client rather than a transaction client for the same
 * reason: withTenant would need the tenant this call exists to discover.
 *
 * EXPIRED IS THE DATABASE'S ANSWER, as lookupInvite's is. This compared
 * `record.expiresAt < new Date()` in JavaScript, on this process's clock, so a
 * replica an hour fast refused a link the database said had thirty minutes left,
 * and one an hour slow honoured a link for ninety. Measured with Auth an hour fast.
 *
 * IMPORTANT: this resolves identity only. It is NOT the authority on whether the
 * token may be spent — the reasons it returns are stale the moment it returns
 * them, because nothing is locked. Spending is decided by the guarded UPDATE in
 * the reset handler, which re-checks all three conditions, expiry included, as
 * part of the write. It did not always: the claim checked usedAt and revokedAt
 * only, so a token that ran out between this read and the claim -- an argon2 hash
 * sits between them -- was still spent.
 */
export async function lookupResetToken(token: string): Promise<ResetLookup> {
  const [record] = await prisma.$queryRaw<
    Array<{ id: string; userId: string; tenantId: string; revoked: boolean; used: boolean; expired: boolean }>
  >`
    SELECT t.id, t."userId", t."tenantId",
           t."revokedAt" IS NOT NULL AS revoked,
           t."usedAt" IS NOT NULL AS used,
           t."expiresAt" <= (now() AT TIME ZONE 'UTC') AS expired
      FROM "PasswordResetToken" t
     WHERE t."tokenHash" = ${hashSecretToken(token)}`;

  if (!record) return { ok: false, reason: 'invalid' };
  if (record.revoked) return { ok: false, reason: 'revoked' };
  if (record.used) return { ok: false, reason: 'used' };
  if (record.expired) return { ok: false, reason: 'expired' };

  return { ok: true, id: record.id, userId: record.userId, tenantId: record.tenantId };
}
