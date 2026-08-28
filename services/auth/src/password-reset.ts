import { prisma } from './db.js';
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
 * Returns the raw token EXACTLY ONCE. Only the hash is stored, so it cannot be
 * recovered afterwards — a lost link must be reissued, not looked up.
 */
export async function issueResetToken(
  tx: Prisma.TransactionClient,
  params: { userId: string; tenantId: string },
): Promise<{ token: string; expiresAt: Date }> {
  await tx.passwordResetToken.updateMany({
    where: { userId: params.userId, usedAt: null, revokedAt: null },
    data: { revokedAt: new Date() },
  });

  const token = newSecretToken();
  const expiresAt = new Date(Date.now() + RESET_TTL_MS);

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
 * IMPORTANT: this resolves identity only. It is NOT the authority on whether the
 * token may be spent — the reasons it returns are stale the moment it returns
 * them, because nothing is locked. Spending is decided by the guarded UPDATE in
 * the reset handler, which re-checks the same conditions as part of the write.
 */
export async function lookupResetToken(token: string): Promise<ResetLookup> {
  const record = await prisma.passwordResetToken.findUnique({
    where: { tokenHash: hashSecretToken(token) },
  });

  if (!record) return { ok: false, reason: 'invalid' };
  if (record.revokedAt) return { ok: false, reason: 'revoked' };
  if (record.usedAt) return { ok: false, reason: 'used' };
  if (record.expiresAt < new Date()) return { ok: false, reason: 'expired' };

  return { ok: true, id: record.id, userId: record.userId, tenantId: record.tenantId };
}
