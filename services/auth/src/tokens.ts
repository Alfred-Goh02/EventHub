import { createHash, randomBytes } from 'node:crypto';
import { prisma } from './db.js';

const REFRESH_TTL_MS = 30 * 24 * 60 * 60 * 1000;

// SHA-256 rather than argon2 on purpose: the input is already 256 bits of
// entropy, so there is nothing to brute-force and slow hashing buys nothing.
const hashToken = (token: string) =>
  createHash('sha256').update(token).digest('hex');

export async function issueRefreshToken(
  userId: string,
  tenantId: string,
  familyId?: string,
): Promise<string> {
  const token = randomBytes(32).toString('base64url');

  await prisma.refreshToken.create({
    data: {
      userId,
      tenantId,
      tokenHash: hashToken(token),
      familyId: familyId ?? randomBytes(16).toString('hex'),
      expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
    },
  });

  return token;
}

export type RotateResult =
  | { ok: true; userId: string; tenantId: string; token: string }
  | { ok: false; reason: 'invalid' | 'expired' | 'reused' };

export async function rotateRefreshToken(presented: string): Promise<RotateResult> {
  // Note: no join to User. RefreshToken carries its own tenantId because this
  // lookup happens BEFORE any tenant context exists — the tenant is a result of
  // the query, not an input to it, so an RLS-filtered join would return nothing.
  const record = await prisma.refreshToken.findUnique({
    where: { tokenHash: hashToken(presented) },
  });

  if (!record || record.revokedAt) {
    return { ok: false, reason: 'invalid' };
  }

  // Already used => someone has a copy. Kill the whole family.
  if (record.usedAt) {
    await revokeFamilyById(record.familyId);
    return { ok: false, reason: 'reused' };
  }

  if (record.expiresAt < new Date()) {
    return { ok: false, reason: 'expired' };
  }

  const token = randomBytes(32).toString('base64url');

  /**
   * THE CLAIM, and why it is a guarded UPDATE rather than the plain one that
   * used to be here.
   *
   * The check above reads `record` on its own connection, outside any
   * transaction and holding no lock. The previous version then wrote
   * `update({ where: { id } })` with no predicate, so two requests presenting
   * the SAME token concurrently both passed the check and both created a
   * successor — one refresh token yielding two independent, live sessions.
   *
   * That defeats the entire point of rotation. Family reuse detection never
   * fires, because neither successor is ever presented twice: the detector is
   * looking for a token used a second time, and there isn't one. Demonstrated
   * against this service before the fix — two racers, two usable successors.
   *
   * `usedAt: null` in the WHERE clause makes the check and the write the same
   * statement. Under READ COMMITTED the second transaction blocks on the row
   * lock, re-evaluates its predicate against the newly committed version, finds
   * usedAt set, and matches nothing. Exactly the mechanism claimInvite() and the
   * ticketing inventory guard already use.
   */
  const rotated = await prisma.$transaction(async (tx) => {
    const claimed = await tx.refreshToken.updateMany({
      where: { id: record.id, usedAt: null, revokedAt: null },
      data: { usedAt: new Date() },
    });

    if (claimed.count !== 1) return false;

    await tx.refreshToken.create({
      data: {
        userId: record.userId,
        tenantId: record.tenantId,
        tokenHash: hashToken(token),
        familyId: record.familyId,
        expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
      },
    });

    return true;
  });

  if (!rotated) {
    /**
     * Lost the race: someone else spent this token in the same instant.
     *
     * That is the same FACT as sequential reuse — two parties held one token —
     * so it gets the same answer, and the family dies. It also kills the
     * winner's brand-new successor, which is deliberate: nothing here can
     * distinguish "my client fired twice" from "an attacker raced me", and a
     * security control that cannot tell those apart must fail closed. The cost
     * is that a double-submitting client gets logged out; the alternative is
     * that a thief keeps a session.
     */
    await revokeFamilyById(record.familyId);
    return { ok: false, reason: 'reused' };
  }

  return { ok: true, userId: record.userId, tenantId: record.tenantId, token };
}

/** Revokes every live token in a family. The one place that write lives. */
async function revokeFamilyById(familyId: string): Promise<void> {
  await prisma.refreshToken.updateMany({
    where: { familyId, revokedAt: null },
    data: { revokedAt: new Date() },
  });
}

export async function revokeFamily(presented: string): Promise<void> {
  const record = await prisma.refreshToken.findUnique({
    where: { tokenHash: hashToken(presented) },
    select: { familyId: true },
  });

  if (!record) return;

  await revokeFamilyById(record.familyId);
}
