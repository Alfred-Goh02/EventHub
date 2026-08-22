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
    await prisma.refreshToken.updateMany({
      where: { familyId: record.familyId, revokedAt: null },
      data: { revokedAt: new Date() },
    });
    return { ok: false, reason: 'reused' };
  }

  if (record.expiresAt < new Date()) {
    return { ok: false, reason: 'expired' };
  }

  const token = randomBytes(32).toString('base64url');

  await prisma.$transaction([
    prisma.refreshToken.update({
      where: { id: record.id },
      data: { usedAt: new Date() },
    }),
    prisma.refreshToken.create({
      data: {
        userId: record.userId,
        tenantId: record.tenantId,
        tokenHash: hashToken(token),
        familyId: record.familyId,
        expiresAt: new Date(Date.now() + REFRESH_TTL_MS),
      },
    }),
  ]);

  return { ok: true, userId: record.userId, tenantId: record.tenantId, token };
}

export async function revokeFamily(presented: string): Promise<void> {
  const record = await prisma.refreshToken.findUnique({
    where: { tokenHash: hashToken(presented) },
    select: { familyId: true },
  });

  if (!record) return;

  await prisma.refreshToken.updateMany({
    where: { familyId: record.familyId, revokedAt: null },
    data: { revokedAt: new Date() },
  });
}
