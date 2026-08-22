import { createHash, randomBytes } from 'node:crypto';
import type { Role } from '@eventhub/types';
import { prisma } from './db.js';

const INVITE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

// SHA-256, not argon2 — the input is already 256 bits of entropy, so there is
// nothing to brute force and a slow hash would buy nothing. Same reasoning as
// refresh tokens.
const hashToken = (token: string) =>
  createHash('sha256').update(token).digest('hex');

/**
 * Creates an invite and returns the raw token EXACTLY ONCE.
 *
 * Only the hash is stored, so this string cannot be recovered afterwards. That
 * is the point: a database dump yields no usable invitations. It also means a
 * lost invite must be reissued rather than looked up.
 */
export async function issueInvite(params: {
  tenantId: string;
  email: string;
  role: Role;
  invitedBy: string;
}): Promise<{ token: string; expiresAt: Date }> {
  const token = randomBytes(32).toString('base64url');
  const expiresAt = new Date(Date.now() + INVITE_TTL_MS);

  await prisma.invite.create({
    data: {
      tenantId: params.tenantId,
      email: params.email.toLowerCase(),
      role: params.role,
      invitedBy: params.invitedBy,
      tokenHash: hashToken(token),
      expiresAt,
    },
  });

  return { token, expiresAt };
}

export type InviteLookup =
  | { ok: true; id: string; tenantId: string; email: string; role: Role }
  | { ok: false; reason: 'invalid' | 'used' | 'revoked' | 'expired' };

/**
 * Resolves a raw invite token to the tenant it grants access to.
 *
 * Note there is no tenantId parameter, and there cannot be one: this runs
 * before any tenant context exists. That is precisely why "Invite" carries no
 * RLS policy — a tenant policy would filter out the very row that tells us
 * which tenant to use.
 */
export async function lookupInvite(token: string): Promise<InviteLookup> {
  const invite = await prisma.invite.findUnique({
    where: { tokenHash: hashToken(token) },
  });

  if (!invite) return { ok: false, reason: 'invalid' };
  if (invite.revokedAt) return { ok: false, reason: 'revoked' };
  if (invite.acceptedAt) return { ok: false, reason: 'used' };
  if (invite.expiresAt < new Date()) return { ok: false, reason: 'expired' };

  return {
    ok: true,
    id: invite.id,
    tenantId: invite.tenantId,
    email: invite.email,
    role: invite.role,
  };
}

/**
 * Marks an invite accepted, and reports whether THIS caller was the one that
 * did it. Returns false if someone else got there first.
 *
 * The `acceptedAt: null` predicate is what makes it single-use under
 * concurrency: two simultaneous requests both pass lookupInvite, but only one
 * updateMany matches a row, so only one gets count 1. Checking `acceptedAt`
 * and then updating as separate steps would let both through.
 */
export async function claimInvite(inviteId: string): Promise<boolean> {
  const { count } = await prisma.invite.updateMany({
    where: { id: inviteId, acceptedAt: null, revokedAt: null },
    data: { acceptedAt: new Date() },
  });
  return count === 1;
}
