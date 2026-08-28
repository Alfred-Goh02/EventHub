import type { Role } from '@eventhub/types';
import { prisma } from './db.js';
import { hashSecretToken as hashToken, newSecretToken } from './secret-token.js';

const INVITE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

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
  const token = newSecretToken();
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
