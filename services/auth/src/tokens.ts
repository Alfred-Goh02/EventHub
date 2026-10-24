import { randomBytes } from 'node:crypto';
import { prisma } from './db.js';
import { hashSecretToken as hashToken, newSecretToken } from './secret-token.js';

const REFRESH_TTL_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * How long a login may be extended by rotation before the user must sign in
 * again. Ninety days.
 *
 * Every rotation issues a successor with a fresh 30-day expiry, so the sliding
 * window never closes on its own: a session used once a month lives forever, and
 * so does a STOLEN token used once a month. Reuse detection only fires if the
 * legitimate holder presents the same token the thief already spent — if the
 * thief simply takes over the chain and the victim never refreshes again,
 * nothing ever notices.
 *
 * An absolute ceiling is the backstop for that. It cannot be extended by any
 * amount of activity, so a compromised chain has a definite end date even when
 * nothing detects it.
 *
 * Measured from the FIRST token in the family rather than a stored column: the
 * family already records its own birth as `min(createdAt)`, and there is an
 * index on familyId to find it with. A new column would be a migration and a
 * second source of truth for a fact the table already holds.
 */
const ABSOLUTE_SESSION_MS = 90 * 24 * 60 * 60 * 1000;

export async function issueRefreshToken(
  userId: string,
  tenantId: string,
  familyId?: string,
): Promise<string> {
  const token = newSecretToken();

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

  /**
   * The absolute ceiling, checked before the sliding one.
   *
   * Rotation resets `expiresAt` to +30 days every time, so the sliding window
   * alone never closes on an active chain — including one an attacker is keeping
   * alive. This asks how old the FAMILY is, which no amount of rotation changes.
   *
   * The family is revoked rather than merely refused, so the outstanding
   * successor cannot be presented either: reaching the ceiling ends the session,
   * it does not pause it.
   *
   * ASKED OF POSTGRES, because createdAt is Postgres's clock. It is filled by the
   * column's DEFAULT (see the schema), and this compared it with `Date.now()`, so
   * the ceiling moved by however far this process's clock and the database's
   * disagree: with Auth an hour fast, a family 90 days less 30 minutes old was
   * refused. See "Timestamps come from the database" in CLAUDE.md.
   */
  const [family] = await prisma.$queryRaw<Array<{ pastCeiling: boolean | null }>>`
    SELECT min("createdAt") < now() - (${ABSOLUTE_SESSION_MS}::bigint * interval '1 millisecond') AS "pastCeiling"
      FROM "RefreshToken" WHERE "familyId" = ${record.familyId}
  `;

  if (family?.pastCeiling === true) {
    await revokeFamilyById(record.familyId);
    return { ok: false, reason: 'expired' };
  }

  if (record.expiresAt < new Date()) {
    return { ok: false, reason: 'expired' };
  }

  const token = newSecretToken();

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
