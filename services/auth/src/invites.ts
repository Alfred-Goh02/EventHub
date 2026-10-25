import { randomUUID } from 'node:crypto';
import type { Role } from '@eventhub/types';
import { prisma, withTenant } from './db.js';
import { AUTH_EMAIL_TOPIC, sealEmailRequest } from './email-request.js';
import { emit } from './outbox.js';
import { hashSecretToken as hashToken, newSecretToken } from './secret-token.js';

/** The advisory-lock namespace for issuing an invitation, so no other lock in this database can collide with it. */
const INVITE_ISSUE_LOCK = 7_301;

/**
 * Issues an invitation and queues the email that carries it, in ONE transaction.
 *
 * THE EMAIL IS NOT THE CALLER'S TO REMEMBER. This used to create the row and return
 * the token, and the route handed the link to the dev sink alone: the sink is off
 * unless configured and refuses to boot in production, so a deployed system issued
 * invitations nobody ever received, and nobody new could join a tenant. Now the row
 * and the sealed request commit together or not at all, as a password reset's do:
 * there is no invitation without an email on its way, and no email for an invitation
 * that does not exist.
 *
 * THE NEWEST INVITATION IS THE ONLY LIVE ONE. Inviting an address again revokes every
 * earlier invitation to it in this tenant that is neither accepted nor revoked, before
 * the new one is made, so the newest link works and every older one is refused. The
 * reason is the reset's: an older link sitting in a mailbox somebody else can read
 * would otherwise stay a way into the tenant for the rest of its week, after the
 * admin had sent a new one precisely because something went wrong with the first.
 * Under an advisory lock per (tenant, address), so two invitations issued at once
 * cannot both find nothing to revoke and leave two live links.
 *
 * THE EXPIRY IS THE DATABASE'S, and the seal carries the same instant. Prisma 7 fills
 * timestamps in the client, and an expiry stamped on this process's clock would end
 * the row and the seal at whatever instant this host believed it was. The seal's exp
 * is the row's expiresAt to the second, rounded down, so the email never outlives the
 * link inside it.
 *
 * THE SUBJECT IS THE INVITE. The invitee is not a user and may never be one, so the
 * seal's subject, the message's key and the envelope's id field are the Invite's id.
 *
 * The raw token leaves this function inside the link, EXACTLY ONCE, for the dev sink;
 * only its hash is stored, so a database dump yields no usable invitations.
 */
export async function issueInvite(params: {
  tenantId: string;
  /** Already trimmed and lower-cased by the route's schema; lower-cased again here. */
  email: string;
  role: Role;
  invitedBy: string;
  /** Where the accept-invite page lives: PUBLIC_APP_URL. */
  appUrl: string;
  /** How long it stays good, INVITE_TTL_HOURS: a week unless configured. */
  ttlHours: number;
}): Promise<{ inviteId: string; link: string; expiresAt: Date; superseded: number }> {
  const email = params.email.toLowerCase();
  const { tenantId } = params;

  return withTenant(tenantId, async (tx) => {
    // chr(31) is the unit separator: it cannot appear in an id or an address, so the
    // concatenation is unambiguous.
    await tx.$executeRaw`
      SELECT pg_advisory_xact_lock(${INVITE_ISSUE_LOCK}::int, hashtext(${tenantId} || chr(31) || ${email}))`;

    // Invite is RLS-exempt, so the tenant in this WHERE is the only scope it has: an
    // admin of one tenant must never revoke another's invitations by re-inviting.
    const superseded = await tx.$executeRaw`
      UPDATE "Invite" SET "revokedAt" = now() AT TIME ZONE 'UTC'
       WHERE "tenantId" = ${tenantId} AND email = ${email}
         AND "acceptedAt" IS NULL AND "revokedAt" IS NULL`;

    const [stamp] = await tx.$queryRaw<Array<{ at: string }>>`
      SELECT to_char((now() AT TIME ZONE 'UTC') + ${params.ttlHours} * interval '1 hour', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS at`;
    if (!stamp) throw new Error('the database returned no time to expire an invitation at');
    const expiresAt = new Date(stamp.at);

    const token = newSecretToken();
    const invite = await tx.invite.create({
      data: {
        tenantId,
        email,
        role: params.role,
        invitedBy: params.invitedBy,
        tokenHash: hashToken(token),
        expiresAt,
      },
      select: { id: true },
    });

    const link = `${params.appUrl}/accept-invite?token=${token}`;

    // Chosen here, because the seal carries it as its jti and the seal is made before
    // the outbox row exists.
    const messageId = randomUUID();
    const sealed = await sealEmailRequest({
      messageId,
      tenantId,
      subject: invite.id,
      email,
      link,
      kind: 'INVITATION',
      expiresAt,
      // Signed, because the email says it: an envelope anyone can edit must not be
      // able to tell an attendee they were made an admin.
      extra: { role: params.role },
    });

    // Only ciphertext and identifiers reach the outbox, and so the topic. The link,
    // the address and the role are inside `sealed`.
    await emit(tx, {
      id: messageId,
      tenantId,
      topic: AUTH_EMAIL_TOPIC,
      eventType: 'InvitationRequested',
      aggregateId: invite.id,
      payload: {
        tenantId,
        // The envelope's subject field, which the contract names userId because a
        // reset came first. For an invitation it is the Invite.
        userId: invite.id,
        kind: 'INVITATION',
        expiresAt: expiresAt.toISOString(),
        sealed,
      },
    });

    return { inviteId: invite.id, link, expiresAt, superseded };
  });
}

/**
 * The mailbox an address reaches, for the per-address invitation budget, and for
 * nothing else.
 *
 * Keyed on the address as typed, the budget was one per spelling, not one per person.
 * Measured: victim+0@gmail.com to victim+99@gmail.com, and v.ictim@gmail.com, each had
 * a budget of their own, so the real limit on what one tenant could send one person was
 * the tenant's whole hundred an hour. Two rules collapse spellings that land in the
 * same mailbox:
 *   - a +tag in the local part, for every domain: subaddressing is near-universal, and
 *     where a provider does not support it, merging two addresses into one budget only
 *     makes the limit stricter;
 *   - dots in the local part, for Gmail only, where they are ignored; googlemail.com is
 *     the same mailboxes. Anywhere else, a.b and ab may be two people.
 *
 * NEVER STORED, NEVER SENT TO. The invitation keeps the address the admin typed, and
 * the email goes there: a +tag is how a person routes their own mail.
 */
export function budgetAddress(email: string): string {
  const at = email.lastIndexOf('@');
  if (at < 1) return email;
  let local = email.slice(0, at);
  let domain = email.slice(at + 1);
  const plus = local.indexOf('+');
  if (plus > 0) local = local.slice(0, plus);
  if (domain === 'googlemail.com') domain = 'gmail.com';
  if (domain === 'gmail.com') local = local.replace(/\./g, '');
  return `${local}@${domain}`;
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
 *
 * EXPIRED IS THE DATABASE'S ANSWER. The expiry was stamped from the database's clock,
 * and the email's seal expires at the same instant; judging it on this process's clock
 * would end the invitation at a different moment from its email. Measured with Auth an
 * hour fast and a one-hour invitation: the link in a fresh email was refused as
 * expired. So the comparison is made where the value came from.
 */
export async function lookupInvite(token: string): Promise<InviteLookup> {
  const [invite] = await prisma.$queryRaw<
    Array<{ id: string; tenantId: string; email: string; role: Role; revoked: boolean; accepted: boolean; expired: boolean }>
  >`
    SELECT i.id, i."tenantId", i.email, i.role::text AS role,
           i."revokedAt" IS NOT NULL AS revoked,
           i."acceptedAt" IS NOT NULL AS accepted,
           i."expiresAt" <= (now() AT TIME ZONE 'UTC') AS expired
      FROM "Invite" i
     WHERE i."tokenHash" = ${hashToken(token)}`;

  if (!invite) return { ok: false, reason: 'invalid' };
  if (invite.revoked) return { ok: false, reason: 'revoked' };
  if (invite.accepted) return { ok: false, reason: 'used' };
  if (invite.expired) return { ok: false, reason: 'expired' };

  return {
    ok: true,
    id: invite.id,
    tenantId: invite.tenantId,
    email: invite.email,
    role: invite.role,
  };
}

/**
 * Withdraws every live invite for an address within one tenant.
 *
 * `Invite.revokedAt` was read in two places and written in none, so the column
 * was decorative and a mis-sent invitation could not be taken back: it stayed
 * redeemable for its full seven days, and whoever received it could join the
 * tenant at the role it named.
 *
 * By EMAIL rather than by id, because there is no endpoint that lists invites —
 * an admin has no way to learn an id, and "revoke the invitation I sent to this
 * address" is the operation they actually have in mind. It also handles the case
 * where the same address was invited more than once.
 *
 * THE tenantId FILTER IS LOAD-BEARING. Invite is deliberately RLS-exempt, so
 * unlike almost every other write in this service there is no policy underneath
 * to catch a missing scope — an admin of one tenant would otherwise be able to
 * revoke another tenant's invitations. The exemption buys the pre-tenant-context
 * lookup that /register needs, and the price is that every OTHER query against
 * this table has to carry its own scope.
 *
 * Already-accepted invites are left alone: revoking one would not remove the
 * user it created, so it would only make the audit trail lie.
 *
 * revokedAt on the database's clock, as issueInvite's supersede stamps it. Nothing
 * compares it with a time -- only whether it is set -- but a column filled from two
 * clocks is one nobody can safely read later.
 */
export async function revokeInvitesFor(params: {
  tenantId: string;
  email: string;
}): Promise<number> {
  return prisma.$executeRaw`
    UPDATE "Invite" SET "revokedAt" = now() AT TIME ZONE 'UTC'
     WHERE "tenantId" = ${params.tenantId} AND email = ${params.email.toLowerCase()}
       AND "acceptedAt" IS NULL AND "revokedAt" IS NULL`;
}
