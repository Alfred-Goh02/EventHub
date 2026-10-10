import type { Logger } from '@eventhub/logger';

/**
 * How a bearer secret reaches a developer without reading mail.
 *
 * Written before the Notification service existed, when an invite link or a reset
 * link had to surface somewhere or the flow could not be exercised at all. Both are
 * emailed now, sealed to Notification (see email-request.ts); this stays as a
 * second, local-only path, and verifiers use it to learn the real secret to search
 * for. It is NOT a delivery path: an invitation that reached only this sink was the
 * bug that left a deployed system unable to admit anyone new.
 *
 * Every option here is bad; this is the least bad one, and the reasoning is
 * worth keeping because the obvious alternatives are worse in ways that are not
 * obvious.
 *
 * REJECTED — return the token in the response body under NODE_ENV !== production.
 *   This hands the secret directly to the caller. /forgot-password is
 *   UNAUTHENTICATED, so that is strictly worse than logging it: an attacker
 *   would not even need log access, just the ability to POST an email address.
 *
 * REJECTED — log it the way /invites used to, unconditionally at warn.
 *   /invites is behind requireAuth + requireRole('ADMIN') and its token creates
 *   a NEW account at a role the admin already chose to grant, so a log reader
 *   gains nothing that admin could not already do. A reset token seizes an
 *   EXISTING account — potentially the seeded ADMIN — and any anonymous caller
 *   can mint a fresh one on demand, indefinitely. Same mechanism, entirely
 *   different blast radius.
 *
 * CHOSEN — an explicit opt-in sink that fails closed on three axes:
 *   1. Off unless AUTH_DEV_TOKEN_SINK is set. Absent config emits nothing.
 *   2. Emitted at DEBUG, below the `info` that every .env and ci.yml sets, so
 *      even switched on it stays out of a default-configured log.
 *   3. The service REFUSES TO START if it is on under NODE_ENV=production —
 *      the same refuse-to-boot shape the relay and ticketing already use for
 *      credentials they must not run without.
 *
 * The field name is deliberately `devOnlyResetUrl` rather than `token`. The
 * logger redacts `token` and friends, so an accidental log of a secret is
 * censored, while this one intentional path is spelled with a name nobody
 * reaches for by accident. Every unplanned route fails closed; only the planned
 * one is legible.
 */

const flag = process.env['AUTH_DEV_TOKEN_SINK'];
const enabled = flag === '1' || flag === 'true';

if (enabled && process.env['NODE_ENV'] === 'production') {
  throw new Error(
    'refusing to start: AUTH_DEV_TOKEN_SINK is enabled under NODE_ENV=production. ' +
      'It writes live invite and password-reset credentials to the log, which is the ' +
      'exact leak the hash-only storage design exists to prevent.',
  );
}

export const devTokenSinkEnabled = enabled;

/**
 * Emits a link a developer can click. Does nothing at all unless the sink is on.
 *
 * `kind` is what the credential does, so a reader can tell an invite from a
 * reset without decoding the URL.
 */
export function emitDevToken(
  logger: Logger,
  kind: 'invite' | 'password-reset',
  fields: { email: string; tenantId: string; url: string },
): void {
  if (!enabled) return;

  logger.debug(
    {
      kind,
      email: fields.email,
      tenantId: fields.tenantId,
      devOnlyResetUrl: fields.url,
    },
    'DEV TOKEN SINK — this would be emailed; never enable in production',
  );
}
