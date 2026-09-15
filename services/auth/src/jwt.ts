import { readFileSync } from 'node:fs';
import { importPKCS8, importSPKI, SignJWT } from 'jose';
import type { Role } from '@eventhub/types';

const ALG = 'RS256';
const ACCESS_TTL = '15m';

// Who mints tokens, and who they are for. Both are VERIFIED on the way back in,
// not merely stamped on the way out — a claim nobody checks is decoration.
//   iss  lets a verifier reject a token minted by some other system.
//   aud  stops a token issued for this API being replayed at a different one
//        that happens to trust the same key.
const ISSUER = process.env['JWT_ISSUER'];
const AUDIENCE = process.env['JWT_AUDIENCE'];

if (!ISSUER || !AUDIENCE) {
  throw new Error('JWT_ISSUER and JWT_AUDIENCE are required');
}

// Exported AFTER the guard, so control-flow narrowing gives these a plain
// `string` type. Declaring them earlier would need a `!` assertion on values
// that have not been checked yet — which would still work at runtime, but the
// assertion would be a claim the code had not yet earned.
export const jwtIssuer: string = ISSUER;
export const jwtAudience: string = AUDIENCE;

// Resolved relative to THIS FILE, not the working directory. `keys/private.pem`
// only worked because dev happens to run from services/auth; it would throw
// ENOENT from the repo root, or in a container with a different WORKDIR.
// ../keys resolves correctly from both src/ and the compiled dist/.
const keyPath = (name: string) => new URL(`../keys/${name}`, import.meta.url);

const privateKey = await importPKCS8(readFileSync(keyPath('private.pem'), 'utf8'), ALG);
export const publicKey = await importSPKI(readFileSync(keyPath('public.pem'), 'utf8'), ALG);

export async function signAccessToken(
  userId: string,
  tenantId: string,
  role: Role,
): Promise<string> {
  return new SignJWT({ tenantId, role })
    .setProtectedHeader({ alg: ALG })
    .setSubject(userId)
    .setIssuer(ISSUER!)
    .setAudience(AUDIENCE!)
    .setIssuedAt()
    .setExpirationTime(ACCESS_TTL)
    .sign(privateKey);
}

/** Who a sealed email request is for, and what kind of token it is. */
export const EMAIL_REQUEST_AUDIENCE = 'eventhub-notification';
export const EMAIL_REQUEST_TYPE = 'auth-email+jwt';

/**
 * Signs the claims sealed inside an email request to Notification.
 *
 * With the access-token key, told apart by audience and type. Notification is to
 * accept only aud `eventhub-notification` with typ `auth-email+jwt`. Every API
 * verifier requires aud JWT_AUDIENCE and a role claim, which this token has
 * neither of. So neither kind of token is accepted as the other.
 *
 * Here rather than in email-request.ts so that the private key never leaves this
 * module.
 */
export async function signEmailRequest(params: {
  subject: string;
  jti: string;
  expiresAt: Date;
  claims: Record<string, string>;
}): Promise<string> {
  return new SignJWT(params.claims)
    .setProtectedHeader({ alg: ALG, typ: EMAIL_REQUEST_TYPE })
    .setSubject(params.subject)
    .setJti(params.jti)
    .setIssuer(ISSUER!)
    .setAudience(EMAIL_REQUEST_AUDIENCE)
    .setIssuedAt()
    .setExpirationTime(params.expiresAt)
    .sign(privateKey);
}

/**
 * There is no verifyAccessToken here, and that is deliberate.
 *
 * This file used to export one: a second implementation of token verification
 * that nothing in the repository called. Found by listing every exported
 * function and asking which ones nothing references — it was the only one.
 *
 * An unused copy of a security check is worse than no copy. It reads as the
 * authoritative one, so somebody following the code arrives at a path that does
 * not run, and somebody needing verification wires up the nearest thing to hand.
 * That nearest thing was strictly WEAKER than the live path: it returned the
 * raw jose payload, so a token minted before `role` existed would arrive with
 * role undefined and flow onward as though it were a real value. The live
 * middleware rejects exactly that.
 *
 * The repository has been here before. `claimInvite()` carried a perfect
 * explanation of why read-then-write is wrong while nothing called it, and the
 * pattern it warned about was inlined elsewhere without the guard.
 *
 * Verification lives in @eventhub/auth-middleware, once, and Auth consumes it
 * through middleware/auth.ts like every other service — passing the `publicKey`
 * exported above. Auth is the only one that also holds the private half.
 */
