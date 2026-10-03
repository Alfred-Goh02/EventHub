import { jwtVerify } from 'jose';
import { publicKeyFromEnv } from '@eventhub/auth-middleware';
import { isRole, type Role } from '@eventhub/types';

const ALG = 'RS256';

/**
 * Auth's PUBLIC key, resolved the same way every other consuming service
 * resolves it — AUTH_PUBLIC_KEY_B64 or AUTH_PUBLIC_KEY_FILE, exactly one.
 *
 * Live holds no private key and cannot mint a token. That is the property RS256
 * was chosen for: a compromise here grants no ability to forge an identity.
 */
const publicKey = await publicKeyFromEnv();

/**
 * Enforced here as the middleware enforces them. They were not: this check took any
 * token Auth's key had signed with the right shape, and Auth signs more than access
 * tokens with that key. Only the missing role claim kept a sealed email request from
 * passing for one; the audience is what is meant to.
 */
const issuer = process.env['JWT_ISSUER'];
const audience = process.env['JWT_AUDIENCE'];
if (!issuer || !audience) {
  throw new Error('JWT_ISSUER and JWT_AUDIENCE are required');
}

export interface SocketClaims {
  userId: string;
  tenantId: string;
  /** The subdomain the token's tenant is served on; null on a token minted before the claim. */
  tenantSlug: string | null;
  role: Role;
  /** Seconds since epoch. The socket schedules its own close from this. */
  exp: number;
}

/**
 * Verifies an access token presented at the WebSocket handshake.
 *
 * WHY THIS EXISTS RATHER THAN REUSING createRequireAuth. That helper is Express
 * middleware — it takes (req, res, next) and writes a 401 body. There is no
 * request here, only an upgrade, and the caller needs something the middleware
 * deliberately does not expose: `exp`. A socket outlives its token, so it has to
 * schedule its own expiry, and for that it needs the number.
 *
 * It is NOT a second, weaker copy of verification. The differences from the
 * middleware are the two that matter for a socket, and the shape check below is
 * the same one — #82 removed a dead helper that skipped exactly this, and the
 * lesson was that an unused copy is the dangerous kind, not a used one with a
 * reason.
 *
 * `algorithms` is an allowlist, not a hint. Omit it and jose honours the token's
 * own `alg` header, so an attacker can present an HS256 token signed with the
 * PUBLIC key as the shared secret — and the public key is, by definition, public.
 */
export async function verifyAccessToken(token: string): Promise<SocketClaims> {
  const { payload } = await jwtVerify(token, publicKey, { algorithms: [ALG], issuer, audience });

  // The signature proves the token is ours and unaltered. It says nothing about
  // the payload having the shape this code expects: a token minted before
  // `role` existed would arrive with role undefined and flow onward as a value.
  if (
    !payload.sub ||
    typeof payload.tenantId !== 'string' ||
    !isRole(payload.role) ||
    typeof payload.exp !== 'number'
  ) {
    throw new Error('malformed token');
  }

  return {
    userId: payload.sub,
    tenantId: payload.tenantId,
    tenantSlug: typeof payload['tenantSlug'] === 'string' ? payload['tenantSlug'] : null,
    role: payload.role,
    exp: payload.exp,
  };
}
