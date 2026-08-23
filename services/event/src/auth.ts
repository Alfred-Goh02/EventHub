import type { Request, Response, NextFunction } from 'express';
import type { Role } from '@eventhub/types';
import { importSPKI, jwtVerify } from 'jose';

/**
 * Token verification for the Event service.
 *
 * DELIBERATELY a local copy for now. This will move into a shared
 * @eventhub/auth-middleware package once it has two real consumers and we can
 * see what actually differs between them — extracting on the first guess
 * usually produces an abstraction shaped like one caller.
 *
 * The important asymmetry with Auth: this service holds only the PUBLIC key. It
 * can prove a token is genuine and unaltered; it cannot mint one. If this
 * service is compromised, the attacker gains no ability to forge identities.
 */

const ALG = 'RS256';

const publicKeyB64 = process.env['AUTH_PUBLIC_KEY_B64'];
const ISSUER = process.env['JWT_ISSUER'];
const AUDIENCE = process.env['JWT_AUDIENCE'];

if (!publicKeyB64 || !ISSUER || !AUDIENCE) {
  throw new Error('AUTH_PUBLIC_KEY_B64, JWT_ISSUER and JWT_AUDIENCE are required');
}

// Base64 in, PEM out. The env var carries base64 because a PEM has newlines,
// and newlines survive neither .env files nor Helm values reliably.
const publicKey = await importSPKI(
  Buffer.from(publicKeyB64, 'base64').toString('utf8'),
  ALG,
);

declare global {
  namespace Express {
    interface Request {
      auth?: { userId: string; tenantId: string; role: Role };
    }
  }
}

const ROLES: readonly Role[] = ['ADMIN', 'ORGANIZER', 'ATTENDEE'];
const isRole = (v: unknown): v is Role =>
  typeof v === 'string' && (ROLES as readonly string[]).includes(v);

export async function requireAuth(req: Request, res: Response, next: NextFunction) {
  const header = req.header('Authorization');

  if (!header?.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'missing bearer token' });
  }

  try {
    const { payload } = await jwtVerify(header.slice('Bearer '.length), publicKey, {
      // All three matter. `algorithms` blocks the alg-confusion attack where a
      // token is signed HS256 using this very public key as the shared secret.
      // `issuer` and `audience` are required here, not merely read, so a token
      // minted by another system or for another audience is rejected outright.
      algorithms: [ALG],
      issuer: ISSUER,
      audience: AUDIENCE,
    });

    if (!payload.sub || typeof payload.tenantId !== 'string' || !isRole(payload.role)) {
      return res.status(401).json({ error: 'malformed token' });
    }

    req.auth = { userId: payload.sub, tenantId: payload.tenantId, role: payload.role };
    next();
  } catch {
    return res.status(401).json({ error: 'invalid or expired token' });
  }
}

export function requireRole(...allowed: Role[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.auth) return res.status(401).json({ error: 'not authenticated' });
    if (!allowed.includes(req.auth.role)) {
      return res.status(403).json({ error: 'insufficient role' });
    }
    next();
  };
}
