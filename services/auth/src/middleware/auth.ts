import type { Request, Response, NextFunction } from 'express';
import type { Role } from '@eventhub/types';
import { verifyAccessToken } from '../jwt.js';

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

  const token = header.slice('Bearer '.length);

  try {
    const payload = await verifyAccessToken(token);

    // Validate the claims rather than assuming them. The signature proves the
    // token is ours and unaltered; it says nothing about the payload having the
    // shape this code expects. An older token minted before `role` existed
    // would otherwise land here as undefined and flow onward as a real value.
    if (!payload.sub || typeof payload.tenantId !== 'string' || !isRole(payload.role)) {
      return res.status(401).json({ error: 'malformed token' });
    }

    req.auth = { userId: payload.sub, tenantId: payload.tenantId, role: payload.role };
    next();
  } catch {
    return res.status(401).json({ error: 'invalid or expired token' });
  }
}

/**
 * Authorization, deliberately separate from authentication.
 *
 * requireAuth answers "who is this?". requireRole answers "may they do this?".
 * Keeping them apart matters: tenant isolation is the database's job via RLS,
 * and permission is the application's. Conflating them produces the failure
 * where removing a role check also quietly removes tenant isolation.
 */
export function requireRole(...allowed: Role[]) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.auth) {
      return res.status(401).json({ error: 'not authenticated' });
    }
    if (!allowed.includes(req.auth.role)) {
      return res.status(403).json({ error: 'insufficient role' });
    }
    next();
  };
}
