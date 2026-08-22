import type { Request, Response, NextFunction } from 'express';
import { verifyAccessToken } from '../jwt.js';

declare global {
  namespace Express {
    interface Request {
      auth?: { userId: string; tenantId: string };
    }
  }
}

export async function requireAuth(req: Request, res: Response, next: NextFunction) {
  const header = req.header('Authorization');

  if (!header?.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'missing bearer token' });
  }

  const token = header.slice('Bearer '.length);

  try {
    const payload = await verifyAccessToken(token);

    if (!payload.sub || typeof payload.tenantId !== 'string') {
      return res.status(401).json({ error: 'malformed token' });
    }

    req.auth = { userId: payload.sub, tenantId: payload.tenantId };
    next();
  } catch {
    return res.status(401).json({ error: 'invalid or expired token' });
  }
}