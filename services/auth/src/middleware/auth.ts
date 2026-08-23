/**
 * Auth's verification middleware — now just configuration.
 *
 * The implementation lives in @eventhub/auth-middleware and is shared with every
 * other service. Auth is the only consumer that already holds an imported key,
 * because it must sign as well as verify, so it passes the one it has rather
 * than reading a second copy from its environment.
 */

import type { RequestHandler } from 'express';
import { createRequireAuth, requireRole } from '@eventhub/auth-middleware';
import { jwtAudience, jwtIssuer, publicKey } from '../jwt.js';

export const requireAuth: RequestHandler = createRequireAuth({
  publicKey,
  issuer: jwtIssuer,
  audience: jwtAudience,
});

export { requireRole };
