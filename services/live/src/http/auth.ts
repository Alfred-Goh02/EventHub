import type { RequestHandler } from 'express';
import { createRequireAuth, publicKeyFromEnv, requireRole } from '@eventhub/auth-middleware';

/**
 * The HTTP half of Live's authentication — the snapshot endpoint only.
 *
 * This is the shared middleware every other service uses, unchanged. The socket
 * path in ../jwt.ts is separate for two reasons that do not apply here: there is
 * no request object at a WebSocket upgrade, and the socket needs `exp` to
 * schedule its own expiry close.
 *
 * Live holds only Auth's PUBLIC key, so it can prove a token is genuine and
 * cannot mint one.
 */
const issuer = process.env['JWT_ISSUER'];
const audience = process.env['JWT_AUDIENCE'];

if (!issuer || !audience) {
  throw new Error('JWT_ISSUER and JWT_AUDIENCE are required');
}

export const requireAuth: RequestHandler = createRequireAuth({
  publicKey: await publicKeyFromEnv(),
  issuer,
  audience,
});

export { requireRole };
