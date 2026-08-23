/**
 * Event's verification middleware — now just configuration.
 *
 * The implementation lives in @eventhub/auth-middleware. This service holds only
 * Auth's PUBLIC key: it can prove a token is genuine and unaltered, and cannot
 * mint one. A compromise here grants no ability to forge identities, which is
 * the entire reason for RS256 over a shared HMAC secret.
 */

import type { RequestHandler } from 'express';
import {
  createRequireAuth,
  publicKeyFromBase64,
  requireRole,
} from '@eventhub/auth-middleware';

const publicKeyB64 = process.env['AUTH_PUBLIC_KEY_B64'];
const issuer = process.env['JWT_ISSUER'];
const audience = process.env['JWT_AUDIENCE'];

if (!publicKeyB64 || !issuer || !audience) {
  throw new Error('AUTH_PUBLIC_KEY_B64, JWT_ISSUER and JWT_AUDIENCE are required');
}

export const requireAuth: RequestHandler = createRequireAuth({
  publicKey: await publicKeyFromBase64(publicKeyB64),
  issuer,
  audience,
});

export { requireRole };
