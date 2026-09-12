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
  publicKeyFromEnv,
  requireRole,
} from '@eventhub/auth-middleware';

const issuer = process.env['JWT_ISSUER'];
const audience = process.env['JWT_AUDIENCE'];

if (!issuer || !audience) {
  throw new Error('JWT_ISSUER and JWT_AUDIENCE are required');
}

/**
 * publicKeyFromEnv rather than reading AUTH_PUBLIC_KEY_B64 here, because there
 * are now two ways the key arrives and only one of them is an environment value.
 * Under docker compose the keypair does not exist until a setup step generates
 * it into a shared volume, so this service is given a PATH instead -- see that
 * function for why both exist and why setting both is refused.
 */
export const requireAuth: RequestHandler = createRequireAuth({
  publicKey: await publicKeyFromEnv(),
  issuer,
  audience,
});

export { requireRole };
