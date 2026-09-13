import type { RequestHandler } from 'express';
import { createRequireAuth, publicKeyFromEnv } from '@eventhub/auth-middleware';
import { env } from './env.js';

/**
 * Search's authentication: the shared middleware, unchanged.
 *
 * THIS IS WHERE THE TENANT COMES FROM, and the only place. The alias a search
 * runs through is built from req.auth.tenantId, a claim in a token signed with
 * Auth's private key and verified here with only its public half. A header, a
 * query parameter or a body field naming a tenant is never read on the search
 * path, so a client has nothing to lie with.
 *
 * `algorithms: ['RS256']` is pinned inside the middleware. Without it a token
 * signed HS256 using the PUBLIC key as the shared secret would verify, and the
 * public key is public.
 */
export const requireAuth: RequestHandler = createRequireAuth({
  publicKey: await publicKeyFromEnv(),
  issuer: env.JWT_ISSUER,
  audience: env.JWT_AUDIENCE,
});
