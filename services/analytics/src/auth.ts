import type { RequestHandler } from 'express';
import { createRequireAuth, publicKeyFromEnv, requireRole } from '@eventhub/auth-middleware';
import { env } from './env.js';

/**
 * Analytics' authentication: the shared middleware, unchanged, as in Search.
 *
 * THIS IS WHERE THE TENANT COMES FROM, and the only place. Every query binds
 * req.auth.tenantId, a claim in a token signed with Auth's private key and verified
 * here with only its public half. A header, a query parameter or a body field
 * naming a tenant is never read, so a client has nothing to lie with.
 *
 * The analytics store has no equivalent of Postgres RLS behind this. ClickHouse
 * row policies exist, but their setting would be supplied by this same process,
 * so they could not survive this process being wrong. The boundary is here and in
 * every query's bound tenant parameter, and scripts/verify-analytics-api.mjs asks
 * every route for another tenant's event.
 *
 * Awaited at module load: a service that cannot verify a token must not start.
 */
export const requireAuth: RequestHandler = createRequireAuth({
  publicKey: await publicKeyFromEnv(),
  issuer: env.JWT_ISSUER,
  audience: env.JWT_AUDIENCE,
});

/**
 * Organisers and admins, never attendees. These responses are a tenant's revenue,
 * sell-through and audience — the commercial picture a competitor would want — and
 * an attendee's token is one sign-up away for anybody.
 *
 * Tenant-wide, not per organiser: the analytics rows carry no organiser, so an
 * organiser sees every event in their tenant. Named as a gap in the README rather
 * than implied away.
 */
export const requireAnalyst: RequestHandler = requireRole('ORGANIZER', 'ADMIN');
