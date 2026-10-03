import { prisma } from './db.js';

/**
 * Which tenant a sign-in is for, from the gateway's X-Tenant-Slug.
 *
 * A tenant is its subdomain: acme.eventhub.com is the tenant whose slug is `acme`.
 * Kong clears whatever tenant headers a client sends, takes the slug from the Host and
 * passes it on (infra/kong/kong.yml); turning it into a tenant id is done here, decided
 * 2026-09-28, so that a new tenant is one row in "Tenant" rather than a gateway reload.
 *
 * The header is trusted for the same reason X-Tenant-ID was before it: only the gateway
 * can set it. That holds only while the gateway is the one way in. A client that can
 * reach this service directly can name any slug, exactly as it could name any id.
 *
 * "Tenant" carries no row policy on purpose -- its RLS migration says so: this lookup is
 * what establishes the tenant context, so it runs before one exists.
 *
 * Only an existing tenant is cached. A miss is asked again every time, so a tenant
 * created a moment ago signs in at once, and a stream of invented subdomains costs one
 * indexed lookup each rather than filling memory. A hit is kept for a minute, so a
 * renamed or deleted tenant can still resolve to its old id for that long; signing in
 * there still takes that tenant's own user and password.
 */

/** One DNS label: what Kong's pattern admits. Anything else is not a slug, and not asked about. */
const SLUG = /^[a-z0-9][a-z0-9-]{0,62}$/;

const TTL_MS = 60_000;
const MAX_ENTRIES = 10_000;
const cache = new Map<string, { tenantId: string; expiresAt: number }>();

export async function resolveTenantSlug(slug: string): Promise<string | null> {
  if (!SLUG.test(slug)) return null;

  const now = Date.now();
  const hit = cache.get(slug);
  if (hit && hit.expiresAt > now) return hit.tenantId;
  cache.delete(slug);

  const tenant = await prisma.tenant.findUnique({ where: { slug }, select: { id: true } });
  if (!tenant) return null;

  // Oldest out first: a Map iterates in insertion order.
  if (cache.size >= MAX_ENTRIES) cache.delete(cache.keys().next().value!);
  cache.set(slug, { tenantId: tenant.id, expiresAt: now + TTL_MS });
  return tenant.id;
}

/**
 * The other way round, for a refresh: the refresh token knows its tenant's id, and the
 * new access token carries the slug. Read every time, so a renamed tenant's next token
 * carries the new name. Null when the tenant no longer exists.
 */
export async function slugOfTenant(tenantId: string): Promise<string | null> {
  const tenant = await prisma.tenant.findUnique({ where: { id: tenantId }, select: { slug: true } });
  return tenant?.slug ?? null;
}
