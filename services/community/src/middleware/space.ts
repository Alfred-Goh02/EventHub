import type { NextFunction, Request, RequestHandler, Response } from 'express';
import { getAuth } from '@eventhub/auth-middleware';
import type { Logger } from '@eventhub/logger';
import { ah } from '@eventhub/http';
import { withTenant } from '../db.js';
import { env } from '../env.js';
import { redis } from '../redis.js';

/**
 * Who may act in a space.
 *
 * SPACE ROLES ARE NOT TOKEN CLAIMS. An access token lives fifteen minutes; demoting a
 * moderator has to take effect now. So the role is read per request, cached briefly,
 * and the cache is dropped by whatever changes it.
 */
const RANK = { MEMBER: 0, MODERATOR: 1, OWNER: 2 } as const;
export type SpaceRole = keyof typeof RANK;

export interface Membership {
  role: SpaceRole;
  status: 'ACTIVE' | 'BANNED';
}

/**
 * The same `declare global` shape @eventhub/auth-middleware uses for req.auth, and for
 * the same reason: augmenting 'express-serve-static-core' directly only resolves when
 * that package is a direct dependency, which it is not here.
 */
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      membership?: Membership;
    }
  }
}

/**
 * THE TENANT IS PART OF THE KEY.
 *
 * It was `member:${spaceId}:${userId}`, and Redis is not tenant-scoped the way the
 * database is: the Postgres read under row security only ever sees the caller's
 * tenant, but the cache in front of it answered for whoever had asked first. Measured
 * with the moderation verifier: a token for another tenant carrying the same user id
 * read space S1's report queue — 200 — because M1's MODERATOR entry was cached under a
 * key the other tenant's request produced too. Row security then scoped the query and
 * nothing leaked, but the ROLE CHECK passed on another tenant's answer.
 *
 * The reverse is worse. The negative is cached as well (see readMembership), so a
 * lookup from another tenant that found no row there wrote `null` under the shared key,
 * and the real member was refused their own space until it expired — a denial of
 * service for anyone who knows a space id and a user id.
 *
 * Same lesson as the dedupe key in CLAUDE.md: identity includes the tenant, because an
 * id is only as unique as the service that minted it, and Auth mints user ids per
 * tenant.
 */
const cacheKey = (tenantId: string, spaceId: string, userId: string) => `member:${tenantId}:${spaceId}:${userId}`;

/** Called by anything that changes a membership, so the next request sees it. */
export async function forgetMembership(tenantId: string, spaceId: string, userId: string): Promise<void> {
  await redis.del(cacheKey(tenantId, spaceId, userId)).catch(() => {
    // A cache that cannot be cleared is a stale answer for MEMBERSHIP_CACHE_TTL_S, not
    // a failed write: the change is already committed in Postgres.
  });
}

/**
 * Reads the caller's membership of req.params.spaceId and refuses if it is not enough.
 *
 * 404 AND NOT 403 for a non-member: a 403 says "this space exists", which maps a
 * tenant's structure for anyone willing to guess ids. The distinction only matters for
 * a stranger, which is exactly who is asking.
 */
export async function readMembership(
  tenantId: string,
  spaceId: string,
  userId: string,
  logger: Logger,
): Promise<Membership | null> {
  const key = cacheKey(tenantId, spaceId, userId);
  try {
    const hit = await redis.get(key);
    // 'null' is a real cached answer: see below.
    if (hit !== null) return JSON.parse(hit) as Membership | null;
  } catch (err) {
    logger.warn({ err }, 'the membership cache could not be read; asking Postgres');
  }

  const rows = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<Membership>>`
      SELECT role::text AS role, status::text AS status
        FROM "Membership"
       WHERE "tenantId" = ${tenantId} AND "spaceId" = ${spaceId} AND "userId" = ${userId}
    `,
  );
  const membership = rows[0] ?? null;
  // THE NEGATIVE IS CACHED TOO. Without it, anyone with a token can make one database
  // query per request by naming space ids they are not in, which is the cheapest
  // denial of service in the service.
  await redis
    .set(key, JSON.stringify(membership), 'EX', env.MEMBERSHIP_CACHE_TTL_S)
    .catch((err: unknown) => logger.warn({ err }, 'the membership cache could not be written'));
  return membership;
}

export function requireMembership(minRole: SpaceRole, logger: Logger): RequestHandler {
  // Wrapped, like every async handler here: Express 4 does not await middleware, so a
  // database error inside this would be an unhandled rejection rather than a 500.
  return ah(async (req: Request, res: Response, next: NextFunction) => {
    const { tenantId, userId } = getAuth(req);
    const spaceId = req.params['spaceId'];
    if (!spaceId) return res.status(400).json({ error: 'a space id is required' });

    const membership = await readMembership(tenantId, spaceId, userId, logger);

    if (!membership) return res.status(404).json({ error: 'not found' });
    if (membership.status === 'BANNED') return res.status(403).json({ error: 'banned from this space' });
    if (RANK[membership.role] < RANK[minRole]) return res.status(403).json({ error: 'insufficient role in this space' });

    req.membership = membership;
    next();
  });
}
