import { randomUUID } from 'node:crypto';
import { Router, type RequestHandler } from 'express';
import { z } from 'zod';
import { getAuth } from '@eventhub/auth-middleware';
import type { Logger } from '@eventhub/logger';
import { ah } from '../async.js';
import { withTenant } from '../db.js';
import { forgetMembership, readMembership, requireMembership, type SpaceRole } from '../middleware/space.js';

/**
 * Spaces and who is in them.
 *
 * Every write runs inside withTenant, so row security scopes it to the caller's tenant
 * whatever the body says, and the tenant comes from the verified token rather than
 * from anything the client sent.
 */

const createSchema = z.object({
  slug: z
    .string()
    .min(1)
    .max(64)
    // A slug is part of a URL and a unique key, so it is constrained here rather than
    // sanitised later: scripts/verify-slug-non-latin.mjs is the Event service learning
    // that the hard way.
    .regex(/^[a-z0-9][a-z0-9-]*$/, 'a slug is lower-case letters, digits and hyphens'),
  name: z.string().min(1).max(200),
  visibility: z.enum(['PUBLIC', 'PRIVATE']).default('PUBLIC'),
  eventId: z.string().min(1).optional(),
});

const memberSchema = z
  .object({
    role: z.enum(['MEMBER', 'MODERATOR', 'OWNER']).optional(),
    status: z.enum(['ACTIVE', 'BANNED']).optional(),
  })
  .refine((body) => body.role !== undefined || body.status !== undefined, {
    message: 'give a role, a status, or both',
  });

export function spaceRoutes(logger: Logger, limit: { middleware: (action: string, perMinute: number) => RequestHandler }, joinsPerMinute: number): Router {
  const router = Router();

  /**
   * Creating a space also makes its creator the OWNER, in ONE transaction.
   *
   * Two statements would leave a space nobody can moderate if the second failed — and
   * the only route that could fix it is one that requires a moderator.
   */
  router.post('/spaces', ah(async (req, res) => {
    const parsed = createSchema.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
    const { tenantId, userId } = getAuth(req);
    const spaceId = randomUUID();

    try {
      await withTenant(tenantId, async (tx) => {
        await tx.$executeRaw`
          INSERT INTO "CommunitySpace" (id, "tenantId", "eventId", slug, name, visibility, "memberCount", "updatedAt")
          VALUES (${spaceId}, ${tenantId}, ${parsed.data.eventId ?? null}, ${parsed.data.slug}, ${parsed.data.name},
                  ${parsed.data.visibility}::"Visibility", 1, now())`;
        await tx.$executeRaw`
          INSERT INTO "Membership" (id, "tenantId", "spaceId", "userId", role, status)
          VALUES (${randomUUID()}, ${tenantId}, ${spaceId}, ${userId}, 'OWNER'::"SpaceRole", 'ACTIVE'::"MemberStatus")`;
      });
    } catch (err) {
      if ((err as { code?: string }).code === 'P2010' || /duplicate key/.test(String(err))) {
        return res.status(409).json({ error: 'a space with that slug already exists' });
      }
      throw err;
    }

    await forgetMembership(spaceId, userId);
    return res.status(201).json({ id: spaceId, slug: parsed.data.slug, visibility: parsed.data.visibility });
  }));

  /**
   * A public space can be joined; a private one answers 404 rather than 403, so its
   * existence is not confirmed to someone guessing ids.
   *
   * Idempotent: joining twice is the same as joining once, because a retry after a
   * dropped response must not be an error.
   */
  router.post('/spaces/:spaceId/join', limit.middleware('join', joinsPerMinute), ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const spaceId = req.params['spaceId']!;

    const joined = await withTenant(tenantId, async (tx) => {
      const rows = await tx.$queryRaw<Array<{ visibility: string }>>`
        SELECT visibility::text AS visibility FROM "CommunitySpace" WHERE id = ${spaceId}`;
      const space = rows[0];
      if (!space || space.visibility !== 'PUBLIC') return null;

      const inserted = await tx.$executeRaw`
        INSERT INTO "Membership" (id, "tenantId", "spaceId", "userId", role, status)
        VALUES (${randomUUID()}, ${tenantId}, ${spaceId}, ${userId}, 'MEMBER'::"SpaceRole", 'ACTIVE'::"MemberStatus")
        ON CONFLICT ("tenantId", "spaceId", "userId") DO NOTHING`;
      // The counter moves in the same statement's transaction, and only when a row was
      // actually added, so a repeated join cannot inflate it.
      if (inserted === 1) {
        await tx.$executeRaw`UPDATE "CommunitySpace" SET "memberCount" = "memberCount" + 1 WHERE id = ${spaceId}`;
      }
      return { added: inserted === 1 };
    });

    if (!joined) return res.status(404).json({ error: 'not found' });
    await forgetMembership(spaceId, userId);
    return res.status(200).json({ spaceId, role: 'MEMBER', joined: joined.added });
  }));

  /** A public space is readable by anyone in the tenant; a private one, by its members. */
  router.get('/spaces/:spaceId', ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const spaceId = req.params['spaceId']!;

    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<{ id: string; slug: string; name: string; visibility: string; memberCount: number }>>`
        SELECT id, slug, name, visibility::text AS visibility, "memberCount"
          FROM "CommunitySpace" WHERE id = ${spaceId}`,
    );
    const space = rows[0];
    if (!space) return res.status(404).json({ error: 'not found' });

    if (space.visibility === 'PRIVATE') {
      const membership = await readMembership(tenantId, spaceId, userId, logger);
      // 404 again, not 403: a private space does not admit to existing.
      if (!membership) return res.status(404).json({ error: 'not found' });
      if (membership.status === 'BANNED') return res.status(403).json({ error: 'banned from this space' });
    }
    return res.json(space);
  }));

  /**
   * A moderator may ban or restore a member; only an OWNER may change roles.
   *
   * The cache is dropped for the user whose membership changed, so the next request
   * sees it — not up to MEMBERSHIP_CACHE_TTL_S later.
   */
  router.patch('/spaces/:spaceId/members/:userId', requireMembership('MODERATOR', logger), ah(async (req, res) => {
    const parsed = memberSchema.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
    const { tenantId } = getAuth(req);
    const spaceId = req.params['spaceId']!;
    const target = req.params['userId']!;
    const actor = req.membership!;

    if (parsed.data.role !== undefined && actor.role !== 'OWNER') {
      return res.status(403).json({ error: 'only an owner changes roles' });
    }

    const changed = await withTenant(tenantId, async (tx) => {
      const rows = await tx.$queryRaw<Array<{ role: SpaceRole }>>`
        SELECT role::text AS role FROM "Membership"
         WHERE "tenantId" = ${tenantId} AND "spaceId" = ${spaceId} AND "userId" = ${target}`;
      const current = rows[0];
      if (!current) return null;
      // An owner is not demoted or banned by a moderator, and not by another owner
      // either: the only way out of ownership is leaving, which checks for the last one.
      if (current.role === 'OWNER') return { protected: true };

      if (parsed.data.role !== undefined) {
        await tx.$executeRaw`
          UPDATE "Membership" SET role = ${parsed.data.role}::"SpaceRole"
           WHERE "tenantId" = ${tenantId} AND "spaceId" = ${spaceId} AND "userId" = ${target}`;
      }
      if (parsed.data.status !== undefined) {
        await tx.$executeRaw`
          UPDATE "Membership" SET status = ${parsed.data.status}::"MemberStatus"
           WHERE "tenantId" = ${tenantId} AND "spaceId" = ${spaceId} AND "userId" = ${target}`;
      }
      return { protected: false };
    });

    if (!changed) return res.status(404).json({ error: 'not a member of this space' });
    if (changed.protected) return res.status(409).json({ error: "an owner's membership cannot be changed here" });

    await forgetMembership(spaceId, target);
    return res.json({ spaceId, userId: target, ...parsed.data });
  }));

  /** Leaving, except for the last owner: a space nobody can moderate is worse than a space nobody left. */
  router.delete('/spaces/:spaceId/members/me', requireMembership('MEMBER', logger), ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const spaceId = req.params['spaceId']!;

    const left = await withTenant(tenantId, async (tx) => {
      if (req.membership!.role === 'OWNER') {
        const owners = await tx.$queryRaw<Array<{ n: bigint }>>`
          SELECT count(*) AS n FROM "Membership"
           WHERE "tenantId" = ${tenantId} AND "spaceId" = ${spaceId} AND role = 'OWNER'::"SpaceRole"`;
        if (Number(owners[0]?.n ?? 0) <= 1) return { lastOwner: true };
      }
      const removed = await tx.$executeRaw`
        DELETE FROM "Membership"
         WHERE "tenantId" = ${tenantId} AND "spaceId" = ${spaceId} AND "userId" = ${userId}`;
      if (removed === 1) {
        await tx.$executeRaw`
          UPDATE "CommunitySpace" SET "memberCount" = greatest("memberCount" - 1, 0) WHERE id = ${spaceId}`;
      }
      return { lastOwner: false, removed };
    });

    if (left.lastOwner) return res.status(409).json({ error: 'the last owner cannot leave' });
    await forgetMembership(spaceId, userId);
    return res.status(204).end();
  }));

  return router;
}
