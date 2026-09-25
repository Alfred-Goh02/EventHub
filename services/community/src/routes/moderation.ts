import { randomUUID } from 'node:crypto';
import { Router, type Request, type RequestHandler, type Response } from 'express';
import { z } from 'zod';
import { getAuth } from '@eventhub/auth-middleware';
import { ah } from '@eventhub/http';
import type { Logger } from '@eventhub/logger';
import { withTenant } from '../db.js';
import { Prisma } from '../generated/prisma/client.js';
import { atLeast, reach, readMembership, requireMembership } from '../middleware/space.js';
import { rankScore } from '../rank.js';

/**
 * Reports, and what a moderator does about them.
 *
 * THE SPACE COMES FROM THE TARGET, NEVER FROM THE REQUEST. Every action here names a
 * post or a comment, and the space whose moderators may act on it is looked up from
 * that row. A route shaped /spaces/:spaceId/moderation with the target in the body
 * would check the caller's role in the space THEY named, and a moderator of their own
 * small space could then hide anything in the tenant by naming their space and
 * someone else's post.
 *
 * EVERY CHANGE AND ITS AUDIT ROW ARE ONE TRANSACTION. ModerationAction is append-only
 * at the privilege level — the app role holds SELECT and INSERT on it and nothing
 * else — and it is only worth that if no status change can commit without it.
 *
 * STATUS CHANGES ARE COMPARE-AND-SET. HIDE moves VISIBLE to HIDDEN and nothing else;
 * UNHIDE moves HIDDEN to VISIBLE and nothing else. So a second HIDE is a no-op that
 * writes no second audit row, and UNHIDE cannot resurrect what its author DELETED.
 */

type TargetType = 'POST' | 'COMMENT';

const reportBody = z.object({
  reason: z.enum(['SPAM', 'ABUSE', 'OFF_TOPIC', 'OTHER']),
  note: z.string().max(500).optional(),
});
const actionBody = z.object({
  action: z.enum(['HIDE', 'UNHIDE']),
  reason: z.string().max(500).optional(),
  /** The report this action answers, when there is one. It must be about THIS target. */
  reportId: z.string().min(1).max(64).optional(),
});
const dismissBody = z.object({ status: z.literal('DISMISSED') });
const queueQuery = z.object({ status: z.enum(['OPEN', 'ACTIONED', 'DISMISSED']).default('OPEN') });

/** The most reports one queue read returns. Overflow is reported, never silently dropped. */
const QUEUE_LIMIT = 100;

interface Target {
  spaceId: string;
  postId: string;
  status: 'VISIBLE' | 'HIDDEN' | 'DELETED';
  /**
   * True when the target cannot be seen whatever its own status says: its post is not
   * visible, or a comment above it is not.
   */
  hiddenAbove: boolean;
}

/** The target's space, post, status, and whether something above it hides it. */
async function resolveTarget(tx: Prisma.TransactionClient, type: TargetType, id: string): Promise<Target | null> {
  const rows =
    type === 'POST'
      ? await tx.$queryRaw<Target[]>`
          SELECT "spaceId", id AS "postId", status::text AS status, false AS "hiddenAbove"
            FROM "Post" WHERE id = ${id}`
      : await tx.$queryRaw<Target[]>`
          SELECT p."spaceId", c."postId", c.status::text AS status,
                 (p.status <> 'VISIBLE'::"ContentStatus" OR EXISTS (
                    SELECT 1 FROM "Comment" h
                     WHERE h."postId" = c."postId" AND h.id <> c.id
                       AND h.status <> 'VISIBLE'::"ContentStatus" AND c.path <@ h.path)) AS "hiddenAbove"
            FROM "Comment" c JOIN "Post" p ON p.id = c."postId"
           WHERE c.id = ${id}`;
  return rows[0] ?? null;
}

class ReportMismatch extends Error {}

export function moderationRoutes(
  logger: Logger,
  limit: { middleware: (action: string, perMinute: number) => RequestHandler },
  reportsPerMinute: number,
): Router {
  const router = Router();

  /**
   * Report a post or a comment. Reporting twice is not louder: the unique key is per
   * reporter per target, and a repeat answers 200 having changed nothing.
   *
   * Only what the reporter can see: an unknown target, a hidden one and one under a
   * hidden comment are all the same 404, so reporting cannot be used to learn which
   * ids exist behind a moderator's decision.
   */
  const report = (type: TargetType): RequestHandler =>
    ah(async (req: Request, res: Response) => {
      const parsed = reportBody.safeParse(req.body);
      if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
      const { tenantId, userId } = getAuth(req);
      const id = req.params[type === 'POST' ? 'postId' : 'commentId']!;

      const target = await withTenant(tenantId, (tx) => resolveTarget(tx, type, id));
      if (!target || target.status !== 'VISIBLE' || target.hiddenAbove) return res.status(404).json({ error: 'not found' });
      const allowed = await reach(tenantId, target.spaceId, userId, 'write', logger);
      if (allowed.status !== 200) return res.status(allowed.status).json({ error: allowed.error ?? 'not found' });

      const inserted = await withTenant(tenantId, (tx) =>
        tx.$executeRaw`
          INSERT INTO "Report" (id, "tenantId", "targetType", "targetId", "reporterId", reason, note, status, "createdAt")
          VALUES (${randomUUID()}, ${tenantId}, ${type}::"TargetType", ${id}, ${userId},
                  ${parsed.data.reason}::"ReportReason", ${parsed.data.note ?? null}, 'OPEN'::"ReportStatus", now())
          ON CONFLICT ("tenantId", "targetType", "targetId", "reporterId") DO NOTHING`,
      );
      return res.status(inserted === 1 ? 201 : 200).json({ targetType: type, targetId: id, reported: inserted === 1 });
    });

  router.post('/posts/:postId/reports', limit.middleware('report', reportsPerMinute), report('POST'));
  router.post('/comments/:commentId/reports', limit.middleware('report', reportsPerMinute), report('COMMENT'));

  /**
   * The moderator's queue for one space.
   *
   * Here the space DOES come from the URL, and correctly: this lists the reports of the
   * space named, and the query only returns reports whose target resolves to that space.
   * Report has no spaceId of its own, so the target is joined to find it.
   */
  router.get('/spaces/:spaceId/reports', requireMembership('MODERATOR', logger), ah(async (req, res) => {
    const parsed = queueQuery.safeParse(req.query);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid query' });
    const { tenantId } = getAuth(req);
    const spaceId = req.params['spaceId']!;

    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<Record<string, unknown>>>`
        SELECT r.id, r."targetType"::text AS "targetType", r."targetId", r."reporterId",
               r.reason::text AS reason, r.note, r.status::text AS status,
               to_char(r."createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "createdAt"
          FROM "Report" r
          LEFT JOIN "Post" p ON r."targetType" = 'POST'::"TargetType" AND p.id = r."targetId"
          LEFT JOIN "Comment" c ON r."targetType" = 'COMMENT'::"TargetType" AND c.id = r."targetId"
          LEFT JOIN "Post" cp ON cp.id = c."postId"
         WHERE r.status = ${parsed.data.status}::"ReportStatus"
           AND COALESCE(p."spaceId", cp."spaceId") = ${spaceId}
         ORDER BY r."createdAt", r.id
         LIMIT ${QUEUE_LIMIT + 1}`,
    );
    const truncated = rows.length > QUEUE_LIMIT;
    return res.json({ reports: rows.slice(0, QUEUE_LIMIT), truncated });
  }));

  /**
   * HIDE or UNHIDE a post or a comment.
   *
   * 404 to anyone who is not a member of the target's space — the target's existence is
   * not confirmed to a stranger — and 403 to a member who is not a moderator there.
   */
  const act = (type: TargetType): RequestHandler =>
    ah(async (req: Request, res: Response) => {
      const parsed = actionBody.safeParse(req.body);
      if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
      const { tenantId, userId } = getAuth(req);
      const id = req.params[type === 'POST' ? 'postId' : 'commentId']!;
      const { action, reason, reportId } = parsed.data;

      const target = await withTenant(tenantId, (tx) => resolveTarget(tx, type, id));
      if (!target) return res.status(404).json({ error: 'not found' });
      // The role is judged in the TARGET'S space. See the note at the top of this file.
      const membership = await readMembership(tenantId, target.spaceId, userId, logger);
      if (!membership || membership.status !== 'ACTIVE') return res.status(404).json({ error: 'not found' });
      if (!atLeast(membership, 'MODERATOR')) return res.status(403).json({ error: 'only a moderator of this space can do that' });

      const [from, to] = action === 'HIDE' ? (['VISIBLE', 'HIDDEN'] as const) : (['HIDDEN', 'VISIBLE'] as const);

      let changed: boolean;
      try {
        changed = await withTenant(tenantId, async (tx) => {
          const moved =
            type === 'POST'
              ? await tx.$executeRaw`
                  UPDATE "Post" SET status = ${to}::"ContentStatus"
                   WHERE id = ${id} AND status = ${from}::"ContentStatus"`
              : await tx.$executeRaw`
                  UPDATE "Comment" SET status = ${to}::"ContentStatus"
                   WHERE id = ${id} AND status = ${from}::"ContentStatus"`;
          // Already in the state asked for, or DELETED by its author: nothing to do, and
          // nothing to audit. A second HIDE is not a second action.
          if (moved !== 1) return false;

          if (type === 'COMMENT') {
            /**
             * THE POST'S COUNT IS RECOMPUTED, NOT DECREMENTED.
             *
             * A hidden comment takes its whole subtree out of the thread, so "minus one"
             * is wrong whenever it had replies — and spam comments are exactly what
             * gets hidden, so their boost to the post's rank must go with them or
             * hiding them leaves the ranking they bought. Counting what is visible now
             * is right however many were affected, and running it twice is running it
             * once: the same reasoning as the feed consumer copying a score instead of
             * adding to it.
             */
            await tx.$executeRaw`
              UPDATE "Post" p
                 SET "commentCount" = v.n,
                     "rankScore" = ${rankScore(Prisma.sql`p."reactionCount"`, Prisma.sql`v.n`, Prisma.sql`p."createdAt"`)}
                FROM (SELECT count(*)::int AS n FROM "Comment" c
                       WHERE c."postId" = ${target.postId} AND c.status = 'VISIBLE'::"ContentStatus"
                         AND NOT EXISTS (
                           SELECT 1 FROM "Comment" h
                            WHERE h."postId" = c."postId" AND h.status <> 'VISIBLE'::"ContentStatus"
                              AND c.path <@ h.path)) v
               WHERE p.id = ${target.postId}`;
          }

          if (reportId !== undefined) {
            /**
             * The report has to be about THIS target and still OPEN. Otherwise a
             * moderator could mark any report in the tenant ACTIONED by attaching its
             * id to an unrelated action — and the throw rolls the hide back with it, so
             * a refused request changes nothing at all.
             */
            const resolved = await tx.$executeRaw`
              UPDATE "Report" SET status = 'ACTIONED'::"ReportStatus"
               WHERE id = ${reportId} AND "targetType" = ${type}::"TargetType" AND "targetId" = ${id}
                 AND status = 'OPEN'::"ReportStatus"`;
            if (resolved !== 1) throw new ReportMismatch();
          }

          await tx.$executeRaw`
            INSERT INTO "ModerationAction" (id, "tenantId", "actorId", "targetType", "targetId", action, reason, "reportId", "createdAt")
            VALUES (${randomUUID()}, ${tenantId}, ${userId}, ${type}::"TargetType", ${id}, ${action}::"ModAction",
                    ${reason ?? null}, ${reportId ?? null}, now())`;
          return true;
        });
      } catch (err) {
        if (err instanceof ReportMismatch) {
          return res.status(400).json({ error: 'that report is not an open report about this target' });
        }
        throw err;
      }

      return res.json({ targetType: type, targetId: id, action, changed });
    });

  router.post('/posts/:postId/moderation', act('POST'));
  router.post('/comments/:commentId/moderation', act('COMMENT'));

  /**
   * Dismiss a report: the content stays as it is. Judged in the space of the report's
   * target, for the same reason as the actions above.
   */
  router.patch('/reports/:reportId', ah(async (req, res) => {
    const parsed = dismissBody.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: 'the only change a report takes is status: DISMISSED' });
    const { tenantId, userId } = getAuth(req);
    const reportId = req.params['reportId']!;

    const found = await withTenant(tenantId, async (tx) => {
      const rows = await tx.$queryRaw<Array<{ targetType: TargetType; targetId: string }>>`
        SELECT "targetType"::text AS "targetType", "targetId" FROM "Report" WHERE id = ${reportId}`;
      const row = rows[0];
      return row ? resolveTarget(tx, row.targetType, row.targetId) : null;
    });
    if (!found) return res.status(404).json({ error: 'not found' });
    const membership = await readMembership(tenantId, found.spaceId, userId, logger);
    if (!membership || membership.status !== 'ACTIVE') return res.status(404).json({ error: 'not found' });
    if (!atLeast(membership, 'MODERATOR')) return res.status(403).json({ error: 'only a moderator of this space can do that' });

    const dismissed = await withTenant(tenantId, (tx) =>
      tx.$executeRaw`
        UPDATE "Report" SET status = 'DISMISSED'::"ReportStatus"
         WHERE id = ${reportId} AND status = 'OPEN'::"ReportStatus"`,
    );
    return res.json({ reportId, changed: dismissed === 1 });
  }));

  return router;
}
