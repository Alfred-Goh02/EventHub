import { randomUUID } from 'node:crypto';
import { Prisma } from './generated/prisma/client.js';
import { rankScore } from './rank.js';

/**
 * What can be seen, and what every change to it leaves behind.
 *
 * Posts, reports and moderation all ask these questions, and each copy of a rule is a
 * place for the copies to disagree. They lived in two routes for a part; the author's
 * delete would have been a third.
 *
 * TWO WAYS TO LEAVE A THREAD, AND THEY ARE NOT THE SAME.
 *   - HIDDEN, by a moderator, takes the whole subtree: the replies to abuse are part of
 *     what was hidden, and promoting them to the top would undo the hide.
 *   - DELETED, by its author, takes only the author's words. The comment stays as a
 *     tombstone and the replies other people wrote stay under it. Otherwise deleting
 *     one comment would erase everyone who answered it — a way to remove other
 *     people's words by removing your own. Decided 2026-09-25.
 * So "is anything above this hiding it" asks about HIDDEN only.
 */

/**
 * True when a comment strictly ABOVE the comment aliased `c` is HIDDEN.
 *
 * `c.path <@ h.path` is "c is under h", answered by the GiST index on path. The alias
 * is pasted in raw, so it is only ever one of this file's callers' own literals.
 */
export const hiddenAbove = (c: 'c') => {
  const a = Prisma.raw(`"${c}"`);
  return Prisma.sql`EXISTS (
    SELECT 1 FROM "Comment" h
     WHERE h."postId" = ${a}."postId" AND h.id <> ${a}.id
       AND h.status = 'HIDDEN'::"ContentStatus" AND ${a}.path <@ h.path)`;
};

/**
 * THE POST'S COMMENT COUNT IS RECOMPUTED, NOT DECREMENTED, after anything leaves.
 *
 * A hidden comment takes its whole subtree, so "minus one" is wrong whenever it had
 * replies — and spam is exactly what gets hidden, so the rank it bought has to go with
 * it. Counting what can be seen now is right however many rows were affected, and
 * running it twice is running it once.
 *
 * What counts is a VISIBLE comment with nothing HIDDEN above it. A tombstone does not
 * count; the replies under one do, because they are still there to read.
 */
export function recountComments(tx: Prisma.TransactionClient, postId: string) {
  return tx.$executeRaw`
    UPDATE "Post" p
       SET "commentCount" = v.n,
           "rankScore" = ${rankScore(Prisma.sql`p."reactionCount"`, Prisma.sql`v.n`, Prisma.sql`p."createdAt"`)}
      FROM (SELECT count(*)::int AS n FROM "Comment" c
             WHERE c."postId" = ${postId} AND c.status = 'VISIBLE'::"ContentStatus"
               AND NOT ${hiddenAbove('c')}) v
     WHERE p.id = ${postId}`;
}

/**
 * The audit row. Call it INSIDE the transaction that made the change: ModerationAction
 * is append-only at the privilege level, which is worth something only if no change can
 * commit without its row.
 */
export function recordAction(
  tx: Prisma.TransactionClient,
  row: {
    tenantId: string;
    spaceId: string;
    actorId: string;
    targetType: 'POST' | 'COMMENT' | 'MEMBER';
    targetId: string;
    action: 'HIDE' | 'UNHIDE' | 'DELETE' | 'BAN' | 'UNBAN';
    reason?: string | null;
    reportId?: string | null;
  },
) {
  return tx.$executeRaw`
    INSERT INTO "ModerationAction" (id, "tenantId", "spaceId", "actorId", "targetType", "targetId", action, reason, "reportId", "createdAt")
    VALUES (${randomUUID()}, ${row.tenantId}, ${row.spaceId}, ${row.actorId}, ${row.targetType}::"TargetType",
            ${row.targetId}, ${row.action}::"ModAction", ${row.reason ?? null}, ${row.reportId ?? null}, now())`;
}
