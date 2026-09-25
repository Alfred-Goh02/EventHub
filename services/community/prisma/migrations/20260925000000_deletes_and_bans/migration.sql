-- ---------------------------------------------------------------------------
-- Bans and authors' deletes join the moderation trail.
--
-- A ban's target is a MEMBER: a user, in one space. Its audit row cannot say which
-- space from the target id alone, and pointing it at the Membership row instead would
-- not survive the member leaving after an unban, when that row is deleted and the
-- append-only trail would be left naming something that no longer exists. So every
-- audit row now carries the space it happened in, and a ban's targetId is the user.
--
-- ONE EXPLICIT TRANSACTION, because Prisma does not make a migration one. It sends the
-- file's statements one at a time, so without BEGIN a failure part-way leaves the
-- statements before it applied. Measured on this file with its backfill broken:
-- without BEGIN the failed deploy left the column and the enum value behind; with it,
-- neither. (ADD VALUE is allowed inside a transaction since Postgres 12, as long as
-- the new value is not used before COMMIT, and nothing here uses it.)
-- ---------------------------------------------------------------------------
BEGIN;

ALTER TYPE "TargetType" ADD VALUE 'MEMBER';

ALTER TABLE "ModerationAction" ADD COLUMN "spaceId" TEXT;

-- Rows written before the column existed take their space from their target: a post
-- directly, a comment through its post. Posts and comments are never deleted, only
-- their status changes, so every existing row has a target to read.
--
-- FORCE row security is lifted for the backfill and restored straight after. A
-- migration runs as the table owner with no app.tenant_id set, which matches no tenant
-- policy, so under FORCE these UPDATEs see ZERO rows and succeed — the bug the
-- fannedOut backfill in 20260924000000 first shipped with. NO FORCE exempts only the
-- owner; the app role stays under the policies throughout.
ALTER TABLE "ModerationAction" NO FORCE ROW LEVEL SECURITY;
ALTER TABLE "Post"             NO FORCE ROW LEVEL SECURITY;
ALTER TABLE "Comment"          NO FORCE ROW LEVEL SECURITY;

UPDATE "ModerationAction" a SET "spaceId" = p."spaceId"
  FROM "Post" p
 WHERE a."targetType" = 'POST' AND p.id = a."targetId" AND p."tenantId" = a."tenantId";

UPDATE "ModerationAction" a SET "spaceId" = p."spaceId"
  FROM "Comment" c JOIN "Post" p ON p.id = c."postId"
 WHERE a."targetType" = 'COMMENT' AND c.id = a."targetId" AND c."tenantId" = a."tenantId";

ALTER TABLE "Comment"          FORCE ROW LEVEL SECURITY;
ALTER TABLE "Post"             FORCE ROW LEVEL SECURITY;
ALTER TABLE "ModerationAction" FORCE ROW LEVEL SECURITY;

-- NOT NULL after the backfill, so a row the backfill could not place stops the
-- migration with an error instead of entering the trail without a space. Measured:
-- with the FORCE lift removed, this is where the deploy failed, and the transaction
-- took everything above with it.
ALTER TABLE "ModerationAction" ALTER COLUMN "spaceId" SET NOT NULL;

COMMIT;
