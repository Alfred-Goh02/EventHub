-- CreateEnum
CREATE TYPE "Visibility" AS ENUM ('PUBLIC', 'PRIVATE');

-- CreateEnum
CREATE TYPE "SpaceRole" AS ENUM ('OWNER', 'MODERATOR', 'MEMBER');

-- CreateEnum
CREATE TYPE "MemberStatus" AS ENUM ('ACTIVE', 'BANNED');

-- CreateEnum
CREATE TYPE "ContentStatus" AS ENUM ('VISIBLE', 'HIDDEN', 'DELETED');

-- CreateEnum
CREATE TYPE "TargetType" AS ENUM ('POST', 'COMMENT');

-- CreateEnum
CREATE TYPE "ReportReason" AS ENUM ('SPAM', 'ABUSE', 'OFF_TOPIC', 'OTHER');

-- CreateEnum
CREATE TYPE "ReportStatus" AS ENUM ('OPEN', 'ACTIONED', 'DISMISSED');

-- CreateEnum
CREATE TYPE "ModAction" AS ENUM ('HIDE', 'UNHIDE', 'DELETE', 'BAN', 'UNBAN');

-- CreateTable
CREATE TABLE "CommunitySpace" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "eventId" TEXT,
    "slug" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "visibility" "Visibility" NOT NULL DEFAULT 'PUBLIC',
    "memberCount" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CommunitySpace_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Membership" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "spaceId" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "role" "SpaceRole" NOT NULL DEFAULT 'MEMBER',
    "status" "MemberStatus" NOT NULL DEFAULT 'ACTIVE',
    "joinedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Membership_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Post" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "spaceId" TEXT NOT NULL,
    "authorId" TEXT NOT NULL,
    "body" TEXT NOT NULL,
    "status" "ContentStatus" NOT NULL DEFAULT 'VISIBLE',
    "reactionCount" INTEGER NOT NULL DEFAULT 0,
    "commentCount" INTEGER NOT NULL DEFAULT 0,
    "rankScore" DOUBLE PRECISION NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "editedAt" TIMESTAMP(3),

    CONSTRAINT "Post_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Comment" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "postId" TEXT NOT NULL,
    "authorId" TEXT NOT NULL,
    "parentId" TEXT,
    "path" ltree NOT NULL,
    "depth" INTEGER NOT NULL,
    "body" TEXT NOT NULL,
    "status" "ContentStatus" NOT NULL DEFAULT 'VISIBLE',
    "reactionCount" INTEGER NOT NULL DEFAULT 0,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Comment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Reaction" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "targetType" "TargetType" NOT NULL,
    "targetId" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "kind" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Reaction_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Report" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "targetType" "TargetType" NOT NULL,
    "targetId" TEXT NOT NULL,
    "reporterId" TEXT NOT NULL,
    "reason" "ReportReason" NOT NULL,
    "note" TEXT,
    "status" "ReportStatus" NOT NULL DEFAULT 'OPEN',
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Report_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ModerationAction" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "actorId" TEXT NOT NULL,
    "targetType" "TargetType" NOT NULL,
    "targetId" TEXT NOT NULL,
    "action" "ModAction" NOT NULL,
    "reason" TEXT,
    "reportId" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ModerationAction_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "FeedEntry" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "postId" TEXT NOT NULL,
    "spaceId" TEXT NOT NULL,
    "rankScore" DOUBLE PRECISION NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "FeedEntry_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OutboxMessage" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "seq" BIGSERIAL NOT NULL,
    "topic" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "schemaVersion" INTEGER NOT NULL DEFAULT 1,
    "aggregateId" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "publishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "lastError" TEXT,
    "nextAttemptAt" TIMESTAMP(3),
    "deadLetteredAt" TIMESTAMP(3),

    CONSTRAINT "OutboxMessage_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "CommunitySpace_tenantId_idx" ON "CommunitySpace"("tenantId");

-- CreateIndex
CREATE UNIQUE INDEX "CommunitySpace_tenantId_slug_key" ON "CommunitySpace"("tenantId", "slug");

-- CreateIndex
CREATE INDEX "Membership_tenantId_userId_idx" ON "Membership"("tenantId", "userId");

-- CreateIndex
CREATE UNIQUE INDEX "Membership_tenantId_spaceId_userId_key" ON "Membership"("tenantId", "spaceId", "userId");

-- CreateIndex
CREATE INDEX "Post_tenantId_spaceId_createdAt_idx" ON "Post"("tenantId", "spaceId", "createdAt" DESC);

-- CreateIndex
CREATE INDEX "Comment_tenantId_postId_idx" ON "Comment"("tenantId", "postId");

-- CreateIndex
CREATE INDEX "Reaction_tenantId_targetId_idx" ON "Reaction"("tenantId", "targetId");

-- CreateIndex
CREATE UNIQUE INDEX "Reaction_tenantId_targetType_targetId_userId_kind_key" ON "Reaction"("tenantId", "targetType", "targetId", "userId", "kind");

-- CreateIndex
CREATE INDEX "Report_tenantId_status_idx" ON "Report"("tenantId", "status");

-- CreateIndex
CREATE UNIQUE INDEX "Report_tenantId_targetType_targetId_reporterId_key" ON "Report"("tenantId", "targetType", "targetId", "reporterId");

-- CreateIndex
CREATE INDEX "ModerationAction_tenantId_targetId_idx" ON "ModerationAction"("tenantId", "targetId");

-- CreateIndex
CREATE INDEX "FeedEntry_tenantId_userId_rankScore_idx" ON "FeedEntry"("tenantId", "userId", "rankScore" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "FeedEntry_tenantId_userId_postId_key" ON "FeedEntry"("tenantId", "userId", "postId");

-- CreateIndex
CREATE INDEX "OutboxMessage_tenantId_idx" ON "OutboxMessage"("tenantId");

-- ===========================================================================
-- Everything below is hand-written. Prisma emits neither row-level security,
-- nor partial indexes, nor GiST, nor a CHECK constraint.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Threads.
--
-- A comment's place in its thread is a materialised path, so a subtree is one
-- indexed query rather than a walk. GiST answers the ancestor and descendant
-- operators (`@>`, `<@`); the btree answers "this post's comments, in thread
-- order", which is the read every post page makes.
-- ---------------------------------------------------------------------------
CREATE INDEX "Comment_path_gist_idx" ON "Comment" USING GIST (path);
CREATE INDEX "Comment_post_path_idx" ON "Comment" ("postId", path);

-- A ceiling on nesting, enforced by the database rather than by the handler that
-- happens to insert. Eight is deep enough that no reader has ever asked for more,
-- and shallow enough that a rendered thread still fits on a screen. Without it, a
-- reply-to-a-reply loop makes paths that grow until the index does.
ALTER TABLE "Comment" ADD CONSTRAINT "Comment_depth_cap" CHECK (nlevel(path) <= 8);

-- ---------------------------------------------------------------------------
-- The feed's indexes.
--
-- Partial on VISIBLE: a hidden post leaves the index entirely, so moderation makes
-- the read cheaper rather than adding a filter to every query.
-- ---------------------------------------------------------------------------
CREATE INDEX "Post_feed_rank_idx"
  ON "Post" ("tenantId", "spaceId", "rankScore" DESC, id DESC)
  WHERE status = 'VISIBLE';

-- The keyset cursor's index: (rankScore, postId) descending, which is exactly the
-- order the cursor walks. OFFSET would re-scan everything it has already skipped.
CREATE INDEX "FeedEntry_keyset_idx"
  ON "FeedEntry" ("tenantId", "userId", "rankScore" DESC, "postId" DESC);

-- ---------------------------------------------------------------------------
-- The outbox's indexes, the same three every other service has: the drain, the
-- prune and the per-aggregate backoff guard. Each is partial, so it holds the rows
-- that matter and empties itself as they publish.
-- ---------------------------------------------------------------------------
CREATE INDEX "OutboxMessage_undelivered_idx"
  ON "OutboxMessage" ("seq")
  WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL;

CREATE INDEX "OutboxMessage_prunable_idx"
  ON "OutboxMessage" ("publishedAt")
  WHERE "publishedAt" IS NOT NULL AND "deadLetteredAt" IS NULL;

CREATE INDEX "OutboxMessage_backoff_idx"
  ON "OutboxMessage" ("tenantId", "aggregateId", "topic", "seq")
  WHERE "publishedAt" IS NULL
    AND "deadLetteredAt" IS NULL
    AND "nextAttemptAt" IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Grants, per table and explicit.
--
-- Not ALTER DEFAULT PRIVILEGES: a table whose migration forgets its RLS block must
-- also be a table the service cannot touch, so the failure is "permission denied"
-- rather than one tenant quietly reading another's rows.
--
-- What each grant says about the design:
--   - a post or a comment is never DELETEd; it moves to a status, so the moderation
--     trail still points at something;
--   - a reaction is deleted rather than updated, because un-reacting is removal;
--   - the audit trail is INSERT and SELECT only. See below.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON "CommunitySpace" TO eventhub_community_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "Membership" TO eventhub_community_app;
GRANT SELECT, INSERT, UPDATE ON "Post" TO eventhub_community_app;
GRANT SELECT, INSERT, UPDATE ON "Comment" TO eventhub_community_app;
GRANT SELECT, INSERT, DELETE ON "Reaction" TO eventhub_community_app;
GRANT SELECT, INSERT, UPDATE ON "Report" TO eventhub_community_app;
GRANT SELECT, INSERT, DELETE ON "FeedEntry" TO eventhub_community_app;
GRANT UPDATE ("rankScore") ON "FeedEntry" TO eventhub_community_app;

-- APPEND-ONLY AT THE PRIVILEGE LEVEL, not by convention. A moderator's own record of
-- what they did is the one table nobody may revise, and "the code never updates it"
-- is not a guarantee — it is a description of today's code.
GRANT SELECT, INSERT ON "ModerationAction" TO eventhub_community_app;

-- The service inserts an outbox row in the transaction that made the change, and
-- selects because Prisma reads the new row back. Delivery state belongs to the relay.
GRANT SELECT, INSERT ON "OutboxMessage" TO eventhub_community_app;
GRANT USAGE, SELECT ON SEQUENCE "OutboxMessage_seq_seq" TO eventhub_community_app;

-- The relay reads rows, marks delivery state and prunes. Its UPDATE is limited to
-- those columns, so it cannot rewrite tenantId or payload.
GRANT SELECT ON "OutboxMessage" TO eventhub_community_relay;
GRANT UPDATE ("publishedAt", "attempts", "lastError", "nextAttemptAt", "deadLetteredAt")
  ON "OutboxMessage" TO eventhub_community_relay;
GRANT DELETE ON "OutboxMessage" TO eventhub_community_relay;

-- ---------------------------------------------------------------------------
-- Row-level security on every table.
--
-- ENABLE turns policies on; FORCE applies them to the table OWNER too, so a backfill
-- or a psql session as the owner sees one tenant at a time as well.
--
-- current_setting(..., true) is NULL when unset, and NULL = anything is not true, so a
-- connection that never set a tenant sees nothing and writes nothing.
-- ---------------------------------------------------------------------------
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['CommunitySpace', 'Membership', 'Post', 'Comment', 'Reaction',
                           'Report', 'ModerationAction', 'FeedEntry', 'OutboxMessage']
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
  END LOOP;

  -- One policy per table, targeted at NO role, so it binds the owner as well.
  --
  -- Role-targeting it at the service would leave the owner with no policy at all
  -- under FORCE, and a table nobody can read is a table nobody can migrate data in,
  -- back-fill or clean up — the owner would be locked out of its own tables while
  -- gaining nothing: it still sees one tenant at a time, which is the point.
  FOREACH t IN ARRAY ARRAY['CommunitySpace', 'Membership', 'Post', 'Comment', 'Reaction',
                           'Report', 'ModerationAction', 'FeedEntry']
  LOOP
    EXECUTE format($f$CREATE POLICY tenant_isolation ON %I
      USING ("tenantId" = current_setting('app.tenant_id', true))
      WITH CHECK ("tenantId" = current_setting('app.tenant_id', true))$f$, t);
  END LOOP;
END $$;

-- Two readers on the outbox, two policies, as in Auth, Event and Ticketing: the
-- service sees its own tenant, and the relay drains by delivery state, which has no
-- tenant in it.
CREATE POLICY outbox_tenant ON "OutboxMessage"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

-- Permissive policies are ORed, so the relay gets this one on top of the tenant
-- policy above, and the service gets no wider reach from its existence.
CREATE POLICY outbox_relay ON "OutboxMessage" TO eventhub_community_relay
  USING (true);
