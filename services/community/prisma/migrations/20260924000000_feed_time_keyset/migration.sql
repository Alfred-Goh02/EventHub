-- ---------------------------------------------------------------------------
-- The feed pages on (createdAt, id), not on rankScore.
--
-- Decided 2026-09-21, from a measurement: a keyset cursor over a MUTABLE key skips
-- and duplicates. Five posts scored 99..95, page size two; page one returned p1 and
-- p2, a reaction moved p4 above the cursor and it appeared on neither page, and when
-- p1 then fell below the cursor it appeared on both. A cursor is exactly-once only
-- over a key that never changes, and every reaction and comment changes rankScore.
--
-- createdAt costs little here. rankScore is ln(activity) + epoch/45000: the time term
-- is about 39,773 today and the activity term rarely passes 5, so a ranked feed was
-- already a chronological one with a nudge.
--
-- ONE EXPLICIT TRANSACTION, because Prisma does not make a migration one: it sends
-- the statements one at a time, and a failure part-way leaves the ones before it
-- applied. Measured on 20260925000000, whose broken backfill failed a deploy: without
-- BEGIN its earlier statements stayed; with it, nothing did.
-- ---------------------------------------------------------------------------
BEGIN;

-- ---------------------------------------------------------------------------
-- Which half of the feed a post belongs to is decided ONCE, per post, when the
-- consumer handles it — and recorded here.
--
-- The obvious rule is "a large space is read at query time", judged at READ time. It
-- loses posts. The consumer judges size at FAN-OUT time, so a post made while a space
-- was large was never fanned out; if the space later shrinks under the threshold, a
-- read-time rule stops querying it, and that post is in neither half. Nobody sees it
-- again. (The opposite crossing — small, then large — puts a post in both halves,
-- which a dedupe hides; this direction has nothing to dedupe.)
--
-- With the decision recorded, the read-time half covers exactly the posts that were
-- NOT fanned out, whatever the space's size is now, and every post is in exactly one
-- half in either direction. It also closes the consumer's lag: a post the consumer
-- has not reached yet is false here, so it is read at query time and appears at once
-- instead of when the backlog clears.
-- ---------------------------------------------------------------------------
ALTER TABLE "Post" ADD COLUMN "fannedOut" BOOLEAN NOT NULL DEFAULT false;

-- Posts the consumer already fanned out before this column existed. Without this they
-- would sit in both halves until the dedupe caught them, for ever.
--
-- FORCE row security is lifted for the backfill, and restored immediately. A migration
-- runs as the table owner with no app.tenant_id set, which matches no tenant policy,
-- so under FORCE this UPDATE sees ZERO rows and succeeds. The first version of this
-- file did exactly that. Measured with the real `prisma migrate deploy`, on one
-- planted post and its FeedEntry: fannedOut stayed false as first written, and became
-- true with FORCE lifted. NO FORCE exempts only the owner; the app role stays under
-- the policies throughout.
ALTER TABLE "Post"      NO FORCE ROW LEVEL SECURITY;
ALTER TABLE "FeedEntry" NO FORCE ROW LEVEL SECURITY;
UPDATE "Post" p SET "fannedOut" = true
 WHERE EXISTS (SELECT 1 FROM "FeedEntry" f WHERE f."postId" = p.id AND f."tenantId" = p."tenantId");
ALTER TABLE "FeedEntry" FORCE ROW LEVEL SECURITY;
ALTER TABLE "Post"      FORCE ROW LEVEL SECURITY;

-- The read-time half: one index-ordered scan per space the reader belongs to, walked
-- by LATERAL. Partial on VISIBLE AND NOT fannedOut, so it holds only the posts that
-- half can return — large spaces' posts and posts still in flight. A space that has
-- never been large costs one descent of this index to find nothing.
CREATE INDEX "Post_feed_readtime_idx"
  ON "Post" ("tenantId", "spaceId", "createdAt" DESC, id DESC)
  WHERE status = 'VISIBLE' AND "fannedOut" = false;

-- The materialised half: a member's own rows, newest first, with the post id as the
-- tie-break so two posts in the same millisecond still have one order.
CREATE INDEX "FeedEntry_time_keyset_idx"
  ON "FeedEntry" ("tenantId", "userId", "createdAt" DESC, "postId" DESC);

-- The two rankScore indexes on FeedEntry had one reader in mind, the old cursor, and
-- that reader does not exist. They were not free to keep: the consumer rewrites
-- FeedEntry.rankScore on every comment and reaction, once per fanned-out copy, and an
-- index on the rewritten column makes each of those an index write as well.
DROP INDEX "FeedEntry_keyset_idx";
DROP INDEX "FeedEntry_tenantId_userId_rankScore_idx";

COMMIT;
