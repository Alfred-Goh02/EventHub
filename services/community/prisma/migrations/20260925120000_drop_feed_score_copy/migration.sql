-- ---------------------------------------------------------------------------
-- FeedEntry no longer carries a copy of the post's score.
--
-- The feed pages on (createdAt, id) and ranks each page by Post.rankScore, read at
-- query time (20260924000000). After that, nothing read FeedEntry.rankScore — and the
-- consumer still rewrote it on every comment and reaction, once per fanned-out row: a
-- thousand-member space paid a thousand row updates per reaction for a column with no
-- reader. Dropping it also drops the app role's column-level UPDATE grant on it, which
-- was the only UPDATE the role held on this table; fanned-out rows are now written
-- once and never changed.
--
-- Deploy with the code that stops writing it. The column is NOT NULL with no default,
-- so an old consumer's INSERT fails against the new schema; that failure throws, the
-- batch is redelivered, and the new code applies it. Nothing is lost in the window.
--
-- One explicit transaction, because Prisma does not make a migration one.
-- ---------------------------------------------------------------------------
BEGIN;

ALTER TABLE "FeedEntry" DROP COLUMN "rankScore";

COMMIT;
