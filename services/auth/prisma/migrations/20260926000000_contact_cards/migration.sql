-- ---------------------------------------------------------------------------
-- Auth publishes each user's address to Notification as a sealed contact card.
--
-- Decided 2026-09-26: Notification learns addresses from a compacted topic Auth
-- publishes, not by calling Auth when it sends. A reply email then never waits on
-- Auth, and Notification keeps a copy it can only read with its own private key.
--
-- contactPublishedAt is when this user's current card was queued, in the same
-- transaction as its outbox row. NULL means no card was ever queued, and is how
-- `contact-cards:publish` finds the users to publish: every user who existed before
-- this column, and anyone a future change sets back to NULL. Publishing twice is
-- harmless — the topic is compacted and Notification keeps the newer card — but a
-- marker keeps a re-run of setup from queueing every card again.
--
-- No backfill here: NULL is already the right value for every existing user.
-- ---------------------------------------------------------------------------
BEGIN;

ALTER TABLE "User" ADD COLUMN "contactPublishedAt" TIMESTAMP(3);

COMMIT;
