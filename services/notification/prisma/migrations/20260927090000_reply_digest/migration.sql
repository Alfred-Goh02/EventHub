-- ---------------------------------------------------------------------------
-- Replies held back by the per-person limit, for the hourly digest.
--
-- Decided 2026-09-27: at most five reply emails per person per hour; the rest are
-- held and sent as one digest an hour, which carries COUNTS AND LINKS ONLY. So a held
-- reply is stored as the post it was on and whether it answered a comment — never its
-- words, and never who wrote it. A reply deleted or hidden in Community leaves nothing
-- here but a count.
--
-- Keyed by the reply's own message id, per tenant and recipient, so a redelivered
-- reply is held once. digestId is set when a flush takes the item into a digest, and
-- is that digest's identity in the sent-log; the item is deleted once the digest is
-- settled, so this table only ever holds what is still waiting.
--
-- One explicit transaction, because Prisma does not make a migration one.
-- ---------------------------------------------------------------------------
BEGIN;

CREATE TABLE "DigestItem" (
    "tenantId"        TEXT NOT NULL,
    "userId"          TEXT NOT NULL,
    "category"        TEXT NOT NULL,
    "sourceMessageId" TEXT NOT NULL,
    "postId"          TEXT NOT NULL,
    "toComment"       BOOLEAN NOT NULL,
    "digestId"        TEXT,
    "createdAt"       TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "DigestItem_pkey" PRIMARY KEY ("tenantId", "userId", "category", "sourceMessageId")
);

-- What the flusher reads: who has been waiting longest.
CREATE INDEX "DigestItem_createdAt_idx" ON "DigestItem" ("createdAt");

-- The app role holds an item, takes it into a digest, and deletes it once the digest
-- is settled. No other column moves: an item cannot be given to another person or
-- another post.
GRANT SELECT, INSERT, DELETE ON "DigestItem" TO eventhub_notification_app;
GRANT UPDATE ("digestId") ON "DigestItem" TO eventhub_notification_app;

ALTER TABLE "DigestItem" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "DigestItem" FORCE  ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON "DigestItem"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

-- The flusher's whole view: who has something waiting, and since when. Three columns,
-- read only, in every tenant. Its own policy, FOR SELECT, so there is no statement it
-- could write through even if a grant were added by mistake; the postId, and so what
-- was said where, is not among the columns.
GRANT SELECT ("tenantId", "userId", "createdAt") ON "DigestItem" TO eventhub_notification_digest;
CREATE POLICY digest_who_waits ON "DigestItem" FOR SELECT TO eventhub_notification_digest USING (true);

COMMIT;
