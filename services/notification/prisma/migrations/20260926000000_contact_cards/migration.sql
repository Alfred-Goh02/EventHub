-- ---------------------------------------------------------------------------
-- Notification's copy of every user's contact card, read from auth.user-contact.
--
-- Decided 2026-09-26: Auth publishes each user's address as a sealed card on a
-- compacted topic, and Notification keeps a copy, so a reply email never waits on
-- Auth. The copy is the SEALED card, not the address: this table holds ciphertext
-- only Notification's private key opens, so a read of the database is not a read of
-- everyone's email address. The card is opened when an email is sent.
--
-- Keyed by (tenantId, userId), the same identity as the topic's key. A user id alone
-- would let one tenant's card replace another's.
--
-- occurredAt is when Auth queued the card. The consumer keeps the LATEST, so an older
-- card replayed after a newer one — a redelivery, a replay from the start of the
-- compacted topic — cannot replace it.
--
-- One explicit transaction, because Prisma does not make a migration one.
-- ---------------------------------------------------------------------------
BEGIN;

CREATE TABLE "ContactCard" (
    "tenantId"   TEXT NOT NULL,
    "userId"     TEXT NOT NULL,
    "sealed"     TEXT NOT NULL,
    "messageId"  TEXT NOT NULL,
    "occurredAt" TIMESTAMP(3) NOT NULL,
    "updatedAt"  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "ContactCard_pkey" PRIMARY KEY ("tenantId", "userId")
);

-- The consumer stores, replaces and — on a tombstone — deletes a card. It may not
-- move a card to another user or tenant: the key columns take no UPDATE.
GRANT SELECT, INSERT, DELETE ON "ContactCard" TO eventhub_notification_app;
GRANT UPDATE ("sealed", "messageId", "occurredAt", "updatedAt") ON "ContactCard" TO eventhub_notification_app;

ALTER TABLE "ContactCard" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "ContactCard" FORCE  ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON "ContactCard"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

COMMIT;
