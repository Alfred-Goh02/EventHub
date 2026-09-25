-- ---------------------------------------------------------------------------
-- What each user has turned off.
--
-- Decided 2026-09-26: the first email a user can opt out of — replies in Community —
-- is ON by default, and every one carries a one-click unsubscribe. So a row here
-- records a CHOICE, and no row means the category's default. A password reset has no
-- row and can never have one that turns it off: it is how a locked-out user gets back
-- in, and the application refuses to write one.
--
-- Rows are never deleted by the service. An opt-out is evidence of what someone asked
-- for; deleting it is how a mailer ends up writing to someone who told it to stop.
--
-- Keyed by tenant, user, category and channel, so adding a channel is data, not a
-- migration.
--
-- One explicit transaction, because Prisma does not make a migration one.
-- ---------------------------------------------------------------------------
BEGIN;

CREATE TABLE "NotificationPreference" (
    "tenantId"  TEXT NOT NULL,
    "userId"    TEXT NOT NULL,
    "category"  TEXT NOT NULL,
    "channel"   TEXT NOT NULL,
    "enabled"   BOOLEAN NOT NULL,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "NotificationPreference_pkey" PRIMARY KEY ("tenantId", "userId", "category", "channel")
);

-- The service records and changes choices; it never deletes them, and never moves one
-- to another user or category.
GRANT SELECT, INSERT ON "NotificationPreference" TO eventhub_notification_app;
GRANT UPDATE ("enabled", "updatedAt") ON "NotificationPreference" TO eventhub_notification_app;

ALTER TABLE "NotificationPreference" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "NotificationPreference" FORCE  ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON "NotificationPreference"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

COMMIT;
