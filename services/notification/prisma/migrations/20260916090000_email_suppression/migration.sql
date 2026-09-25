-- The suppression list: addresses not to send to again, because the server this
-- service relays through refused them as non-existent. channels/smtp.ts decides which
-- refusals count; dispatch/suppression.ts says why only a hash of the address is kept.
--
-- EVERY SUPPRESSION ENDS. The refusal comes from the relay, which can be wrong about a
-- live mailbox, and a password reset is how a locked-out user gets back in. So a row
-- carries expiresAt (SUPPRESSION_TTL_DAYS from now), and a later bounce moves it out
-- again rather than being ignored.

-- CreateTable
CREATE TABLE "EmailSuppression" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "addressHash" TEXT NOT NULL,
    "reason" TEXT NOT NULL,
    "smtpStatus" TEXT,
    "sourceDeliveryId" TEXT,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "bounces" INTEGER NOT NULL DEFAULT 1,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "EmailSuppression_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "EmailSuppression_tenantId_addressHash_key" ON "EmailSuppression"("tenantId", "addressHash");

-- SELECT and INSERT for the process that sends: it checks the list and adds to it.
--
-- UPDATE ONLY ON THE COLUMNS A NEW BOUNCE CHANGES, the same column-level shape
-- Ticketing's sweeper has. A later hard bounce extends the window and counts itself;
-- it may not rewrite which tenant or which address a row is about.
--
-- NO DELETE. Lifting a suppression is a person's decision, made with
-- `suppression:remove` as the owner, so a bug in the sender can suppress an address
-- but never quietly un-suppress one.
GRANT SELECT, INSERT ON "EmailSuppression" TO eventhub_notification_app;
GRANT UPDATE ("expiresAt", "smtpStatus", "sourceDeliveryId", "bounces")
  ON "EmailSuppression" TO eventhub_notification_app;

-- The same isolation as the sent-log, for the same reasons: see
-- 20260915090100_notification_rls. FORCE binds the owner too, so the removal script
-- sets its tenant like everything else does.
ALTER TABLE "EmailSuppression" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "EmailSuppression" FORCE  ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON "EmailSuppression"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));
