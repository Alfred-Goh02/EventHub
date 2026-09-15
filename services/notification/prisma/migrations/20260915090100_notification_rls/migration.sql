-- Row-level security for the sent-log.
--
-- THE FIELD GUIDE EXEMPTS THIS TABLE, and this build does not. Its reason was
-- that the idempotency claim runs before tenant context exists. Here it does not:
-- the claim is made while handling a message, every message this service consumes
-- carries its tenant, and the consumer sets app.tenant_id from the message before
-- claiming. An exemption would leave the only table this service has readable
-- across tenants, to accommodate a constraint that does not apply.
--
-- The unique constraint is (tenantId, idempotencyKey), not the key alone. A unique
-- index is not filtered by policy, so a key-only constraint would let a claim
-- conflict with a row its own tenant cannot see.
--
-- Grants are explicit per table, as in every other service: a table whose
-- migration forgot its RLS block is also a table the app role cannot touch, so the
-- service fails with "permission denied" instead of quietly reading across tenants.

-- INSERT is the claim. SELECT reads a claim back, and is how "did we send this?"
-- is answered. UPDATE records the outcome: the status, attempts, the provider's id.
--
-- No DELETE. The log outlives Kafka's retention on purpose, and retention cleanup
-- gets its own role when it is built rather than handing DELETE to the process
-- that sends.
GRANT SELECT, INSERT, UPDATE ON "NotificationDelivery" TO eventhub_notification_app;

-- ENABLE turns policies on. FORCE applies them to the table OWNER too, so a
-- backfill or a psql session as the owner sees one tenant at a time as well.
ALTER TABLE "NotificationDelivery" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "NotificationDelivery" FORCE  ROW LEVEL SECURITY;

-- USING filters rows that exist; WITH CHECK constrains rows being written, so a
-- claim cannot be inserted under another tenant, or updated into one.
--
-- current_setting(..., true) is NULL when unset, and NULL = anything is not true,
-- so a connection that never set a tenant sees nothing and writes nothing.
CREATE POLICY tenant_isolation ON "NotificationDelivery"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));
