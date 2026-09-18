-- What the stuck-delivery sweeper may do. db/init/01-bootstrap.sql says why the role
-- exists at all; this is its reach inside the database.

-- SELECT, and a column-level UPDATE that can only say "this one was abandoned".
--
-- It cannot create a delivery, cannot change whose it is, cannot change its
-- idempotency key, and cannot count an attempt. The columns are the whole bound on a
-- role that crosses tenants, exactly as they are for Ticketing's sweeper.
GRANT SELECT ON "NotificationDelivery" TO eventhub_notification_sweeper;
GRANT UPDATE ("status", "failedAt", "lastError") ON "NotificationDelivery" TO eventhub_notification_sweeper;

-- NOTHING on "EmailSuppression". A sweep of unfinished deliveries has no business
-- reading a list of addresses, hashed or not.

-- Its own policy, because a tenant-scoped one would hide every row it exists to find.
-- The existing tenant_isolation policy is left alone: Postgres ORs permissive
-- policies together, so the app role stays filtered by tenant exactly as before, and
-- only the sweeper gains the wider view.
--
-- WITH CHECK PINS THE OUTCOME. The grants above stop the sweeper writing other
-- columns, but nothing in a grant stops it writing the wrong VALUE into status. A row
-- it has touched must end FAILED, so it can abandon a delivery and can never mark one
-- SENT or SUPPRESSED — the two outcomes that would tell an operator, wrongly, that
-- the mail went out or was withheld on purpose.
CREATE POLICY sweeper_all ON "NotificationDelivery" TO eventhub_notification_sweeper
  USING (true) WITH CHECK (status = 'FAILED'::"DeliveryStatus");
