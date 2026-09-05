-- Let the sweeper prune the two tables that grow forever, and no more than that.
--
-- Nothing pruned ProcessedMessage or IdempotencyKey, and NO ROLE COULD. Both
-- carry a tenant_isolation policy with no TO clause, so it applies to PUBLIC,
-- and FORCE ROW LEVEL SECURITY binds even the table owner. Measured: as
-- eventhub_ticketing_owner with no app.tenant_id set, SELECT count(*) on
-- ProcessedMessage returned 0 of 29 real rows and a DELETE removed 0 -- exit
-- code 0, no error. A maintenance script written the obvious way would have
-- reported "nothing to prune" forever.
--
-- A retention sweep has no tenant. It asks "what is older than a year", which
-- is a question with no tenant in it, and there is no Tenant table in this
-- database to loop over even if it wanted one. So it needs a role-targeted
-- policy the way the sweeper's other work does -- bounded by GRANTS rather than
-- by policy, which is the pattern this service already uses for anything
-- crossing tenants.

-- ---------------------------------------------------------------------------
-- 1. An index a CROSS-TENANT prune can actually use.
--
-- ProcessedMessage already has one on ("processedAt"). IdempotencyKey has
-- ("tenantId", "createdAt"), whose leading column is exactly the thing a
-- retention sweep does not filter on -- so `WHERE "createdAt" < cutoff` cannot
-- use it and would scan the whole table.
--
-- That matters more than it sounds: the sweeper role carries
-- statement_timeout=15s, so past a certain size the prune stops completing at
-- all, and it stops in the direction where the table keeps growing.
-- ---------------------------------------------------------------------------
CREATE INDEX "IdempotencyKey_createdAt_idx" ON "IdempotencyKey" ("createdAt");

-- ---------------------------------------------------------------------------
-- 2. Grants, and they are deliberately lopsided.
--
-- DELETE is table-wide, because deleting a row is all-or-nothing.
--
-- SELECT is COLUMN-LEVEL, because the prune only ever needs to ask two things:
-- which row, and how old. IdempotencyKey.responseBody is a cached customer
-- response, and a retention job has no business being able to read one across
-- every tenant in the system. The same argument applies to ProcessedMessage's
-- messageId, which identifies another service's traffic.
--
-- This is the same shape as the relay's column-level UPDATE: the credential can
-- do its job and cannot do the thing next to its job.
-- ---------------------------------------------------------------------------
GRANT SELECT ("id", "processedAt") ON "ProcessedMessage" TO eventhub_ticketing_sweeper;
GRANT DELETE ON "ProcessedMessage" TO eventhub_ticketing_sweeper;

GRANT SELECT ("id", "createdAt") ON "IdempotencyKey" TO eventhub_ticketing_sweeper;
GRANT DELETE ON "IdempotencyKey" TO eventhub_ticketing_sweeper;

-- ---------------------------------------------------------------------------
-- 3. Role-targeted policies, USING (true).
--
-- Permissive and cross-tenant, exactly like checkpoint_consumer on
-- ConsumerCheckpoint -- because a Kafka offset has no tenant and neither does
-- "older than a year".
--
-- FOR ALL rather than FOR DELETE, and that is not laziness. The prune compares
-- what it can SEE against what it DELETED and refuses to call a mismatch
-- success; a policy covering only DELETE would leave the count blind and the
-- comparison meaningless. The narrowing that limits reach is the column-level
-- SELECT above, which is a better instrument for it than the policy.
--
-- The existing tenant_isolation policy is left alone. Postgres ORs permissive
-- policies together, so the app role keeps being filtered by tenant exactly as
-- before, and only the sweeper gains the wider view.
-- ---------------------------------------------------------------------------
CREATE POLICY processed_message_pruner ON "ProcessedMessage"
  TO eventhub_ticketing_sweeper
  USING (true);

CREATE POLICY idempotency_key_pruner ON "IdempotencyKey"
  TO eventhub_ticketing_sweeper
  USING (true);
