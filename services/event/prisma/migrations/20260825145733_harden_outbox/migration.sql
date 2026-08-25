-- ===========================================================================
-- Harden the outbox so a relay can be written against it.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- topic: add nullable, backfill, then enforce NOT NULL.
--
-- Prisma generated `ADD COLUMN "topic" TEXT NOT NULL` and warned it cannot work
-- on a non-empty table. The three-step version is also what you would do in
-- production against a live table, where a single NOT NULL add takes an ACCESS
-- EXCLUSIVE lock and rewrites the table.
-- ---------------------------------------------------------------------------
ALTER TABLE "OutboxMessage" ADD COLUMN "topic" TEXT;

-- The backfill needs RLS suspended, and the reason is worth understanding.
--
-- Migrations run as eventhub_event_owner. OutboxMessage carries ROLE-TARGETED
-- policies (TO eventhub_event_app and TO eventhub_event_relay), so the owner
-- matches neither — and under FORCE ROW LEVEL SECURITY that means it sees ZERO
-- rows. The first version of this migration ran the UPDATE below, silently
-- affected 0 rows, and then failed on SET NOT NULL because the existing row was
-- still null.
--
-- NO FORCE restores the owner's normal exemption for the duration of the
-- backfill. It is safe here because a migration already holds an exclusive lock
-- and no application traffic can interleave. FORCE is restored immediately.
ALTER TABLE "OutboxMessage" NO FORCE ROW LEVEL SECURITY;

UPDATE "OutboxMessage" SET "topic" = 'events.activity' WHERE "topic" IS NULL;

ALTER TABLE "OutboxMessage" FORCE ROW LEVEL SECURITY;

ALTER TABLE "OutboxMessage" ALTER COLUMN "topic" SET NOT NULL;

-- ---------------------------------------------------------------------------
-- Failure handling. Without these, one unserialisable row is re-selected by
-- ORDER BY seq on every poll, fails, is re-selected, forever — and nothing
-- behind it is ever published.
-- ---------------------------------------------------------------------------
ALTER TABLE "OutboxMessage"
  ADD COLUMN "attempts"       INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN "lastError"      TEXT,
  ADD COLUMN "nextAttemptAt"  TIMESTAMP(3),
  ADD COLUMN "deadLetteredAt" TIMESTAMP(3);

-- ---------------------------------------------------------------------------
-- Replace the composite index with a partial one.
--
-- (publishedAt, seq) cannot supply ORDER BY seq: Postgres does not treat
-- IS NULL on the leading column as fixing it, so the plan restricts rows and
-- then SORTS them — and the sort grows with the backlog, so the query gets
-- slower exactly when the relay is already behind.
--
-- The partial index has no leading column to fix, so seq order comes straight
-- from the index. It also self-empties as rows are published and dead-lettered,
-- staying a few pages forever rather than tracking table size.
-- ---------------------------------------------------------------------------
DROP INDEX "OutboxMessage_publishedAt_seq_idx";

CREATE INDEX "OutboxMessage_undelivered_idx"
  ON "OutboxMessage" ("seq")
  WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL;

-- ---------------------------------------------------------------------------
-- Narrow the relay's write privilege to the columns it actually needs.
--
-- It previously held table-level UPDATE, which let it rewrite tenantId and
-- payload — i.e. publish one tenant's data under another tenant's key. Column
-- privileges make that a permission error rather than a policy question.
--
-- Row locking still works: FOR UPDATE requires UPDATE privilege on at least one
-- column, not the whole table.
--
-- DELETE is granted because nothing else can prune this table. Published rows
-- accumulate forever otherwise, and the relay is the only process that knows
-- which are safely delivered.
-- ---------------------------------------------------------------------------
REVOKE UPDATE ON "OutboxMessage" FROM eventhub_event_relay;

GRANT UPDATE ("publishedAt", "attempts", "lastError", "nextAttemptAt", "deadLetteredAt")
  ON "OutboxMessage" TO eventhub_event_relay;

GRANT DELETE ON "OutboxMessage" TO eventhub_event_relay;

-- NOTE: the relay's idle_in_transaction_session_timeout and statement_timeout
-- are NOT set here. They are ALTER ROLE, which is cluster-level, and
-- eventhub_event_owner is deliberately not a superuser and has no CREATEROLE —
-- so it cannot alter another role. Attempting it here failed with
-- 'permission denied to alter role'. Role attributes live in
-- db/init/01-bootstrap.sql, which runs as the superuser.
