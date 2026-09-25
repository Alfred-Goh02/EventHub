-- Run the empty-slug backfill again, with row security out of its way.
--
-- 20260905090500_backfill_empty_slugs never changed a row. Event is under FORCE ROW
-- LEVEL SECURITY (20260823081317_init_event), a migration runs as
-- eventhub_event_owner with no app.tenant_id set, and FORCE binds the owner to the
-- tenant policy -- so the UPDATE saw no rows, matched none, and succeeded. Nothing
-- after it depended on the rows it should have changed, so nothing failed.
-- 20260825145733_harden_outbox had already learned this on the outbox; it was only
-- loud there because SET NOT NULL came next.
--
-- Measured as the owner, in one rolled-back transaction with one planted
-- empty-slug event: the original statement, verbatim, under FORCE -> UPDATE 0. The
-- same statement with FORCE lifted -> UPDATE 2: the planted row, and one real row the
-- original migration had left behind on the machine it was measured on.
--
-- The statement is the original's, unchanged, between NO FORCE and FORCE on the one
-- table it reads and writes.
--
-- BEGIN and COMMIT are not decoration. prisma migrate deploy sends a file's
-- statements one at a time, so without them each commits on its own. Measured with it
-- on a throwaway table, a statement failing between NO FORCE and FORCE: without
-- BEGIN, FORCE was left OFF and the UPDATE before the failure stayed committed; with
-- BEGIN, FORCE stayed on and the UPDATE was rolled back. Inside the transaction the
-- first ALTER's ACCESS EXCLUSIVE lock (measured) is held until COMMIT, so nothing
-- else reads or writes Event while FORCE is off.
--
-- scripts/verify-migration-force-rls.mjs fails any UPDATE or DELETE a migration runs
-- against a table under FORCE. The original stays in the history, because an applied
-- migration is never edited; the verifier allows it by name and points here.
BEGIN;

ALTER TABLE "Event" NO FORCE ROW LEVEL SECURITY;

UPDATE "Event"
   SET slug = 'event-' || substr(replace(gen_random_uuid()::text, '-', ''), 1, 10)
 WHERE slug = '';

ALTER TABLE "Event" FORCE ROW LEVEL SECURITY;

COMMIT;
