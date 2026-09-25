-- Delete the leftover in_progress idempotency keys again, with row security out of
-- its way.
--
-- 20260905093000_drop_idempotency_lease deleted nothing. IdempotencyKey is under
-- FORCE ROW LEVEL SECURITY with a tenant_isolation policy that has no TO clause
-- (20260825163649_init_ticketing), a migration runs as eventhub_ticketing_owner with
-- no app.tenant_id set, and FORCE binds the owner to that policy -- so the DELETE saw
-- no rows and succeeded. 20260906110000_sweeper_may_prune measured the same blindness
-- on ProcessedMessage a day later, from a maintenance script: 0 of 29 rows, exit 0.
--
-- That migration's own comment says what the survivors cost. With the takeover gone,
-- each 'in_progress' row makes its key answer 409 until something deletes it -- and
-- the only thing that now does is the sweeper's year-long retention prune.
--
-- Measured as the owner, in one rolled-back transaction with one planted in_progress
-- key: the original statement, verbatim, under FORCE -> DELETE 0. With FORCE lifted
-- -> DELETE 53: the planted key and 52 real ones on the machine it was measured on,
-- 49 of them older than the migration that was meant to remove them.
--
-- Deleting every such row is still safe, for the reason the original gave: the claim
-- and the work commit in ONE transaction, so a committed 'in_progress' row is a claim
-- nothing will ever finish. A claim still in flight holds a lock on this table, the
-- first ALTER below waits for it (ACCESS EXCLUSIVE, measured), and by then it has
-- committed as completed or rolled back.
--
-- The statement is the original's, unchanged, between NO FORCE and FORCE on the one
-- table it reads and writes.
--
-- BEGIN and COMMIT are not decoration. prisma migrate deploy sends a file's
-- statements one at a time, so without them each commits on its own. Measured with it
-- on a throwaway table, a statement failing between NO FORCE and FORCE: without
-- BEGIN, FORCE was left OFF and the write before the failure stayed committed; with
-- BEGIN, FORCE stayed on and the write was rolled back. Inside the transaction the
-- ALTER's lock is held until COMMIT, so nothing else touches the table while FORCE is
-- off.
--
-- scripts/verify-migration-force-rls.mjs fails any UPDATE or DELETE a migration runs
-- against a table under FORCE. The original stays in the history, because an applied
-- migration is never edited; the verifier allows it by name and points here.
BEGIN;

ALTER TABLE "IdempotencyKey" NO FORCE ROW LEVEL SECURITY;

DELETE FROM "IdempotencyKey" WHERE "state" = 'in_progress';

ALTER TABLE "IdempotencyKey" FORCE ROW LEVEL SECURITY;

COMMIT;
