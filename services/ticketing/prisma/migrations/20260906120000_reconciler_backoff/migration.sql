-- Stop one unanswerable payment occupying a reconciler slot on every pass.
--
-- The candidate query is ORDER BY "startedAt" LIMIT n with no cursor and no
-- record of what has already been tried. A payment whose provider lookup fails
-- DETERMINISTICALLY -- a malformed provider reference, a provider that 500s for
-- one id -- is therefore selected first on every pass, forever.
--
-- The previous fix (#43) stopped it abandoning the rest of the BATCH, which was
-- the urgent half: one throwing lookup used to escape the loop and leave every
-- payment behind it unexamined. What it could not fix without a migration is
-- that the row still consumes a slot each time, so a handful of poison rows
-- shrink the effective batch size and, at RECONCILE_BATCH_SIZE of them, stop
-- the job examining anything else at all.
--
-- Two columns, and the second is the one that does the work.
ALTER TABLE "Payment"
  ADD COLUMN "lookupAttempts" INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN "nextLookupAt"   TIMESTAMP(3);

-- ---------------------------------------------------------------------------
-- The index the candidate query will now read.
--
-- Partial on exactly the reconciler's predicate -- unsettled rows -- so it stays
-- small and empties itself as payments settle, the same shape as the outbox's
-- undelivered index. Without it the added `nextLookupAt IS NULL OR ... <= now()`
-- clause would make the scan wider rather than narrower.
--
-- NULLS FIRST is not decoration: a row that has never been looked up has a NULL
-- nextLookupAt and must sort ahead of one that is merely due again.
-- ---------------------------------------------------------------------------
CREATE INDEX "Payment_reconcilable_idx"
  ON "Payment" ("nextLookupAt" NULLS FIRST, "startedAt")
  WHERE "settledAt" IS NULL;

-- ---------------------------------------------------------------------------
-- The sweeper credential runs the reconciler, and it must be able to record an
-- attempt. Column-level, like every other write this role has: it may say "I
-- tried and could not", and it may not touch the amount, the status, or who the
-- payment belongs to.
-- ---------------------------------------------------------------------------
GRANT SELECT ("lookupAttempts", "nextLookupAt") ON "Payment" TO eventhub_ticketing_sweeper;
GRANT UPDATE ("lookupAttempts", "nextLookupAt") ON "Payment" TO eventhub_ticketing_sweeper;
