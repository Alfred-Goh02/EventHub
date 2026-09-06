-- Make the reconciler's candidate query an ORDERED index scan.
--
-- The previous migration added the columns and an index, and the index IS used
-- -- but only as a bitmap filter, with a Sort on top of it:
--
--   Sort  (Sort Key: "nextLookupAt" NULLS FIRST, "startedAt")
--     -> Bitmap Heap Scan on "Payment"
--          Filter: ("nextLookupAt" IS NULL OR "nextLookupAt" <= now())
--          -> Bitmap Index Scan on "Payment_reconcilable_idx"
--
-- The OR is what does it. A bitmap scan is unordered, so Postgres sorts the
-- whole candidate set before the LIMIT can take fifty rows from it -- and that
-- set is every unsettled payment, not fifty. On a busy day the sort grows with
-- the backlog while the LIMIT stays the same, and this role carries
-- statement_timeout=15s.
--
-- Which is the same shape as the problem the index was added to solve, one layer
-- up: the plan reads the whole thing to return a page of it.
--
-- Removing the NULL removes the OR. A payment that has never been looked up is
-- due NOW -- a fact about the row, not an absence of one -- so the column
-- becomes NOT NULL and the predicate becomes a plain range.

-- ---------------------------------------------------------------------------
-- FORCE row security is lifted for the backfill, and restored immediately.
--
-- Migrations run as the table OWNER, which matches no tenant policy, so under
-- FORCE it sees ZERO rows. The first version of this migration did not do this:
-- the UPDATE silently affected nothing and SET NOT NULL then failed with
-- "column contains null values" on rows the UPDATE had never been allowed to
-- touch. Same pattern, and same reason, as 20260828130157.
-- ---------------------------------------------------------------------------
ALTER TABLE "Payment" NO FORCE ROW LEVEL SECURITY;

-- Due at the moment the attempt started, which is exactly what "NULLS FIRST,
-- then startedAt" meant. Ordering by nextLookupAt alone now reproduces the old
-- order rather than approximating it.
UPDATE "Payment" SET "nextLookupAt" = "startedAt" WHERE "nextLookupAt" IS NULL;

ALTER TABLE "Payment" FORCE ROW LEVEL SECURITY;

ALTER TABLE "Payment"
  ALTER COLUMN "nextLookupAt" SET DEFAULT now(),
  ALTER COLUMN "nextLookupAt" SET NOT NULL;

-- One key, no NULLS clause needed, and it can now serve the ORDER BY as well as
-- the filter -- so the LIMIT stops the scan instead of trimming a finished sort.
DROP INDEX "Payment_reconcilable_idx";

CREATE INDEX "Payment_reconcilable_idx"
  ON "Payment" ("nextLookupAt", "startedAt")
  WHERE "settledAt" IS NULL;
