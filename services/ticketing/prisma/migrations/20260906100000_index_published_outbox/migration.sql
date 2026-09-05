-- An index for the half of the table the prune actually reads.
--
-- The only index that mentioned publishedAt was
--
--   ("seq") WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL
--
-- which is the exact OPPOSITE of what a retention sweep wants. It exists for
-- the drain, and it self-empties as rows are published; a prune asking for rows
-- that ARE published could not use it and would sequentially scan the whole
-- table, finding almost nothing, forever.
--
-- That is not merely slow. The relay role carries statement_timeout=15s (see
-- db/init/01-bootstrap.sql), so past a certain table size the prune stops
-- completing at all -- and it stops in the direction where the table keeps
-- growing, which is the failure the prune exists to prevent.
--
-- Partial in the mirror image of the drain's index, so the two together stay
-- small: this one holds only rows that are published and not dead-lettered,
-- i.e. exactly the prune's candidates, and rows leave it when they are deleted.
CREATE INDEX "OutboxMessage_prunable_idx"
  ON "OutboxMessage" ("publishedAt")
  WHERE "publishedAt" IS NOT NULL AND "deadLetteredAt" IS NULL;
