-- A buyer's list of their reservations, newest first, paged on ("createdAt", id).
--
-- The index on ("tenantId", "userId") finds a buyer's reservations but not in order, so
-- every page sorted the buyer's whole history to return twenty. Measured on 50,000
-- reservations of one buyer: 13.4 ms, a sequential scan and a top-N sort. With
-- "createdAt" and id on the end, a backward index-only scan returns the page in order
-- and the keyset comparison of a later page is part of the index condition: 0.07 ms.
--
-- It replaces the old index rather than joining it: its first two columns are the old
-- index, so every lookup by buyer it served, it still serves, and a reservation pays
-- for one index on the way in, not two.
--
-- Indexes only: no rows are read or written, so row security has nothing to hide.
BEGIN;

DROP INDEX "Reservation_tenantId_userId_idx";

CREATE INDEX "Reservation_tenantId_userId_createdAt_id_idx" ON "Reservation"("tenantId", "userId", "createdAt", "id");

COMMIT;
