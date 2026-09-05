-- Remove the idempotency-key lease, and the rows only it could act on.
--
-- The claim used ON CONFLICT ... DO UPDATE to TAKE A KEY OVER once its
-- 60-second lease expired, which means it ran the work again. For
-- POST /reservations, "run the work again" is a SECOND HOLD -- the exact thing
-- an Idempotency-Key exists to prevent. Reproduced by planting such a row: one
-- key, two reservations, held 1 -> 2.
--
-- The lease existed for a crash between claiming and completing. That cannot
-- happen: the claim and the work commit in ONE transaction, so a crash rolls
-- both back and no claimed-but-unfinished row is left behind. Measured after
-- that became true -- 12 concurrent requests on one key gave 12 x 201, a single
-- reservation id, and zero in-flight 409s.
--
-- 1. CLEAR THE ROWS THE TAKEOVER COULD FIRE ON.
--
-- Every surviving 'in_progress' row predates the fix that made the claim and
-- the work share a transaction. They were committed by guards that RETURNED
-- instead of throwing -- and those guards fired BEFORE any reservation was
-- created, so no work was done under them and nothing is orphaned by removing
-- them.
--
-- They have to go. Without the takeover clause, each one would make its key
-- answer 409 forever.
DELETE FROM "IdempotencyKey" WHERE "state" = 'in_progress';

-- 2. DROP THE COLUMN AND ITS INDEX.
--
-- Nothing reads leaseExpiresAt once the takeover is gone, and a column named
-- for a mechanism that no longer exists is how the next reader comes to believe
-- in one. This repository has already had to correct two comments describing
-- controls that had moved.
--
-- The index goes with it, replaced by one on createdAt -- which is the honest
-- column for asking how old a key is. leaseExpiresAt only worked as an age
-- proxy by accident, because completing a key never updated it.
DROP INDEX IF EXISTS "IdempotencyKey_tenantId_leaseExpiresAt_idx";

ALTER TABLE "IdempotencyKey" DROP COLUMN "leaseExpiresAt";

CREATE INDEX "IdempotencyKey_tenantId_createdAt_idx"
  ON "IdempotencyKey"("tenantId", "createdAt");
