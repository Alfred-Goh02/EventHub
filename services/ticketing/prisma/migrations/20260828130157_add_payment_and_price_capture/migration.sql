-- CreateEnum
CREATE TYPE "PaymentStatus" AS ENUM ('PENDING', 'SUCCEEDED', 'FAILED', 'UNKNOWN');

-- ---------------------------------------------------------------------------
-- unitPriceCents: add nullable, backfill, then enforce NOT NULL.
--
-- Prisma generated a single NOT NULL add and warned it cannot work on a
-- non-empty table. The three-step version is also what you would do against a
-- live table, where one NOT NULL add takes an ACCESS EXCLUSIVE lock and rewrites
-- the whole thing.
--
-- The backfill needs FORCE suspended, and the reason is the same one this
-- database has taught twice already: migrations run as eventhub_ticketing_owner,
-- which matches no tenant policy, so under FORCE it sees ZERO rows. The UPDATE
-- would silently affect nothing and SET NOT NULL would then fail on rows it
-- never touched. FORCE is restored immediately afterwards.
-- ---------------------------------------------------------------------------
ALTER TABLE "ReservationItem" ADD COLUMN "unitPriceCents" INTEGER;

ALTER TABLE "ReservationItem" NO FORCE ROW LEVEL SECURITY;
ALTER TABLE "TicketType"      NO FORCE ROW LEVEL SECURITY;

-- Existing holds predate price capture, so the current price is the best
-- available answer. It is also harmless: every one of them is already EXPIRED.
UPDATE "ReservationItem" i
   SET "unitPriceCents" = t."priceCents"
  FROM "TicketType" t
 WHERE t.id = i."ticketTypeId"
   AND t."tenantId" = i."tenantId"
   AND i."unitPriceCents" IS NULL;

-- Anything the join could not resolve gets 0 rather than blocking the migration.
UPDATE "ReservationItem" SET "unitPriceCents" = 0 WHERE "unitPriceCents" IS NULL;

ALTER TABLE "TicketType"      FORCE ROW LEVEL SECURITY;
ALTER TABLE "ReservationItem" FORCE ROW LEVEL SECURITY;

ALTER TABLE "ReservationItem" ALTER COLUMN "unitPriceCents" SET NOT NULL;

ALTER TABLE "ReservationItem"
  ADD CONSTRAINT "ReservationItem_price_non_negative"
  CHECK ("unitPriceCents" >= 0);

-- ---------------------------------------------------------------------------
-- Payment: one row per ATTEMPT to charge.
--
-- Written BEFORE the provider is called, which is the whole point. A process
-- that dies mid-charge otherwise leaves a reservation stuck in PENDING_PAYMENT
-- with nothing recording that a charge was attempted, no provider reference to
-- ask about, and no amount to compare — leaving only two bad options: release
-- the seats when the money may have moved, or hold them forever.
-- ---------------------------------------------------------------------------
CREATE TABLE "Payment" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "reservationId" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "amountCents" INTEGER NOT NULL,
    "currency" TEXT NOT NULL DEFAULT 'SGD',
    "status" "PaymentStatus" NOT NULL DEFAULT 'PENDING',
    "providerRef" TEXT,
    "failureReason" TEXT,
    "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "settledAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Payment_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "Payment_tenantId_reservationId_idx" ON "Payment"("tenantId", "reservationId");
CREATE UNIQUE INDEX "Payment_tenantId_id_key" ON "Payment"("tenantId", "id");

-- The reconciliation job's index. Partial, so it holds only attempts that never
-- settled and stays small no matter how much payment history accumulates —
-- the same reasoning as the outbox's undelivered index.
CREATE INDEX "Payment_unsettled_idx"
  ON "Payment" ("startedAt")
  WHERE "settledAt" IS NULL;

ALTER TABLE "Payment"
  ADD CONSTRAINT "Payment_amount_non_negative"
  CHECK ("amountCents" >= 0);

-- A settled payment must say WHEN, and an unsettled one must not claim to have.
-- Cheap to state, and it makes "settled" mean exactly one thing.
ALTER TABLE "Payment"
  ADD CONSTRAINT "Payment_settled_consistency"
  CHECK (
    ("status" IN ('SUCCEEDED', 'FAILED') AND "settledAt" IS NOT NULL)
    OR ("status" IN ('PENDING', 'UNKNOWN') AND "settledAt" IS NULL)
  );

-- ---------------------------------------------------------------------------
-- Grants. Explicit per table, as everywhere in this database.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON "Payment" TO eventhub_ticketing_app;

-- The reconciliation job runs as the sweeper: it is the existing cross-tenant
-- role, and asking "which attempts never settled" is the same shape of question
-- as "which holds expired" — neither has a tenant.
GRANT SELECT ON "Payment" TO eventhub_ticketing_sweeper;
GRANT UPDATE ("status", "providerRef", "failureReason", "settledAt", "updatedAt")
  ON "Payment" TO eventhub_ticketing_sweeper;

-- Reconciliation ends an attempt in one of two directions, and the sweeper's
-- existing grants only cover one of them.
--
-- On FAILURE it releases: remaining += n, held -= n. Already granted.
-- On SUCCESS it settles:  held -= n, sold += n -- and "sold" was NEVER granted,
-- because until now nothing outside the request path could sell a seat. A
-- reconciler that finds the money did move must be able to finish the sale, or
-- it can only ever resolve half the cases it exists to resolve.
GRANT UPDATE ("sold") ON "TicketType" TO eventhub_ticketing_sweeper;

-- And it must be able to mark the reservation CONFIRMED, which needs the column
-- that records when.
GRANT UPDATE ("confirmedAt", "paymentStartedAt") ON "Reservation" TO eventhub_ticketing_sweeper;

-- ---------------------------------------------------------------------------
-- Row-level security. No exemptions in this database.
-- ---------------------------------------------------------------------------
ALTER TABLE "Payment" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "Payment" FORCE  ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON "Payment" TO eventhub_ticketing_app
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

-- The reconciliation job crosses tenants by design, exactly like the sweep:
-- "which payments never settled" is a question about the whole database.
CREATE POLICY reconciler_all ON "Payment" TO eventhub_ticketing_sweeper
  USING (true) WITH CHECK (true);
