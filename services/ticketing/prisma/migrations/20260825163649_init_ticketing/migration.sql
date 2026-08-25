-- CreateEnum
CREATE TYPE "ReservationStatus" AS ENUM ('HELD', 'PENDING_PAYMENT', 'CONFIRMED', 'EXPIRED', 'CANCELLED');

-- CreateTable
CREATE TABLE "EventProjection" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "slug" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "capacity" INTEGER NOT NULL,
    "startsAt" TIMESTAMP(3) NOT NULL,
    "endsAt" TIMESTAMP(3) NOT NULL,
    "organiserId" TEXT NOT NULL,
    "deleted" BOOLEAN NOT NULL DEFAULT false,
    "sourceVersion" INTEGER NOT NULL,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "EventProjection_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "TicketType" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "eventId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "priceCents" INTEGER NOT NULL,
    "total" INTEGER NOT NULL,
    "remaining" INTEGER NOT NULL,
    "held" INTEGER NOT NULL DEFAULT 0,
    "sold" INTEGER NOT NULL DEFAULT 0,
    "version" INTEGER NOT NULL DEFAULT 1,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "TicketType_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Reservation" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "eventId" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "status" "ReservationStatus" NOT NULL DEFAULT 'HELD',
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "paymentStartedAt" TIMESTAMP(3),
    "confirmedAt" TIMESTAMP(3),
    "version" INTEGER NOT NULL DEFAULT 1,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Reservation_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ReservationItem" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "reservationId" TEXT NOT NULL,
    "ticketTypeId" TEXT NOT NULL,
    "quantity" INTEGER NOT NULL,

    CONSTRAINT "ReservationItem_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "IdempotencyKey" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "endpoint" TEXT NOT NULL,
    "requestHash" TEXT NOT NULL,
    "state" TEXT NOT NULL DEFAULT 'in_progress',
    "responseStatus" INTEGER,
    "responseBody" JSONB,
    "leaseExpiresAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "IdempotencyKey_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ProcessedMessage" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "consumerGroup" TEXT NOT NULL,
    "messageId" TEXT NOT NULL,
    "processedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "ProcessedMessage_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ConsumerCheckpoint" (
    "id" TEXT NOT NULL,
    "consumerGroup" TEXT NOT NULL,
    "topic" TEXT NOT NULL,
    "partition" INTEGER NOT NULL,
    "lastOffset" BIGINT NOT NULL,
    "lastMessageOccurred" TIMESTAMP(3),
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ConsumerCheckpoint_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OutboxMessage" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "seq" BIGSERIAL NOT NULL,
    "topic" TEXT NOT NULL,
    "eventType" TEXT NOT NULL,
    "schemaVersion" INTEGER NOT NULL DEFAULT 1,
    "aggregateId" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "publishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "lastError" TEXT,
    "nextAttemptAt" TIMESTAMP(3),
    "deadLetteredAt" TIMESTAMP(3),

    CONSTRAINT "OutboxMessage_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "EventProjection_tenantId_status_idx" ON "EventProjection"("tenantId", "status");

-- CreateIndex
CREATE UNIQUE INDEX "EventProjection_tenantId_id_key" ON "EventProjection"("tenantId", "id");

-- CreateIndex
CREATE INDEX "TicketType_tenantId_eventId_idx" ON "TicketType"("tenantId", "eventId");

-- CreateIndex
CREATE UNIQUE INDEX "TicketType_tenantId_id_key" ON "TicketType"("tenantId", "id");

-- CreateIndex
CREATE INDEX "Reservation_tenantId_userId_idx" ON "Reservation"("tenantId", "userId");

-- CreateIndex
CREATE INDEX "Reservation_tenantId_eventId_idx" ON "Reservation"("tenantId", "eventId");

-- CreateIndex
CREATE UNIQUE INDEX "Reservation_tenantId_id_key" ON "Reservation"("tenantId", "id");

-- CreateIndex
CREATE INDEX "ReservationItem_tenantId_ticketTypeId_idx" ON "ReservationItem"("tenantId", "ticketTypeId");

-- CreateIndex
CREATE UNIQUE INDEX "ReservationItem_tenantId_id_key" ON "ReservationItem"("tenantId", "id");

-- CreateIndex
CREATE UNIQUE INDEX "ReservationItem_tenantId_reservationId_ticketTypeId_key" ON "ReservationItem"("tenantId", "reservationId", "ticketTypeId");

-- CreateIndex
CREATE INDEX "IdempotencyKey_tenantId_leaseExpiresAt_idx" ON "IdempotencyKey"("tenantId", "leaseExpiresAt");

-- CreateIndex
CREATE UNIQUE INDEX "IdempotencyKey_tenantId_userId_endpoint_key_key" ON "IdempotencyKey"("tenantId", "userId", "endpoint", "key");

-- CreateIndex
CREATE INDEX "ProcessedMessage_processedAt_idx" ON "ProcessedMessage"("processedAt");

-- CreateIndex
CREATE UNIQUE INDEX "ProcessedMessage_tenantId_consumerGroup_messageId_key" ON "ProcessedMessage"("tenantId", "consumerGroup", "messageId");

-- CreateIndex
CREATE UNIQUE INDEX "ConsumerCheckpoint_consumerGroup_topic_partition_key" ON "ConsumerCheckpoint"("consumerGroup", "topic", "partition");

-- CreateIndex
CREATE INDEX "OutboxMessage_tenantId_idx" ON "OutboxMessage"("tenantId");

-- AddForeignKey
ALTER TABLE "TicketType" ADD CONSTRAINT "TicketType_tenantId_eventId_fkey" FOREIGN KEY ("tenantId", "eventId") REFERENCES "EventProjection"("tenantId", "id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ReservationItem" ADD CONSTRAINT "ReservationItem_tenantId_reservationId_fkey" FOREIGN KEY ("tenantId", "reservationId") REFERENCES "Reservation"("tenantId", "id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ReservationItem" ADD CONSTRAINT "ReservationItem_tenantId_ticketTypeId_fkey" FOREIGN KEY ("tenantId", "ticketTypeId") REFERENCES "TicketType"("tenantId", "id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- ===========================================================================
-- Everything below is hand-written. Prisma never emits RLS, CHECK constraints
-- or partial indexes, and each of these carries a decision.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- THE INVENTORY INVARIANT.
--
--   remaining + held + sold = total, and none of them may go negative.
--
-- This is what makes overselling impossible rather than unlikely. Every
-- reservation path decrements `remaining` with a guarded conditional UPDATE,
-- but a guard can be forgotten in one code path; a CHECK constraint cannot. If
-- a bug ever breaks the arithmetic the transaction fails loudly instead of
-- quietly selling a seat twice.
--
-- It also documents the model: `total` never moves, and the other three only
-- ever move between each other.
-- ---------------------------------------------------------------------------
ALTER TABLE "TicketType"
  ADD CONSTRAINT "TicketType_counts_sum"
  CHECK ("remaining" + "held" + "sold" = "total");

ALTER TABLE "TicketType"
  ADD CONSTRAINT "TicketType_counts_non_negative"
  CHECK ("remaining" >= 0 AND "held" >= 0 AND "sold" >= 0 AND "total" >= 0);

ALTER TABLE "ReservationItem"
  ADD CONSTRAINT "ReservationItem_quantity_positive"
  CHECK ("quantity" > 0);

-- ---------------------------------------------------------------------------
-- The sweeper's index, predicated on STATUS rather than on time.
--
-- The obvious move - a partial index over live holds, mirroring the
-- Event_tenantId_slug_live precedent - does not build:
--
--   CREATE INDEX ... WHERE "expiresAt" > now();
--   ERROR: functions in index predicate must be marked IMMUTABLE
--
-- now() is STABLE, not IMMUTABLE, and an index whose membership changed as the
-- clock advanced would be incoherent. So expiry is a swept STATE TRANSITION,
-- not a view. Predicating on status keeps the index tiny - only live holds are
-- in it - so the sweeper is an index scan over a small relation no matter how
-- much reservation history accumulates.
-- ---------------------------------------------------------------------------
CREATE INDEX "Reservation_sweep_idx"
  ON "Reservation" ("expiresAt")
  WHERE "status" = 'HELD';

-- Same reasoning as the Event service: a leading IS NULL column cannot supply
-- ORDER BY, so a composite index restricts rows and then sorts, degrading
-- exactly as the backlog grows.
CREATE INDEX "OutboxMessage_undelivered_idx"
  ON "OutboxMessage" ("seq")
  WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL;

-- ---------------------------------------------------------------------------
-- Grants, explicit per table. There is no ALTER DEFAULT PRIVILEGES in this
-- database: a forgotten RLS block should also be a forgotten grant, so the
-- service fails with "permission denied" rather than serving another tenant.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE ON "EventProjection"    TO eventhub_ticketing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "TicketType"         TO eventhub_ticketing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "Reservation"        TO eventhub_ticketing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "ReservationItem"    TO eventhub_ticketing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "IdempotencyKey"     TO eventhub_ticketing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "ProcessedMessage"   TO eventhub_ticketing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "ConsumerCheckpoint" TO eventhub_ticketing_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "OutboxMessage"      TO eventhub_ticketing_app;
GRANT USAGE, SELECT ON SEQUENCE "OutboxMessage_seq_seq"      TO eventhub_ticketing_app;

-- The relay: read rows, mark delivery state, prune. It must NOT be able to
-- rewrite tenantId or payload - that would let it publish one tenant's data
-- under another tenant's key.
GRANT SELECT ON "OutboxMessage" TO eventhub_ticketing_relay;
GRANT UPDATE ("publishedAt", "attempts", "lastError", "nextAttemptAt", "deadLetteredAt")
  ON "OutboxMessage" TO eventhub_ticketing_relay;
GRANT DELETE ON "OutboxMessage" TO eventhub_ticketing_relay;

-- The sweeper: read and update holds, return inventory, emit a release message.
-- It cannot create reservations or touch anything else.
GRANT SELECT ON "Reservation" TO eventhub_ticketing_sweeper;
GRANT UPDATE ("status", "version", "updatedAt") ON "Reservation" TO eventhub_ticketing_sweeper;
GRANT SELECT ON "ReservationItem" TO eventhub_ticketing_sweeper;
GRANT SELECT ON "TicketType" TO eventhub_ticketing_sweeper;
GRANT UPDATE ("remaining", "held", "version", "updatedAt") ON "TicketType" TO eventhub_ticketing_sweeper;
GRANT SELECT, INSERT ON "OutboxMessage" TO eventhub_ticketing_sweeper;
GRANT USAGE, SELECT ON SEQUENCE "OutboxMessage_seq_seq" TO eventhub_ticketing_sweeper;

-- ---------------------------------------------------------------------------
-- Row-level security. Every table, no exemptions.
--
-- Unlike Auth, nothing here is looked up before tenant context exists: every
-- request arrives with a verified JWT carrying tenantId, and the consumer reads
-- tenantId off the message before writing. So there is no equivalent of
-- RefreshToken or Invite, and this database's CI exemption list is empty.
-- ---------------------------------------------------------------------------
ALTER TABLE "EventProjection"    ENABLE ROW LEVEL SECURITY;
ALTER TABLE "EventProjection"    FORCE  ROW LEVEL SECURITY;
ALTER TABLE "TicketType"         ENABLE ROW LEVEL SECURITY;
ALTER TABLE "TicketType"         FORCE  ROW LEVEL SECURITY;
ALTER TABLE "Reservation"        ENABLE ROW LEVEL SECURITY;
ALTER TABLE "Reservation"        FORCE  ROW LEVEL SECURITY;
ALTER TABLE "ReservationItem"    ENABLE ROW LEVEL SECURITY;
ALTER TABLE "ReservationItem"    FORCE  ROW LEVEL SECURITY;
ALTER TABLE "IdempotencyKey"     ENABLE ROW LEVEL SECURITY;
ALTER TABLE "IdempotencyKey"     FORCE  ROW LEVEL SECURITY;
ALTER TABLE "ProcessedMessage"   ENABLE ROW LEVEL SECURITY;
ALTER TABLE "ProcessedMessage"   FORCE  ROW LEVEL SECURITY;
ALTER TABLE "ConsumerCheckpoint" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "ConsumerCheckpoint" FORCE  ROW LEVEL SECURITY;
ALTER TABLE "OutboxMessage"      ENABLE ROW LEVEL SECURITY;
ALTER TABLE "OutboxMessage"      FORCE  ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON "EventProjection"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "TicketType" TO eventhub_ticketing_app
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "Reservation" TO eventhub_ticketing_app
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "ReservationItem" TO eventhub_ticketing_app
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "IdempotencyKey"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "ProcessedMessage"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

-- ConsumerCheckpoint is per (group, topic, partition) and carries no tenantId -
-- Kafka partitions are not tenant-scoped. It is written only by the consumer
-- process, so it gets a role-targeted policy rather than a tenant predicate.
CREATE POLICY checkpoint_consumer ON "ConsumerCheckpoint" TO eventhub_ticketing_app
  USING (true) WITH CHECK (true);

-- The sweeper crosses tenants BY DESIGN: it selects by expiry, which has no
-- tenant. It gets its own role-targeted policies on the three tables it needs,
-- rather than a blanket bypass.
CREATE POLICY sweeper_reservations ON "Reservation" TO eventhub_ticketing_sweeper
  USING (true) WITH CHECK (true);
CREATE POLICY sweeper_items ON "ReservationItem" TO eventhub_ticketing_sweeper
  USING (true);
CREATE POLICY sweeper_inventory ON "TicketType" TO eventhub_ticketing_sweeper
  USING (true) WITH CHECK (true);

-- Outbox: tenant-scoped for the service, delivery-state-scoped for the relay,
-- insert-only for the sweeper. The same three-way split as the Event service.
CREATE POLICY outbox_tenant ON "OutboxMessage" TO eventhub_ticketing_app
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY outbox_relay ON "OutboxMessage" TO eventhub_ticketing_relay
  USING (true);

CREATE POLICY outbox_sweeper ON "OutboxMessage" TO eventhub_ticketing_sweeper
  USING (true) WITH CHECK (true);
