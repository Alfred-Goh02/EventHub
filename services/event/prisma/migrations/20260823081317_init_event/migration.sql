-- CreateEnum
CREATE TYPE "EventStatus" AS ENUM ('DRAFT', 'PUBLISHED', 'CANCELLED', 'COMPLETED');

-- CreateTable
CREATE TABLE "Venue" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "address" TEXT,
    "capacity" INTEGER,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Venue_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Event" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "organiserId" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "slug" TEXT NOT NULL,
    "description" TEXT,
    "status" "EventStatus" NOT NULL DEFAULT 'DRAFT',
    "capacity" INTEGER NOT NULL,
    "startsAt" TIMESTAMP(3) NOT NULL,
    "endsAt" TIMESTAMP(3) NOT NULL,
    "version" INTEGER NOT NULL DEFAULT 1,
    "deletedAt" TIMESTAMP(3),
    "venueId" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Event_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Session" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "eventId" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "speaker" TEXT,
    "startsAt" TIMESTAMP(3) NOT NULL,
    "endsAt" TIMESTAMP(3) NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "Session_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "OutboxMessage" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "seq" BIGSERIAL NOT NULL,
    "eventType" TEXT NOT NULL,
    "schemaVersion" INTEGER NOT NULL DEFAULT 1,
    "aggregateId" TEXT NOT NULL,
    "payload" JSONB NOT NULL,
    "publishedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "OutboxMessage_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "Venue_tenantId_idx" ON "Venue"("tenantId");

-- CreateIndex
CREATE UNIQUE INDEX "Venue_tenantId_id_key" ON "Venue"("tenantId", "id");

-- CreateIndex
CREATE INDEX "Event_tenantId_idx" ON "Event"("tenantId");

-- CreateIndex
CREATE INDEX "Event_tenantId_status_idx" ON "Event"("tenantId", "status");

-- CreateIndex
CREATE INDEX "Event_tenantId_startsAt_idx" ON "Event"("tenantId", "startsAt");

-- CreateIndex
CREATE UNIQUE INDEX "Event_tenantId_id_key" ON "Event"("tenantId", "id");

-- CreateIndex
CREATE INDEX "Session_tenantId_eventId_idx" ON "Session"("tenantId", "eventId");

-- CreateIndex
CREATE UNIQUE INDEX "Session_tenantId_id_key" ON "Session"("tenantId", "id");

-- CreateIndex
CREATE INDEX "OutboxMessage_publishedAt_seq_idx" ON "OutboxMessage"("publishedAt", "seq");

-- CreateIndex
CREATE INDEX "OutboxMessage_tenantId_idx" ON "OutboxMessage"("tenantId");

-- AddForeignKey
ALTER TABLE "Event" ADD CONSTRAINT "Event_tenantId_venueId_fkey" FOREIGN KEY ("tenantId", "venueId") REFERENCES "Venue"("tenantId", "id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Session" ADD CONSTRAINT "Session_tenantId_eventId_fkey" FOREIGN KEY ("tenantId", "eventId") REFERENCES "Event"("tenantId", "id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- ===========================================================================
-- Everything below is hand-written. Prisma never emits row-level security, so
-- a table added without this block would be readable across every tenant with
-- no error raised anywhere.
--
-- The three pieces RLS needs, all of which must be present:
--   1. A policy filtering on a per-transaction setting.
--   2. FORCE plus a non-owner runtime role. An owner silently bypasses its own
--      policies; a SUPERUSER bypasses them even with FORCE.
--   3. set_config(..., true) inside a transaction at runtime, because a bare
--      SET persists on a pooled connection into the next request.
--
-- Grants are explicit per table. There is no ALTER DEFAULT PRIVILEGES in this
-- database, deliberately: it means forgetting an RLS block also means
-- forgetting the grant, so the service fails loudly with "permission denied"
-- rather than quietly serving another tenant's rows. Fail closed.
-- ===========================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON "Venue"         TO eventhub_event_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "Event"         TO eventhub_event_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "Session"       TO eventhub_event_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "OutboxMessage" TO eventhub_event_app;

-- The relay only ever reads undelivered rows and marks them published. It never
-- inserts and never deletes, so it gets exactly those two privileges.
GRANT SELECT, UPDATE ON "OutboxMessage" TO eventhub_event_relay;
GRANT USAGE, SELECT ON SEQUENCE "OutboxMessage_seq_seq" TO eventhub_event_app;

ALTER TABLE "Venue"   ENABLE ROW LEVEL SECURITY;
ALTER TABLE "Venue"   FORCE  ROW LEVEL SECURITY;
ALTER TABLE "Event"   ENABLE ROW LEVEL SECURITY;
ALTER TABLE "Event"   FORCE  ROW LEVEL SECURITY;
ALTER TABLE "Session" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "Session" FORCE  ROW LEVEL SECURITY;
ALTER TABLE "OutboxMessage" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "OutboxMessage" FORCE  ROW LEVEL SECURITY;

-- USING filters rows that already exist; WITH CHECK constrains rows being
-- written. Without WITH CHECK a caller could INSERT a row belonging to another
-- tenant, or UPDATE one of its own rows to move it out - silent corruption
-- rather than a leak, so nothing alerts.
CREATE POLICY tenant_isolation ON "Venue"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "Event"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY tenant_isolation ON "Session"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

-- OutboxMessage carries TWO policies because it has two readers with different
-- needs. The service is tenant-scoped like everything else. The relay drains by
-- DELIVERY STATE, not by tenant, so it must see every tenant's undelivered rows
-- - which is why it is a separate role with its own policy rather than a reason
-- to reach for the superuser.
CREATE POLICY outbox_tenant ON "OutboxMessage" TO eventhub_event_app
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));

CREATE POLICY outbox_relay ON "OutboxMessage" TO eventhub_event_relay
  USING (true);

-- Slug is unique only among LIVE rows, so a deleted event's slug can be reused.
-- Prisma cannot express a partial index, so this is raw SQL and the model
-- deliberately carries no @@unique([tenantId, slug]) - if it did, Prisma would
-- adopt this index and then generate a findUnique that can match several
-- soft-deleted rows for a call typed to return at most one.
CREATE UNIQUE INDEX "Event_tenantId_slug_live"
  ON "Event" ("tenantId", "slug")
  WHERE "deletedAt" IS NULL;
