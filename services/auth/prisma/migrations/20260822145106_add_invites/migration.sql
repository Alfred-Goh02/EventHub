-- CreateTable
CREATE TABLE "Invite" (
    "id" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "role" "Role" NOT NULL DEFAULT 'ATTENDEE',
    "tokenHash" TEXT NOT NULL,
    "invitedBy" TEXT NOT NULL,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "acceptedAt" TIMESTAMP(3),
    "revokedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Invite_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Invite_tokenHash_key" ON "Invite"("tokenHash");

-- CreateIndex
CREATE INDEX "Invite_tenantId_idx" ON "Invite"("tenantId");

-- CreateIndex
CREATE INDEX "Invite_tenantId_email_idx" ON "Invite"("tenantId", "email");

-- ---------------------------------------------------------------------------
-- Explicit grant. There is no ALTER DEFAULT PRIVILEGES in this database, so a
-- new table starts with NO access for the runtime role. That is deliberate: it
-- means forgetting the RLS block below would also mean forgetting this grant,
-- and the service fails loudly with "permission denied for table Invite"
-- instead of quietly serving one tenant's invites to another.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE, DELETE ON "Invite" TO eventhub_auth_app;

-- ---------------------------------------------------------------------------
-- "Invite" is deliberately EXEMPT from a tenant policy, for the same structural
-- reason as "RefreshToken":
--
--   /register looks an invite up by its token hash, and the tenant is the
--   RESULT of that lookup, not an input to it. A tenant policy would filter the
--   row out before it could tell us which tenant to filter by.
--
-- What protects it instead: the token is 32 bytes of crypto-random entropy and
-- only its SHA-256 hash is stored, so a row cannot be found without already
-- holding the secret — the same argument as RefreshToken.
--
-- The cost, stated plainly: admin-facing reads of this table (listing or
-- revoking invites) are NOT protected by the database. They must filter by the
-- tenant from the verified JWT in application code. This is the one table in
-- this database where a forgotten WHERE clause leaks across tenants.
-- ---------------------------------------------------------------------------
