-- Password reset tokens.
--
-- ---------------------------------------------------------------------------
-- ROW-LEVEL SECURITY: DELIBERATELY EXEMPT. Read this before adding a policy.
-- ---------------------------------------------------------------------------
-- This table has no tenant policy, for the same structural reason as "Invite"
-- and "RefreshToken": it is located by a token hash BEFORE any tenant context
-- exists. app.tenant_id is what a policy filters on, and the whole point of the
-- lookup is to discover which tenant to set it to. A policy here would filter
-- out the very row that answers the question, and /reset-password would be
-- impossible rather than merely restricted.
--
-- What protects it instead is the token: 32 bytes of CSPRNG output, of which
-- only the SHA-256 is ever stored (see src/secret-token.ts). The hash IS the
-- access control. A database dump yields no usable reset links, and there is
-- nothing to enumerate.
--
-- The cost, stated plainly: any bug that reaches this table with a caller-
-- supplied id is cross-tenant by construction, because nothing below the
-- application will stop it. Every read of this table therefore goes through
-- lookupResetToken(), which takes a raw token and nothing else -- there is no
-- code path that accepts an id.
--
-- This exemption is ALSO listed in .github/workflows/ci.yml, and it has to be:
-- the "Assert no table is missing RLS" step fails the build otherwise. An
-- exemption nobody wrote down is indistinguishable from a policy somebody
-- forgot, and this repository fails closed on that difference.
--
-- ---------------------------------------------------------------------------
-- "tokenHash" is uniquely indexed WITHOUT "tenantId".
-- ---------------------------------------------------------------------------
-- The invariant recorded in 20260818113200_enable_rls says no unique index may
-- omit tenantId, because unique enforcement is not subject to row security and
-- a cross-tenant duplicate raises a 23505 that reveals another tenant's row.
--
-- This is the same documented exception "Invite" and "RefreshToken" already
-- take, and it is sound for the same reason: a collision between two 256-bit
-- random values is not reachable, so the 23505 that would leak is an event that
-- cannot occur. The invariant is about values a caller can CHOOSE -- an email,
-- a slug -- and a caller chooses nothing here.

-- CreateTable
CREATE TABLE "PasswordResetToken" (
    "id" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "tenantId" TEXT NOT NULL,
    "tokenHash" TEXT NOT NULL,
    "expiresAt" TIMESTAMP(3) NOT NULL,
    "usedAt" TIMESTAMP(3),
    "revokedAt" TIMESTAMP(3),
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "PasswordResetToken_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "PasswordResetToken_tokenHash_key" ON "PasswordResetToken"("tokenHash");

-- CreateIndex
CREATE INDEX "PasswordResetToken_userId_idx" ON "PasswordResetToken"("userId");

-- CreateIndex
CREATE INDEX "PasswordResetToken_tenantId_idx" ON "PasswordResetToken"("tenantId");

-- AddForeignKey
ALTER TABLE "PasswordResetToken" ADD CONSTRAINT "PasswordResetToken_userId_fkey" FOREIGN KEY ("userId") REFERENCES "User"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- ---------------------------------------------------------------------------
-- Grants, explicit per table, as everywhere in this database.
--
-- There is no ALTER DEFAULT PRIVILEGES here on purpose: a forgotten RLS block
-- should also be a forgotten grant, so the service dies with "permission denied
-- for table PasswordResetToken" rather than quietly serving rows across every
-- tenant. Fail closed.
-- ---------------------------------------------------------------------------
GRANT SELECT, INSERT, UPDATE ON "PasswordResetToken" TO eventhub_auth_app;

-- No DELETE. Nothing in this service deletes a reset token: they are marked
-- used or revoked and left in place, so the history of who asked for what
-- survives, and an expired row is inert because lookupResetToken rejects on
-- expiresAt. Withholding the grant means a future "cleanup" written in the
-- request path fails loudly instead of destroying that record.
