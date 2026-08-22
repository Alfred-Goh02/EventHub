-- Row-Level Security: tenant isolation enforced by Postgres, not by application code.
--
-- Three pieces are required, and all three matter:
--   1. A policy filtering on a per-transaction session variable.
--   2. FORCE + a non-owner runtime role. A table owner silently BYPASSES its own
--      policies, and a SUPERUSER bypasses them even with FORCE. Roles are created
--      in db/init/01-bootstrap.sql: eventhub_auth_owner migrates and owns, and is
--      deliberately NOT a superuser; eventhub_auth_app runs the service.
--   3. set_config(..., true) inside a transaction at runtime. A bare SET persists
--      on the connection, and Prisma pools connections, so the next request would
--      inherit the previous request's tenant.
--
-- INVARIANT for every table added to this database from now on:
--   no unique index may omit tenantId, because unique enforcement is NOT subject
--   to row security — a cross-tenant duplicate would be rejected with a 23505 that
--   reveals the existence of another tenant's row.
--
-- Tables deliberately WITHOUT a tenant policy, and why:
--   "Tenant"       - must be readable before any tenant context exists; resolving a
--                    subdomain slug to a tenant id is what establishes that context.
--   "RefreshToken" - located by a 256-bit token hash, so the hash IS the access
--                    control. The tenant is a result of the lookup, not an input to
--                    it, so a policy would make /refresh impossible.

-- Grants are explicit per table, NOT via ALTER DEFAULT PRIVILEGES.
--
-- This is a deliberate choice and the reason is worth keeping: Prisma never emits
-- RLS, so a future table's migration can easily forget its policy block. With
-- default privileges, that table would inherit full read/write access and be
-- visible across every tenant, with no error raised anywhere - it fails OPEN.
-- Granting explicitly means a forgotten policy is also a forgotten grant, and the
-- service fails loudly with "permission denied for table X". Fail closed.
GRANT SELECT, INSERT, UPDATE, DELETE ON "Tenant"       TO eventhub_auth_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "User"         TO eventhub_auth_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON "RefreshToken" TO eventhub_auth_app;

ALTER TABLE "User" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "User" FORCE ROW LEVEL SECURITY;

-- USING filters rows that already exist (SELECT/UPDATE/DELETE).
-- WITH CHECK constrains rows being written (INSERT/UPDATE) - without it, a caller
-- could insert a row belonging to another tenant, or move one of its own rows out.
-- current_setting(..., true) returns NULL when unset, and "tenantId" = NULL is never
-- true, so the failure mode is "see nothing" rather than "see everything".
CREATE POLICY tenant_isolation ON "User"
  USING ("tenantId" = current_setting('app.tenant_id', true))
  WITH CHECK ("tenantId" = current_setting('app.tenant_id', true));
