/**
 * Development seed. Recreates the tenants and first users that would otherwise
 * have to be typed into psql by hand after every `docker compose down -v`.
 *
 * Idempotent: uses upsert throughout, so running it twice changes nothing.
 *
 * Note the asymmetry between the two halves, because it is the RLS design
 * showing through:
 *
 *   Tenant rows insert normally. "Tenant" has no tenant policy — it is the
 *   table that ESTABLISHES tenant context, so it cannot depend on it.
 *
 *   User rows do NOT. "User" has FORCE ROW LEVEL SECURITY and a WITH CHECK
 *   clause, and this script connects as eventhub_auth_owner, which is
 *   deliberately not a superuser. So an INSERT with no app.tenant_id set is
 *   rejected outright. Each user is created inside a transaction that sets the
 *   tenant first — exactly what withTenant() does at runtime.
 */

import { PrismaPg } from '@prisma/adapter-pg';
import { hash } from '@node-rs/argon2';
import { PrismaClient } from '../src/generated/prisma/client.js';

const url = process.env['DATABASE_URL'];
if (!url) throw new Error('DATABASE_URL is required to seed (the migration/owner role)');

const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: url }) });

const TENANTS = [
  { id: 'tenant_acme', name: 'Acme Corp', slug: 'acme' },
  { id: 'tenant_globex', name: 'Globex', slug: 'globex' },
];

// The same email in both tenants, on purpose: it demonstrates that
// @@unique([tenantId, email]) scopes identity per tenant rather than globally.
const USERS = [
  { tenantId: 'tenant_acme', email: 'alice@acme.com', password: 'hunter2222' },
  { tenantId: 'tenant_globex', email: 'alice@acme.com', password: 'hunter2222' },
];

async function main() {
  for (const t of TENANTS) {
    await prisma.tenant.upsert({
      where: { id: t.id },
      update: { name: t.name, slug: t.slug },
      create: t,
    });
    console.log(`tenant  ${t.id} (${t.slug})`);
  }

  for (const u of USERS) {
    const passwordHash = await hash(u.password);

    await prisma.$transaction(async (tx) => {
      // Without this the INSERT fails: "new row violates row-level security
      // policy for table User". The seed is subject to the same rules the
      // service is, which is the point of not seeding as a superuser.
      await tx.$executeRaw`SELECT set_config('app.tenant_id', ${u.tenantId}, true)`;

      await tx.user.upsert({
        where: { tenantId_email: { tenantId: u.tenantId, email: u.email } },
        update: {},
        create: { tenantId: u.tenantId, email: u.email, passwordHash },
      });
    });

    console.log(`user    ${u.email} in ${u.tenantId}`);
  }
}

main()
  .then(() => console.log('seed complete'))
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
