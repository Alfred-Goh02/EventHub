import { PrismaPg } from '@prisma/adapter-pg';
import { loadBaseEnv } from '@eventhub/config';
import { PrismaClient, type Prisma } from './generated/prisma/client.js';

loadBaseEnv();

// The running service connects as a role that owns nothing. This is not
// defence in depth, it is the whole mechanism: a table OWNER silently bypasses
// its own RLS policies, and a SUPERUSER bypasses them even with FORCE.
const runtimeUrl = process.env['APP_DATABASE_URL'];

if (!runtimeUrl) {
  throw new Error(
    'APP_DATABASE_URL is required — it is the restricted role that RLS policies apply to',
  );
}

const adapter = new PrismaPg({ connectionString: runtimeUrl });

export const prisma = new PrismaClient({ adapter });

/**
 * Refuses to start if the runtime credential can bypass row security.
 *
 * Worth the twenty lines: a misconfigured connection string does not produce an
 * error, it produces a service that returns every tenant's data and looks
 * perfectly healthy. That is exactly how this project's own Auth service ran
 * for a while — the Postgres Docker image makes POSTGRES_USER a superuser, so
 * RLS was configured correctly and enforcing nothing.
 */
export async function assertRuntimeRoleCannotBypassRls(): Promise<void> {
  const rows = await prisma.$queryRaw<Array<{ rolsuper: boolean; rolbypassrls: boolean }>>`
    SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user
  `;

  const role = rows[0];
  if (!role) throw new Error('could not read current_user from pg_roles');

  if (role.rolsuper || role.rolbypassrls) {
    throw new Error(
      'refusing to start: APP_DATABASE_URL connects as a role that BYPASSES row-level security ' +
        `(rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls}). ` +
        'Tenant isolation would be silently disabled.',
    );
  }
}

/**
 * Runs `fn` in a transaction with the tenant context set, so RLS filters every
 * query it makes.
 *
 * The `true` third argument makes the setting LOCAL — it dies with the
 * transaction. A non-local SET persists on the pooled connection and leaks into
 * whichever request picks that connection up next, which is a cross-tenant leak
 * that only appears under concurrency.
 *
 * set_config with a bound parameter, not string interpolation into SET LOCAL,
 * which would be an injection hole.
 */
export async function withTenant<T>(
  tenantId: string,
  fn: (tx: Prisma.TransactionClient) => Promise<T>,
): Promise<T> {
  return prisma.$transaction(async (tx) => {
    await tx.$executeRaw`SELECT set_config('app.tenant_id', ${tenantId}, true)`;
    return fn(tx);
  });
}
