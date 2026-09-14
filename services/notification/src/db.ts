import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient, type Prisma } from './generated/prisma/client.js';
import { env } from './env.js';

/**
 * The same shape as every other service's db.ts, repeated rather than shared for
 * the reason services/live/src/db.ts gives: a shared package would have to export
 * a PrismaClient typed against one service's schema.
 */

// The running service connects as a role that owns nothing. A table OWNER
// silently bypasses its own RLS policies, and a SUPERUSER bypasses them even with
// FORCE, so this is the whole mechanism rather than defence in depth.
const adapter = new PrismaPg({ connectionString: env.APP_DATABASE_URL });

export const prisma = new PrismaClient({ adapter });

/**
 * Refuses to start if the runtime credential can bypass row security.
 *
 * A misconfigured connection string does not produce an error; it produces a
 * service that reads every tenant's rows and looks healthy. The Postgres image
 * makes POSTGRES_USER a superuser, and this project's Auth once ran as exactly
 * that, with RLS configured correctly and enforcing nothing.
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
 * query it makes. LOCAL (the `true`), so the setting dies with the transaction
 * instead of leaking into whichever request next borrows the pooled connection,
 * and a bound parameter rather than interpolation into SET LOCAL.
 *
 * THE TENANT ID PASSED HERE COMES FROM THE MESSAGE BEING HANDLED. A notification
 * is triggered by an event, and every event this service consumes carries the
 * tenant it belongs to.
 */
export async function withTenant<T>(tenantId: string, fn: (tx: Prisma.TransactionClient) => Promise<T>): Promise<T> {
  return prisma.$transaction(async (tx) => {
    await tx.$executeRaw`SELECT set_config('app.tenant_id', ${tenantId}, true)`;
    return fn(tx);
  });
}
