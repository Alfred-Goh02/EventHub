import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient, type Prisma } from './generated/prisma/client.js';
import { env } from './env.js';

/**
 * The same shape as every other service's db.ts, repeated rather than shared for the
 * reason services/live/src/db.ts gives: a shared package would have to export a
 * PrismaClient typed against one service's schema.
 */

// The running service connects as a role that owns nothing. A table OWNER silently
// bypasses its own RLS policies, and a SUPERUSER bypasses them even with FORCE, so
// this is the whole mechanism rather than defence in depth.
const adapter = new PrismaPg({ connectionString: env.APP_DATABASE_URL });

export const prisma = new PrismaClient({ adapter });

/**
 * Refuses to start if the runtime credential can bypass row security.
 *
 * A misconfigured connection string does not produce an error; it produces a service
 * that reads every tenant's rows and looks healthy.
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

  /**
   * AND IT MUST OWN NOTHING, which is a separate question from bypassing.
   *
   * With FORCE ROW LEVEL SECURITY the owner is filtered like anyone else, so the check
   * above passes for the owner's own connection — as it did the first time this was
   * run. What the owner can still do is turn FORCE off, drop the policy, or add a
   * table without one: isolation becomes a setting it controls rather than one applied
   * to it. The service's credential is meant to own nothing at all, and that is worth
   * asserting rather than assuming, because the wrong URL produces a service that
   * works perfectly until the day it matters.
   */
  const owned = await prisma.$queryRaw<Array<{ relname: string }>>`
    SELECT c.relname FROM pg_class c
     WHERE c.relnamespace = 'public'::regnamespace
       AND c.relkind IN ('r', 'p')
       AND pg_get_userbyid(c.relowner) = current_user
     ORDER BY c.relname
  `;
  if (owned.length > 0) {
    throw new Error(
      `refusing to start: APP_DATABASE_URL connects as a role that OWNS ${owned.length} table(s) here ` +
        `(${owned.map((t) => t.relname).join(', ')}). An owner can drop a policy or turn FORCE off, so ` +
        'tenant isolation would be a setting this process controls rather than one applied to it.',
    );
  }
}

/**
 * Runs `fn` in a transaction with the tenant context set, so RLS filters every query it
 * makes. LOCAL (the `true`), so the setting dies with the transaction instead of leaking
 * into whichever request next borrows the pooled connection, and a bound parameter
 * rather than interpolation into SET LOCAL.
 *
 * THE TENANT ID COMES FROM THE VERIFIED TOKEN, never from a header or a body field: a
 * request that could name its own tenant would need no other vulnerability.
 */
export async function withTenant<T>(tenantId: string, fn: (tx: Prisma.TransactionClient) => Promise<T>): Promise<T> {
  return prisma.$transaction(async (tx) => {
    await tx.$executeRaw`SELECT set_config('app.tenant_id', ${tenantId}, true)`;
    return fn(tx);
  });
}
