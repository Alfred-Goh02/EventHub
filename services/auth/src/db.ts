import { PrismaPg } from '@prisma/adapter-pg';
import { loadBaseEnv } from '@eventhub/config';
import { PrismaClient, type Prisma } from './generated/prisma/client.js';

// Validates DATABASE_URL and friends at startup. DATABASE_URL is the OWNER
// connection and is used by prisma.config.ts for migrations only.
loadBaseEnv();

// The running service connects as a separate, non-owner role. This is not
// belt-and-braces: a table owner silently bypasses its own RLS policies, so
// connecting as the owner would disable tenant isolation entirely.
const runtimeUrl = process.env['APP_DATABASE_URL'];

if (!runtimeUrl) {
  throw new Error(
    'APP_DATABASE_URL is required — it is the restricted role that RLS policies apply to',
  );
}

const adapter = new PrismaPg({ connectionString: runtimeUrl });

export const prisma = new PrismaClient({ adapter });

/**
 * Runs `fn` inside a transaction with the tenant context set, so every query
 * it makes is filtered by the RLS policy on tenant-scoped tables.
 *
 * The `true` third argument to set_config makes the setting LOCAL — it dies
 * with the transaction. A non-local setting would persist on the pooled
 * connection and leak into whichever request picked that connection up next.
 *
 * set_config with a bound parameter is used rather than string-interpolating
 * into `SET LOCAL`, which would be a SQL injection hole.
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

/**
 * The database's clock, plus `plusMs`, read on the client or transaction given.
 *
 * For every expiry this service writes: a reset token's and a refresh token's. Each is
 * judged against now() in SQL -- by the lookups and by the guarded claims that spend
 * them -- so it is stamped on the clock it is judged by. `new Date(Date.now() + ttl)`
 * was this process's clock instead: measured with Auth an hour fast, a thirty-minute
 * reset link was stored to expire 5400 s after the database's now(), and a link the
 * database said had thirty minutes left was refused as expired. See "Timestamps come
 * from the database" in CLAUDE.md.
 *
 * Read and passed, rather than written into the statement, because Prisma has no way
 * to put an expression in `data` -- the same reason Ticketing's databaseNow gives.
 * An invitation's expiry is read the same way, in invites.ts.
 */
export async function databaseNow(client: Prisma.TransactionClient, plusMs = 0): Promise<Date> {
  const rows = await client.$queryRaw<Array<{ at: Date }>>`
    SELECT now() + (${plusMs}::bigint * interval '1 millisecond') AS at`;
  const at = rows[0]?.at;
  if (!(at instanceof Date)) throw new Error('the database returned no time to stamp an expiry with');
  return at;
}

/**
 * Refuses to start if the runtime role can bypass row-level security.
 *
 * Ticketing and the relay have asserted this since they were built; auth did
 * not — and auth is the service the check was written for. Its RLS migration
 * spells out that a table owner silently bypasses its own policies and a
 * superuser bypasses them even with FORCE, then relies on APP_DATABASE_URL
 * pointing at the right role. Nothing verified that at runtime.
 *
 * The failure it catches is the quiet one: point APP_DATABASE_URL at
 * eventhub_auth_owner by accident — a copy-paste between two adjacent lines of
 * the same .env — and every route keeps working, every test passes, and tenant
 * isolation is simply off. There is no error, because bypassing a policy is not
 * an error condition. It is the feature working as designed for the wrong role.
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
        `(rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls})`,
    );
  }

  /**
   * And that it is not the table OWNER, which is the likelier mistake.
   *
   * An owner is neither superuser nor rolbypassrls, so the check above passes
   * for it — but ownership bypasses FORCE just as completely. Asking Postgres
   * who owns the table is the only way to tell the two roles apart.
   */
  const owned = await prisma.$queryRaw<Array<{ count: bigint }>>`
    SELECT count(*) AS count
      FROM pg_class
     WHERE relnamespace = 'public'::regnamespace
       AND relkind = 'r'
       AND pg_get_userbyid(relowner) = current_user
  `;

  if (Number(owned[0]?.count ?? 0) > 0) {
    throw new Error(
      'refusing to start: APP_DATABASE_URL connects as the OWNER of its tables. An owner ' +
        'bypasses its own row-level security policies even under FORCE, so tenant isolation ' +
        'would be silently disabled. Use the app role, not the owner role.',
    );
  }
}
