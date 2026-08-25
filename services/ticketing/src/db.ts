import { PrismaPg } from '@prisma/adapter-pg';
import { loadBaseEnv } from '@eventhub/config';
import { PrismaClient, type Prisma } from './generated/prisma/client.js';

loadBaseEnv();

const runtimeUrl = process.env['APP_DATABASE_URL'];

if (!runtimeUrl) {
  throw new Error(
    'APP_DATABASE_URL is required — it is the restricted role that RLS policies apply to',
  );
}

const adapter = new PrismaPg({
  connectionString: runtimeUrl,
  /**
   * An explicit pool size, so the number is a decision rather than a library
   * default nobody knew about.
   *
   * It matters more here than in the other services. Reservations contend on a
   * single TicketType row: every request for the same ticket type queues behind
   * whoever holds that row lock. A pool larger than the database can usefully
   * serve just moves the queue from the application into Postgres, where it is
   * harder to see.
   */
  max: Number(process.env['DB_POOL_MAX'] ?? 20),
});

export const prisma = new PrismaClient({ adapter });

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
}

/**
 * Runs `fn` in a transaction with the tenant context set.
 *
 * NO NETWORK CALL MAY APPEAR INSIDE THE CALLBACK.
 *
 * That rule matters more in this service than in the others. A reservation
 * holds a row lock on a TicketType from the moment it decrements until COMMIT,
 * and every other buyer for that ticket type is queued behind it. Awaiting a
 * payment provider — or anything else off-box — inside that window converts a
 * millisecond lock into a multi-second one, and throughput for that ticket type
 * collapses to one sale per round trip.
 *
 * The payment path therefore commits the HELD -> PENDING_PAYMENT transition
 * first, calls the provider outside any transaction, and commits the result
 * separately.
 */
export async function withTenant<T>(
  tenantId: string,
  fn: (tx: Prisma.TransactionClient) => Promise<T>,
  options?: { isolationLevel?: Prisma.TransactionIsolationLevel; timeout?: number },
): Promise<T> {
  return prisma.$transaction(async (tx) => {
    await tx.$executeRaw`SELECT set_config('app.tenant_id', ${tenantId}, true)`;
    return fn(tx);
  }, options);
}

/**
 * withTenant plus a retry for serialisation failures and deadlocks.
 *
 * Reservations contend, so these are expected rather than exceptional:
 *   40001 / P2034 — serialisation failure or write conflict
 *   40P01         — deadlock, e.g. a multi-type cart locking rows in a
 *                   different order from a concurrent one
 *
 * The retry is bounded and jittered. Unbounded retries turn a contention spike
 * into a stampede, and un-jittered ones make every loser retry at the same
 * instant and collide again.
 */
export async function withTenantRetry<T>(
  tenantId: string,
  fn: (tx: Prisma.TransactionClient) => Promise<T>,
  options?: { isolationLevel?: Prisma.TransactionIsolationLevel; attempts?: number },
): Promise<T> {
  const maxAttempts = options?.attempts ?? 3;
  let lastError: unknown;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await withTenant(tenantId, fn, { isolationLevel: options?.isolationLevel });
    } catch (err) {
      lastError = err;
      const code = (err as { code?: string }).code;
      const retryable = code === 'P2034' || code === '40001' || code === '40P01';
      if (!retryable || attempt === maxAttempts) throw err;

      const backoff = 10 * 2 ** (attempt - 1);
      const jitter = Math.floor(Math.random() * backoff);
      await new Promise((r) => setTimeout(r, backoff + jitter));
    }
  }

  throw lastError;
}
