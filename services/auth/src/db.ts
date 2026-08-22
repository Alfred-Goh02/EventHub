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
