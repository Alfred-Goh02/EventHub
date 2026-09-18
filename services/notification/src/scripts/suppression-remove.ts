import { PrismaPg } from '@prisma/adapter-pg';
import { z } from 'zod';
import { PrismaClient } from '../generated/prisma/client.js';
import { addressHash } from '../dispatch/suppression.js';

/**
 * Lifts a suppression, so the address is mailed again.
 *
 *   pnpm --filter @eventhub/notification run suppression:remove -- <tenantId> <address>
 *
 * FOR A SUPPRESSION THAT IS WRONG OR OUT OF DATE: the mailbox exists after all, or the
 * refusal was the recipient's own misconfiguration. Until it is lifted, that user
 * cannot receive a password reset.
 *
 * AS THE OWNER. The process that sends holds SELECT and INSERT on this table and
 * nothing more, so a bug in it can add a suppression but never remove one. RLS is
 * forced on the owner as well, so this sets the tenant like everything else.
 *
 * The address is never printed. Reads DATABASE_URL and nothing else, for the reason
 * dlq-replay.ts gives.
 */

const { DATABASE_URL } = z.object({ DATABASE_URL: z.string().min(1) }).parse(process.env);

/**
 * The arguments, with the separator the documented command carries.
 *
 * `pnpm run <script> -- a b` hands the script `['--', 'a', 'b']` in every shell but
 * PowerShell, where pnpm.ps1 swallows the `--` first. Without this, the tenant id is
 * read as "--", the real tenant id is hashed as the address, and the command reports
 * that the address is not suppressed — the one message an operator must not be given
 * while a user is locked out.
 */
function args(): string[] {
  const rest = process.argv.slice(2);
  if (rest[0] === '--') rest.shift();
  return rest;
}

async function main(): Promise<void> {
  const [tenantId, address] = args();
  if (!tenantId || !address) {
    console.error('usage: suppression-remove <tenantId> <address>');
    process.exitCode = 2;
    return;
  }
  // The tenant comes first. Swapped, the address would be hashed as a tenant id and
  // the run would delete nothing, with no sign of why.
  if (tenantId.includes('@')) {
    console.error('usage: suppression-remove <tenantId> <address> — the tenant id comes first');
    process.exitCode = 2;
    return;
  }

  const prisma = new PrismaClient({ adapter: new PrismaPg({ connectionString: DATABASE_URL }) });
  try {
    const removed = await prisma.$transaction(async (tx) => {
      await tx.$executeRaw`SELECT set_config('app.tenant_id', ${tenantId}, true)`;
      return tx.$executeRaw`
        DELETE FROM "EmailSuppression" WHERE "tenantId" = ${tenantId} AND "addressHash" = ${addressHash(address)}`;
    });
    if (removed === 0) {
      console.error(`that address is not suppressed in ${tenantId}`);
      process.exitCode = 1;
      return;
    }
    console.log(`lifted the suppression for that address in ${tenantId}`);
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
