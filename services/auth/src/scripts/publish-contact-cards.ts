import { prisma, withTenant } from '../db.js';
import { queueContactCard } from '../contact-card.js';

/**
 * Queues a contact card for every user who has never had one.
 *
 * `/register` queues a card in the same transaction as the user, so this is for
 * everyone who existed before contact cards did, and for the seeded users setup
 * creates. It finds them by contactPublishedAt IS NULL, so running it again queues
 * nothing — setup runs it on every `up`.
 *
 * AS THE APP ROLE, ONE TENANT AT A TIME, the way the service itself writes. Run as
 * the owner with no tenant set, FORCE row security would show it no users at all and
 * it would report success having queued nothing — the bug three migrations in this
 * repository shipped with. The count it prints is the evidence.
 *
 * SKIP LOCKED, so two runs at once split the users between them instead of both
 * queueing a card for each.
 *
 * Run: pnpm --filter @eventhub/auth run contact-cards:publish
 */

const BATCH = 200;

async function main(): Promise<void> {
  const tenants = await prisma.tenant.findMany({ select: { id: true }, orderBy: { id: 'asc' } });
  let queued = 0;
  for (const { id: tenantId } of tenants) {
    for (;;) {
      const n = await withTenant(tenantId, async (tx) => {
        const users = await tx.$queryRaw<Array<{ id: string; email: string }>>`
          SELECT u.id, u.email FROM "User" u
           WHERE u."tenantId" = ${tenantId} AND u."contactPublishedAt" IS NULL
           ORDER BY u.id
           LIMIT ${BATCH}
           FOR UPDATE SKIP LOCKED`;
        for (const u of users) await queueContactCard(tx, { tenantId, userId: u.id, email: u.email });
        return users.length;
      });
      queued += n;
      if (n < BATCH) break;
    }
  }
  console.log(`contact cards queued: ${queued}, across ${tenants.length} tenant(s)`);
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
