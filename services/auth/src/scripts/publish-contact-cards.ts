import { prisma, withTenant } from '../db.js';
import { queueContactCard } from '../contact-card.js';

/**
 * Queues a contact card for every user whose card is missing or out of date.
 *
 * `/register` queues a card in the same transaction as the user, so this is for
 * everyone who existed before contact cards did, for the seeded users setup creates,
 * and for every card made before cards named the tenant's subdomain. It picks out
 *   - users who never had a card: contactPublishedAt IS NULL;
 *   - users whose card was made under a slug that is not the tenant's slug now:
 *     contactSlug IS DISTINCT FROM it. NULL is distinct from every slug, so this is
 *     every card from before the slug was on it, and later the cards of a renamed
 *     tenant, whose reply links would otherwise open the old subdomain.
 * Each card it queues stamps both columns, so running it again queues nothing --
 * setup runs it on every `up`. A tenant whose slug Kong would not route still gets
 * cards, without a slug, and is not picked again for it: contactSlug records the slug
 * the card was made under, not whether it could be used.
 *
 * AS THE APP ROLE, ONE TENANT AT A TIME, the way the service itself writes. Run as
 * the owner with no tenant set, FORCE row security would show it no users at all and
 * it would report success having queued nothing — the bug three migrations in this
 * repository shipped with. The count it prints is the evidence.
 *
 * SKIP LOCKED, so two runs at once split the users between them instead of both
 * queueing a card for each.
 *
 * TENANT IDS, OPTIONALLY, to keep a run to those tenants. Setup gives none, and every
 * tenant is covered. A verifier gives its own: a card is signed with the keys of the
 * checkout that runs this, so a run across a shared database would re-sign every
 * user's card with a working tree's keys -- which the compose Notification cannot open
 * -- and mark them all published, so setup would never make them again. Now that every
 * card made before the slug is due again, an unscoped verifier run would be exactly
 * that. A leading `--` is pnpm's, on every shell but PowerShell, and is dropped
 * (CLAUDE.md).
 *
 * Run: pnpm --filter @eventhub/auth run contact-cards:publish [-- <tenant-id>...]
 */

const BATCH = 200;

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const only = (args[0] === '--' ? args.slice(1) : args).filter((a) => a.length > 0);
  const tenants = await prisma.tenant.findMany({
    where: only.length > 0 ? { id: { in: only } } : undefined,
    select: { id: true },
    orderBy: { id: 'asc' },
  });
  const unknown = only.filter((id) => !tenants.some((t) => t.id === id));
  if (unknown.length > 0) {
    throw new Error(`no such tenant: ${unknown.join(', ')}; nothing was queued`);
  }
  let queued = 0;
  let withoutSlug = 0;
  for (const { id: tenantId } of tenants) {
    for (;;) {
      const n = await withTenant(tenantId, async (tx) => {
        // The tenant's slug as it is now, read in the same statement as the users, not
        // once before the loop: a rename during a run would otherwise leave every card
        // just stamped with the new slug "distinct" from the old one read here, and
        // the loop would pick the same users again until it was stopped.
        const users = await tx.$queryRaw<Array<{ id: string; email: string }>>`
          SELECT u.id, u.email FROM "User" u
           WHERE u."tenantId" = ${tenantId}
             AND (u."contactPublishedAt" IS NULL
                  OR u."contactSlug" IS DISTINCT FROM (SELECT t.slug FROM "Tenant" t WHERE t.id = u."tenantId"))
           ORDER BY u.id
           LIMIT ${BATCH}
           FOR UPDATE SKIP LOCKED`;
        for (const u of users) {
          const card = await queueContactCard(tx, { tenantId, userId: u.id, email: u.email });
          if (card.tenantSlug === null) withoutSlug += 1;
        }
        return users.length;
      });
      queued += n;
      if (n < BATCH) break;
    }
  }
  console.log(
    `contact cards queued: ${queued}, across ${tenants.length} tenant(s)` +
      (withoutSlug > 0 ? `; ${withoutSlug} of them name no subdomain, because their tenant's slug is not one the gateway routes` : ''),
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => prisma.$disconnect());
