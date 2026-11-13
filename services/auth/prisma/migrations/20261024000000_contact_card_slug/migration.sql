-- ---------------------------------------------------------------------------
-- A contact card names the tenant's subdomain, so a reply email can link to the
-- recipient's own tenant's host.
--
-- Every emailed link was built on one tenantless base, PUBLIC_APP_URL, and landed on
-- a host that cannot serve any tenant: Kong takes the tenant from the subdomain, and a
-- session is bound to it. Auth's own links now take the slug from the request or the
-- tenant row. Notification's reply and digest links need the RECIPIENT's tenant's
-- slug, and the one thing Notification reads per user is the contact card, so the card
-- carries it, signed (src/contact-card.ts).
--
-- contactSlug is the tenant's slug when the user's current card was queued, stamped in
-- the same statement as contactPublishedAt. It is how `contact-cards:publish` finds the
-- cards to make again: every user whose card was made under a slug other than the
-- tenant's current one. That is every existing card, which predates slugs, and later
-- the cards of a tenant that was renamed.
--
-- NO BACKFILL, on purpose. NULL is the right value for every existing user: their
-- card carries no slug. Writing anything here would also be an UPDATE on "User", which
-- is under FORCE row security and would match nothing as the owner with no tenant set
-- (CLAUDE.md). An ADD COLUMN reads no rows and is not filtered.
--
-- In a transaction, as every migration here that could be extended is: Prisma sends a
-- migration's statements one at a time and wraps nothing.
-- ---------------------------------------------------------------------------
BEGIN;

ALTER TABLE "User" ADD COLUMN "contactSlug" TEXT;

COMMIT;
