-- Give every empty slug a unique value.
--
-- slugify() kept only [a-z0-9], so a title in any non-Latin script produced the
-- EMPTY STRING. With a partial unique index on (tenantId, slug) WHERE deletedAt
-- IS NULL, the first such title in a tenant took the empty slug and every one
-- after it was refused -- with "an event with that title already exists", about
-- two titles that share nothing.
--
-- The route no longer produces an empty slug. This clears the rows that already
-- have one, because otherwise the constraint stays poisoned for exactly the
-- tenants that hit the bug: their one surviving empty-slug row would keep
-- blocking nothing in particular, and a second would still be impossible if the
-- old build ever ran again.
--
-- Same shape as the route's fallback, so a reader does not have to work out
-- whether they match. gen_random_uuid() rather than the route's randomBytes for
-- the obvious reason -- there is no Node here -- and 10 hex characters either
-- way.
UPDATE "Event"
   SET slug = 'event-' || substr(replace(gen_random_uuid()::text, '-', ''), 1, 10)
 WHERE slug = '';
