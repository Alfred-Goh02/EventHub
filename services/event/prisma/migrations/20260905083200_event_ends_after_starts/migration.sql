-- An event must not end before it starts.
--
-- POST /events has always refused a backwards pair, with a Zod .refine(). PATCH
-- never checked at all, and there was no constraint underneath either -- so an
-- event created correctly could be patched into nonsense two different ways:
--
--   PATCH { startsAt: <later>, endsAt: <earlier> }   -> 200
--   PATCH { startsAt: <past the stored endsAt> }     -> 200
--
-- The second is the one a refinement on the request body cannot catch on its
-- own: PATCH is partial, so half the comparison lives in the database.
--
-- This is the same argument the Ticketing schema makes about its inventory
-- CHECK: a guard can be forgotten in one code path, a constraint cannot. The
-- route now validates the MERGED values so a client gets a 400 rather than a
-- 500 -- but the route is the error message and this is the guarantee.
--
-- Safe to apply: verified 0 violations across the existing rows before writing
-- it. A backwards pair would fail this migration rather than being silently
-- accepted, which is the correct direction for a constraint that is supposed to
-- have been true all along.
ALTER TABLE "Event"
  ADD CONSTRAINT "Event_ends_after_starts"
  CHECK ("endsAt" > "startsAt");
