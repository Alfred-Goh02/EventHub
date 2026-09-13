/**
 * A database name that is safe to use as an identifier.
 *
 * ClickHouse's typed query parameters bind VALUES, not identifiers, so a database
 * name can never be a parameter. It is set once on the client and validated with
 * this instead, so nothing shaped like SQL can arrive through it.
 *
 * ITS OWN MODULE, WITH NO SIDE EFFECTS. It lived in env.ts first, and the
 * migration runner imported it from there — which evaluated env.ts's loadEnv as
 * a side effect of importing a regex, so the runner refused to start without the
 * service's PORT and app credentials. That is the rule CLAUDE.md gained from
 * Chapter 6, broken within the hour of writing it down.
 */
export const DATABASE_NAME = /^[a-z_][a-z0-9_]{0,62}$/;
