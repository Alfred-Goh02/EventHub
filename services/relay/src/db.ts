import pg from 'pg';

/**
 * Raw `pg`, not Prisma.
 *
 * The drain query needs `FOR UPDATE SKIP LOCKED`, which Prisma's query builder
 * cannot express — and reaching for `$queryRaw` on every statement while still
 * paying for a generated client would be the worst of both. The relay touches
 * exactly one table with four statements; a query builder earns nothing here.
 */
export function createPool(connectionString: string): pg.Pool {
  return new pg.Pool({
    connectionString,
    // Small on purpose. The relay is one drain loop plus a metrics endpoint; a
    // large pool would just hold idle connections a service actually needs.
    max: 4,
    // Bounded so a wedged connection is replaced rather than leaked. The role
    // also carries idle_in_transaction_session_timeout and statement_timeout,
    // set in db/init/01-bootstrap.sql — an open transaction pins xmin and blocks
    // VACUUM across the whole cluster, so this matters beyond the relay.
    connectionTimeoutMillis: 10_000,
    idleTimeoutMillis: 30_000,
  });
}

/**
 * Refuses to start if the relay credential can read business tables.
 *
 * The relay is granted SELECT on OutboxMessage and nothing else. That is a
 * deliberate blast-radius decision: it reads across every tenant (its policy is
 * USING(true)), so it must not also be able to read tenant data. This asserts
 * the negative rather than trusting it, because the failure is silent — a relay
 * pointed at an over-privileged credential works perfectly and is simply far
 * more dangerous if it leaks.
 */
export async function assertRelayIsLeastPrivilege(
  pool: pg.Pool,
  forbiddenTable: string,
): Promise<void> {
  const client = await pool.connect();
  try {
    const who = await client.query<{ rolsuper: boolean; rolbypassrls: boolean }>(
      'SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user',
    );
    const role = who.rows[0];
    if (!role) throw new Error('could not read current_user from pg_roles');

    if (role.rolsuper || role.rolbypassrls) {
      throw new Error(
        'refusing to start: RELAY_DATABASE_URL connects as a role that bypasses row-level ' +
          `security (rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls})`,
      );
    }

    /**
     * The table must EXIST before its inaccessibility proves anything.
     *
     * The first version of this check hardcoded "Event" and simply required the
     * SELECT to fail. Pointed at the Ticketing database — which has no Event
     * table at all — it passed on "relation does not exist", i.e. because the
     * table was ABSENT rather than because access was denied. A check that
     * passes because the thing it guards is missing is worse than no check: it
     * reports a privilege boundary that was never tested.
     *
     * So: confirm the table is there, THEN confirm reading it is refused.
     */
    const exists = await client.query<{ n: string }>(
      `SELECT count(*)::text AS n FROM pg_class
        WHERE relname = $1 AND relnamespace = 'public'::regnamespace AND relkind = 'r'`,
      [forbiddenTable],
    );

    if (Number(exists.rows[0]?.n ?? 0) === 0) {
      throw new Error(
        `refusing to start: RELAY_FORBIDDEN_TABLE is "${forbiddenTable}" but no such table ` +
          'exists in this database, so the privilege check would pass without testing anything. ' +
          'Set it to a business table that DOES exist here.',
      );
    }

    let couldRead = false;
    try {
      await client.query(`SELECT 1 FROM "${forbiddenTable}" LIMIT 1`);
      couldRead = true;
    } catch {
      // Expected: permission denied.
    }

    if (couldRead) {
      throw new Error(
        `refusing to start: the relay credential can read "${forbiddenTable}". It should have ` +
          'SELECT on OutboxMessage only — it reads across every tenant, so it must not also ' +
          'be able to read tenant data.',
      );
    }
  } finally {
    client.release();
  }
}
