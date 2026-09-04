import pg from 'pg';
import type { Logger } from '@eventhub/logger';

/**
 * Stops a terminated idle connection from killing the process.
 *
 * pg.Pool is an EventEmitter, and an EventEmitter that emits 'error' with no
 * listener THROWS — which in Node is an uncaught exception and an exit. So the
 * three lines below are not a logging nicety; they are the difference between
 * the pool quietly replacing a connection and the relay dying. Reproduced by
 * terminating one idle backend against the unfixed code:
 *
 *   node:events:487
 *         throw er; // Unhandled 'error' event
 *   error: terminating connection due to administrator command
 *
 * Nothing exotic gets you there. A Postgres restart, a managed failover, a
 * maintenance script running pg_terminate_backend, idle_session_timeout, or a
 * load balancer reaping an idle TCP connection all produce it — and the relay,
 * which exists to survive exactly this kind of interruption and republish what
 * it had not yet delivered, was the process least able to tolerate it.
 *
 * Swallowed on purpose. This fires for a client sitting IDLE in the pool, so no
 * query is in flight and no caller is waiting; pg has already discarded the
 * client, and the next checkout opens a fresh one. An error on a client that is
 * CHECKED OUT does not arrive here — it rejects that query() instead, where
 * drainOnce's own error handling takes it and the attempt rolls back.
 *
 * Ticketing carries its own copy of this, in services/ticketing/src/pool.ts,
 * because the two services share no database package. Both are named the same
 * thing so scripts/verify-pool-resilience.mjs can require that EVERY
 * `new pg.Pool(` in this repository is wrapped in a call to it.
 */
function attachPoolErrorHandler(pool: pg.Pool, logger: Logger, name: string): pg.Pool {
  pool.on('error', (err) => {
    /**
     * Fields, not the error object. pg's DatabaseError carries a `client`
     * property, and a structured logger serialising it walks the whole
     * client -- connection parameters, the socket, pg's entire builtin type
     * OID table. One termination produced a log line several kilobytes long,
     * and a Postgres restart terminates every connection at once. The
     * SQLSTATE is the diagnosis: 57P01 admin_shutdown, 08006 connection_failure.
     */
    const e = err as NodeJS.ErrnoException & { code?: string };
    logger.warn(
      { pool: name, code: e.code, reason: e.message },
      'idle database connection was terminated; the pool will replace it',
    );
  });
  return pool;
}

/**
 * Raw `pg`, not Prisma.
 *
 * The drain query needs `FOR UPDATE SKIP LOCKED`, which Prisma's query builder
 * cannot express — and reaching for `$queryRaw` on every statement while still
 * paying for a generated client would be the worst of both. The relay touches
 * exactly one table with four statements; a query builder earns nothing here.
 */
export function createPool(connectionString: string, logger: Logger): pg.Pool {
  return attachPoolErrorHandler(
    new pg.Pool({
      connectionString,
      // Named so pg_stat_activity says which pool a connection belongs to. An
      // operator looking at a stuck backend should not have to guess, and the
      // resilience verifier uses it to find exactly the connection it means to
      // terminate rather than any connection on the same role.
      application_name: 'eventhub-relay',
      // Small on purpose. The relay is one drain loop plus a metrics endpoint; a
      // large pool would just hold idle connections a service actually needs.
      max: 4,
      // Bounded so a wedged connection is replaced rather than leaked. The role
      // also carries idle_in_transaction_session_timeout and statement_timeout,
      // set in db/init/01-bootstrap.sql — an open transaction pins xmin and blocks
      // VACUUM across the whole cluster, so this matters beyond the relay.
      connectionTimeoutMillis: 10_000,
      idleTimeoutMillis: 30_000,
    }),
    logger,
    'eventhub-relay',
  );
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
