import type pg from 'pg';
import type { Logger } from '@eventhub/logger';

/**
 * Stops a terminated idle connection from killing the process.
 *
 * `pg.Pool` is an EventEmitter, and an EventEmitter that emits `'error'` with no
 * listener THROWS: Node then has an uncaught exception and exits. What triggers it is
 * ordinary operation — Postgres restarting, a failover, pg_terminate_backend, an idle
 * timeout, a load balancer dropping an idle TCP connection.
 *
 * The pool behind `prisma` is guarded by Prisma's adapter, so the consumer survives a
 * terminated application connection. The sweeper's raw pool is not, and an unguarded
 * one there would take the whole service down — the dispatch consumer with it — over a
 * connection that exists to end abandoned deliveries once a minute.
 *
 * A third copy of the same helper, beside Ticketing's and the relay's, because these
 * services share no database package. All three are named the same thing, which is
 * what lets scripts/verify-pool-resilience.mjs require that every `new pg.Pool(` in
 * the repository is wrapped in a call to it.
 *
 * SWALLOWING IS CORRECT HERE: the event fires for a client sitting IDLE in the pool,
 * with no query in flight and no caller waiting. pg has already removed it; the next
 * checkout opens a fresh connection. An error on a client that is checked out rejects
 * the query instead, where the caller's own handling belongs.
 */
export function attachPoolErrorHandler(pool: pg.Pool, logger: Logger, name: string): pg.Pool {
  pool.on('error', (err) => {
    // Fields, not the error object: pg's DatabaseError carries a `client`, and a
    // structured logger serialising it walks the connection parameters, the socket and
    // pg's whole type table. A restart terminates every connection at once, which is
    // when the log most needs to stay readable.
    const e = err as NodeJS.ErrnoException & { code?: string };
    logger.warn(
      { pool: name, code: e.code, reason: e.message },
      'idle database connection was terminated; the pool will replace it',
    );
  });
  return pool;
}
