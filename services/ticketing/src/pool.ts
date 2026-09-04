import type pg from 'pg';
import type { Logger } from '@eventhub/logger';

/**
 * Stops a terminated idle connection from killing the process.
 *
 * `pg.Pool` is an EventEmitter, and an EventEmitter that emits `'error'` with no
 * listener THROWS. Node then has an uncaught exception and exits. So the missing
 * three lines below are not a logging nicety — they are the difference between
 * a pool quietly replacing a connection and the whole service dying.
 *
 * What triggers it is entirely ordinary operations, not a bug anywhere:
 *
 *   - Postgres restarts, or a managed instance fails over
 *   - an operator runs pg_terminate_backend, or a maintenance job does
 *   - idle_session_timeout or a connection-count limit reaps the connection
 *   - a NAT gateway or load balancer drops an idle TCP connection
 *
 * Reproduced against the unfixed code by terminating one idle backend:
 *
 *   node:events:487
 *         throw er; // Unhandled 'error' event
 *   error: terminating connection due to administrator command
 *
 * The relay died outright. So did the ticketing API — and that one is the
 * surprise, because the pool that killed it was not the one serving requests.
 * Prisma's adapter installs its own handler on the pool behind `prisma`, so
 * terminating an APPLICATION connection is survived cleanly. The two raw pools
 * in the webhook route are the unguarded ones, and taking either down takes the
 * entire HTTP service with it — reservations, payments, health checks, all of
 * it, because of a pool that exists to resolve one column for one webhook.
 *
 * That asymmetry is why this is worth a named helper rather than an inline
 * `.on('error')`: the obvious test — kill the app's connection, watch it
 * recover — passes on a process that is still one terminated side-connection
 * away from exiting.
 *
 * The relay carries its own copy, in services/relay/src/db.ts, because the two
 * services share no database package. Both are named the same thing so
 * scripts/verify-pool-resilience.mjs can require that EVERY `new pg.Pool(` in
 * this repository is wrapped in a call to it — which is the assertion that
 * catches the SIXTH pool, not this one.
 *
 * SWALLOWING IS CORRECT HERE, and that deserves saying because it looks wrong.
 * This event fires for a client sitting IDLE in the pool, so there is no query
 * in flight and no caller waiting on a result. `pg` has already removed the
 * client from the pool by the time this runs; the next checkout opens a fresh
 * connection. An error on a client that IS checked out does not arrive here —
 * it rejects the `query()` call, where the caller's own error handling belongs.
 */
export function attachPoolErrorHandler(pool: pg.Pool, logger: Logger, name: string): pg.Pool {
  pool.on('error', (err) => {
    /**
     * Fields, not the error object.
     *
     * pg's DatabaseError carries a `client` property, and a structured
     * logger serialising it walks the whole client: connection parameters,
     * the socket, pg's entire builtin type OID table. One termination
     * produced a single log line several kilobytes long. A Postgres restart
     * terminates every connection at once, so the moment an operator most
     * needs to read the log is the moment it is least readable.
     *
     * The SQLSTATE and the message are the whole diagnosis anyway: 57P01 is
     * admin_shutdown, 57P02 crash_shutdown, 08006 connection_failure.
     */
    const e = err as NodeJS.ErrnoException & { code?: string };
    logger.warn(
      { pool: name, code: e.code, reason: e.message },
      'idle database connection was terminated; the pool will replace it',
    );
  });
  return pool;
}
