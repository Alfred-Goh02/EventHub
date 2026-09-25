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
 * checkout opens a fresh connection.
 *
 * IT DOES NOT COVER A CHECKED-OUT CLIENT. This used to end "an error on a client that
 * is checked out rejects the query instead", which is true only while a query is in
 * flight. pg-pool removes its idle listener on checkout, so between two queries a
 * checked-out Client has no 'error' listener — and pg emits there exactly when there
 * is no query to reject. Node throws and the process exits. The sweeper's role carries
 * idle_in_transaction_session_timeout = '30s' (db/init/01-bootstrap.sql), which is the
 * server-side rule that manufactures that case; a Postgres restart does it to any
 * checked-out client. Measured in the relay: unhandled 'error', FATAL 25P03, exit 1.
 *
 * withClient() below closes that half; this handler keeps the idle half.
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

/**
 * Checks a client out with an 'error' listener on it for the whole checkout.
 *
 * The other half of the hazard above: that one covers a client sitting idle IN the
 * pool, this covers one that has been handed out, where pg leaves no listener at all.
 *
 * DESTROYED, NOT RETURNED — `release(err)` makes pg drop a client whose connection
 * died rather than lend the dead socket to the next caller.
 *
 * THE removeListener MATTERS: pg reuses Client objects across checkouts, so one added
 * per checkout and never removed piles up until Node prints MaxListenersExceededWarning.
 *
 * A third copy, beside Ticketing's and the relay's, because these services share no
 * database package. All three are named the same thing, which is what lets
 * scripts/verify-relay-idle-in-transaction.mjs require that every `pool.connect()` in
 * the repository goes through one.
 */
export async function withClient<T>(
  pool: pg.Pool,
  logger: Logger,
  name: string,
  fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();

  let poisoned: Error | null = null;
  const onError = (err: Error): void => {
    poisoned = err;
    // Fields, not the error object: same reason as above.
    const e = err as NodeJS.ErrnoException & { code?: string };
    logger.warn(
      { pool: name, code: e.code, reason: e.message },
      'the database terminated a connection that was checked out; it will not be reused',
    );
  };

  client.on('error', onError);

  try {
    return await fn(client);
  } finally {
    client.removeListener('error', onError);
    // release(err) destroys; release() returns it to the pool.
    if (poisoned) client.release(poisoned);
    else client.release();
  }
}
