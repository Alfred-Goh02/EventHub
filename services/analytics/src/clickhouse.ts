import { createClient, type ClickHouseClient } from '@clickhouse/client';

export interface ClickHouseTarget {
  url: string;
  database: string;
  username: string;
  password: string;
}

/**
 * The one place a ClickHouse client is constructed.
 *
 * A factory rather than a module-level client, because this service connects as
 * two different users: the running service as analytics_app, which can only
 * SELECT and INSERT, and the migration runner as analytics_migrator. A singleton
 * would have to pick one, and whichever it picked would be wrong for the other.
 *
 * `database` is set on the client, so migrations and queries use unqualified
 * table names and the target database is a parameter rather than a literal baked
 * into SQL. That keeps a database-per-tenant layout possible later without
 * interpolating identifiers into statements — an identifier cannot be a bound
 * query parameter, so the fewer places one is written, the fewer places one can
 * be injected.
 *
 * HTTP on 8123, which is what @clickhouse/client speaks. Note the v1 option is
 * `url`; older examples use `host`, which is silently ignored.
 */
export function createClickHouse(target: ClickHouseTarget): ClickHouseClient {
  return createClient({
    url: target.url,
    database: target.database,
    username: target.username,
    password: target.password,
    request_timeout: 30_000,
    max_open_connections: 10,
  });
}
