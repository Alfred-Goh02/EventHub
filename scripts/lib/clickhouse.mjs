import { spawnSync } from 'node:child_process';
import { join } from 'node:path';

/**
 * What the Analytics verifiers share: ClickHouse over HTTP as a named user, the
 * migration runner, and a scratch database per run with the service's grants on it.
 *
 * HTTP rather than the service's client, so a verifier can act as the admin, the
 * migrator and the app in one run and see each one's refusals as they are.
 */

export const CH_URL = process.env['CLICKHOUSE_URL'] ?? 'http://127.0.0.1:8123';

export const ADMIN = {
  user: process.env['CLICKHOUSE_ADMIN_USER'] ?? 'eventhub',
  password: process.env['CLICKHOUSE_ADMIN_PASSWORD'] ?? 'eventhub',
};
export const APP = { user: 'analytics_app', password: 'analytics_app' };
export const MIGRATOR = { user: 'analytics_migrator', password: 'analytics_migrator' };

/**
 * One statement over HTTP, as one user. Settings and query parameters go on the
 * URL, which is how ClickHouse takes them per query.
 */
export async function ch(as, sql, { database, settings = {}, params = {}, rows } = {}) {
  const url = new URL(CH_URL);
  if (database) url.searchParams.set('database', database);
  for (const [k, v] of Object.entries(settings)) url.searchParams.set(k, String(v));
  for (const [k, v] of Object.entries(params)) url.searchParams.set(`param_${k}`, String(v));
  const body = rows ? `${sql} FORMAT JSONEachRow\n${rows.map((r) => JSON.stringify(r)).join('\n')}` : sql;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'X-ClickHouse-User': as.user, 'X-ClickHouse-Key': as.password },
    body,
  });
  return { ok: res.ok, text: await res.text() };
}

export async function json(as, sql, database) {
  const r = await ch(as, `${sql} FORMAT JSON`, { database });
  if (!r.ok) throw new Error(`query failed: ${sql}\n${r.text}`);
  return JSON.parse(r.text).data;
}

export const denied = (r) => !r.ok && /ACCESS_DENIED|Code: 497/.test(r.text);

/** 'YYYY-MM-DD HH:mm:ss.SSS' in UTC, the format the service writes DateTime64(3, 'UTC') in. */
export const toChTime = (d = new Date()) => d.toISOString().replace('T', ' ').replace('Z', '');

/** Runs the real migration runner into `database`, as the migrator. */
export function migrate(root, database) {
  const out = spawnSync(process.execPath, ['dist/scripts/migrate.js'], {
    cwd: join(root, 'services', 'analytics'),
    env: {
      PATH: process.env['PATH'],
      SystemRoot: process.env['SystemRoot'],
      CLICKHOUSE_URL: CH_URL,
      CLICKHOUSE_DATABASE: database,
      CLICKHOUSE_MIGRATOR_USER: MIGRATOR.user,
      CLICKHOUSE_MIGRATOR_PASSWORD: MIGRATOR.password,
    },
    encoding: 'utf8',
  });
  const text = `${out.stdout}${out.stderr}`;
  const applied = text.match(/"applied":(\d+)/);
  return { code: out.status, applied: applied ? Number(applied[1]) : null, text };
}

/** A database for one run, with the same grants the real one has. Throws rather than let a run continue half set up. */
export async function createScratchDatabase(name) {
  for (const sql of [
    `CREATE DATABASE ${name}`,
    `GRANT ALL ON ${name}.* TO ${MIGRATOR.user}`,
    `GRANT SELECT, INSERT ON ${name}.* TO ${APP.user}`,
  ]) {
    const r = await ch(ADMIN, sql);
    if (!r.ok) throw new Error(`setup failed: ${sql}\n${r.text}`);
  }
}

/** Drops it and its grants. Returns whether it is really gone. */
export async function dropScratchDatabase(name) {
  await ch(ADMIN, `DROP DATABASE IF EXISTS ${name} SYNC`);
  await ch(ADMIN, `REVOKE ALL ON ${name}.* FROM ${MIGRATOR.user}`);
  await ch(ADMIN, `REVOKE ALL ON ${name}.* FROM ${APP.user}`);
  const left = await ch(ADMIN, `SELECT count() FROM system.databases WHERE name = '${name}'`);
  return left.text.trim() === '0';
}
