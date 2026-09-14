/**
 * Asserts that database-per-service holds at the CONNECTION boundary: each
 * service's database accepts exactly the roles db/init grants it, and refuses
 * every other login role in the cluster.
 *
 * DERIVED FROM db/init/01-bootstrap.sql, NOT WRITTEN OUT. This replaced a CI step
 * that listed every direction by hand. Its refusals grew with each chapter, and its
 * positive control had quietly stopped at three services while a fourth database
 * existed — the drift CLAUDE.md's "derive a list from its source" rule describes.
 * Adding a fifth database would have meant eight more lines to remember. Here the
 * databases, their CONNECT grants and every login role are read from
 * the one file that creates them, so a new service is covered the moment it is
 * bootstrapped.
 *
 * REFUSED FOR THE RIGHT REASON. The old step passed whenever psql failed, for any
 * reason — a wrong port would have made every refusal a pass. Each refusal here
 * must come back as Postgres error 42501, "permission denied for database", and
 * each database's own roles must connect through the same code path first.
 *
 * Run (with Postgres up and db/init applied): node scripts/verify-database-isolation.mjs
 */

import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const HOST = process.env['PGHOST'] ?? '127.0.0.1';
const PORT = Number(process.env['PGPORT'] ?? 5432);

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/** Strips SQL line comments, so a role or database named in prose is not read as one. */
const sql = readFileSync(join(ROOT, 'db', 'init', '01-bootstrap.sql'), 'utf8')
  .split(/\r?\n/)
  .map((line) => line.replace(/--.*$/, ''))
  .join('\n');

/**
 * Every CREATE ROLE, then sorted, rather than a pattern that matches only the
 * roles shaped the way today's are. `CREATE ROLE x WITH LOGIN`, or a login role
 * with no password, would slip past a stricter pattern and simply never be tested.
 * NOLOGIN roles are skipped: nothing can connect as them.
 */
const roles = new Map();
const untestable = [];
for (const m of sql.matchAll(/CREATE ROLE\s+(\w+)([^;]*);/g)) {
  if (!/\bLOGIN\b/.test(m[2])) continue;
  const password = m[2].match(/PASSWORD\s+'([^']*)'/);
  if (password) roles.set(m[1], password[1]);
  else untestable.push(m[1]);
}

// The name only. Requiring OWNER here would skip a database created without one.
const databases = [...sql.matchAll(/CREATE DATABASE\s+(\w+)/g)].map((m) => m[1]);

const allowed = new Map();
for (const m of sql.matchAll(/GRANT\s+CONNECT\s+ON\s+DATABASE\s+(\w+)\s+TO\s+([\w\s,]+?);/g)) {
  const list = allowed.get(m[1]) ?? new Set();
  for (const role of m[2].split(',').map((r) => r.trim()).filter(Boolean)) list.add(role);
  allowed.set(m[1], list);
}

async function tryConnect(role, database) {
  const client = new pg.Client({ host: HOST, port: PORT, user: role, password: roles.get(role), database, connectionTimeoutMillis: 5_000 });
  try {
    await client.connect();
    await client.query('SELECT 1');
    return { ok: true };
  } catch (err) {
    return { ok: false, code: err.code, message: err.message };
  } finally {
    await client.end().catch(() => {});
  }
}

async function main() {
  console.log('  what db/init creates');
  check(databases.length >= 5, `CONTROL: ${databases.length} databases parsed: ${databases.join(', ')}`);
  check(roles.size >= databases.length * 2, `CONTROL: ${roles.size} login roles parsed`);
  for (const database of databases) {
    const grants = allowed.get(database);
    check(Boolean(grants?.size), `CONTROL: ${database} grants CONNECT to ${grants ? [...grants].join(', ') : 'nobody'}`);
  }
  const unknown = [...allowed.values()].flatMap((set) => [...set]).filter((r) => !roles.has(r));
  check(unknown.length === 0, unknown.length === 0 ? 'every granted role is created with a password in the same file' : `granted but never created: ${unknown.join(', ')}`);
  check(
    untestable.length === 0,
    untestable.length === 0
      ? 'every login role has a password here, so every one is tested'
      : `created with LOGIN but no password this check can use, so never tested: ${untestable.join(', ')}`,
  );
  if (failures > 0) return;

  console.log('');
  console.log("  each database's own roles connect");
  for (const database of databases) {
    for (const role of allowed.get(database)) {
      const r = await tryConnect(role, database);
      check(r.ok, `${role} -> ${database}${r.ok ? '' : `: ${r.code ?? ''} ${r.message}`}`);
    }
  }
  // If the connection template were wrong, every refusal below would be a pass.
  if (failures > 0) return;

  console.log('');
  console.log('  every other login role is refused, with permission denied');
  let refusals = 0;
  for (const database of databases) {
    for (const role of roles.keys()) {
      if (allowed.get(database).has(role)) continue;
      const r = await tryConnect(role, database);
      const refused = !r.ok && r.code === '42501';
      if (refused) refusals += 1;
      else check(false, `${role} -> ${database} ${r.ok ? 'CONNECTED' : `failed for another reason: ${r.code ?? ''} ${r.message}`}`);
    }
  }
  const expected = databases.reduce((n, d) => n + roles.size - allowed.get(d).size, 0);
  check(refusals === expected, `${refusals} of ${expected} cross-service connections refused with 42501`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every database accepts exactly the roles db/init grants it, and refuses the rest');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
