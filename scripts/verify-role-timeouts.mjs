/**
 * Asserts that every role db/init gives a session GUC actually carries it — in
 * the catalog, and in a real session opened as that role.
 *
 * WHAT DEPENDS ON THIS. Six roles — the four relays and the two sweepers — are
 * given `idle_in_transaction_session_timeout = '30s'` and
 * `statement_timeout = '15s'` in db/init/01-bootstrap.sql. The relay's pool says
 * so itself, in services/relay/src/db.ts:
 *
 *   "The role also carries idle_in_transaction_session_timeout and
 *    statement_timeout, set in db/init/01-bootstrap.sql — an open transaction
 *    pins xmin and blocks VACUUM across the whole cluster, so this matters
 *    beyond the relay."
 *
 * So the code names a control that, until this file, nothing asserted: measured,
 * six roles given the settings and zero verifiers mentioning them. The drain
 * holds a transaction open across a Kafka round-trip so FOR UPDATE SKIP LOCKED
 * keeps its claim; a relay that hangs mid-batch pins xmin for every table in
 * every database in the cluster. The timeout is what ends that.
 *
 * Its failure is silent in the worst way. If a later migration dropped and
 * recreated one of these roles, the ALTER ROLE would go with it, nothing would
 * error, every test would stay green, and the only symptom would be table bloat
 * across the whole cluster — weeks later, somewhere else.
 *
 * DERIVED FROM db/init/01-bootstrap.sql, NOT WRITTEN OUT. The roles, their
 * settings, their values, their passwords and the databases they may connect to
 * are all read out of the one file that creates them. A seventh role given a
 * timeout is covered the moment it is bootstrapped, and so is a seventh setting.
 * CLAUDE.md has three examples of a hand-written copy of a list falling behind
 * the list, silently.
 *
 * THREE WAYS THIS COULD PASS WITHOUT CHECKING ANYTHING, and what stops each:
 *
 *   The parse matches nothing. A regex that stops matching leaves an empty
 *     expectation, and a loop over nothing agrees with itself. So the parsed
 *     count is asserted non-empty and printed, and any ALTER ROLE statement the
 *     parser did NOT understand — `ALTER ROLE ... IN DATABASE ... SET`, a RESET —
 *     is a failure rather than a silent skip.
 *   The catalog read returns nothing. Asserted non-empty before it is compared.
 *   The value is right for the wrong reason. `SHOW statement_timeout` returning
 *     15s proves nothing if the CLUSTER default is 15s — the community.feed
 *     retention bug in CLAUDE.md, where a check read a broker default back and
 *     agreed with itself. So provenance is asserted separately from the value:
 *     the setting must be attached to the ROLE in pg_db_role_setting, and a role
 *     the file does NOT alter is opened as the control, to show what a session
 *     without the setting gets instead.
 *
 * The catalog and the live session are both here on purpose. The catalog says
 * the setting is attached to the role; the session says a backend actually
 * starts with it. An `ALTER ROLE ... IN DATABASE` override would satisfy the
 * first reading of the row and defeat the second, which is why the read covers
 * per-database rows too.
 *
 * No superuser. Each role opens its own session with the password the bootstrap
 * gives it, against a database the bootstrap grants it CONNECT on — so this runs
 * with exactly the credentials the services use, and pg_db_role_setting is
 * readable from any of them.
 *
 * Run (with Postgres up and db/init applied): node scripts/verify-role-timeouts.mjs
 */

import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
/** Named with forward slashes because it is printed; read through join() below. */
const BOOTSTRAP = 'db/init/01-bootstrap.sql';
const HOST = process.env['PGHOST'] ?? '127.0.0.1';
const PORT = Number(process.env['PGPORT'] ?? 5432);

let failures = 0;
let passes = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (ok) {
    passes += 1;
  } else {
    failures += 1;
    process.exitCode = 1;
  }
}

// ---------------------------------------------------------------------------
// What the bootstrap says
// ---------------------------------------------------------------------------

/**
 * Line comments are stripped first. This file explains its own ALTER ROLEs in
 * prose directly above them — "These are ALTER ROLE, which is cluster-level" —
 * and a scanner that reads comments finds statements that are not statements.
 */
const sql = readFileSync(join(ROOT, 'db', 'init', '01-bootstrap.sql'), 'utf8')
  .split(/\r?\n/)
  .map((line) => line.replace(/--.*$/, ''))
  .join('\n');

/** Login roles and the password this script can open a session with. */
const passwords = new Map();
for (const m of sql.matchAll(/CREATE ROLE\s+(\w+)([^;]*);/g)) {
  if (!/\bLOGIN\b/.test(m[2])) continue;
  const password = m[2].match(/PASSWORD\s+'([^']*)'/);
  if (password) passwords.set(m[1], password[1]);
}

/** role -> databases it may CONNECT to, so each role can be opened somewhere. */
const connectable = new Map();
for (const m of sql.matchAll(/GRANT\s+CONNECT\s+ON\s+DATABASE\s+(\w+)\s+TO\s+([\w\s,]+?);/g)) {
  for (const role of m[2].split(',').map((r) => r.trim()).filter(Boolean)) {
    const seen = connectable.get(role) ?? new Set();
    seen.add(m[1]);
    connectable.set(role, seen);
  }
}

/**
 * Every ALTER ROLE statement, then matched one at a time — rather than a global
 * matchAll of the shape we expect, which cannot tell "no such statement" from
 * "a statement I did not recognise". Anything here that does not parse is
 * reported below instead of vanishing.
 */
const alterStatements = sql
  .split(';')
  .map((s) => s.trim().replace(/\s+/g, ' '))
  .filter((s) => /^ALTER ROLE\b/i.test(s));

const ONE_SET = /^ALTER ROLE (\w+) SET (\w+)\s*(?:=|TO)\s*(.+)$/i;
const expected = new Map(); // role -> Map(guc -> value as the file states it)
const unparsed = [];
for (const statement of alterStatements) {
  const m = statement.match(ONE_SET);
  if (!m) {
    unparsed.push(statement);
    continue;
  }
  const forRole = expected.get(m[1]) ?? new Map();
  forRole.set(m[2].toLowerCase(), m[3].trim().replace(/^'(.*)'$/, '$1'));
  expected.set(m[1], forRole);
}

const settingCount = [...expected.values()].reduce((n, m) => n + m.size, 0);
const gucs = [...new Set([...expected.values()].flatMap((m) => [...m.keys()]))].sort();
/** Every value the file asks for, per GUC — what the control must NOT coincide with. */
const wantedValues = new Map(
  gucs.map((g) => [g, new Set([...expected.values()].map((m) => m.get(g)).filter(Boolean))]),
);

// ---------------------------------------------------------------------------

function clientFor(role, database) {
  return new pg.Client({
    host: HOST,
    port: PORT,
    user: role,
    password: passwords.get(role),
    database,
    application_name: 'verify-role-timeouts',
    connectionTimeoutMillis: 5_000,
  });
}

/** The database a role may connect to, chosen from the file rather than assumed. */
const databaseFor = (role) => [...(connectable.get(role) ?? [])].sort()[0];

/** Reads the GUCs of interest out of a live session. */
async function showAll(client) {
  const values = new Map();
  for (const guc of gucs) {
    const r = await client.query(`SHOW ${guc}`);
    values.set(guc, Object.values(r.rows[0])[0]);
  }
  return values;
}

/**
 * Postgres normalises a GUC on the way in, so the file's '30s' and a session's
 * '30s' are comparable but '30000ms' and '30s' are not. Both sides go through
 * the server, inside a transaction that is rolled back, rather than through a
 * unit table written here that would have to be kept correct.
 */
async function normalise(client, guc, value) {
  try {
    await client.query('BEGIN');
    const r = await client.query('SELECT set_config($1, $2, true) AS v', [guc, value]);
    return r.rows[0].v;
  } catch {
    return value;
  } finally {
    await client.query('ROLLBACK').catch(() => {});
  }
}

async function main() {
  console.log(`  what ${BOOTSTRAP} asks for`);

  /**
   * THE CONTROL THE REST OF THIS FILE RESTS ON. Every loop below iterates the
   * parsed expectation; if the parse returned nothing, every loop runs zero
   * times and the script reports success over an empty set. CLAUDE.md records
   * that failure twice — `seen.sort()` and `relayB.length >= 0` — so the count
   * is asserted and printed rather than assumed.
   */
  check(
    expected.size > 0 && settingCount > 0,
    `CONTROL: ${settingCount} setting(s) on ${expected.size} role(s) parsed from ${alterStatements.length} ALTER ROLE statement(s): ` +
      [...expected.keys()].sort().join(', '),
  );
  check(
    gucs.length > 0,
    `CONTROL: the settings are ${gucs.join(', ') || '<none>'}`,
  );
  check(
    unparsed.length === 0,
    unparsed.length === 0
      ? 'every ALTER ROLE in the file is one this check understands'
      : `ALTER ROLE statement(s) this check cannot read, so they are NOT verified: ${unparsed.join(' | ')}`,
  );

  const uncreated = [...expected.keys()].filter((r) => !passwords.has(r));
  check(
    uncreated.length === 0,
    uncreated.length === 0
      ? 'every altered role is created with a password in the same file, so every one can be opened'
      : `altered but not created with a usable password here: ${uncreated.join(', ')}`,
  );
  const unreachable = [...expected.keys()].filter((r) => !databaseFor(r));
  check(
    unreachable.length === 0,
    unreachable.length === 0
      ? 'and every one is granted CONNECT somewhere, so the session check can reach it'
      : `no CONNECT grant in this file, so a session cannot be opened: ${unreachable.join(', ')}`,
  );

  /**
   * The control role: created here, reachable, and given no settings by the
   * file. It is what says the values below came from the role rather than from
   * the cluster.
   */
  const controlRole = [...passwords.keys()]
    .filter((r) => !expected.has(r) && databaseFor(r))
    .sort()[0];
  check(Boolean(controlRole), `CONTROL: ${controlRole ?? 'no'} role is altered by nothing in the file, and is the baseline below`);

  if (failures > 0) return;

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the settings are attached to the roles, in pg_db_role_setting');
  // -------------------------------------------------------------------------
  const reader = clientFor(controlRole, databaseFor(controlRole));
  await reader.connect();

  const catalog = await reader.query(`
    SELECT r.rolname                    AS role,
           coalesce(d.datname, '')      AS database,
           s.setconfig                  AS setconfig
      FROM pg_db_role_setting s
      JOIN pg_roles r ON r.oid = s.setrole
      LEFT JOIN pg_database d ON d.oid = s.setdatabase
     ORDER BY r.rolname, 2
  `);

  // A read that came back empty would make every comparison below vacuous.
  check(catalog.rowCount > 0, `CONTROL: the catalog read returned ${catalog.rowCount} role setting row(s)`);
  if (failures > 0) {
    await reader.end().catch(() => {});
    return;
  }

  /** role -> Map(guc -> value), cluster-wide rows only (setdatabase = 0). */
  const clusterWide = new Map();
  /** Rows scoped to one database, which OVERRIDE the cluster-wide ones. */
  const overrides = [];
  for (const row of catalog.rows) {
    const parsed = new Map();
    for (const entry of row.setconfig ?? []) {
      const at = entry.indexOf('=');
      if (at > 0) parsed.set(entry.slice(0, at).toLowerCase(), entry.slice(at + 1));
    }
    if (row.database === '') clusterWide.set(row.role, parsed);
    else for (const guc of parsed.keys()) if (gucs.includes(guc)) overrides.push(`${row.role} in ${row.database}: ${guc}=${parsed.get(guc)}`);
  }

  for (const role of [...expected.keys()].sort()) {
    const actual = clusterWide.get(role);
    if (!actual) {
      check(false, `${role}: NO cluster-wide settings at all — the bootstrap's ALTER ROLE is not applied to this cluster`);
      continue;
    }
    for (const [guc, value] of [...expected.get(role)].sort()) {
      const got = actual.get(guc);
      check(
        got === value,
        got === value
          ? `${role}: ${guc} = ${got}, set on the role`
          : `${role}: ${guc} is ${got === undefined ? 'NOT SET on the role' : `${got}`}, the file says ${value}`,
      );
    }
    const extra = [...actual.keys()].filter((g) => !expected.get(role).has(g));
    check(
      extra.length === 0,
      extra.length === 0
        ? `${role}: and nothing beyond what the file sets`
        : `${role}: carries ${extra.join(', ')}, which ${BOOTSTRAP} does not set — a hand-made ALTER ROLE is invisible to this repository`,
    );
  }

  /**
   * The other direction. A role given a timeout in psql by hand looks identical
   * to one given it by the bootstrap, and disappears on the next
   * `docker compose down -v` with nothing to recreate it.
   */
  const undeclared = [...clusterWide.keys()].filter((r) => !expected.has(r));
  check(
    undeclared.length === 0,
    undeclared.length === 0
      ? `no role carries settings ${BOOTSTRAP} does not give it`
      : `carry cluster-wide settings the file never sets: ${undeclared.join(', ')}`,
  );
  check(
    overrides.length === 0,
    overrides.length === 0
      ? 'and no per-database ALTER ROLE overrides any of them'
      : `per-database settings override the cluster-wide one: ${overrides.join('; ')}`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and a session opened as each role actually starts with them');
  // -------------------------------------------------------------------------
  /**
   * The catalog says the setting is attached; this says a backend starts with
   * it. Watch the effect, not the call.
   */
  for (const role of [...expected.keys()].sort()) {
    const database = databaseFor(role);
    const client = clientFor(role, database);
    try {
      await client.connect();
      const live = await showAll(client);
      for (const [guc, value] of [...expected.get(role)].sort()) {
        const want = await normalise(client, guc, value);
        const got = live.get(guc);
        check(got === want, `${role} on ${database}: SHOW ${guc} -> ${got}${got === want ? '' : `, expected ${want}`}`);
      }
    } catch (err) {
      check(false, `${role} on ${database}: could not open a session: ${err.code ?? ''} ${err.message}`);
    } finally {
      await client.end().catch(() => {});
    }
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and a role the file does not alter gets none of it');
  // -------------------------------------------------------------------------
  /**
   * PROVENANCE, not just the number. If the CLUSTER default for
   * statement_timeout were already 15s, every SHOW above would print 15s with
   * the ALTER ROLE deleted, and the whole section would be reading a default
   * back and agreeing with itself — which is exactly the community.feed
   * retention bug in CLAUDE.md. This is the measurement that rules it out.
   */
  const baseline = await showAll(reader);
  check(
    !clusterWide.has(controlRole),
    `${controlRole} has no row in pg_db_role_setting, as the file implies`,
  );
  for (const guc of gucs) {
    const got = baseline.get(guc);
    const wanted = [...wantedValues.get(guc)];
    check(
      !wanted.includes(got),
      wanted.includes(got)
        ? `${guc} is ${got} for ${controlRole} TOO — that is the cluster default, so a session showing it proves nothing about the role`
        : `CONTROL: ${controlRole} gets ${guc} = ${got}; the roles above get ${wanted.join('/')} from the ALTER ROLE, not from the cluster`,
    );
  }

  await reader.end().catch(() => {});

  console.log('');
  console.log(`  ${passes} ok, ${failures} failed`);
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log(
    `every role ${BOOTSTRAP} bounds carries both timeouts, on the role and in its own sessions`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
