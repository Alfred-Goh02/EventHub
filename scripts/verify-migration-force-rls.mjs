/**
 * Asserts no migration runs an UPDATE or DELETE that row security turns into nothing.
 *
 * Every service's tables are under FORCE ROW LEVEL SECURITY, with a tenant policy on
 * current_setting('app.tenant_id', true). A migration runs as the table OWNER, which
 * is not a superuser, with no app.tenant_id set. FORCE binds the owner to its own
 * policies, so an UPDATE or DELETE in a migration matches ZERO rows and succeeds: no
 * error, exit code 0, and a migration history that says it ran.
 *
 * Two merged migrations did exactly that:
 *
 *   event/20260905090500_backfill_empty_slugs         UPDATE "Event"
 *   ticketing/20260905093000_drop_idempotency_lease   DELETE FROM "IdempotencyKey"
 *
 * Measured as each owner, in a rolled-back transaction with one planted row: each
 * statement, verbatim, under FORCE -> 0 rows. With FORCE lifted -> 2 and 53, the
 * planted row plus real rows the migration had been written to fix. Three other
 * migrations hit the same wall and lifted FORCE around their backfill -- but only
 * because a SET NOT NULL after each one failed on the rows the UPDATE had skipped.
 * These two had nothing after them to fail.
 *
 * WHAT THIS CHECKS. Every service's migrations, walked in the order Prisma applies
 * them, tracking per table whether FORCE is on. Any UPDATE or DELETE that names a
 * table under FORCE at that point fails -- as its target, or anywhere else in the
 * statement, because a join or subquery over a FORCE table sees zero rows too, and
 * the write then matches nothing. 20260828130157 lifts FORCE on TicketType for
 * exactly that reason: its UPDATE writes ReservationItem but reads TicketType.
 *
 * INSERT is out of scope. Under FORCE a row the policy refuses fails WITH CHECK, and
 * that is loud.
 *
 * FORCE IS RECOGNISED IN THE FORMS THIS REPOSITORY WRITES IT, and anything else that
 * mentions it FAILS rather than being skipped:
 *
 *   - ALTER TABLE "X" [NO] FORCE ROW LEVEL SECURITY, any whitespace, IF EXISTS, ONLY,
 *     a schema prefix, or as one action in a comma-separated list;
 *   - a DO block's loop, as in community's init migration:
 *       FOREACH t IN ARRAY ARRAY['A', 'B', ...] LOOP
 *         EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
 *       END LOOP;
 *     which is expanded once per element, in order, before it is read.
 *
 * An UPDATE or DELETE whose table is only known at run time (format's %I outside such
 * a loop, or a string built with ||) fails the same way. A detector that skips what
 * it cannot parse reports a clean sweep of whatever it happened to understand.
 *
 * Comments are stripped first, including inside a DO block's body, so a comment
 * explaining an UPDATE is not read as one. A function body is runtime code, not
 * something the migration runs, so it is skipped and counted.
 *
 * THE TWO HISTORICAL NO-OPS ARE ALLOWED BY NAME, because an applied migration is never
 * edited. Each entry names the migration that re-did its statement properly, and is
 * checked in both directions: an entry that no longer matches a flagged statement
 * fails as stale -- an allowance for nothing is a hole waiting for something -- and
 * the migration it names must exist, come later, and write the same table with FORCE
 * lifted.
 *
 * Static, on purpose: no database. Replaying the migrations would only prove the rows
 * that happened to exist, and the originals ran against real rows and still reported
 * success.
 *
 * Run: node scripts/verify-migration-force-rls.mjs
 */

import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/**
 * Applied migrations that wrote to a table under FORCE, and so changed nothing. Only
 * for history: a new migration fixes itself instead of joining this list.
 */
const ALLOWED = [
  {
    file: 'services/event/prisma/migrations/20260905090500_backfill_empty_slugs/migration.sql',
    table: 'Event',
    redoneBy: 'services/event/prisma/migrations/20260925100000_rerun_empty_slug_backfill/migration.sql',
  },
  {
    file: 'services/ticketing/prisma/migrations/20260905093000_drop_idempotency_lease/migration.sql',
    table: 'IdempotencyKey',
    redoneBy: 'services/ticketing/prisma/migrations/20260925100000_rerun_in_progress_key_delete/migration.sql',
  },
];

/**
 * Applied migrations that lift FORCE outside an explicit transaction.
 *
 * PRISMA DOES NOT RUN A MIGRATION FILE AS ONE TRANSACTION: it sends the statements one
 * at a time. Measured on community's 20260925000000 with its backfill broken so the
 * deploy failed part-way: without BEGIN, the statements before the failure stayed
 * applied; with BEGIN and COMMIT, none did. So a file that lifts FORCE and then fails
 * before restoring it leaves the table without FORCE — the owner then bypasses its own
 * policies until someone notices. Each of these applied cleanly, so nothing was left
 * lifted; they are history, and an applied migration is never edited.
 */
const UNGUARDED_ALLOWED = [
  'services/event/prisma/migrations/20260825145733_harden_outbox/migration.sql',
  'services/ticketing/prisma/migrations/20260828130157_add_payment_and_price_capture/migration.sql',
  'services/ticketing/prisma/migrations/20260906130000_reconciler_ordered_scan/migration.sql',
];

// ---------------------------------------------------------------------------
// Reading SQL
// ---------------------------------------------------------------------------

const IDENT = String.raw`(?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)`;
const QUALIFIED = String.raw`${IDENT}(?:\s*\.\s*${IDENT})?`;

/** A (possibly schema-qualified) name, as the table Postgres resolves it to. */
function tableName(qualified) {
  const parts = [...qualified.matchAll(new RegExp(IDENT, 'g'))].map((m) => m[0]);
  return identName(parts[parts.length - 1]);
}

/** Quoted names keep their case; bare ones fold to lower case, as Postgres does. */
function identName(ident) {
  return ident.startsWith('"') ? ident.slice(1, -1).replace(/""/g, '"') : ident.toLowerCase();
}

/**
 * Splits SQL into code, comments, string literals and dollar-quoted bodies.
 *
 * Quoted identifiers stay code. `--` and nested block comments become a space.
 * A dollar-quoted body is kept whole and unread: whether it is a DO block to walk,
 * a function to skip or a format() string to expand is decided by whoever holds it.
 */
function lex(sql) {
  const out = [];
  let code = '';
  const flush = () => {
    if (code) out.push({ kind: 'code', text: code });
    code = '';
  };
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    const d = sql[i + 1];
    if (c === '-' && d === '-') {
      const end = sql.indexOf('\n', i);
      i = end === -1 ? sql.length : end;
      flush();
      out.push({ kind: 'comment' });
      continue;
    }
    if (c === '/' && d === '*') {
      let depth = 1;
      let j = i + 2;
      while (j < sql.length && depth > 0) {
        if (sql[j] === '/' && sql[j + 1] === '*') {
          depth += 1;
          j += 2;
        } else if (sql[j] === '*' && sql[j + 1] === '/') {
          depth -= 1;
          j += 2;
        } else {
          j += 1;
        }
      }
      if (depth > 0) throw new Error('unterminated block comment');
      flush();
      out.push({ kind: 'comment' });
      i = j;
      continue;
    }
    if (c === "'") {
      // E'...' strings take backslash escapes; every string takes '' as a quote.
      const escapes = /[Ee]/.test(sql[i - 1] ?? '') && !/[A-Za-z0-9_$]/.test(sql[i - 2] ?? '');
      let j = i + 1;
      for (;;) {
        if (j >= sql.length) throw new Error('unterminated string literal');
        if (escapes && sql[j] === '\\') j += 2;
        else if (sql[j] === "'" && sql[j + 1] === "'") j += 2;
        else if (sql[j] === "'") break;
        else j += 1;
      }
      flush();
      out.push({ kind: 'string', text: sql.slice(i, j + 1) });
      i = j + 1;
      continue;
    }
    if (c === '"') {
      let j = i + 1;
      for (;;) {
        if (j >= sql.length) throw new Error('unterminated quoted identifier');
        if (sql[j] === '"' && sql[j + 1] === '"') j += 2;
        else if (sql[j] === '"') break;
        else j += 1;
      }
      code += sql.slice(i, j + 1);
      i = j + 1;
      continue;
    }
    if (c === '$' && !/[A-Za-z0-9_$]/.test(sql[i - 1] ?? '')) {
      const tag = /^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(i, i + 64))?.[0];
      if (tag) {
        const end = sql.indexOf(tag, i + tag.length);
        if (end === -1) throw new Error(`unterminated dollar quote ${tag}`);
        flush();
        out.push({ kind: 'dollar', tag, body: sql.slice(i + tag.length, end) });
        i = end + tag.length;
        continue;
      }
    }
    code += c;
    i += 1;
  }
  flush();
  return out;
}

/** Tokens back to text. `mask` blanks what literals hold, keeping their shape. */
function render(tokens, { mask }) {
  return tokens
    .map((t) => {
      if (t.kind === 'code') return t.text;
      if (t.kind === 'comment') return ' ';
      if (t.kind === 'string') return mask ? `'${' '.repeat(t.text.length - 2)}'` : t.text;
      return mask ? `${t.tag}${' '.repeat(t.body.length)}${t.tag}` : `${t.tag}${t.body}${t.tag}`;
    })
    .join('');
}

/** The statements of a script, split at semicolons that are code. */
function statements(sql) {
  const out = [];
  let cur = [];
  for (const t of lex(sql)) {
    if (t.kind !== 'code') {
      cur.push(t);
      continue;
    }
    t.text.split(';').forEach((piece, k) => {
      if (k > 0) {
        out.push(cur);
        cur = [];
      }
      if (piece) cur.push({ kind: 'code', text: piece });
    });
  }
  out.push(cur);
  return out.filter((s) => render(s, { mask: false }).trim() !== '');
}

/**
 * Unrolls every `FOREACH v IN ARRAY ARRAY['A', ...] LOOP ... END LOOP` in a DO body.
 *
 * Each iteration is the loop body with every `EXECUTE format(<sql>, v)` replaced by
 * the SQL it would execute for that element, so whatever else the body does is kept,
 * in order. A loop over anything other than a literal array is left as it is, and its
 * format() strings are then what the unrecognised-FORCE check fails on.
 */
function expandLoops(body) {
  const LOOP = /\bFOREACH\s+([A-Za-z_]\w*)\s+IN\s+ARRAY\s+ARRAY\s*\[([^\]]*)\]\s*LOOP\b([\s\S]*?)\bEND\s+LOOP\b/gi;
  const EXECUTE_FORMAT =
    /\bEXECUTE\s+format\s*\(\s*(?:'((?:[^']|'')*)'|(\$\w*\$)([\s\S]*?)\2)\s*,\s*([A-Za-z_]\w*)\s*\)/gi;

  return body.replace(LOOP, (whole, variable, list, loopBody) => {
    const elements = [...list.matchAll(/'((?:[^']|'')*)'/g)].map((m) => m[1].replace(/''/g, "'"));
    const leftover = list.replace(/'((?:[^']|'')*)'/g, '').replace(/[\s,]/g, '');
    if (elements.length === 0 || leftover !== '') return whole;

    return elements
      .map((element) =>
        loopBody.replace(EXECUTE_FORMAT, (call, quoted, _tag, dollared, arg) => {
          if (arg.toLowerCase() !== variable.toLowerCase()) return call;
          const template = quoted !== undefined ? quoted.replace(/''/g, "'") : dollared;
          return template.replace(/%([%ILs])/g, (_, f) =>
            f === '%'
              ? '%'
              : f === 'I'
                ? `"${element.replace(/"/g, '""')}"`
                : f === 'L'
                  ? `'${element.replace(/'/g, "''")}'`
                  : element,
          );
        }),
      )
      .join('');
  });
}

const CREATE_TABLE = new RegExp(
  String.raw`\bCREATE\s+(?:(?:GLOBAL|LOCAL)\s+)?(?:(?:TEMP|TEMPORARY|UNLOGGED)\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(${QUALIFIED})`,
  'gi',
);
const DROP_TABLE = new RegExp(
  String.raw`\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(${QUALIFIED}(?:\s*,\s*${QUALIFIED})*)`,
  'gi',
);
const ALTER_TABLE = new RegExp(
  String.raw`\bALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?(${QUALIFIED})([\s\S]*)$`,
  'gi',
);
const RENAME_TO = new RegExp(String.raw`^\s*RENAME\s+TO\s+(${QUALIFIED})`, 'i');
const FORCE_ACTION = /(?:^|,)\s*(NO\s+)?FORCE\s+ROW\s+LEVEL\s+SECURITY\b/gi;
const FORCE_MENTION = /\bFORCE\s+ROW\s+LEVEL\s+SECURITY\b/gi;
const WRITE = new RegExp(
  String.raw`\b(?:(UPDATE)\s+(?:ONLY\s+)?(${QUALIFIED})(?:\s+(?:AS\s+)?${IDENT})?\s+SET\b|(DELETE)\s+FROM\s+(?:ONLY\s+)?(${QUALIFIED}))`,
  'gi',
);
const DYNAMIC_WRITE = /\b(?:UPDATE(?:\s+ONLY)?|DELETE\s+FROM(?:\s+ONLY)?)\s+(?:%[IsL]|'\s*\|\|)/gi;

// ---------------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------------

/** Every service with Prisma migrations, from the directory tree rather than a list. */
function servicesWithMigrations() {
  return readdirSync(join(ROOT, 'services'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(ROOT, 'services', d.name, 'prisma', 'migrations')))
    .map((d) => d.name)
    .sort();
}

/** A service's migrations in Prisma's order: directory name, lexically. */
function migrationsOf(service) {
  const dir = join(ROOT, 'services', service, 'prisma', 'migrations');
  return readdirSync(dir)
    .filter((name) => statSync(join(dir, name)).isDirectory())
    .sort()
    .map((name) => ({
      name,
      id: `${service}/${name}`,
      file: `services/${service}/prisma/migrations/${name}/migration.sql`,
    }));
}

function walk() {
  const writes = [];
  const problems = [];
  const snapshots = new Map();
  /** Top-level NO FORCE statements, and whether an explicit transaction was open. */
  const lifts = [];
  /** Tables a migration ended with FORCE lifted: `${migration.id} "${table}"`. */
  const leftLifted = [];
  let migrationCount = 0;
  let functionBodies = 0;

  const services = servicesWithMigrations();
  for (const service of services) {
    /** name -> { force, everForced } */
    const tables = new Map();

    /**
     * `text` is what the statement is matched in. `whole` is the same statement with
     * its literals left in, and is where FORCE is COUNTED -- so a FORCE hidden in a
     * literal this walk did not open (a DO block it failed to recognise, say) is a
     * mention nothing accounts for, and fails, instead of being blanked out of sight.
     */
    const scan = (text, migration, whole = text) => {
      const events = [];
      for (const m of text.matchAll(CREATE_TABLE)) events.push({ at: m.index, kind: 'create', table: tableName(m[1]) });
      for (const m of text.matchAll(DROP_TABLE)) {
        for (const q of m[1].matchAll(new RegExp(QUALIFIED, 'g'))) {
          events.push({ at: m.index, kind: 'drop', table: tableName(q[0]) });
        }
      }
      let attributed = 0;
      for (const m of text.matchAll(ALTER_TABLE)) {
        const table = tableName(m[1]);
        const rename = RENAME_TO.exec(m[2]);
        if (rename) events.push({ at: m.index, kind: 'rename', table, to: tableName(rename[1]) });
        for (const a of m[2].matchAll(FORCE_ACTION)) {
          events.push({ at: m.index, kind: 'force', table, on: !a[1] });
          attributed += 1;
        }
      }
      const mentioned = (whole.match(FORCE_MENTION) ?? []).length;
      if (mentioned > attributed) {
        problems.push(
          `${migration.id} mentions FORCE ROW LEVEL SECURITY ${mentioned} time(s) and only ${attributed} ` +
            `could be tied to a table: ${whole.trim().replace(/\s+/g, ' ').slice(0, 160)}`,
        );
      }
      for (const m of text.matchAll(DYNAMIC_WRITE)) {
        problems.push(
          `${migration.id} writes to a table named only at run time, which this walk cannot check: ` +
            text.slice(m.index).trim().replace(/\s+/g, ' ').slice(0, 160),
        );
      }
      for (const m of text.matchAll(WRITE)) {
        events.push({
          at: m.index,
          kind: 'write',
          verb: m[1] ? 'UPDATE' : 'DELETE FROM',
          table: tableName(m[2] ?? m[4]),
          extent: text.slice(m.index),
        });
      }

      events.sort((a, b) => a.at - b.at);
      for (const e of events) {
        if (e.kind === 'create') tables.set(e.table, { force: false, everForced: false });
        else if (e.kind === 'drop') tables.delete(e.table);
        else if (e.kind === 'rename') {
          if (tables.has(e.table)) tables.set(e.to, tables.get(e.table));
          tables.delete(e.table);
        } else if (e.kind === 'force') {
          const t = tables.get(e.table) ?? { force: false, everForced: false };
          t.force = e.on;
          t.everForced ||= e.on;
          tables.set(e.table, t);
        } else {
          const named = new Set([e.table]);
          for (const r of e.extent.matchAll(new RegExp(IDENT, 'g'))) {
            const n = identName(r[0]);
            if (tables.has(n)) named.add(n);
          }
          writes.push({
            service,
            migration,
            verb: e.verb,
            table: e.table,
            known: tables.has(e.table),
            forced: [...named].filter((n) => tables.get(n)?.force),
            lifted: [...named].filter((n) => tables.get(n) && !tables.get(n).force && tables.get(n).everForced),
          });
        }
      }
    };

    /** Whether the migration being walked has an explicit transaction open. */
    const tx = { open: false };

    const walkScript = (sql, migration, { inDo }) => {
      for (const stmt of statements(sql)) {
        // Rendered, not the first code token: comments split the code into pieces,
        // and the first piece of a commented statement is the blank line above it.
        const head = render(stmt, { mask: true });
        if (!inDo) {
          if (/^\s*(?:BEGIN|START\s+TRANSACTION)\b/i.test(head)) tx.open = true;
          else if (/^\s*(?:COMMIT|END)\b/i.test(head)) tx.open = false;
          // A lift inside a DO block is one statement with its restore, so only a
          // top-level lift depends on the file being one transaction.
          if (/\bNO\s+FORCE\s+ROW\s+LEVEL\s+SECURITY\b/i.test(head)) lifts.push({ migration, guarded: tx.open });
        }
        if (!inDo && /^\s*DO\b/i.test(head)) {
          const body = stmt.find((t) => t.kind === 'dollar' || t.kind === 'string');
          if (!body) {
            problems.push(`${migration.id} has a DO block whose body this walk could not find`);
            continue;
          }
          const code = body.kind === 'dollar' ? body.body : body.text.slice(1, -1).replace(/''/g, "'");
          walkScript(expandLoops(render(lex(code), { mask: false })), migration, { inDo: true });
          continue;
        }
        const isFunction = !inDo && /^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\b/i.test(head);
        if (isFunction) functionBodies += 1;
        // Outside a DO block a literal is data, and blanked. Inside one it may be
        // SQL for EXECUTE, so it is read. A function body is neither: it runs later,
        // as whoever calls it, so not even its FORCE mentions count.
        const whole = render(stmt, { mask: false });
        scan(inDo ? whole : head, migration, isFunction ? head : whole);
      }
    };

    for (const migration of migrationsOf(service)) {
      const path = join(ROOT, migration.file);
      if (!existsSync(path)) {
        problems.push(`${migration.id} has no migration.sql`);
        continue;
      }
      migrationCount += 1;
      tx.open = false;
      try {
        walkScript(readFileSync(path, 'utf8'), migration, { inDo: false });
      } catch (err) {
        problems.push(`${migration.id} could not be read as SQL: ${err.message}`);
      }
      if (tx.open) problems.push(`${migration.id} opens a transaction it never commits`);
      // A lift that is never restored leaves the owner outside its own policies for
      // every migration after this one, and every psql session as the owner.
      for (const [name, t] of tables) if (t.everForced && !t.force) leftLifted.push(`${migration.id} "${name}"`);
      snapshots.set(migration.id, new Map([...tables].map(([name, t]) => [name, t.force])));
    }
  }

  return { services, migrationCount, functionBodies, writes, problems, snapshots, lifts, leftLifted };
}

function main() {
  const { services, migrationCount, functionBodies, writes, problems, snapshots, lifts, leftLifted } = walk();

  // -------------------------------------------------------------------------
  console.log('  the walk has to have found something');
  // -------------------------------------------------------------------------
  /**
   * Every assertion below is "this statement ran with FORCE off". If the walk reads
   * nothing, all of them hold of nothing and the file reports a clean history.
   */
  check(services.length > 0, `CONTROL: ${services.length} service(s) with migrations: ${services.join(', ')}`);
  check(migrationCount > 0, `CONTROL: ${migrationCount} migration(s) read, in the order Prisma applies them`);
  check(
    writes.length > 0,
    `CONTROL: ${writes.length} UPDATE/DELETE statement(s) examined` +
      (functionBodies > 0 ? ` (${functionBodies} function body(ies) skipped as runtime code)` : ''),
  );
  if (services.length === 0 || migrationCount === 0 || writes.length === 0) {
    console.error('\nFAIL: nothing to check');
    process.exitCode = 1;
    return;
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and it has to recognise FORCE in each form the migrations write it');
  // -------------------------------------------------------------------------
  /**
   * If a form went unrecognised, the table would read as never forced and every
   * write to it would pass. These fail when that happens, for each form in use.
   */
  const forceAfter = (id, table) => snapshots.get(id)?.get(table);
  check(
    forceAfter('event/20260823081317_init_event', 'Event') === true,
    'CONTROL: plain ALTER TABLE -- Event is under FORCE after event/20260823081317_init_event ' +
      `(read: ${forceAfter('event/20260823081317_init_event', 'Event')})`,
  );
  check(
    forceAfter('community/20260918114213_init_community', 'Post') === true,
    "CONTROL: the DO block's FOREACH loop -- Post is under FORCE after " +
      `community/20260918114213_init_community (read: ${forceAfter('community/20260918114213_init_community', 'Post')})`,
  );
  const lifted = writes.filter((w) => w.lifted.length > 0);
  check(
    lifted.length > 0,
    `CONTROL: NO FORCE -- ${lifted.length} statement(s) ran with FORCE lifted on a table that had it`,
  );
  check(
    problems.length === 0,
    problems.length === 0
      ? 'nothing mentioning FORCE, and no write, was left in a form this walk cannot read'
      : `${problems.length} statement(s) this walk could not read, and will not guess about:\n` +
          problems.map((p) => `            ${p}`).join('\n'),
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  every UPDATE and DELETE a migration runs, against the FORCE state at that point');
  // -------------------------------------------------------------------------
  const used = new Set();
  for (const w of writes) {
    const label = `${w.migration.id}: ${w.verb} "${w.table}"`;
    if (!w.known) {
      check(
        false,
        `${label} -- no earlier migration of ${w.service} creates that table, so whether it is under ` +
          'FORCE is unknown here, and unknown is not a pass',
      );
      continue;
    }
    if (w.forced.length === 0) {
      check(
        true,
        w.lifted.length > 0
          ? `${label}, with FORCE lifted on ${w.lifted.map((t) => `"${t}"`).join(', ')}`
          : `${label}, never under FORCE`,
      );
      continue;
    }
    const allowance = ALLOWED.find((a) => a.file === w.migration.file && w.forced.includes(a.table));
    if (allowance && w.forced.every((t) => t === allowance.table)) {
      used.add(allowance);
      check(
        true,
        `${label} ran under FORCE and changed nothing. Allowed: applied history, re-done by ` +
          allowance.redoneBy.replace(/^services\/(\w+)\/prisma\/migrations\/([^/]+)\/migration\.sql$/, '$1/$2'),
      );
      continue;
    }
    check(
      false,
      `${label} names ${w.forced.map((t) => `"${t}"`).join(', ')} while under FORCE. A migration runs as ` +
        'the owner with no app.tenant_id, so FORCE shows it zero rows and this statement will match ' +
        'nothing and succeed. Lift FORCE around it, on every table it reads or writes, inside ' +
        'BEGIN/COMMIT: ALTER TABLE ... NO FORCE ROW LEVEL SECURITY; <statement>; ALTER TABLE ... FORCE ' +
        'ROW LEVEL SECURITY.',
    );
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and every allowance still matches something, and names the migration that re-did it');
  // -------------------------------------------------------------------------
  for (const a of ALLOWED) {
    check(
      used.has(a),
      used.has(a)
        ? `${a.file} on "${a.table}" -- matched`
        : `${a.file} on "${a.table}" matched no statement under FORCE. A stale allowance is a hole: ` +
          'remove it, or find out why the walk no longer sees what it allowed',
    );

    const service = a.file.split('/')[1];
    const original = writes.find((w) => w.migration.file === a.file)?.migration.name ?? '';
    const redo = writes.filter((w) => w.migration.file === a.redoneBy);
    const redoName = a.redoneBy.split('/')[4] ?? '';
    const sameService = a.redoneBy.split('/')[1] === service;
    const clean = redo.some((w) => w.table === a.table && w.known && w.forced.length === 0);
    check(
      sameService && redoName > original && clean,
      sameService && redoName > original && clean
        ? `  re-done by ${a.redoneBy}, which writes "${a.table}" with FORCE lifted`
        : `  ${a.redoneBy} must be a later migration of ${service} that writes "${a.table}" with FORCE ` +
          `lifted; it ${redo.length === 0 ? 'has no UPDATE or DELETE this walk found' : 'does not'}`,
    );
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  a lift is inside an explicit transaction, and FORCE is back by the end of the file');
  // -------------------------------------------------------------------------
  /**
   * Prisma sends a migration's statements one at a time, so without BEGIN a failure
   * between the lift and the restore leaves the table without FORCE. See
   * UNGUARDED_ALLOWED above for the measurement.
   */
  const guarded = lifts.filter((l) => l.guarded);
  check(guarded.length > 0, `CONTROL: ${guarded.length} of ${lifts.length} top-level lift(s) were read inside BEGIN ... COMMIT`);
  const unguardedFiles = new Set(lifts.filter((l) => !l.guarded).map((l) => l.migration.file));
  for (const file of unguardedFiles) {
    const allowed = UNGUARDED_ALLOWED.includes(file);
    check(
      allowed,
      allowed
        ? `${file} lifts FORCE outside a transaction. Allowed: applied history that completed`
        : `${file} lifts FORCE outside an explicit transaction. Prisma does not make a migration one: ` +
            'a failure before the restore leaves the table without FORCE. Wrap the file in BEGIN; ... COMMIT;',
    );
  }
  for (const file of UNGUARDED_ALLOWED) {
    check(
      unguardedFiles.has(file),
      unguardedFiles.has(file) ? `  ${file} -- allowance matched` : `${file} is allowed to lift FORCE unguarded but no longer does. A stale allowance is a hole: remove it`,
    );
  }
  check(
    leftLifted.length === 0,
    leftLifted.length === 0
      ? 'every table that was ever under FORCE has it back by the end of each migration'
      : `these migrations end with FORCE lifted: ${leftLifted.join('; ')}`,
  );

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('no migration writes to a table that row security hides from it');
}

main();
