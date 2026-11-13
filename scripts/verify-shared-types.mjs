/**
 * Asserts that the shared vocabulary matches the things it claims to describe.
 *
 * packages/types opens by saying "a shared type that disagrees with reality is
 * worse than no type, because consumers trust it". Two of its three types
 * disagreed with reality:
 *
 *   ReservationStatus   declared PENDING, which is not a state this system has,
 *                       and omitted HELD and PENDING_PAYMENT — the two the whole
 *                       seat and payment design turns on.
 *   TicketReservedEvent had no tenantId, in a system whose every row-level
 *                       policy reads app.tenant_id; no state; and flattened the
 *                       items ARRAY to a single ticketTypeId and quantity, so it
 *                       could not describe a two-line cart.
 *
 * Neither was imported anywhere, which is the only reason nothing broke. They
 * would have been wrong for the first consumer to trust them, and the failure
 * would have looked like a bug in that consumer.
 *
 * So this compares the shared types against their SOURCES — the Prisma enums,
 * and the list of Avro records that already have a generator. It reads the real
 * files rather than a copy, because a check that compares two hand-maintained
 * lists just adds a third thing to keep in step.
 *
 * Run: node scripts/verify-shared-types.mjs
 */

import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** Resolved from this file, not from cwd — CI runs scripts from various places. */
const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const read = (p) => readFileSync(join(ROOT, p), 'utf8');

/** Members of a Prisma `enum Name { ... }` block. */
function prismaEnum(schemaPath, name) {
  const text = read(schemaPath);
  const block = text.match(new RegExp(`enum\\s+${name}\\s*\\{([\\s\\S]*?)\\}`));
  if (!block) return null;
  return block[1]
    .split(/\r?\n/)
    .map((l) => l.replace(/\/\/.*$/, '').trim())
    .filter((l) => /^[A-Z_]+$/.test(l));
}

/** Members of an exported TypeScript string-literal union. */
function tsUnion(tsPath, name) {
  const text = read(tsPath);
  const block = text.match(new RegExp(`export type ${name} =([\\s\\S]*?);`));
  if (!block) return null;
  return [...block[1].matchAll(/'([^']+)'/g)].map((m) => m[1]);
}

/** Members of an exported `as const` string array. */
function tsConstArray(tsPath, name) {
  const text = read(tsPath);
  const block = text.match(new RegExp(`export const ${name} = \\[([\\s\\S]*?)\\] as const;`));
  if (!block) return null;
  return [...block[1].matchAll(/'([^']+)'/g)].map((m) => m[1]);
}

const same = (a, b) => a.length === b.length && [...a].sort().every((v, i) => v === [...b].sort()[i]);

const TYPES = 'packages/types/src/index.ts';

function main() {
  // -------------------------------------------------------------------------
  console.log('  the shared enums must match the databases they describe');
  // -------------------------------------------------------------------------
  const cases = [
    {
      label: 'ReservationStatus',
      union: tsUnion(TYPES, 'ReservationStatus'),
      source: prismaEnum('services/ticketing/prisma/schema.prisma', 'ReservationStatus'),
      from: 'ticketing schema.prisma',
    },
    {
      label: 'Role',
      union: tsConstArray(TYPES, 'ROLES'),
      source: prismaEnum('services/auth/prisma/schema.prisma', 'Role'),
      from: 'auth schema.prisma',
    },
  ];

  for (const c of cases) {
    /**
     * Both sides have to have been FOUND. A regex that stops matching — a
     * renamed type, a reformatted enum — would otherwise compare null to null
     * and pass, which is the failure mode this whole file is about.
     */
    check(
      Array.isArray(c.union) && c.union.length > 0,
      c.union?.length ? `read ${c.union.length} members from ${c.label}` : `could not parse ${c.label}`,
    );
    check(
      Array.isArray(c.source) && c.source.length > 0,
      c.source?.length ? `read ${c.source.length} from ${c.from}` : `could not parse it in ${c.from}`,
    );

    if (c.union?.length && c.source?.length) {
      const missing = c.source.filter((v) => !c.union.includes(v));
      const invented = c.union.filter((v) => !c.source.includes(v));
      check(
        same(c.union, c.source),
        same(c.union, c.source)
          ? `${c.label} matches ${c.from}`
          : `${c.label} disagrees with ${c.from} —` +
            (missing.length ? ` missing ${missing.join(', ')};` : '') +
            (invented.length ? ` declares ${invented.join(', ')} which does not exist;` : ''),
      );
    }
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and nothing here hand-copies a record a generator already emits');
  // -------------------------------------------------------------------------
  /**
   * The ratchet. TicketReservedEvent was a hand-written duplicate of a record
   * that contracts/generated already produces from the .avsc — checked in, and
   * regenerated and diffed by CI, so it cannot drift. The hand copy could only
   * drift, and had.
   *
   * This catches the next one by name: for every Avro record with a generated
   * type, the shared package must not declare an interface of its own for it.
   */
  const generatedDir = join(ROOT, 'contracts/generated');
  const generated = existsSync(generatedDir)
    ? readdirSync(generatedDir).filter((f) => f.endsWith('.ts'))
    : [];

  check(generated.length > 0, `found ${generated.length} generated contract file(s) to check against`);

  const sharedText = read(TYPES);
  const duplicated = [];

  for (const file of generated) {
    const text = readFileSync(join(generatedDir, file), 'utf8');
    for (const m of text.matchAll(/export interface (\w+)/g)) {
      // `export interface X` in the shared package, not a mention in prose.
      if (new RegExp(`export interface ${m[1]}\\b`).test(sharedText)) {
        duplicated.push(`${m[1]} (generated in ${file})`);
      }
    }
  }

  check(
    duplicated.length === 0,
    duplicated.length === 0
      ? 'no generated record is re-declared by hand'
      : `hand-copied from a generated contract, which can only drift:\n        ${duplicated.join('\n        ')}`,
  );

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the shared vocabulary says what the schemas and contracts actually say');
}

main();
