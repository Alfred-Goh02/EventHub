/**
 * Generates TypeScript types from the committed .avsc files.
 *
 * Reads contracts/avro/*.avsc and writes contracts/generated/*.ts.
 *
 * Generated FROM THE COMMITTED FILES, never from a live registry. A build that
 * needs port 8081 to be reachable is not a reproducible build, and it would make
 * the types depend on whatever happens to be registered rather than on what is
 * in the repository. The .avsc is the source of truth; the registry is a cache
 * of it that happens to also enforce compatibility.
 *
 * The output is committed, and CI regenerates and compares, so editing a schema
 * without regenerating fails the build rather than shipping types that quietly
 * disagree with the wire format.
 *
 * That comparison used to be `git diff --exit-code` alone, which compares
 * TRACKED files. A brand-new .avsc produces a brand-new .ts, which is
 * untracked, and git diff ignores it -- so the one case where the generated
 * file is MISSING rather than merely stale was the one case the gate could not
 * see. CI runs `git ls-files --others` for that half, and keeps git diff for
 * the other: git status --porcelain reports both but calls content-identical
 * files modified when core.autocrlf is true, which is most Windows checkouts.
 *
 * Run: node scripts/generate-avro-types.mjs
 */

import { readFileSync, readdirSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const IN = join(ROOT, 'contracts', 'avro');
const OUT = join(ROOT, 'contracts', 'generated');

const pascal = (s) => s.replace(/(^|[^A-Za-z0-9])([a-z])/g, (_, __, c) => c.toUpperCase());

/**
 * Renders an Avro type as a TypeScript type, collecting the named types it meets
 * along the way into `ctx` so the caller can emit declarations for them.
 *
 * Both enums AND records have to be collected. An earlier version collected only
 * enums and returned `t.name` for a record, which produced output referencing an
 * interface that was never declared — `items: Array<ReservationLine>` with no
 * ReservationLine anywhere in the file. It went unnoticed because nothing
 * imported the generated types and no tsconfig covered them, so the files were
 * documentation that merely looked like code. Hence contracts/tsconfig.json and
 * the CI step that type-checks this output.
 */
/**
 * Records that a named type belongs to this short name, and refuses if two
 * different Avro types want the same one.
 *
 * Avro names are FULLY QUALIFIED -- eventhub.events.EventSnapshot -- and this
 * generator threw the namespace away and emitted the short name. Two records
 * called EventSnapshot in different namespaces are different types, and both
 * would have become `export interface EventSnapshot`.
 *
 * Within one file that is worse than a name clash, because the record branch
 * below skips a name it has already claimed: the second record is silently
 * DROPPED and every field referencing it is typed as the first one. Across two
 * files it is two same-named exports from one package describing different
 * shapes, and an import of the wrong one type-checks.
 *
 * Neither produces an error today. This makes both a build failure. The
 * registry spans the whole run, not one file, because everything in
 * contracts/generated is imported from the same place.
 */
function claim(ctx, kind, t) {
  const namespace = t.namespace ?? ctx.namespace;
  const full = namespace ? `${namespace}.${t.name}` : t.name;
  const held = ctx.registry.get(t.name);

  if (held && held.full !== full) {
    throw new Error(
      `two ${kind}s want the TypeScript name ${t.name}: ${held.full} (${held.file}) and ` +
        `${full} (${ctx.file}). Avro names are fully qualified and TypeScript names are ` +
        `not, so one of these would silently replace the other. Rename one, or teach this ` +
        `generator to qualify its output.`,
    );
  }

  if (!held) ctx.registry.set(t.name, { full, file: ctx.file });
}

function tsType(t, ctx) {
  if (typeof t === 'string') {
    switch (t) {
      case 'string': case 'bytes': return 'string';
      case 'int': case 'long': case 'float': case 'double': return 'number';
      case 'boolean': return 'boolean';
      case 'null': return 'null';
      default: return t; // a named type declared elsewhere
    }
  }

  if (Array.isArray(t)) {
    // A union. ["null", "string"] is the idiomatic nullable field.
    return [...new Set(t.map((m) => tsType(m, ctx)))].join(' | ');
  }

  if (t.type === 'enum') {
    claim(ctx, 'enum', t);
    ctx.enums.set(t.name, t.symbols);
    return t.name;
  }
  if (t.type === 'array') return `Array<${tsType(t.items, ctx)}>`;
  if (t.type === 'map') return `Record<string, ${tsType(t.values, ctx)}>`;
  if (t.type === 'record') {
    claim(ctx, 'record', t);
    if (!ctx.records.has(t.name)) {
      // Claim the name BEFORE recursing into the fields. Avro permits a record
      // to reference itself, and without this the recursion would not terminate.
      ctx.records.set(t.name, null);
      ctx.records.set(t.name, {
        doc: t.doc,
        fields: (t.fields ?? []).map((f) => ({
          name: f.name,
          type: tsType(f.type, ctx),
          doc: f.doc,
        })),
      });
    }
    return t.name;
  }

  return tsType(t.type, ctx);
}

function wrap(text, indent = ' * ') {
  if (!text) return [];
  const words = String(text).split(/\s+/);
  const lines = [];
  let line = '';
  for (const w of words) {
    if ((line + ' ' + w).trim().length > 76) { lines.push(line.trim()); line = w; }
    else line = (line + ' ' + w).trim();
  }
  if (line) lines.push(line);
  return ['/**', ...lines.map((l) => `${indent}${l}`), ' */'];
}

function generate(schema, sourceFile, registry) {
  const ctx = {
    enums: new Map(),
    records: new Map(),
    // Inherited by any nested type that does not declare its own, which is how
    // Avro name resolution works.
    namespace: schema.namespace,
    file: sourceFile,
    registry,
  };
  claim(ctx, 'record', schema);
  const fields = schema.fields.map((f) => ({
    name: f.name,
    type: tsType(f.type, ctx),
    doc: f.doc,
    // `optional` was computed here and never read. Deleting it rather than
    // wiring it up, because emitting `field?: T` would be WRONG: a decoded Avro
    // record always carries every field. A default is what the READER supplies
    // when resolving an older writer schema, so the field is present either way,
    // and `?` would tell consumers to handle an undefined that cannot occur.
  }));
  const { enums } = ctx;

  const out = [];
  out.push('// GENERATED FILE — do not edit by hand.');
  out.push(`// Source: contracts/avro/${sourceFile}`);
  out.push('// Regenerate: node scripts/generate-avro-types.mjs');
  out.push('');

  for (const [name, symbols] of enums) {
    out.push(`export type ${name} =`);
    out.push(symbols.map((s) => `  | '${s}'`).join('\n') + ';');
    out.push('');
  }

  // Nested records, declared before the record that uses them. TypeScript hoists
  // interfaces so the order is not required, but reading top-down is easier when
  // a type is defined before it is referenced.
  for (const [name, rec] of ctx.records) {
    if (rec.doc) out.push(...wrap(rec.doc));
    out.push(`export interface ${name} {`);
    for (const f of rec.fields) {
      if (f.doc) out.push(...wrap(f.doc).map((l) => '  ' + l));
      out.push(`  ${f.name}: ${f.type};`);
    }
    out.push('}');
    out.push('');
  }

  if (schema.doc) out.push(...wrap(schema.doc));
  out.push(`export interface ${schema.name} {`);
  for (const f of fields) {
    if (f.doc) out.push(...wrap(f.doc).map((l) => '  ' + l));
    out.push(`  ${f.name}: ${f.type};`);
  }
  out.push('}');
  out.push('');

  return out.join('\n');
}

mkdirSync(OUT, { recursive: true });

const files = readdirSync(IN).filter((f) => f.endsWith('.avsc'));

/**
 * Shared across every file, so a name claimed by one schema is claimed for all
 * of them. contracts/generated is one package as far as an importer is
 * concerned, so two files exporting the same name is the same problem as one
 * file trying to.
 */
const registry = new Map();

for (const file of files) {
  const schema = JSON.parse(readFileSync(join(IN, file), 'utf8'));
  // events.activity-value.avsc -> events-activity.ts
  const outName = file.replace(/-value\.avsc$/, '').replace(/\./g, '-') + '.ts';
  writeFileSync(join(OUT, outName), generate(schema, file, registry));
  console.log(`${file}  ->  contracts/generated/${outName}  (${schema.name})`);
}
