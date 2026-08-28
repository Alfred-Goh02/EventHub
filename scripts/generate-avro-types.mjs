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
 * The output is committed, and CI regenerates and runs `git diff --exit-code`,
 * so editing a schema without regenerating fails the build rather than shipping
 * types that quietly disagree with the wire format.
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
    ctx.enums.set(t.name, t.symbols);
    return t.name;
  }
  if (t.type === 'array') return `Array<${tsType(t.items, ctx)}>`;
  if (t.type === 'map') return `Record<string, ${tsType(t.values, ctx)}>`;
  if (t.type === 'record') {
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

function generate(schema, sourceFile) {
  const ctx = { enums: new Map(), records: new Map() };
  const fields = schema.fields.map((f) => ({
    name: f.name,
    type: tsType(f.type, ctx),
    doc: f.doc,
    optional: Array.isArray(f.type) && f.type.includes('null') && 'default' in f,
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
for (const file of files) {
  const schema = JSON.parse(readFileSync(join(IN, file), 'utf8'));
  // events.activity-value.avsc -> events-activity.ts
  const outName = file.replace(/-value\.avsc$/, '').replace(/\./g, '-') + '.ts';
  writeFileSync(join(OUT, outName), generate(schema, file));
  console.log(`${file}  ->  contracts/generated/${outName}  (${schema.name})`);
}
