/**
 * Registers the committed Avro schemas with the Schema Registry, and pins each
 * subject's compatibility mode.
 *
 * The .avsc files in contracts/avro are the SOURCE OF TRUTH. The registry is a
 * cache of them, not the other way round — which is why TypeScript types are
 * generated from the committed files and never from a live registry. A build
 * that needs port 8081 to be up is not a reproducible build.
 *
 * Run: node scripts/register-schemas.mjs
 */

import { readFileSync, readdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';
const DIR = join(dirname(fileURLToPath(import.meta.url)), '..', 'contracts', 'avro');

/**
 * BACKWARD_TRANSITIVE, not BACKWARD.
 *
 * BACKWARD checks a new schema only against the IMMEDIATELY PREVIOUS version.
 * That is enough when consumers only ever read recent data — but events.activity
 * is compacted and Ticketing rebuilds its projection by replaying from offset 0,
 * so it reads data written under every historical schema.
 *
 * A chain of individually-BACKWARD-compatible changes can be collectively
 * incompatible with v1: drop a field in v2, add it back with a different type in
 * v3, and each step passes while v3 cannot read v1. TRANSITIVE checks against
 * ALL previous versions and rejects that.
 *
 * Set per subject rather than relying on the broker-level default, because the
 * env var only supplies a default for NEW subjects and is silently overridden by
 * any existing per-subject config.
 */
const COMPATIBILITY = 'BACKWARD_TRANSITIVE';

async function req(path, options = {}) {
  const res = await fetch(`${REGISTRY}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/vnd.schemaregistry.v1+json',
      ...(options.headers ?? {}),
    },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${options.method ?? 'GET'} ${path} -> ${res.status} ${text}`);
  return text ? JSON.parse(text) : null;
}

async function main() {
  const files = readdirSync(DIR).filter((f) => f.endsWith('.avsc'));
  if (files.length === 0) throw new Error(`no .avsc files in ${DIR}`);

  for (const file of files) {
    // The filename IS the subject. Under the default TopicNameStrategy the
    // subject for a topic's values is `<topic>-value`, so naming the file
    // events.activity-value.avsc keeps the mapping obvious and removes any
    // chance of registering against the wrong subject.
    const subject = file.replace(/\.avsc$/, '');
    const schema = readFileSync(join(DIR, file), 'utf8');

    // Compatibility FIRST. Registering then tightening would let an
    // incompatible schema through on the very run that sets up the rule.
    await req(`/config/${subject}`, {
      method: 'PUT',
      body: JSON.stringify({ compatibility: COMPATIBILITY }),
    });

    const { id } = await req(`/subjects/${subject}/versions`, {
      method: 'POST',
      body: JSON.stringify({ schemaType: 'AVRO', schema }),
    });

    const versions = await req(`/subjects/${subject}/versions`);
    const config = await req(`/config/${subject}`);

    console.log(
      `${subject.padEnd(24)} id=${id}  versions=[${versions.join(', ')}]  compatibility=${config.compatibilityLevel}`,
    );
  }
}

main().catch((err) => {
  console.error(String(err.message ?? err));
  process.exit(1);
});
