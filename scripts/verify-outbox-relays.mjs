/**
 * Asserts that every outbox in the repository has a relay in compose.
 *
 * An outbox row is a message that has committed and has not been sent. Only a relay
 * sends it, and a relay drains exactly one database per instance
 * (RELAY_DATABASE_URL). Compose ran one relay, for Event. Ticketing has had an
 * outbox since Chapter 4, and on one local stack 290 of its messages, 236 seat holds
 * and 54 purchases, had been waiting eleven days for a relay that was never
 * started. Nothing errored: a message nobody sends looks exactly like a quiet day.
 * The compose CI job never makes a reservation, so nothing there could notice.
 *
 * DERIVED, NOT LISTED. Which services have an outbox comes from their Prisma
 * schemas, which database each one uses comes from compose, and the relay's default
 * forbidden table comes from the relay's own env.ts.
 *
 * Run: node scripts/verify-outbox-relays.mjs   (needs the docker CLI, not a running engine)
 */

import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
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

/** `//` and `///` comments removed, so a model named in prose is not read as one. */
const withoutComments = (text) =>
  text
    .split(/\r?\n/)
    .map((line) => line.replace(/\/\/.*$/, ''))
    .join('\n');

const databaseOf = (url) => new URL(url).pathname.replace(/^\//, '');
const userOf = (url) => decodeURIComponent(new URL(url).username);

function main() {
  console.log('  what the repository has');

  const schemas = new Map();
  for (const dir of readdirSync(join(ROOT, 'services'), { withFileTypes: true })) {
    const path = join(ROOT, 'services', dir.name, 'prisma', 'schema.prisma');
    if (dir.isDirectory() && existsSync(path)) {
      const models = [...withoutComments(readFileSync(path, 'utf8')).matchAll(/^model (\w+)\s*\{/gm)].map((m) => m[1]);
      schemas.set(dir.name, models);
    }
  }
  const withOutbox = [...schemas.entries()].filter(([, models]) => models.includes('OutboxMessage')).map(([name]) => name).sort();

  const relayEnv = withoutComments(readFileSync(join(ROOT, 'services', 'relay', 'src', 'env.ts'), 'utf8'));
  const defaultForbidden = relayEnv.match(/RELAY_FORBIDDEN_TABLE:\s*z\.[^\n]*\.default\('([^']+)'\)/)?.[1];

  check(schemas.size >= 4, `CONTROL: ${schemas.size} Prisma schemas read`);
  check(withOutbox.length >= 2, `CONTROL: ${withOutbox.length} services have an OutboxMessage table: ${withOutbox.join(', ')}`);
  check(Boolean(defaultForbidden), `CONTROL: the relay's default RELAY_FORBIDDEN_TABLE is "${defaultForbidden}"`);
  if (failures > 0) return;

  const config = JSON.parse(
    execFileSync('docker', ['compose', 'config', '--format', 'json'], { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }),
  );
  const services = config.services ?? {};
  const relays = Object.entries(services)
    .filter(([, s]) => s.environment?.RELAY_DATABASE_URL)
    .map(([name, s]) => ({
      name,
      database: databaseOf(s.environment.RELAY_DATABASE_URL),
      user: userOf(s.environment.RELAY_DATABASE_URL),
      forbidden: s.environment.RELAY_FORBIDDEN_TABLE ?? defaultForbidden,
    }));
  check(relays.length >= 1, `CONTROL: compose runs ${relays.length} relay(s): ${relays.map((r) => `${r.name} -> ${r.database}`).join(', ')}`);
  if (failures > 0) return;

  console.log('');
  console.log('  every outbox is drained, by a relay that can read nothing else');
  const outboxDatabases = new Set();
  for (const name of withOutbox) {
    const env = services[name]?.environment ?? {};
    if (!env.DATABASE_URL) {
      check(false, `${name} has an outbox but no DATABASE_URL in compose, so its database cannot be named`);
      continue;
    }
    const database = databaseOf(env.DATABASE_URL);
    outboxDatabases.add(database);

    const draining = relays.filter((r) => r.database === database);
    check(
      draining.length > 0,
      draining.length > 0
        ? `${name}'s outbox in ${database} is drained by ${draining.map((r) => r.name).join(', ')}`
        : `${name} has an outbox in ${database} and no relay in compose drains it: its messages commit and are never sent`,
    );

    const ownRoles = [userOf(env.DATABASE_URL), env.APP_DATABASE_URL ? userOf(env.APP_DATABASE_URL) : null].filter(Boolean);
    for (const relay of draining) {
      check(
        !ownRoles.includes(relay.user),
        `${relay.name} connects as ${relay.user}, which is not ${name}'s ${ownRoles.join(' or ')}`,
      );
      check(
        schemas.get(name).includes(relay.forbidden) && relay.forbidden !== 'OutboxMessage',
        `${relay.name}'s forbidden table "${relay.forbidden}" is one of ${name}'s business tables, so its boot check tests a real privilege`,
      );
    }
  }

  for (const relay of relays) {
    check(outboxDatabases.has(relay.database), `${relay.name} drains ${relay.database}, which belongs to a service with an outbox`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every outbox has a relay in compose');
}

try {
  main();
} catch (err) {
  console.error(err.stderr?.toString() || err);
  process.exitCode = 1;
}
