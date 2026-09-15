/**
 * Asserts that a private key can be read only inside the one container meant to
 * hold it.
 *
 * Compose used to mount the jwt-keys volume, private.pem included, into every
 * service that verifies a token: Event, Ticketing, Live, Search and Analytics. Each
 * needed public.pem and nothing else. The mount was read-only and private.pem was
 * mode 0600, which looked like enough and was not: every container runs as the same
 * `node` user that owns the file. Any of those five could have minted an access
 * token for any user, in any tenant, at any role.
 *
 * WATCHED, NOT READ. This looks from inside each running container, as the user the
 * service runs as, for any readable file holding a PEM private key. It does not
 * read docker-compose.yml, because the compose file is what already looked right.
 *
 * DERIVED. The services checked are every service in `docker compose config` built
 * from the application image, so a new one is covered the moment it is added. The
 * places searched include every mount each container actually has.
 *
 * Run (with the compose stack up): node scripts/verify-key-custody.mjs
 */

import { execFileSync } from 'node:child_process';

const APP_IMAGE = 'eventhub/app:local';

/** The private keys each service alone may read. Every other service may read none. */
const HOLDERS = {
  auth: ['/app/services/auth/keys/private.pem'],
  // Opens sealed email requests. Nothing else may be able to.
  notification: ['/app/services/notification/keys/private.pem'],
};

/**
 * Services from the application image that are not long-running, each with the
 * reason. Being on this list is a claim, so it has to be a defensible one.
 */
const ONE_SHOT = {
  setup: 'generates the keypair, so it must write it; it exits before anything serves',
};

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const docker = (args) => execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });

/** `docker compose ps --format json` is a JSON array on older Compose and one object per line on newer. */
function parseJsonList(text) {
  const trimmed = text.trim();
  if (!trimmed) return [];
  if (trimmed.startsWith('[')) return JSON.parse(trimmed);
  return trimmed.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

/**
 * Runs as the image's user, which is the user the service runs as. Skips
 * node_modules, where libraries ship test fixtures that are not anybody's key.
 */
const FIND_PRIVATE_KEYS = (roots) =>
  `find ${roots.map((r) => `'${r}'`).join(' ')} -path '*/node_modules' -prune -o -type f -readable -print0 2>/dev/null` +
  ` | xargs -0 -r grep -lE -- '-----BEGIN ([A-Z]+ )?PRIVATE KEY-----' 2>/dev/null; true`;

function main() {
  console.log('  what compose runs');
  const config = JSON.parse(docker(['compose', 'config', '--format', 'json']));
  const appServices = Object.entries(config.services)
    .filter(([, service]) => service.image === APP_IMAGE)
    .map(([name]) => name)
    .sort();
  const serving = appServices.filter((name) => !(name in ONE_SHOT));

  check(serving.length >= 8, `CONTROL: ${serving.length} long-running services from ${APP_IMAGE}: ${serving.join(', ')}`);
  for (const holder of Object.keys(HOLDERS)) {
    check(serving.includes(holder), `CONTROL: ${holder}, which holds a private key, is one of them`);
  }
  for (const name of Object.keys(ONE_SHOT)) {
    check(appServices.includes(name), `${name} is exempt (${ONE_SHOT[name]}) and still exists in compose`);
  }
  if (failures > 0) return;

  const containers = new Map(parseJsonList(docker(['compose', 'ps', '--format', 'json'])).map((c) => [c.Service, c]));

  console.log('');
  console.log('  what each running service can read');
  for (const service of serving) {
    const container = containers.get(service);
    if (!container || container.State !== 'running') {
      check(false, `${service} is not running, so nothing about it was checked`);
      continue;
    }

    const mounts = JSON.parse(docker(['inspect', '--format', '{{json .Mounts}}', container.Name])) ?? [];
    // A mount inside /app or /run is searched once, as part of its parent. Listing
    // both made every key under /run/eventhub/keys appear twice.
    const candidates = [...new Set(['/app', '/run', ...mounts.map((m) => m.Destination)])];
    const roots = candidates.filter((root) => !candidates.some((other) => other !== root && root.startsWith(`${other}/`)));
    const readable = [
      ...new Set(
        docker(['exec', container.Name, 'sh', '-c', FIND_PRIVATE_KEYS(roots)])
          .split(/\r?\n/)
          .map((line) => line.trim())
          .filter(Boolean),
      ),
    ].sort();

    const allowed = HOLDERS[service] ?? [];
    const unexpected = readable.filter((path) => !allowed.includes(path));
    const missing = allowed.filter((path) => !readable.includes(path));

    // The positive control: a search that finds nothing anywhere would pass every
    // service below. The holder has to find its own key through the same search.
    if (allowed.length > 0) {
      check(
        missing.length === 0,
        missing.length === 0
          ? `CONTROL: ${service} can read its own ${allowed.join(', ')}`
          : `CONTROL: ${service} cannot read its own ${missing.join(', ')}, so this search sees nothing`,
      );
    }

    const volumes = mounts.map((m) => `${m.Name ?? m.Source}:${m.Destination}`).join(', ') || 'no mounts';
    check(
      unexpected.length === 0,
      unexpected.length === 0
        ? `${service} can read no other private key (searched ${roots.join(', ')})`
        : `${service} can read ${unexpected.join(', ')} (mounts: ${volumes})`,
    );
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every private key is readable only in the container that holds it');
}

try {
  main();
} catch (err) {
  console.error(err.stderr?.toString() || err);
  process.exitCode = 1;
}
