/**
 * Asserts that every service in compose sets its own stop_grace_period.
 *
 * With none set, the stop timeout is whatever the engine that creates the container
 * decides, and that is not the documented 10 seconds everywhere. Docker Desktop
 * before 4.81 stores StopTimeout=1 on every container created without one
 * (moby/moby#52775: "a deliberate choice on the Docker Desktop side to make sure
 * containers don't block VM shutdown"). Measured here on Desktop 4.77.0: a raw API
 * create with no StopTimeout came back with 1, and every EventHub container had it.
 * So `docker compose stop` sent SIGTERM and then SIGKILL one second later, to Kafka
 * and Postgres as much as to Notification (exit 137).
 *
 * The containers in CI get 10 s from a Linux engine, so nothing there could see it.
 *
 * DERIVED, NOT LISTED. The services are every service in `docker compose config`,
 * with every profile compose reports enabled, so one behind a profile is checked too.
 *
 * Run: node scripts/verify-stop-grace-periods.mjs   (needs the docker CLI, not a running engine)
 */

import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
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

const compose = (args) => execFileSync('docker', ['compose', ...args], { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });

// The engine keeps whole seconds: compose sends int(d.Seconds()), so 1500ms is 1.
const UNIT_MS = { h: 3_600_000, m: 60_000, s: 1_000, ms: 1, us: 0.001, µs: 0.001, ns: 0.000001 };
function wholeSeconds(text) {
  const parts = [...String(text).matchAll(/([\d.]+)(ms|us|µs|ns|h|m|s)/g)];
  if (parts.length === 0 || parts.map((p) => p[0]).join('') !== String(text)) return null;
  return Math.trunc(parts.reduce((ms, [, n, unit]) => ms + Number(n) * UNIT_MS[unit], 0) / 1000);
}

function main() {
  console.log('  what compose has');

  const profiles = compose(['config', '--profiles']).split(/\r?\n/).map((p) => p.trim()).filter(Boolean);
  const config = JSON.parse(compose([...profiles.flatMap((p) => ['--profile', p]), 'config', '--format', 'json']));
  const services = Object.entries(config.services ?? {});
  const profiled = services.filter(([, s]) => (s.profiles ?? []).length > 0).map(([name]) => name);

  check(services.length >= 15, `CONTROL: ${services.length} services read from compose`);
  check(profiles.length >= 1 && profiled.length >= 1, `CONTROL: ${profiles.length} profile(s) found and enabled, and ${profiled.join(', ') || 'no service'} behind one is included`);
  check(wholeSeconds('1m30s') === 90 && wholeSeconds('1500ms') === 1 && wholeSeconds('15') === null, 'CONTROL: durations parse as compose prints them');
  if (failures > 0) return;

  console.log('');
  console.log('  every service says how long it gets to stop');
  for (const [name, service] of services.sort(([a], [b]) => a.localeCompare(b))) {
    const period = service.stop_grace_period;
    if (period === undefined) {
      check(false, `${name} sets no stop_grace_period, so its stop timeout is whatever the engine picks (1 s on Docker Desktop before 4.81)`);
      continue;
    }
    const seconds = wholeSeconds(period);
    check(
      seconds !== null && seconds >= 2,
      seconds === null
        ? `${name}: stop_grace_period "${period}" does not parse`
        : `${name} gets ${seconds} s (stop_grace_period ${period})${seconds < 2 ? ', which is no longer than the 1 s being avoided' : ''}`,
    );
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every compose service sets its own stop_grace_period');
}

try {
  main();
} catch (err) {
  console.error(err.stderr?.toString() || err);
  process.exitCode = 1;
}
