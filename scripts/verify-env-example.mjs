/**
 * Asserts that a fresh clone can actually start each service.
 *
 * Every service ships a .env.example whose first line says "Copy to .env and
 * adjust". Nothing checked that the copy WORKS. CI sets its environment from
 * the workflow file instead, so .env.example is the one piece of configuration
 * in this repository that no job ever exercised — and it drifted exactly the
 * way an unexercised file does.
 *
 * Found by trying it: services/ticketing/.env.example omitted
 * PAYMENT_WEBHOOK_SECRET, and Ticketing REFUSES TO START without it, by design,
 * because a webhook route with no secret is an unauthenticated write path into
 * payment state. So the documented way to run this service produced:
 *
 *   Error: PAYMENT_WEBHOOK_SECRET is required — the webhook route is
 *          unauthenticated without it
 *
 * The refusal is correct. The instructions were wrong. Nothing could tell the
 * difference, because the instructions were never run.
 *
 * So this does the only thing that actually proves the claim: it boots each
 * service with ITS .env.example AND NOTHING ELSE, and waits for the service to
 * answer. A missing variable, a stale URL, a renamed key, a new mandatory
 * secret — all of them fail here, and none of them can be argued away.
 *
 * ONE EXEMPTION, printed rather than assumed. AUTH_PUBLIC_KEY_B64 is left empty
 * in the examples because it is GENERATED, not configured: it is the base64 of
 * services/auth/keys/public.pem, which is gitignored and created by the setup
 * step. Supplying it here is the same thing the setup instructions tell a human
 * to do. Every other variable comes from the example file untouched.
 *
 * Run: node scripts/verify-env-example.mjs
 */

import { spawn } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

/** Scratch ports, well away from anything a developer is likely to be running. */
const SERVICES = [
  { name: 'auth', portVar: 'PORT', port: 3901, path: '/health' },
  { name: 'event', portVar: 'PORT', port: 3902, path: '/health' },
  { name: 'ticketing', portVar: 'PORT', port: 3903, path: '/health' },
  { name: 'relay', portVar: 'RELAY_METRICS_PORT', port: 3904, path: '/metrics' },
];

const KEY_PATH = join('services', 'auth', 'keys', 'public.pem');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Parse a .env file the way `node --env-file` does for the cases these files
 * use: KEY=value, blank lines, and # comments. No quoting or interpolation,
 * because none of these files use any.
 */
function parseEnvFile(path) {
  const out = {};
  for (const raw of readFileSync(path, 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }
  return out;
}

async function waitForHttp(port, path, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}${path}`);
      if (res.ok) return true;
    } catch {
      // not up yet
    }
    await sleep(400);
  }
  return false;
}

async function bootFromExample(svc, publicKeyB64) {
  const dir = join('services', svc.name);
  const example = join(dir, '.env.example');

  if (!existsSync(example)) {
    check(false, `${svc.name}: no .env.example at all`);
    return;
  }

  const parsed = parseEnvFile(example);

  /**
   * The environment is the example file, plus PATH so node can run at all, plus
   * the one generated secret. Deliberately NOT process.env: inheriting the
   * developer's shell is exactly how a missing variable stays invisible, since
   * their own .env is usually already exported somewhere.
   */
  const env = {
    PATH: process.env['PATH'],
    SystemRoot: process.env['SystemRoot'],
    ...parsed,
    [svc.portVar]: String(svc.port),
  };

  const suppliedKey = 'AUTH_PUBLIC_KEY_B64' in parsed && parsed['AUTH_PUBLIC_KEY_B64'] === '';
  if (suppliedKey) env['AUTH_PUBLIC_KEY_B64'] = publicKeyB64;

  const child = spawn(process.execPath, ['dist/index.js'], {
    cwd: dir,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let output = '';
  child.stdout.on('data', (c) => (output += c));
  child.stderr.on('data', (c) => (output += c));

  let exited = null;
  child.on('exit', (code) => (exited = code));

  const up = await waitForHttp(svc.port, svc.path, 25_000);

  /**
   * Lead with the thrown message, not the tail of the output.
   *
   * A Node stack trace puts the useful line FIRST and then several frames of
   * module loader noise, so a plain tail shows the noise and hides the reason.
   * The whole point of this check is to name the missing variable.
   */
  const lines = output.trim().split(/\r?\n/);
  const reason = lines.find((l) => /^(Error|[A-Za-z]*Error):/.test(l.trim()));
  const detail = reason ? [reason, ...lines.slice(0, 3)] : lines.slice(0, 8);

  check(
    up,
    up
      ? `${svc.name} booted from .env.example alone` +
        (suppliedKey ? ' (AUTH_PUBLIC_KEY_B64 supplied from the generated keypair)' : '')
      : `${svc.name} did NOT come up on port ${svc.port}` +
        (exited !== null ? ` — it exited with code ${exited}` : ' — still running but not answering') +
        `\n          ${detail.join('\n          ')}`,
  );

  child.kill();
  // Give the port back before the next service starts, so a failure to bind is
  // never mistaken for a configuration problem.
  await sleep(500);
}

async function main() {
  console.log('  booting every service from its .env.example and nothing else');
  console.log('');

  if (!existsSync(KEY_PATH)) {
    console.error(`FAIL: ${KEY_PATH} is missing — generate the keypair first`);
    process.exit(1);
  }
  const publicKeyB64 = readFileSync(KEY_PATH).toString('base64');

  for (const svc of SERVICES) {
    await bootFromExample(svc, publicKeyB64);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} service(s) could not start from their documented configuration`);
    process.exit(1);
  }
  console.log('every service starts from the configuration its .env.example documents');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
