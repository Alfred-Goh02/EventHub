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
 * AND THE OTHER DIRECTION, FOR AUTH: the example is a laptop's configuration, and
 * Auth run from it as production must refuse. Its PUBLIC_APP_URL is localhost,
 * and a production Auth that kept it would email every password-reset link
 * pointing at the reader's own machine. See authInProduction below.
 *
 * Run: node scripts/verify-env-example.mjs
 */

import { spawn } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

/** Scratch ports, well away from anything a developer is likely to be running. */
const SERVICES = [
  { name: 'auth', portVar: 'PORT', port: 3901, path: '/health' },
  { name: 'event', portVar: 'PORT', port: 3902, path: '/health' },
  { name: 'ticketing', portVar: 'PORT', port: 3903, path: '/health' },
  { name: 'relay', portVar: 'RELAY_METRICS_PORT', port: 3904, path: '/metrics' },
  { name: 'live', portVar: 'PORT', port: 3905, path: '/health' },
  { name: 'community', portVar: 'PORT', port: 3910, path: '/ready' },
  // /ready: it answers 200 only once the example's APP_DATABASE_URL has reached
  // Postgres, and the process only listens after refusing a role that bypasses RLS.
  { name: 'notification', portVar: 'PORT', port: 3909, path: '/ready' },
  // /ready, not /health. Search listens whether or not the cluster is there, so
  // /health proves only that the process started. /ready answers 200 only once
  // the example's OPENSEARCH_NODE has actually been reached.
  { name: 'search', portVar: 'PORT', port: 3906, path: '/ready' },
  // /ready for the same reason as Search: it answers 200 only once ClickHouse has
  // been reached as the example's analytics_app user, not merely once listening.
  { name: 'analytics', portVar: 'PORT', port: 3907, path: '/ready' },
  // The same package's second process, from its own entrypoint and its own port
  // variable. /ready answers once it has connected to Kafka, subscribed, and
  // reached ClickHouse, all from the example's addresses and credentials.
  { name: 'analytics', label: 'analytics ingest', entry: 'dist/consumer.js', portVar: 'INGEST_PORT', port: 3908, path: '/ready' },
];

const KEY_PATH = join('services', 'auth', 'keys', 'public.pem');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
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

  const child = spawn(process.execPath, [svc.entry ?? 'dist/index.js'], {
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
      ? `${svc.label ?? svc.name} booted from .env.example alone` +
        (suppliedKey ? ' (AUTH_PUBLIC_KEY_B64 supplied from the generated keypair)' : '')
      : `${svc.label ?? svc.name} did NOT come up on port ${svc.port}` +
        (exited !== null ? ` — it exited with code ${exited}` : ' — still running but not answering') +
        `\n          ${detail.join('\n          ')}`,
  );

  child.kill();
  // Give the port back before the next service starts, so a failure to bind is
  // never mistaken for a configuration problem.
  await sleep(500);
}

/** Away from the scratch ports above, so a straggler there cannot answer for it. */
const AUTH_PRODUCTION_PORT = 3911;

/** Auth from its .env.example and nothing else, as production, plus `overrides`. */
function startAuthInProduction(overrides) {
  const dir = join('services', 'auth');
  const child = spawn(process.execPath, ['dist/index.js'], {
    cwd: dir,
    env: {
      PATH: process.env['PATH'],
      SystemRoot: process.env['SystemRoot'],
      ...parseEnvFile(join(dir, '.env.example')),
      NODE_ENV: 'production',
      PORT: String(AUTH_PRODUCTION_PORT),
      ...overrides,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (c) => (output += c));
  child.stderr.on('data', (c) => (output += c));
  const proc = { child, code: undefined, log: () => output };
  // 'close', not 'exit': it fires once the output has been read to the end, so the
  // refusal is in log() by the time anything looks for it.
  proc.exited = new Promise((resolve) =>
    child.on('close', (code) => {
      proc.code = code;
      resolve(code);
    }),
  );
  return proc;
}

/** Whichever comes first: the process exits, or it answers /health. */
async function exitOrListen(proc, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (proc.code !== undefined) return { exit: proc.code };
    try {
      if ((await fetch(`http://localhost:${AUTH_PRODUCTION_PORT}/health`)).ok) return { listening: true };
    } catch {
      // not listening
    }
    await sleep(250);
  }
  return {};
}

/**
 * Auth, as production, from the same example: it must refuse the example's origin.
 *
 * PUBLIC_APP_URL is where every invite and password-reset link points, and the
 * example's is http://localhost:3000 -- right on a laptop. A production Auth that
 * forgot to set it would start, seal every reset link to the reader's own machine,
 * and email it; password reset would be broken for everyone, and nothing would say
 * so. So Auth refuses to start under NODE_ENV=production unless it is https, and
 * this boots it exactly the way a forgetful deployment would: the example, and
 * NODE_ENV=production, and nothing else.
 *
 * The control is the same process given an https origin, which must start. Without
 * it, a guard that refused every production boot, for any reason, would pass.
 */
async function authInProduction() {
  const example = parseEnvFile(join('services', 'auth', '.env.example'));

  /**
   * The control below is "something answered /health on this port", so first make
   * sure nothing already does. A leftover process here would make a refusal look
   * like a start, and a broken start look like a working control.
   */
  const taken = await fetch(`http://localhost:${AUTH_PRODUCTION_PORT}/health`).then(() => true, () => false);
  check(!taken, taken ? `something is already listening on :${AUTH_PRODUCTION_PORT} -- stop it, then rerun` : `:${AUTH_PRODUCTION_PORT} is free`);
  if (taken) return;

  const refused = startAuthInProduction({});
  try {
    const outcome = await exitOrListen(refused, 30_000);
    // Anchored on "Error:". The stack trace also prints the throwing line of source,
    // and a looser pattern would match that instead of the message.
    const named = refused.log().match(/Error: refusing to start: (.+?) must be https/)?.[1] ?? '';
    check(
      outcome.exit !== undefined && outcome.exit !== 0 && named.includes('PUBLIC_APP_URL'),
      `with the example's PUBLIC_APP_URL (${example['PUBLIC_APP_URL'] ?? 'absent'}), a production auth refuses to ` +
        `start and names it (${outcome.listening ? 'it STARTED' : `exit ${outcome.exit ?? 'none'}`}: ${named || 'no such message'})`,
    );
  } finally {
    refused.child.kill();
    await refused.exited;
  }

  const control = startAuthInProduction({ PUBLIC_APP_URL: 'https://app.eventhub.example' });
  try {
    const outcome = await exitOrListen(control, 30_000);
    // Still ours: it answered, and it is the process this started, still running.
    const ours = outcome.listening === true && control.code === undefined;
    const lines = control.log().trim().split(/\r?\n/);
    const reason = lines.find((l) => /^(Error|[A-Za-z]*Error):/.test(l.trim())) ?? lines.slice(0, 3).join(' | ');
    check(
      ours,
      ours
        ? 'CONTROL: with an https PUBLIC_APP_URL, the same production process starts'
        : `CONTROL: with an https PUBLIC_APP_URL, the production process did NOT start (exit ${outcome.exit ?? 'none'}): ${reason}`,
    );
  } finally {
    control.child.kill();
    await control.exited;
  }
}

async function main() {
  console.log('  booting every service from its .env.example and nothing else');
  console.log('');

  if (!existsSync(KEY_PATH)) {
    console.error(`FAIL: ${KEY_PATH} is missing — generate the keypair first`);
    process.exit(1);
  }
  const publicKeyB64 = readFileSync(KEY_PATH).toString('base64');

  /**
   * THE LIST ABOVE IS HAND-WRITTEN, SO IT IS CHECKED AGAINST THE DIRECTORY.
   *
   * It was written with four services and stayed at four while a fifth and a
   * sixth arrived. Chapter 5 added Live with its own .env.example and eleven
   * variables, and this file went on printing "every service starts from the
   * configuration its .env.example documents" — true of every service it had
   * heard of. A check whose coverage is a literal is a check that silently
   * stops covering, which is the same trap verify-dockerfile-workspaces.mjs
   * exists for in the image.
   *
   * Asserted BEFORE anything boots, so a missing service is the first thing
   * reported rather than a line after a run that looked clean.
   */
  const onDisk = readdirSync('services', { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join('services', d.name, 'package.json')))
    .map((d) => d.name)
    .sort();
  const listed = new Set(SERVICES.map((s) => s.name));
  const unlisted = onDisk.filter((n) => !listed.has(n));
  check(onDisk.length > 0, `services/ holds ${onDisk.length} service(s): ${onDisk.join(', ')}`);
  check(
    unlisted.length === 0,
    unlisted.length === 0
      ? 'and every one of them is booted below'
      : `${unlisted.join(', ')} ${unlisted.length === 1 ? 'is' : 'are'} in services/ and never booted ` +
        'here, so a broken .env.example there would pass this check',
  );
  console.log('');

  for (const svc of SERVICES) {
    await bootFromExample(svc, publicKeyB64);
  }

  console.log('');
  console.log("  and auth, as production, refuses the example's localhost origin");
  console.log('');
  await authInProduction();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('every service starts from the configuration its .env.example documents,');
  console.log("and auth will not run in production on the example's origin");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
