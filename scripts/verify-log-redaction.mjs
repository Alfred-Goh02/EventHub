/**
 * Asserts that the logger actually redacts what its list says it redacts.
 *
 * The list in packages/logger is a backstop, not the control — the control is
 * not passing secrets to the logger. But a backstop nobody exercises is a list
 * of strings, and this one had three holes that reading it did not reveal:
 *
 *   ctx.authorization = NESTED-authorization      no `*.authorization`, while
 *                                                 all six neighbours had theirs
 *   secret     = TOP-secret                       absent entirely
 *   privateKey = TOP-privateKey                   absent entirely
 *
 * Both of the absent ones name variables this codebase holds. webhooks.ts reads
 * PAYMENT_WEBHOOK_SECRET into `const secret`, which is exactly the `{ secret }`
 * shape a debug log produces; jwt.ts holds the RS256 signing key, the one value
 * that can mint a token for any user in any tenant.
 *
 * WHY A CHILD PROCESS. createLogger writes to stdout and takes no destination,
 * so the only honest way to see what it emits is to run it and read what came
 * out. Reaching inside pino, or re-implementing the redaction to compare
 * against, would test a copy — which is the failure #71 was about.
 *
 * Run: node scripts/verify-log-redaction.mjs
 */

import { spawnSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { existsSync } from 'node:fs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const LOGGER = join(ROOT, 'packages', 'logger', 'dist', 'index.js');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/** The canary values. Each must come back as [redacted], never as itself. */
const SECRET = 'SHOULD-NOT-APPEAR';

/**
 * Every key the list claims to cover, in both the bare and the nested form —
 * because the nested form is where the holes were.
 */
const KEYS = [
  'token',
  'tokenHash',
  'password',
  'passwordHash',
  'refreshToken',
  'accessToken',
  'authorization',
  'secret',
  'privateKey',
  'link',
];

function main() {
  if (!existsSync(LOGGER)) {
    console.error(`FAIL: ${LOGGER} does not exist — run \`pnpm build\` first`);
    process.exitCode = 1;
    return;
  }

  const payload = {};
  for (const k of KEYS) payload[k] = SECRET;
  payload.ctx = {};
  for (const k of KEYS) payload.ctx[k] = SECRET;
  // The shape middleware dumps wholesale when working out what went wrong.
  payload.req = { headers: { authorization: SECRET }, body: { password: SECRET } };

  const src =
    // pathToFileURL, not the path itself: on Windows the ESM loader rejects a
    // bare absolute path with ERR_UNSUPPORTED_ESM_URL_SCHEME, because it reads
    // the drive letter as a URL scheme.
    `import { createLogger } from ${JSON.stringify(pathToFileURL(LOGGER).href)};\n` +
    `createLogger('verify-redaction').info(${JSON.stringify(payload)}, 'probe');\n`;

  const run = spawnSync(process.execPath, ['--input-type=module', '-e', src], {
    encoding: 'utf8',
    env: { ...process.env, LOG_LEVEL: 'info' },
  });

  const out = `${run.stdout}${run.stderr}`;

  // -------------------------------------------------------------------------
  console.log('  the probe has to have produced a log line');
  // -------------------------------------------------------------------------
  /**
   * Without this, every assertion below is satisfied by NO OUTPUT AT ALL. A
   * logger that failed to load, or a level that suppressed the line, would leave
   * an empty string in which the canary is genuinely absent — and this file would
   * report that nothing leaked, which would be true and worthless.
   */
  const line = out.split('\n').find((l) => l.trim().startsWith('{') && l.includes('probe'));
  check(Boolean(line), line ? `the logger emitted a line (${line.length} bytes)` : `no log line: ${out.slice(0, 300)}`);
  if (!line) {
    console.error('\nFAIL: nothing to inspect, so nothing below would mean anything');
    process.exitCode = 1;
    return;
  }

  let parsed;
  try {
    parsed = JSON.parse(line);
  } catch {
    check(false, 'the line is JSON');
    process.exitCode = 1;
    return;
  }

  // And that the canary is a value the probe actually set, so "absent" means
  // "redacted" rather than "never sent".
  check(
    JSON.stringify(payload).includes(SECRET),
    'and the probe really did send the canary value',
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and no key the list claims to cover survives, at either depth');
  // -------------------------------------------------------------------------
  for (const k of KEYS) {
    const top = parsed[k];
    const nested = parsed.ctx?.[k];
    const ok = top !== SECRET && nested !== SECRET;
    check(
      ok,
      ok
        ? `${k} and ctx.${k} are both [redacted]`
        : `${k}=${JSON.stringify(top)} ctx.${k}=${JSON.stringify(nested)} — this value reached a ` +
          'log line, where it is readable by anything that can read logs',
    );
  }

  check(
    parsed.req?.headers?.authorization !== SECRET && parsed.req?.body?.password !== SECRET,
    'and the dumped request object carries neither its header nor its password',
  );

  // The blunt check, in case a key is added to the probe and not to KEYS.
  const leaks = (line.match(new RegExp(SECRET, 'g')) ?? []).length;
  check(leaks === 0, leaks === 0 ? 'the canary appears nowhere in the line' : `the canary appears ${leaks} time(s) in the line`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('everything the redaction list names is redacted, nested as well as bare');
}

main();
