/**
 * Asserts that every environment variable a service READS appears in the
 * `.env.example` a reader is told to copy.
 *
 * The README's setup says "each service ships a committed .env.example; copy it
 * to .env". If the code reads a variable that file does not mention, that
 * sentence is wrong in the way that is hardest to notice: the service starts,
 * because the variable has a default or is only consulted on one path, and the
 * reader never learns the knob exists. Five had drifted out when this was
 * written -- AUTH_DEV_TOKEN_SINK and PUBLIC_APP_URL in Auth, DB_POOL_MAX,
 * PAYMENT_TIMEOUT_MS and PAYMENT_FAKE_LATENCY_MS in Ticketing.
 *
 * AUTH_DEV_TOKEN_SINK is the one that shows why it matters. It is the switch
 * that prints invite and reset tokens where a developer can see them, and with
 * it undocumented the README's own invite flow has a step a reader cannot
 * complete: the token goes nowhere and nothing says why.
 *
 * THREE PLACES A VARIABLE CAN BE READ, and a check that knew only the first
 * would have been vacuous for the relay, which uses none of it:
 *
 *   process.env['X']            the direct read
 *   src/env.ts zod schema       the relay's whole configuration
 *   @eventhub/config base       NODE_ENV, PORT, DATABASE_URL, LOG_LEVEL, for
 *                               every service that calls loadBaseEnv
 *
 * So the parse is asserted before anything is concluded from it. A service that
 * appears to read NO variables has not passed; it has failed to be understood,
 * and that is reported as a failure rather than as a clean sweep.
 *
 * Run: node scripts/verify-env-documented.mjs
 */

import { readFileSync, existsSync, readdirSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const SERVICES = ['analytics', 'auth', 'community', 'event', 'live', 'notification', 'relay', 'search', 'ticketing'];

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/**
 * Variables a service reads that a reader setting up by hand does NOT need in
 * their .env, each with the reason. Being on this list is a claim, so it has to
 * be a defensible one — the same bar scripts/verify-readme-setup.mjs sets for
 * the setup scripts it excuses.
 */
const NOT_FOR_READERS = {
  // Supplied per-request by the test that spawns a private instance, never from
  // a .env — see scripts/verify-login-rate-limit.mjs.
  TZ: 'set by the runtime, not by this project',
};

function tsFiles(dir) {
  const out = [];
  const walk = (d) => {
    for (const name of readdirSync(d)) {
      const p = join(d, name);
      if (statSync(p).isDirectory()) {
        if (name === 'generated' || name === 'node_modules') continue;
        walk(p);
      } else if (name.endsWith('.ts')) out.push(p);
    }
  };
  walk(dir);
  return out;
}

/**
 * Keys of the zod object in a schema file, at ANY indentation.
 *
 * This matched exactly two spaces, which was the layout of every schema when it
 * was written. Reformat Search's `baseEnvSchema.omit(...).extend({...})` as a
 * chain across lines and its keys move to four spaces: the review of Chapter 6
 * did exactly that, deleted two required variables from .env.example, and this
 * file printed "search: reads 3" and passed. Same count at any indentation on
 * every schema in the repository when this changed, so widening it found
 * nothing new and invented nothing.
 */
function schemaKeys(text) {
  return new Set([...text.matchAll(/^[ \t]*([A-Z][A-Z0-9_]*)[ \t]*:/gm)].map((m) => m[1]));
}

/**
 * Comments removed before anything is detected in a file.
 *
 * The first version of this scanned raw source and reported that the relay reads
 * PORT and DATABASE_URL. It does not — it deliberately does NOT call
 * loadBaseEnv, and says so in a comment that names the function twice while
 * explaining why. The detector read the explanation as a use and would have had
 * me document two variables the relay has no idea about.
 *
 * Cheap and slightly approximate: a `//` inside a string literal is treated as a
 * comment. That direction is safe here — it can only cause a read to be missed,
 * which shows up as a missing variable rather than an invented one, and every
 * env name in this codebase is read through the two forms below rather than
 * built from a string.
 *
 * LINE STRUCTURE IS PRESERVED, and the first version did not preserve it.
 * Replacing a multi-line block comment with a single space joins the lines
 * around it, so a schema key documented by a JSDoc block stops being at the
 * start of a line and the key regex — which is line-anchored — stops seeing it.
 * That is not a loud failure: Search reported "reads 3" instead of 10 and passed,
 * because three documented variables trivially cover three detected ones.
 * Blanking each character except the newlines keeps every line and column where
 * it was.
 */
const decomment = (text) =>
  text
    .replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' '))
    .replace(/(^|[^:])\/\/.*$/gm, '$1');

const baseEnv = schemaKeys(readFileSync(join(ROOT, 'packages/config/src/index.ts'), 'utf8'));

function readsOf(service) {
  const src = join(ROOT, 'services', service, 'src');
  const names = new Set();
  const omitted = new Set();
  /** Each file that declares a schema, with how many keys are its OWN. */
  const schemaFiles = [];
  let usesBase = false;

  for (const file of tsFiles(src)) {
    const text = decomment(readFileSync(file, 'utf8'));
    for (const m of text.matchAll(/process\.env\[['"]([A-Z0-9_]+)['"]\]/g)) names.add(m[1]);
    for (const m of text.matchAll(/process\.env\.([A-Z0-9_]+)/g)) names.add(m[1]);
    // The CALL, not the name. See decomment above for what reading the bare
    // identifier cost.
    if (/\bloadBaseEnv\s*\(/.test(text)) usesBase = true;

    /**
     * The other way a service builds on the shared base: composing it.
     *
     *   baseEnvSchema.omit({ DATABASE_URL: true }).extend({ ... })
     *
     * Search does this because it owns no database. The first version of this
     * check knew only `loadBaseEnv(` and `.safeParse(process.env)`, so it read
     * Search as using NO environment variables at all — and said so, loudly,
     * rather than reporting a clean sweep over nothing. That refusal is the
     * vacuity guard below earning its place; without it this file would have
     * passed a service it did not understand.
     *
     * OMITTED KEYS ARE SUBTRACTED. Crediting a service with a variable it
     * explicitly declared it does not have would demand documentation for
     * configuration that cannot affect it.
     */
    if (/\bbaseEnvSchema\b/.test(text)) {
      usesBase = true;
      for (const m of text.matchAll(/\.omit\(\s*\{([^}]*)\}\s*\)/g)) {
        for (const k of m[1].matchAll(/([A-Z][A-Z0-9_]*)\s*:\s*true/g)) omitted.add(k[1]);
      }
    }

    /**
     * Any file that validates a zod schema against process.env declares
     * configuration, wherever it happens to live.
     *
     * Keyed on the safeParse call rather than on a filename. The first version
     * looked only at src/env.ts, which is the relay's pattern — and Ticketing
     * keeps two more of these, in reconciler/index.ts and sweeper/index.ts, so
     * every variable belonging to its two background jobs was invisible to a
     * check whose entire job is noticing invisible variables.
     */
    if (/\.safeParse\(\s*process\.env\s*\)/.test(text) || /\bloadEnv\s*\(/.test(text)) {
      const keys = schemaKeys(text);
      for (const k of keys) names.add(k);
      schemaFiles.push({
        file: file.slice(ROOT.length + 1).replace(/\\/g, '/'),
        own: [...keys].filter((k) => !baseEnv.has(k)).length,
      });
    }
  }

  if (usesBase) for (const k of baseEnv) if (!omitted.has(k)) names.add(k);
  return { names, schemaFiles };
}

/** Keys in a .env.example, whether live or commented out. */
function documentedIn(service) {
  const p = join(ROOT, 'services', service, '.env.example');
  if (!existsSync(p)) return null;
  const keys = new Set();
  for (const line of readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.trim().match(/^#?\s*([A-Z][A-Z0-9_]*)=/);
    if (m) keys.add(m[1]);
  }
  return keys;
}

function main() {
  // -------------------------------------------------------------------------
  console.log('  the parse has to have found something');
  // -------------------------------------------------------------------------
  /**
   * Every assertion below is "the code reads X, so .env.example must mention X".
   * If the reads come back empty, every one of them is vacuously true and this
   * file reports success having compared nothing — which is the exact failure
   * this repository keeps finding, so it is the first thing asserted.
   */
  check(baseEnv.size > 0, `@eventhub/config declares ${baseEnv.size} shared variable(s)`);

  /**
   * THE SERVICE LIST ABOVE IS A LITERAL, SO IT IS CHECKED AGAINST THE DIRECTORY.
   * The same trap verify-env-example.mjs and verify-dockerfile-workspaces.mjs
   * guard against: a service nobody added here is a service whose variables
   * are never compared with anything, reported as a clean sweep.
   */
  const onDisk = readdirSync(join(ROOT, 'services'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(ROOT, 'services', d.name, 'package.json')))
    .map((d) => d.name);
  const unlisted = onDisk.filter((n) => !SERVICES.includes(n));
  check(
    unlisted.length === 0,
    unlisted.length === 0
      ? `every one of the ${onDisk.length} services in services/ is checked below`
      : `${unlisted.join(', ')} ${unlisted.length === 1 ? 'is' : 'are'} in services/ and never checked here`,
  );

  const reads = {};
  const docs = {};
  let parseOk = baseEnv.size > 0 && unlisted.length === 0;

  for (const s of SERVICES) {
    const scanned = readsOf(s);
    reads[s] = scanned.names;
    docs[s] = documentedIn(s);

    /**
     * EVERY SCHEMA FILE MUST CONTRIBUTE KEYS OF ITS OWN.
     *
     * `reads > 0` alone was satisfied by the shared base: a service built on
     * baseEnvSchema always picks up NODE_ENV, PORT and LOG_LEVEL, so a schema
     * whose own keys the parse missed entirely still read as "3" and passed.
     * A file that validates a schema and declares nothing beyond the base has
     * no reason to exist, so zero own keys means the parse failed, not that the
     * file is empty.
     */
    for (const { file, own } of scanned.schemaFiles) {
      check(
        own > 0,
        own > 0
          ? `${file} declares ${own} variable(s) of its own`
          : `${file} validates a schema and the parse found none of its own keys — ` +
            'the parse is broken for this file, so its variables are unchecked',
      );
      if (own === 0) parseOk = false;
    }

    const ok = reads[s].size > 0 && docs[s] !== null && docs[s].size > 0;
    check(
      ok,
      ok
        ? `${s}: reads ${reads[s].size}, documents ${docs[s].size}`
        : `${s}: read ${reads[s].size} variable(s) and found ` +
          `${docs[s] === null ? 'no .env.example at all' : `${docs[s].size} documented`} — ` +
          'nothing below this can mean anything',
    );
    if (!ok) parseOk = false;
  }

  if (!parseOk) {
    console.error('\nFAIL: cannot check anything against a failed parse');
    process.exitCode = 1;
    return;
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and every variable the code reads is one the reader is given');
  // -------------------------------------------------------------------------
  for (const s of SERVICES) {
    const missing = [...reads[s]].filter((n) => !docs[s].has(n) && !(n in NOT_FOR_READERS)).sort();
    check(
      missing.length === 0,
      missing.length === 0
        ? `${s} — every variable it reads is in its .env.example`
        : `${s} reads ${missing.join(', ')} and its .env.example never mentions ` +
          `${missing.length === 1 ? 'it' : 'them'}. A reader copying that file gets a service ` +
          'that starts and behaves differently from the one described, with nothing to say why.',
    );
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the configuration a reader is handed is the configuration the code reads');
}

main();
