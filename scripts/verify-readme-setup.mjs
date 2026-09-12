/**
 * Asserts the README's setup instructions actually stand a system up.
 *
 * They did not. Following "Running it" to the letter gave you:
 *
 *   - migrations for AUTH only, though the repo has three services with
 *     migrations and the text says "migrate each service"
 *   - no Kafka topics, because the broker runs with
 *     KAFKA_AUTO_CREATE_TOPICS_ENABLE=false on purpose
 *   - no Avro schemas registered
 *
 * None of that fails loudly. The relay's first publish dies with
 * "This server does not host this topic-partition", which names neither the
 * missing topic nor the script that would create it, and the schema lookup 404s
 * on a subject nobody registered. A reader who has just followed the setup has
 * no reason to suspect the setup.
 *
 * The README already says ci.yml is the authoritative setup, which is an honest
 * admission that a second copy of a procedure drifts from the first. This is the
 * check that stops the drift being silent.
 *
 * THE RATCHET RUNS THE RIGHT WAY ROUND. Anything CI runs to make the system
 * usable must appear in the README, and a NEW setup script fails this until
 * somebody either documents it or records here why a human does not need it.
 * The default for an undocumented step is red, not green.
 *
 * Run: node scripts/verify-readme-setup.mjs
 */

import { readFileSync, existsSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const readme = readFileSync(join(ROOT, 'README.md'), 'utf8');
const ci = readFileSync(join(ROOT, '.github/workflows/ci.yml'), 'utf8');

/**
 * Scripts CI runs that a person setting up by hand does NOT need, each with the
 * reason. Being on this list is a claim, so it has to be a defensible one.
 */
const NOT_SETUP = {
  'kafka-smoke.mjs': 'a broker round-trip assertion, not a thing the system needs to exist',
  'generate-avro-types.mjs':
    'its output is committed; CI regenerates only to diff for staleness, and a ' +
    'reader who runs it gets no files they did not already clone',
};

/** Steps, in order, as (name, run, working-directory) triples. */
function ciSteps(text) {
  const blocks = text.split(/^ {6}- name: /m).slice(1);
  return blocks.map((b) => {
    const name = b.split(/\r?\n/)[0].trim();
    const wd = b.match(/^ {8}working-directory: (.+)$/m)?.[1]?.trim() ?? '.';
    return { name, wd, body: b };
  });
}

const steps = ciSteps(ci);

function main() {
  // -------------------------------------------------------------------------
  console.log('  the parse has to have found something');
  // -------------------------------------------------------------------------
  /**
   * Every assertion below is of the form "CI does X, so the README must say X".
   * If the CI parse returns nothing, every one of them is vacuously true and the
   * whole file reports success while checking nothing. That is the exact failure
   * this repository keeps finding, so it is the first thing asserted.
   */
  check(steps.length > 10, `parsed ${steps.length} steps from ci.yml`);
  check(readme.length > 1000, `read ${readme.length} bytes of README.md`);
  if (steps.length <= 10 || readme.length <= 1000) {
    console.error('\nFAIL: cannot check anything against a failed parse');
    process.exitCode = 1;
    return;
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  every service CI migrates is a service the README tells you to migrate');
  // -------------------------------------------------------------------------
  /**
   * Only what is inside a fenced block counts as an instruction.
   *
   * The first version of this check searched the whole README for the service
   * name near the word "migrate", and reported `event` as documented. What it
   * had matched was
   *
   *     pnpm --filter @eventhub/auth exec prisma migrate deploy
   *
   * -- the "event" in "@eventhub". The AUTH command was being read as proof that
   * the EVENT service was covered. A check for a command has to look at commands.
   */
  const commands = [...readme.matchAll(/```(?:bash|sh)?\r?\n([\s\S]*?)```/g)].map((m) => m[1]);

  check(commands.length > 0, `the README contains ${commands.length} fenced command block(s)`);

  const migrateCommands = commands.filter((c) => /prisma migrate deploy/.test(c));

  check(
    migrateCommands.length > 0,
    migrateCommands.length > 0
      ? `${migrateCommands.length} of them run migrations`
      : 'none of them run migrations, so the README does not document migrating at all',
  );

  const migrated = steps
    .filter((s) => /prisma migrate deploy/.test(s.body))
    .map((s) => s.wd.replace(/^services\//, ''));

  check(migrated.length > 0, `CI migrates ${migrated.length} service(s): ${migrated.join(', ')}`);

  for (const service of migrated) {
    // Named as a workspace filter or as a directory. Either is an instruction a
    // reader can follow; a bare mention of the word elsewhere is not.
    //
    // String.raw, because a plain template literal ate the backslash: `\w` in
    // one is just `w`, so the lookahead was (?![w-]) and a command filtering
    // @eventhub/events counted as migrating `event`.
    const named = new RegExp(String.raw`(@eventhub/|services/)${service}(?![\w-])`);
    const documented = migrateCommands.some((c) => named.test(c));
    check(
      documented,
      documented
        ? `${service} — a documented command migrates it`
        : `${service} is migrated by CI and no command in the README migrates it. Its tables ` +
          'will not exist, and the service fails on its first query rather than at startup.',
    );
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and every setup script CI runs is a script the README tells you to run');
  // -------------------------------------------------------------------------
  const scripts = new Set();
  for (const step of steps) {
    for (const m of step.body.matchAll(/node (?:\S*\/)?scripts\/([\w.-]+\.mjs)/g)) {
      const file = m[1];
      // verify-* are assertions about a system that already exists. They are not
      // how you build one, and a reader does not need to run them by hand.
      if (file.startsWith('verify-')) continue;
      if (file in NOT_SETUP) continue;
      scripts.add(file);
    }
  }

  check(scripts.size > 0, `CI runs ${scripts.size} setup script(s): ${[...scripts].join(', ')}`);

  for (const file of scripts) {
    const documented = readme.includes(file);
    check(
      documented,
      documented
        ? `${file} — the README tells you to run it`
        : `${file} is required to stand the system up and the README never mentions it`,
    );
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the paths it gives you resolve from the repository root');
  // -------------------------------------------------------------------------
  /**
   * CI can afford a bare `node scripts/generate-keys.mjs` because the step sets
   * working-directory. A reader pasting from the README has no such context, so
   * a path that is correct in the workflow can be wrong in the prose — and the
   * error, MODULE_NOT_FOUND, reads as a broken repository rather than a broken
   * instruction.
   */
  const cited = [...readme.matchAll(/node ((?:[\w.-]+\/)*[\w.-]+\.mjs)/g)].map((m) => m[1]);

  check(cited.length > 0, `the README cites ${cited.length} script invocation(s)`);

  for (const path of new Set(cited)) {
    const there = existsSync(join(ROOT, path));
    check(there, there ? `${path} exists` : `${path} does not exist — the README sends you to nothing`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the documented setup builds the system the workflow builds');
}

main();
