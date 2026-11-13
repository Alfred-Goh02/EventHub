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

import { readFileSync, existsSync, readdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
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
   *
   * EVERY FENCE IS MATCHED, AND THE SHELL ONES ARE THEN SELECTED. The obvious
   * regex -- ```(?:bash|sh)?\n -- looks like it does this and does something
   * worse. It does not match a fence tagged anything else, so it skips that
   * fence's OPENING line and then matches its CLOSING line as an opener: from
   * there the pairing is inverted, and every following block's contents are read
   * as prose while the prose between blocks is read as commands.
   *
   * Adding an architecture diagram to the README is what found this. One
   * ```mermaid block ahead of the commands, and all three migrate assertions
   * failed at once while the README plainly contained all three commands. The
   * same inversion can hide the opposite way: prose that happens to mention
   * `prisma migrate deploy` would have been accepted as the command.
   *
   * So: match the info string, whatever it is, and filter afterwards.
   */
  const SHELL_FENCES = new Set(['', 'bash', 'sh', 'shell', 'console']);
  const fences = [...readme.matchAll(/```([\w-]*)\r?\n([\s\S]*?)```/g)];

  /**
   * Every fence marker accounted for, as exactly one opener or one closer.
   *
   * This is the guard on the parse itself, and it exists because the failure it
   * catches is invisible. An info string this regex cannot match does not produce
   * an error; it produces a silent off-by-one in the pairing, and the assertions
   * below then describe the wrong halves of the file. Counting is the cheapest
   * way to notice, and it fails loudly the moment someone writes a fence tag
   * shaped differently -- ```js {1,3} , say.
   */
  const markers = (readme.match(/```/g) ?? []).length;
  check(
    markers === fences.length * 2,
    markers === fences.length * 2
      ? `all ${markers} fence markers pair into ${fences.length} block(s)`
      : `${markers} fence markers but only ${fences.length} block(s) parsed. Some fence has an ` +
        'info string this parse does not recognise, so the pairing has slipped and every ' +
        'assertion below is reading the wrong half of the file.',
  );

  const commands = fences.filter((m) => SHELL_FENCES.has(m[1])).map((m) => m[2]);

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
  console.log('  and every service CI seeds is a service the README tells you to seed');
  // -------------------------------------------------------------------------
  /**
   * Found by the review of Chapter 6, by deleting the README's seed command: every
   * assertion in this file still passed. Seeding is how the admin in the README's
   * own sign-in example comes to exist, and no detector here looked for it — the
   * migrate check matches `prisma migrate deploy`, the script check matches
   * `node scripts/*.mjs`, and `pnpm exec prisma db seed` is neither.
   */
  const withoutComments = (body) =>
    body
      .split(/\r?\n/)
      .filter((l) => !l.trim().startsWith('#'))
      .join('\n');
  const seeded = steps
    .filter((s) => /prisma db seed/.test(withoutComments(s.body)))
    .map((s) => s.wd.replace(/^services\//, ''));

  check(seeded.length > 0, `CI seeds ${seeded.length} service(s): ${seeded.join(', ')}`);

  for (const service of seeded) {
    const named = new RegExp(String.raw`(@eventhub/|services/)${service}(?![\w-])`);
    const documented = commands.some((c) =>
      c.split(/\r?\n/).some((line) => /prisma db seed/.test(line) && named.test(line)),
    );
    check(
      documented,
      documented
        ? `${service} — a documented command seeds it`
        : `${service} is seeded by CI and no command in the README seeds it, so the account the ` +
          "README's sign-in example uses will not exist",
    );
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the number it claims for itself is the number there are');
  // -------------------------------------------------------------------------
  /**
   * The README advertises a count of verification scripts, and it had drifted
   * from 13 to 43 -- it was understating the work by thirty files.
   *
   * A number in prose has no way of noticing that the directory moved on, and
   * this is the one number a reader is most likely to check, because it is the
   * repository's own claim about how much of itself it proves. So it is asserted
   * rather than maintained: adding a verifier now fails this until the sentence
   * is updated, which takes one character and costs nothing next to a README
   * that quietly understates the thing it is advertising.
   */
  const verifierCount = readdirSync(join(ROOT, 'scripts')).filter(
    (f) => f.startsWith('verify-') && f.endsWith('.mjs'),
  ).length;
  const claimed = readme.match(/(\d+)\s+executable proofs/);

  check(
    claimed !== null,
    claimed !== null
      ? `the README states a count: "${claimed[0]}"`
      : 'the README no longer states a count of verification scripts, so this cannot check it',
  );
  if (claimed) {
    const stated = Number(claimed[1]);
    check(
      stated === verifierCount,
      stated === verifierCount
        ? `and there are ${verifierCount} of them`
        : `the README says ${stated} verification scripts and scripts/ holds ${verifierCount}`,
    );
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the databases and roles it says db/init creates are the ones it creates');
  // -------------------------------------------------------------------------
  /**
   * The same kind of number, with the same fate: the README said "eighteen roles" after
   * a nineteenth was bootstrapped, and a review noticed, not a check. Counted from the
   * one file that creates them, statements only: an anchored `CREATE ROLE` never
   * matches a comment that mentions one.
   */
  const bootstrap = readFileSync(join(ROOT, 'db', 'init', '01-bootstrap.sql'), 'utf8');
  const created = {
    databases: (bootstrap.match(/^CREATE DATABASE /gm) ?? []).length,
    roles: (bootstrap.match(/^CREATE ROLE /gm) ?? []).length,
  };
  const NUMBER_WORDS = [
    'zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
    'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen', 'eighteen', 'nineteen', 'twenty',
    'twenty-one', 'twenty-two', 'twenty-three', 'twenty-four', 'twenty-five', 'twenty-six', 'twenty-seven', 'twenty-eight', 'twenty-nine', 'thirty',
  ];
  const asNumber = (word) => (/^\d+$/.test(word) ? Number(word) : NUMBER_WORDS.indexOf(word.toLowerCase()));
  const said = readme.match(/([\w-]+) databases and ([\w-]+) roles/);
  check(
    said !== null && created.databases > 0 && created.roles > 0,
    said
      ? `the README says "${said[0]}", and db/init creates ${created.databases} database(s) and ${created.roles} role(s)`
      : 'the README no longer says how many databases and roles db/init creates, so this cannot check it',
  );
  if (said) {
    check(
      asNumber(said[1]) === created.databases && asNumber(said[2]) === created.roles,
      `the README's numbers are db/init's (${said[1]} = ${created.databases}? ${said[2]} = ${created.roles}?)`,
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
  console.log('  and every package script CI runs to stand the system up is one the README gives you');
  // -------------------------------------------------------------------------
  /**
   * SETUP ALSO HIDES IN PACKAGE SCRIPTS, and the loop above cannot see it.
   *
   * That loop matches `node scripts/*.mjs`, which was every setup step when it
   * was written. Chapter 6 created the search index as `pnpm run bootstrap:index`
   * in services/search — without it Search has no index, and the first document
   * written would make OpenSearch invent a mapping, typing tenantId as analysed
   * text — and this file went on calling the README complete, because nothing it
   * knew how to read had changed.
   *
   * YAML comments are dropped before matching. A step's comment block can say
   * "pnpm run" while explaining something, and a detector that reads the
   * explanation as a use asks for documentation of a step that does not exist.
   *
   * Build and test scripts are not setup: `pnpm build` is already the README's
   * first command, and nothing needs a test run to exist.
   */
  const NOT_SETUP_PKG = new Set(['build', 'test', 'lint', 'typecheck']);

  /**
   * pnpm's own commands, which are never package scripts. `pnpm exec prisma ...`
   * is covered by the migrate and seed checks, `install` by the README's first
   * command.
   */
  const PNPM_BUILTINS = new Set([
    'install', 'i', 'add', 'remove', 'rm', 'update', 'up', 'exec', 'dlx', 'why',
    'list', 'ls', 'outdated', 'store', 'fetch', 'rebuild', 'prune', 'link', 'unlink',
  ]);

  /**
   * Every `pnpm` invocation on a line, as { script, service } — PARSED, not
   * matched with one regex.
   *
   * The first version was /pnpm (?:--filter (\S+) )?run ([\w:.-]+)/, and the
   * review of Chapter 6 showed what that misses: `pnpm bootstrap:index` (run is
   * optional), `-F`, `--filter=`, `-C` and `--dir`, flags after `run`, and a
   * double space. It also read `pnpm run --filter x y` as a script called
   * `--filter`. And the documented-side check was a substring, so README text
   * running `bootstrap:index` counted as documenting a script named `bootstrap`.
   * Tokens fix all of it; the script name is compared whole.
   */
  function pnpmInvocations(line, defaultService) {
    const out = [];
    const t = line.trim().split(/\s+/);
    for (let i = 0; i < t.length; i++) {
      if (t[i] !== 'pnpm') continue;
      let service = defaultService;
      let script = null;
      let sawRun = false;
      for (let j = i + 1; j < t.length; j++) {
        const tok = t[j];
        if (tok === '&&' || tok === '||' || tok === ';' || tok === '|') break;
        if (tok === '--filter' || tok === '-F' || tok === '-C' || tok === '--dir') {
          const v = t[++j] ?? '';
          service = v.replace(/^@eventhub\//, '').replace(/^\.?\/?services\//, '');
          continue;
        }
        if (tok.startsWith('--filter=')) {
          service = tok.slice('--filter='.length).replace(/^@eventhub\//, '');
          continue;
        }
        if (tok.startsWith('-')) continue;
        if (tok === 'run' && !sawRun) {
          sawRun = true;
          continue;
        }
        if (!sawRun && PNPM_BUILTINS.has(tok)) break;
        script = tok;
        break;
      }
      if (script) out.push({ script, service });
    }
    return out;
  }

  const pkgSteps = [];
  for (const step of steps) {
    const wdService = step.wd.startsWith('services/') ? step.wd.replace(/^services\//, '') : null;
    for (const line of step.body.split(/\r?\n/)) {
      // YAML comments are prose, and prose saying "pnpm run" is not a step.
      const code = line.trim().startsWith('#') ? '' : line.replace(/^\s*run:\s*/, '');
      for (const inv of pnpmInvocations(code, wdService)) {
        if (NOT_SETUP_PKG.has(inv.script)) continue;
        pkgSteps.push(inv);
      }
    }
  }

  check(
    pkgSteps.length > 0,
    `CI runs ${pkgSteps.length} setup package script(s): ` +
      pkgSteps.map((p) => `${p.service} ${p.script}`).join(', '),
  );

  for (const { service, script } of pkgSteps) {
    const documented = commands.some((c) =>
      c
        .split(/\r?\n/)
        .some((line) =>
          pnpmInvocations(line, null).some((inv) => inv.script === script && inv.service === service),
        ),
    );
    check(
      documented,
      documented
        ? `${service} ${script} — a documented command runs it`
        : `CI runs \`${script}\` in ${service} to stand the system up, and no command in the ` +
          'README runs it',
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
