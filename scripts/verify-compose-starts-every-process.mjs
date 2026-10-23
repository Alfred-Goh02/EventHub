/**
 * Asserts that every long-running process the services define is one compose starts.
 *
 * It has gone missing twice. Ticketing's hold sweeper and then its payment reconciler
 * were each written as a process of their own -- `node dist/sweeper/index.js`, `node
 * dist/reconciler/index.js` -- and compose started neither, so every abandoned checkout
 * kept its seats, and then every charge the provider never answered stayed UNKNOWN
 * with its seats held. Each was found by a person enumerating entry points by hand,
 * and then fixed with a verifier for that one process. This is the enumeration, kept.
 *
 * AN ENTRY POINT is a file under services/<service>/src that, at the top level and
 * with comments stripped, does one of three things a process does and a module does
 * not: runs a main() (`main().catch`, `void main()`, `await main()` or a top-level
 * `async function main`), registers a SIGTERM handler, or calls `.listen(`. Analytics'
 * two processes have no main() and are found by the other two, which is why there are
 * three; each branch is shown to match a known file before anything is concluded.
 *
 * EACH ONE MUST BE EITHER
 *   - run by a compose service: its `working_dir` is /app/services/<service> and its
 *     `command` is `node dist/<path>.js`, read from `docker compose config` with every
 *     profile enabled, never from a list here; or
 *   - a ONE-SHOT: anything under src/scripts/ by the repository's convention, or named
 *     in ONE_SHOT below with its reason. A one-shot must also look like one: a file
 *     there that listens or handles SIGTERM is a long-running process in the wrong
 *     folder, and fails.
 *
 * And the other way round: every `node dist/...` command compose runs must have a
 * source file, so a renamed entry point cannot leave compose starting nothing.
 *
 * Static: needs the docker CLI for `compose config`, not a running engine.
 *
 * Run: node scripts/verify-compose-starts-every-process.mjs
 */

import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

/**
 * Entry points outside src/scripts/ that compose does not run, each with the reason it
 * may be. Empty: every one-shot in this repository lives in src/scripts/. Being on
 * this list is a claim, so each entry needs a reason a reviewer can check.
 */
const ONE_SHOT = {};

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/**
 * Comments blanked, lines kept -- the same treatment verify-env-documented.mjs gives
 * source, for the same reason: a comment that says "main()" or "process.on('SIGTERM'"
 * while explaining something is not a process.
 */
const decomment = (text) =>
  text.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, ' ')).replace(/(^|[^:])\/\/.*$/gm, '$1');

/** Which of the three markers a file has, at the top level (column 0). */
function markers(source) {
  const text = decomment(source);
  return {
    main: /^(async function main\b|main\(\)|void main\(\)|await main\(\))/m.test(text),
    sigterm: /^process\.on\(\s*['"]SIGTERM['"]/m.test(text),
    listen: /^[A-Za-z_$][\w$]*\.listen\(/m.test(text),
  };
}
const isEntry = (m) => m.main || m.sigterm || m.listen;

function tsFiles(dir, out = []) {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) {
      if (name === 'generated' || name === 'node_modules') continue;
      tsFiles(p, out);
    } else if (name.endsWith('.ts') && !name.endsWith('.d.ts')) out.push(p);
  }
  return out;
}

const rel = (p) => relative(ROOT, p).replace(/\\/g, '/');

function composeConfig() {
  const compose = (args) => execFileSync('docker', ['compose', ...args], { cwd: ROOT, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  const profiles = compose(['config', '--profiles']).split(/\r?\n/).map((p) => p.trim()).filter(Boolean);
  return JSON.parse(compose([...profiles.flatMap((p) => ['--profile', p]), 'config', '--format', 'json']));
}

function main() {
  console.log('  the detector');
  // Each branch on a planted text, so a pattern that matches nothing cannot pass
  // everything below by finding no entry points at all.
  const planted = {
    main: markers('async function main() {}\nmain().catch(() => {});\n'),
    sigterm: markers("process.on('SIGTERM', () => {});\n"),
    listen: markers('app.listen(3000, () => {});\n'),
    commented: markers("/* main().catch()\nprocess.on('SIGTERM', f) */\n// app.listen(1)\n"),
    indented: markers("function f() {\n  process.on('SIGTERM', g);\n  server.listen(1);\n}\n"),
  };
  check(
    planted.main.main && planted.sigterm.sigterm && planted.listen.listen,
    'CONTROL: each marker matches a planted line: main(), a SIGTERM handler, .listen()',
  );
  check(
    !isEntry(planted.commented) && !isEntry(planted.indented),
    'CONTROL: a marker inside a comment, or not at the top level, is not one',
  );

  const services = readdirSync(join(ROOT, 'services'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(ROOT, 'services', d.name, 'src')))
    .map((d) => d.name);
  const entries = [];
  for (const service of services) {
    for (const file of tsFiles(join(ROOT, 'services', service, 'src'))) {
      const m = markers(readFileSync(file, 'utf8'));
      if (isEntry(m)) entries.push({ service, file: rel(file), m });
    }
  }
  const byFile = new Map(entries.map((e) => [e.file, e]));
  const reconciler = byFile.get('services/ticketing/src/reconciler/index.ts');
  const ingest = byFile.get('services/analytics/src/consumer.ts');
  check(
    reconciler?.m.main === true,
    `CONTROL: the main() branch finds services/ticketing/src/reconciler/index.ts (${reconciler ? 'found' : 'not found'})`,
  );
  check(
    ingest !== undefined && !ingest.m.main && ingest.m.sigterm && ingest.m.listen,
    `CONTROL: the other two find services/analytics/src/consumer.ts, which has no main() (${ingest ? JSON.stringify(ingest.m) : 'not found'})`,
  );
  check(entries.length >= 15, `CONTROL: ${entries.length} entry points across ${services.length} services`);

  console.log('');
  console.log('  what compose runs');
  const config = composeConfig();
  const commands = [];
  for (const [name, svc] of Object.entries(config.services ?? {})) {
    const cmd = Array.isArray(svc.command) ? svc.command : [];
    const dir = String(svc.working_dir ?? '').match(/^\/app\/services\/([^/]+)\/?$/);
    if (cmd[0] === 'node' && /^dist\/.+\.js$/.test(cmd[1] ?? '') && dir) {
      commands.push({ name, file: `services/${dir[1]}/src/${cmd[1].slice('dist/'.length, -'.js'.length)}.ts` });
    }
  }
  check(commands.length >= 10, `CONTROL: ${commands.length} compose services run \`node dist/...\` from a service directory`);
  for (const c of commands.sort((a, b) => a.name.localeCompare(b.name))) {
    check(
      existsSync(join(ROOT, c.file)) && byFile.has(c.file),
      `${c.name} runs ${c.file}${existsSync(join(ROOT, c.file)) ? (byFile.has(c.file) ? '' : ', which exists but is not detected as an entry point') : ', which does not exist'}`,
    );
  }
  const started = new Map();
  for (const c of commands) started.set(c.file, [...(started.get(c.file) ?? []), c.name]);

  console.log('');
  console.log('  every entry point is started, or is a one-shot that behaves like one');
  for (const e of entries.sort((a, b) => a.file.localeCompare(b.file))) {
    const how = Object.entries(e.m).filter(([, v]) => v).map(([k]) => k).join('+');
    if (started.has(e.file)) {
      check(true, `${e.file} (${how}) is run by compose: ${started.get(e.file).join(', ')}`);
      continue;
    }
    const inScripts = e.file.startsWith(`services/${e.service}/src/scripts/`);
    const reason = ONE_SHOT[e.file];
    if (inScripts || reason) {
      const lingers = [e.m.sigterm && 'handles SIGTERM', e.m.listen && 'listens'].filter(Boolean).join(' and ');
      check(
        !lingers,
        lingers
          ? `${e.file} is filed as a one-shot but ${lingers}: a long-running process compose does not start`
          : `${e.file} is a one-shot (${reason ?? 'src/scripts/'}), and neither listens nor waits for SIGTERM`,
      );
      continue;
    }
    check(
      false,
      `${e.file} (${how}) is a process no compose service runs. Add a service whose command is ` +
        `node ${e.file.replace(`services/${e.service}/src/`, 'dist/').replace(/\.ts$/, '.js')} in /app/services/${e.service}, ` +
        'or, if it is run by hand, move it to src/scripts/ or name it in ONE_SHOT with the reason',
    );
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every long-running process the services define is one compose starts');
}

main();
