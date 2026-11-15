/**
 * Plans every Secret infra/stores names, from where compose keeps them. infra/eks/up.mjs
 * makes them, with the cluster's own credentials in place of compose's
 * (infra/eks/credentials.mjs); this file says which Secrets, which keys, and where each
 * value starts.
 *
 *   node infra/stores/secrets.mjs --print      the plan, keys only
 *
 * It no longer makes them itself: compose's values are development credentials written
 * into this repository, and `--context` refuses rather than put them in a cluster.
 *
 * NOTHING IS LISTED HERE. Which Secrets, and which keys, come from the chart itself,
 * rendered: every secretKeyRef a store's container reads, and every Secret volume a store
 * mounts. Where each value comes from is compose's own description of that store, by the
 * names the chart already uses:
 *   - an env var read from a Secret takes compose's value for THE SAME variable of THE
 *     SAME service: postgres's POSTGRES_PASSWORD from compose's postgres environment;
 *   - a Secret mounted at a path takes EVERY file of the directory compose bind-mounts at
 *     that path in that service: db/init for postgres, db/clickhouse-init for clickhouse.
 * So a password changed in compose, or a third init file added to db/init, reaches the
 * cluster with no edit here, and a Secret the chart names that compose cannot fill is a
 * refusal, not an empty value.
 *
 * Values are never printed. --print shows each Secret's keys and where each came from.
 */

import { execFileSync } from 'node:child_process';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const run = (cmd, args, opts = {}) => execFileSync(cmd, args, { cwd: ROOT, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], maxBuffer: 64 * 1024 * 1024, ...opts });

// Thrown, not exited: the verifier calls plan() and records a refusal as its own FAIL.
// Run as a script, main's caller prints it and exits 1.
function fail(msg) {
  throw new Error(msg);
}

/** The chart, rendered as JSON: one object per resource. A copy's path, for a red run. */
export function renderChart(chartDir = join(ROOT, 'infra', 'stores'), extraArgs = []) {
  const out = run('helm', ['template', 'stores', chartDir, '--set', 'render=json', ...extraArgs]);
  return out
    .split(/^---\s*$/m)
    .map((doc) => doc.split(/\r?\n/).filter((l) => !l.startsWith('# Source:')).join('\n').trim())
    .filter(Boolean)
    .map((doc) => JSON.parse(doc));
}

/** What each Secret the chart names needs, and from where: { secret: { key: { from, read } } }. */
export function plan(docs, compose) {
  const wanted = {};
  const want = (secret, key, from, read) => {
    wanted[secret] ??= {};
    if (wanted[secret][key] && wanted[secret][key].from !== from) fail(`${secret}/${key} would come from both ${wanted[secret][key].from} and ${from}`);
    wanted[secret][key] = { from, read };
  };
  for (const w of docs.filter((d) => d.kind === 'StatefulSet' || d.kind === 'Deployment')) {
    const name = w.metadata.name;
    const svc = compose.services[name];
    if (!svc) fail(`the chart runs a store named ${name}, and compose has no service of that name to take its secrets from`);
    const pod = w.spec.template.spec;
    for (const c of pod.containers) {
      for (const e of c.env ?? []) {
        const ref = e.valueFrom?.secretKeyRef;
        if (!ref) continue;
        if (ref.key !== e.name) fail(`${name}: ${e.name} reads key ${ref.key} of ${ref.name}; a key is named for the variable it fills, so compose's value for it can be found`);
        const value = svc.environment?.[e.name];
        if (value === undefined || value === null || String(value) === '') fail(`${name}: ${e.name} comes from Secret ${ref.name}, and compose's ${name} service sets no ${e.name}`);
        want(ref.name, ref.key, `docker-compose.yml ${name}.environment.${e.name}`, () => String(value));
      }
      for (const m of c.volumeMounts ?? []) {
        const vol = (pod.volumes ?? []).find((v) => v.name === m.name);
        if (!vol?.secret) continue;
        const bind = (svc.volumes ?? []).find((v) => v.type === 'bind' && v.target === m.mountPath);
        if (!bind) fail(`${name}: Secret ${vol.secret.secretName} is mounted at ${m.mountPath}, and compose bind-mounts nothing there`);
        const files = readdirSync(bind.source).filter((f) => !f.startsWith('.') && statSync(join(bind.source, f)).isFile()).sort();
        if (files.length === 0) fail(`${name}: compose's ${relative(ROOT, bind.source)} has no files to put in ${vol.secret.secretName}`);
        for (const f of files) {
          const path = join(bind.source, f);
          want(vol.secret.secretName, f, relative(ROOT, path).split(sep).join('/'), () => readFileSync(path, 'utf8'));
        }
      }
    }
  }
  return wanted;
}

function main() {
  // Read here, not at the top: scripts/verify-store-chart.mjs imports this file for plan()
  // and renderChart(), and a check at the top would end the verifier.
  // `pnpm run x -- a b` hands the script the `--` on every shell but PowerShell (CLAUDE.md).
  const argv = process.argv.slice(2).filter((a) => a !== '--');
  const option = (name) => {
    const i = argv.indexOf(name);
    return i === -1 ? undefined : argv[i + 1];
  };
  const PRINT = argv.includes('--print');
  const CONTEXT = option('--context');
  // Compose's values are development credentials written into this repository. The
  // installer makes these Secrets with the cluster's own (infra/eks/credentials.mjs); made
  // from here, they would put compose's passwords on the stores' disks, or, after the
  // first boot, lock every service out of its database.
  if (CONTEXT) fail("this makes compose's development credentials, which no cluster should hold: node infra/eks/up.mjs makes these Secrets with the cluster's own. --print shows the plan");
  if (!PRINT) fail('give --print to see which Secrets the chart names, and where each value comes from');

  const compose = JSON.parse(run('docker', ['compose', '-p', 'eventhub-rebuild', 'config', '--format', 'json']));
  const wanted = plan(renderChart(), compose);
  const names = Object.keys(wanted).sort();
  if (names.length === 0) fail('the chart names no Secret; that is a chart that renders nothing, not one with no secrets');

  console.log(JSON.stringify(Object.fromEntries(names.map((n) => [n, Object.fromEntries(Object.entries(wanted[n]).map(([k, v]) => [k, v.from]))])), null, 2));
}

// Run only as a script: scripts/verify-store-chart.mjs imports plan() and renderChart().
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (e) {
    console.error(`FAIL: ${e.message}`);
    process.exit(1);
  }
}
