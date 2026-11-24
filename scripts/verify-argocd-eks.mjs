/**
 * Asserts that Argo CD on EKS deploys what main describes, from the images CI published for
 * the commit it deploys -- every one, and nothing under a tag git does not name.
 *
 * GITOPS FAILS QUIETLY, so its Applications are rendered here as Argo CD renders them, with
 * $ARGOCD_APP_REVISION_SHORT set to a commit's first seven digits:
 *   - infra/argocd/eks/root.json makes exactly the Applications infra/eks/up.mjs --gitops
 *     waits for (gitopsApps): one per service chart (verify-argocd.mjs evaluates that
 *     generator), and the gateway, the web app and setup, each from its own directory, in
 *     the default namespace, with automated sync that prunes and self-heals;
 *   - every image they deploy from GitHub's registry is exactly the reference
 *     scripts/publish-images.mjs publishes for that commit -- the services' and setup's
 *     eventhub/app, the web app's eventhub/web -- and the gateway keeps compose's own Kong;
 *   - no tag is written anywhere: each is $ARGOCD_APP_REVISION_SHORT, the commit deployed;
 *   - every pod that pulls from the registry runs as the default service account, which is
 *     where the installer puts the pull Secret, and every service runs one replica;
 *   - setup runs compose's own setup command (infra/argocd/env/eks-setup.json is a copy, as
 *     Helm cannot read compose: it must be exactly what compose resolves);
 *   - both clusters' Argo CD is the one version, from infra/argocd/settings.mjs.
 *
 * SETUP FIRST, AND THE WAIT ENDS. On 2026-11-15 a fresh install restarted six services 11
 * times between them, each started before setup had made what it needs. So:
 *   - setup is an earlier sync wave than every other child, and the root hands every child
 *     the commit it resolved main to, so the waves order every commit, not only the first
 *     (infra/argocd/eks/apps/values.yaml says why both are needed); a branch name is refused;
 *   - the installer puts infra/argocd/eks/application-health.lua into argocd-cm, reads it
 *     back, and does so before it applies the root;
 *   - that script, run by Argo CD's own CLI at the version the cluster runs, answers Healthy
 *     only for a child Synced and Healthy at the commit it was given; Progressing, which a
 *     wave waits on, for every state on the way there, a stale Healthy about the previous
 *     commit included; and Degraded, which ends the wait, for every failure of THIS commit
 *     and none of an earlier one. Without the script, Argo CD has no health for an
 *     Application at all: the control.
 *
 * Needs helm and docker (for `docker compose config`, and to run Argo CD's CLI from its
 * image, quay.io/argoproj/argocd at ARGOCD_VERSION; ARGOCD_CLI_IMAGE names another image with
 * that CLI on PATH, and the run refuses one at any other version) on PATH:
 *   node scripts/verify-argocd-eks.mjs [root]
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, posix, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { rootChildren } from './lib/argocd-children.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const REV = '0123456789abcdef0123456789abcdef01234567';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const section = (title) => {
  console.log('');
  console.log(`  ${title}`);
};

/**
 * The key Argo CD reads an Application's health check from (docs/operator-manual/health.md),
 * written here rather than taken from the installer: the installer's own constant could be
 * misspelt, and a check that took it would agree with the misspelling.
 */
const HEALTH_KEY = 'resource.customizations.health.argoproj.io_Application';

/** Source with its comments removed: a scan must match a call, not a sentence about one. */
const strip = (src) => src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:'"`])\/\/.*$/gm, '$1');

/**
 * An Application in every state a child passes through after the root hands it commit `rev`,
 * with the health a wave must read for each. OLD is the commit before it.
 */
function healthCases(rev) {
  const OLD = 'fedcba9876543210fedcba9876543210fedcba98';
  const app = (status) => ({
    apiVersion: 'argoproj.io/v1alpha1',
    kind: 'Application',
    metadata: { name: 'setup', namespace: 'argocd' },
    spec: { project: 'default', source: { repoURL: 'git@github.com:o/r.git', path: 'infra/setup', targetRevision: rev }, destination: { server: 'https://kubernetes.default.svc', namespace: 'default' } },
    ...(status ? { status } : {}),
  });
  const sync = (status, revision, compared = revision) => ({ status, ...(revision ? { revision } : {}), comparedTo: { source: { repoURL: 'git@github.com:o/r.git', path: 'infra/setup', targetRevision: compared } } });
  const failedOp = (revision) => ({ phase: 'Failed', message: 'one or more objects failed to apply', operation: { sync: { revision } }, syncResult: { revision } });
  const compareError = [{ type: 'ComparisonError', message: 'Failed to load target state: helm template failed' }];
  return [
    { name: 'no status yet: the root has only just made it', want: 'Progressing', app: app(null) },
    { name: 'still Synced and Healthy about the previous commit: the stale answer', want: 'Progressing', app: app({ sync: sync('Synced', OLD), health: { status: 'Healthy' } }) },
    { name: 'compared at this commit and OutOfSync, which Argo CD calls Healthy while the Job is missing', want: 'Progressing', app: app({ sync: sync('OutOfSync', rev), health: { status: 'Healthy' } }) },
    { name: "compared at this commit, OutOfSync, Degraded by the previous commit's failed Job", want: 'Progressing', app: app({ sync: sync('OutOfSync', rev), health: { status: 'Degraded' } }) },
    { name: "Synced at this commit, its Job still running", want: 'Progressing', app: app({ sync: sync('Synced', rev), health: { status: 'Progressing' } }) },
    { name: 'Synced at this commit and Healthy: its Job finished', want: 'Healthy', app: app({ sync: sync('Synced', rev), health: { status: 'Healthy' } }) },
    { name: 'Synced at this commit, its Job failed', want: 'Degraded', app: app({ sync: sync('Synced', rev), health: { status: 'Degraded', message: 'Job has reached the specified backoff limit' } }) },
    { name: 'its sync of this commit failed', want: 'Degraded', app: app({ sync: sync('OutOfSync', rev), health: { status: 'Missing' }, operationState: failedOp(rev) }) },
    { name: 'a failed sync of the previous commit, now compared at this one', want: 'Progressing', app: app({ sync: sync('OutOfSync', rev), health: { status: 'Healthy' }, operationState: failedOp(OLD) }) },
    { name: 'this commit cannot be compared at all', want: 'Degraded', app: app({ sync: sync('Unknown', null, rev), health: { status: 'Healthy' }, conditions: compareError }) },
    { name: 'a comparison error about the previous commit', want: 'Progressing', app: app({ sync: sync('Unknown', null, OLD), health: { status: 'Healthy' }, conditions: compareError }) },
  ];
}

/**
 * Each case's health as Argo CD's own CLI computes it from argocd-cm with `script` in it, in
 * one container: { version, image, unconfigured, cases: { name: { status, message } } }, or
 * { error }. `unconfigured` is the CLI's answer with argocd-cm empty: the control.
 */
function argocdHealth(version, script, cases) {
  const image = process.env.ARGOCD_CLI_IMAGE || `quay.io/argoproj/argocd:${version}`;
  // realpath: the mount must name the long path, never Windows' 8.3 short form of it.
  const dir = realpathSync.native(mkdtempSync(join(tmpdir(), 'argocd-health-')));
  try {
    mkdirSync(join(dir, 'cases'));
    const cm = (data) => JSON.stringify({ apiVersion: 'v1', kind: 'ConfigMap', metadata: { name: 'argocd-cm', namespace: 'argocd' }, data });
    writeFileSync(join(dir, 'argocd-cm.json'), cm({ [HEALTH_KEY]: script }));
    writeFileSync(join(dir, 'argocd-cm-empty.json'), cm({}));
    cases.forEach((c, i) => writeFileSync(join(dir, 'cases', `${String(i).padStart(2, '0')}.json`), JSON.stringify(c.app)));
    // The image runs as its own user, not the one that made the files.
    for (const p of [dir, join(dir, 'cases')]) chmodSync(p, 0o755);
    const sh = [
      'echo "VERSION $(argocd version --client --short)"',
      'for f in /w/cases/*.json; do echo "CASE $(basename "$f" .json)"; argocd admin settings resource-overrides health "$f" --argocd-cm-path /w/argocd-cm.json 2>&1; done',
      'echo "CASE unconfigured"; argocd admin settings resource-overrides health /w/cases/00.json --argocd-cm-path /w/argocd-cm-empty.json 2>&1',
    ].join('\n');
    const r = spawnSync('docker', ['run', '--rm', '-v', `${dir}:/w:ro`, '--entrypoint', 'sh', image, '-c', sh], { encoding: 'utf8', maxBuffer: 16 << 20, timeout: 10 * 60_000 });
    if (r.error || r.status !== 0) return { error: `${image}: ${(r.stderr || r.stdout || String(r.error?.message ?? '')).trim().slice(-400)}` };
    const out = { image, version: null, unconfigured: null, cases: {} };
    let current = null;
    for (const line of r.stdout.split(/\r?\n/)) {
      const v = /^VERSION argocd: (v\d+\.\d+\.\d+)/.exec(line);
      if (v) out.version = v[1];
      const c = /^CASE (\S+)$/.exec(line);
      if (c) {
        current = c[1] === 'unconfigured' ? 'unconfigured' : cases[Number(c[1])]?.name;
        if (current && current !== 'unconfigured') out.cases[current] = {};
        continue;
      }
      if (!current) continue;
      if (current === 'unconfigured') {
        if (!line.startsWith('{')) out.unconfigured = [out.unconfigured, line.trim()].filter(Boolean).join(' ');
        continue;
      }
      const s = /^STATUS: (\S+)/.exec(line);
      if (s) out.cases[current].status = s[1];
      const m = /^MESSAGE: (.*)$/.exec(line);
      if (m) out.cases[current].message = m[1];
      // A script that fails in Argo CD's Lua runtime prints no STATUS, only this.
      const fatal = /"level":"fatal","msg":"((?:[^"\\]|\\.)*)"/.exec(line);
      if (fatal) out.cases[current].message = `the script failed: ${JSON.parse(`"${fatal[1]}"`)}`;
    }
    return out;
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

/** Rendered objects, or the error text. */
function helmTemplate(dir, args) {
  const r = spawnSync('helm', ['template', 'v', dir, ...args], { encoding: 'utf8', maxBuffer: 64 << 20 });
  if (r.error || r.status !== 0) return { error: (r.stderr || r.stdout || String(r.error?.message ?? '')).trim() };
  // render=json where the chart offers it; every chart here emits one object per document.
  const docs = r.stdout
    .split(/^---\s*$/m)
    .map((d) => d.split(/\r?\n/).filter((l) => !l.startsWith('# Source:')).join('\n').trim())
    .filter(Boolean)
    .map((d) => JSON.parse(d));
  return { docs };
}

async function main() {
  const rootArg = process.argv.slice(2).find((a) => a !== '--');
  const root = rootArg ? resolve(rootArg) : ROOT;
  console.log(`  reading ${rootArg ? root : 'this tree'}`);
  const eks = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const pub = await import(pathToFileURL(join(root, 'scripts', 'publish-images.mjs')).href);
  const sv = await import(pathToFileURL(join(root, 'infra', 'argocd', 'eks', 'setup-values.mjs')).href);
  const origin = execFileSync('git', ['remote', 'get-url', 'origin'], { cwd: ROOT, encoding: 'utf8' }).trim();
  const owner = /github\.com[:/]([^/]+)\//.exec(origin)?.[1];
  const published = Object.fromEntries((await pub.plan({ root, sha: REV, owner })).map((i) => [i.name, i.ref]));
  check(Object.keys(published).length > 0, `CONTROL: for commit ${REV.slice(0, 7)}, CI publishes ${Object.values(published).join(', ')}`);

  // ---------------------------------------------------------------------------
  section('the Applications the EKS root makes are the ones the installer waits for');
  const rootApp = JSON.parse(readFileSync(join(root, 'infra', 'argocd', 'eks', 'root.json'), 'utf8'));
  const children = rootChildren(root, rootApp, REV);
  if (children.error) check(false, `the root's chart did not render at ${REV.slice(0, 7)}: ${children.error}`);
  const manifests = children.files.map((x) => x.m);
  const charts = eks.allCharts(root);
  const set = manifests.find((m) => m.kind === 'ApplicationSet');
  const plain = manifests.filter((m) => m.kind === 'Application');
  // verify-argocd.mjs evaluates the ApplicationSet's generator against charts/; here, its
  // Applications are the service charts.
  const names = [...(set ? charts : []), ...plain.map((a) => a.metadata.name)].sort();
  const waited = [...eks.gitopsApps(charts)].sort();
  check(JSON.stringify(names) === JSON.stringify(waited), `exactly gitopsApps(): ${names.length} (${plain.map((a) => `${a.metadata.name} from ${a.spec.source.path}`).join(', ')}, and one per service chart)`);
  for (const a of plain) {
    const s = a.spec;
    check(
      s.destination?.namespace === 'default' && s.syncPolicy?.automated?.prune === true && s.syncPolicy?.automated?.selfHeal === true && (a.metadata.finalizers ?? []).includes('resources-finalizer.argocd.argoproj.io') && s.source.helm?.releaseName === a.metadata.name && existsSync(join(root, ...s.source.path.split('/'), 'Chart.yaml')),
      `${a.metadata.name}: a chart that exists, in default, released under its own name, synced automatically with prune and self-heal, its resources deleted with it`,
    );
  }

  // ---------------------------------------------------------------------------
  section('setup is synced, and has finished, before anything else: on a fresh install, and on every commit');
  const revParam = (rootApp.spec.source.helm?.parameters ?? []).find((p) => p.name === 'revision');
  check(revParam?.value === '$ARGOCD_APP_REVISION' && revParam?.forceString === true, `the root hands its chart the commit it resolved main to: revision=${revParam?.value}${revParam?.forceString ? ', as a string' : ' -- not forced to a string, so an all-digit SHA would arrive as a number'}`);
  const pinnedTo = (m) => (m.kind === 'ApplicationSet' ? [m.spec?.template?.spec?.source?.targetRevision, m.spec?.generators?.[0]?.git?.revision] : [m.spec?.source?.targetRevision]);
  const unpinned = manifests.filter((m) => pinnedTo(m).some((r) => r !== REV));
  check(manifests.length > 0 && unpinned.length === 0, `every child is at that commit (${REV.slice(0, 7)} for this run), so each commit changes every child, and the root syncs it in wave order${unpinned.length ? ` -- not: ${unpinned.map((m) => `${m.metadata.name} at ${pinnedTo(m).join('/')}`).join(', ')}` : ''}`);
  // The health check compares commits; a branch name would never equal one, and the root would wait for ever.
  const refused = ['main', REV.slice(0, 7), ''].map((r) => [r, rootChildren(root, rootApp, r).error]);
  check(refused.every(([, e]) => /full commit SHA/.test(e ?? '')), `and the chart refuses anything but a full commit SHA: ${refused.map(([r, e]) => `${JSON.stringify(r)} ${e ? 'refused' : 'RENDERED'}`).join(', ')}`);
  const waveOf = (m) => m.metadata?.annotations?.['argocd.argoproj.io/sync-wave'] ?? '0';
  check(manifests.every((m) => /^-?\d+$/.test(waveOf(m))), `every child's sync wave is a whole number (${manifests.map((m) => `${m.metadata.name} ${waveOf(m)}`).join(', ')})`);
  const setupApp = manifests.find((m) => m.kind === 'Application' && m.metadata.name === 'setup');
  const rest = manifests.filter((m) => m !== setupApp);
  const late = rest.filter((m) => !setupApp || Number(waveOf(m)) <= Number(waveOf(setupApp)));
  check(Boolean(setupApp) && rest.length > 0 && late.length === 0, `setup is wave ${setupApp ? waveOf(setupApp) : '(none)'}, before every other child: ${rest.map((m) => `${m.metadata.name} ${waveOf(m)}`).join(', ')}${late.length ? ` -- not before ${late.map((m) => m.metadata.name).join(', ')}` : ''}`);

  // ---------------------------------------------------------------------------
  section('the health check those waves wait on: installed before the root, and judged by Argo CD itself');
  const settings = await import(pathToFileURL(join(root, 'infra', 'argocd', 'settings.mjs')).href);
  const script = eks.applicationHealthScript(root);
  let cm = { data: { 'kept.setting': 'x' } };
  const calls = [];
  const fakeKubectl = (args) => {
    calls.push(args.join(' '));
    if (args.includes('patch')) {
      cm = { data: { ...cm.data, ...JSON.parse(args[args.indexOf('-p') + 1]).data } };
      return '';
    }
    if (args.includes('get')) return JSON.stringify(cm);
    throw new Error(`unexpected kubectl ${args.join(' ')}`);
  };
  const installed = eks.installApplicationHealth(fakeKubectl, root);
  const patchedCm = calls.some((c) => /^-n argocd patch configmap argocd-cm --type merge /.test(c));
  check(installed.ok && patchedCm && cm.data[HEALTH_KEY] === script && cm.data['kept.setting'] === 'x', `installApplicationHealth puts application-health.lua into argocd-cm as ${HEALTH_KEY}, leaves argocd-cm's other settings, and reads it back (${installed.why})`);
  const deaf = (args) => (args.includes('get') ? JSON.stringify({ data: {} }) : '');
  const missed = eks.installApplicationHealth(deaf, root);
  check(!missed.ok, `CONTROL: and a patch that did not land is reported, not passed (${missed.why})`);
  const upSrc = strip(readFileSync(join(root, 'infra', 'eks', 'up.mjs'), 'utf8'));
  const gitopsBody = /async function gitops\([\s\S]*?\r?\n\}\r?\n/.exec(upSrc)?.[0] ?? '';
  const atHealth = gitopsBody.indexOf('installApplicationHealth(');
  const atRoot = gitopsBody.search(/kubectl\(\['apply', '-f', join\('infra', 'argocd', 'eks', 'root\.json'\)\]\)/);
  check(atHealth >= 0 && atRoot >= 0 && atHealth < atRoot, `gitops() installs it before it applies the root, whose first sync is the fresh install (at ${atHealth} and ${atRoot} in its body)`);

  const verdicts = argocdHealth(settings.ARGOCD_VERSION, script, healthCases(REV));
  if (verdicts.error) {
    check(false, `Argo CD's CLI did not run: ${verdicts.error}`);
  } else {
    check(verdicts.version === settings.ARGOCD_VERSION, `CONTROL: judged by Argo CD ${verdicts.version}, the version the cluster runs (${verdicts.image})`);
    check(/not configured/.test(verdicts.unconfigured ?? ''), `CONTROL: without the script, Argo CD has no health for an Application at all, so a wave would wait for nothing (${verdicts.unconfigured})`);
    for (const c of healthCases(REV)) {
      const v = verdicts.cases[c.name];
      check(v?.status === c.want, `${c.want.padEnd(11)} ${c.name}${v?.status === c.want ? '' : ` -- Argo CD says ${v?.status ?? '(nothing)'}`}${v?.message ? ` (${v.message.slice(0, 140)})` : ''}`);
    }
  }

  // ---------------------------------------------------------------------------
  section(`each Application rendered as Argo CD renders it, at ${REV.slice(0, 7)}`);
  // [name, chart dir, value files (relative to it), parameters]
  const targets = [];
  const tsrc = set?.spec?.template?.spec?.source;
  for (const c of charts) targets.push({ name: c, dir: `charts/${c}`, files: tsrc?.helm?.valueFiles ?? [], params: tsrc?.helm?.parameters ?? [] });
  for (const a of plain) targets.push({ name: a.metadata.name, dir: a.spec.source.path, files: a.spec.source.helm?.valueFiles ?? [], params: a.spec.source.helm?.parameters ?? [] });
  const tagParams = targets.flatMap((t) => t.params.filter((p) => p.name === 'image.tag').map((p) => `${t.name}=${p.value}`));
  check(tagParams.length > 0 && tagParams.every((p) => p.endsWith('=$ARGOCD_APP_REVISION_SHORT')), `every image.tag is $ARGOCD_APP_REVISION_SHORT, the commit deployed (${tagParams.length} Application(s))`);
  const envValues = readFileSync(join(root, 'infra', 'argocd', 'env', 'eks.yaml'), 'utf8').replace(/^\s*#.*$/gm, '');
  check(!/^\s*tag\s*:/m.test(envValues), 'and infra/argocd/env/eks.yaml writes no tag of its own');

  let rendered = 0;
  for (const t of targets) {
    const dir = join(root, ...t.dir.split('/'));
    if (t.dir.startsWith('charts/')) spawnSync('helm', ['dependency', 'build', dir], { encoding: 'utf8' });
    const args = ['--set', 'render=json'];
    let missing = null;
    for (const f of t.files) {
      const p = join(dir, ...f.split('/'));
      if (!existsSync(p)) missing = posix.join(t.dir, f);
      args.push('-f', p);
    }
    for (const p of t.params) args.push('--set', `${p.name}=${String(p.value).replace('$ARGOCD_APP_REVISION_SHORT', REV.slice(0, 7))}`);
    if (missing) {
      check(false, `${t.name}: value file ${missing} does not exist`);
      continue;
    }
    const r = helmTemplate(dir, args);
    if (r.error) {
      check(false, `${t.name} did not render: ${r.error.slice(-300)}`);
      continue;
    }
    rendered += 1;
    const pods = r.docs.map((d) => d.spec?.template?.spec ?? (d.kind === 'Pod' ? d.spec : null)).filter(Boolean);
    const images = pods.flatMap((p) => [...(p.initContainers ?? []), ...(p.containers ?? [])].map((c) => c.image));
    const want = t.name === 'web' ? published['eventhub/web'] : t.name === 'kong' ? null : published['eventhub/app'];
    if (want === null) {
      check(images.length > 0 && images.every((i) => !i.startsWith('ghcr.io/')), `${t.name}: compose's own image, not one CI builds (${[...new Set(images)].join(', ')})`);
    } else {
      const fromRegistry = images.filter((i) => i.startsWith('ghcr.io/'));
      check(images.length > 0 && fromRegistry.length > 0 && fromRegistry.every((i) => i === want), `${t.name}: ${want}, exactly what CI publishes for the commit${fromRegistry.some((i) => i !== want) ? ` -- renders ${[...new Set(fromRegistry)].join(', ')}` : ''}`);
      const sa = [...new Set(pods.map((p) => p.serviceAccountName ?? 'default'))];
      check(sa.every((s) => s === 'default'), `${t.name}: its pods run as the default service account, which holds the pull Secret (${sa.join(', ')})`);
    }
    if (t.dir.startsWith('charts/')) {
      const reps = r.docs.filter((d) => d.kind === 'Deployment').map((d) => d.spec.replicas);
      check(reps.every((n) => n === 1), `${t.name}: one replica (${reps.join(', ') || 'no Deployment'})`);
    }
  }
  check(rendered === targets.length, `CONTROL: ${rendered} of ${targets.length} Application(s) rendered`);

  // ---------------------------------------------------------------------------
  section("setup is compose's setup, and both clusters run one Argo CD");
  const composeRun = spawnSync('docker', ['compose', '-f', join(root, 'docker-compose.yml'), '-p', 'eventhub-rebuild', 'config', '--format', 'json'], { encoding: 'utf8', maxBuffer: 64 << 20 });
  if (composeRun.status !== 0) {
    check(false, `docker compose config: ${composeRun.stderr.trim().slice(-300)}`);
  } else {
    const have = readFileSync(join(root, 'infra', 'argocd', 'env', 'eks-setup.json'), 'utf8').replace(/\r\n/g, '\n');
    const want = sv.setupValues(JSON.parse(composeRun.stdout));
    check(have === want, `infra/argocd/env/eks-setup.json is exactly compose's setup command${have === want ? '' : ': run node infra/argocd/eks/setup-values.mjs'}`);
  }
  const both =['infra/argocd/bootstrap.mjs', 'infra/eks/up.mjs'].map((f) => [f, strip(readFileSync(join(root, ...f.split('/')), 'utf8'))]);
  check(both.every(([, src]) => /from '(\.\/|\.\.\/argocd\/)settings\.mjs'/.test(src) && !/argo-cd\/v\d/.test(src) && !/'v\d+\.\d+\.\d+'/.test(src)), "bootstrap.mjs (kind) and up.mjs (EKS) both take Argo CD's version and manifest from infra/argocd/settings.mjs, and neither pins one of its own");

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log("Argo CD on EKS deploys main from the images CI published for each commit, under no tag git does not name");
}

main().catch((e) => {
  check(false, `the verifier itself failed: ${e.stack || e.message}`);
});
