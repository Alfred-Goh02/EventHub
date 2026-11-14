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
 * Needs helm and docker (for `docker compose config`) on PATH:
 *   node scripts/verify-argocd-eks.mjs [root]
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, posix, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

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
  const appsDir = join(root, ...rootApp.spec.source.path.split('/'));
  const manifests = readdirSync(appsDir).filter((f) => f.endsWith('.json')).map((f) => JSON.parse(readFileSync(join(appsDir, f), 'utf8')));
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
  const strip = (src) => src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:'"`])\/\/.*$/gm, '$1');
  const both = ['infra/argocd/bootstrap.mjs', 'infra/eks/up.mjs'].map((f) => [f, strip(readFileSync(join(root, ...f.split('/')), 'utf8'))]);
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
