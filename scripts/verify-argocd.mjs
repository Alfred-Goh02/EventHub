/**
 * Asserts that ArgoCD, given either cluster's root -- infra/argocd/root.json (kind) or
 * infra/argocd/eks/root.json (EKS) -- runs exactly the services in charts/, from this
 * repository's main branch, and that no credential is in git to do it. What else the EKS root
 * deploys, and from which images, is verify-argocd-eks.mjs.
 *
 * GITOPS FAILS QUIETLY. An ApplicationSet whose generator misses a chart simply never
 * creates that Application: nothing errors, the service is just not there. One that
 * reads a branch nobody merges to keeps running last month's code, Synced and Healthy.
 * So the generator is evaluated here, against the directory, rather than trusted:
 *
 *   - root.json points at this repository (the `origin` remote), at `main`, at a
 *     directory that exists, with automated sync that prunes and self-heals;
 *   - every Application or ApplicationSet under the root's path names the same
 *     repository and branch -- `main` itself under kind's root, a directory; under EKS's, a
 *     Helm chart, the commit the root resolved main to, which it hands each one as
 *     $ARGOCD_APP_REVISION (infra/argocd/eks/apps/values.yaml says why). The chart is
 *     rendered for a commit, as Argo CD renders it (scripts/lib/argocd-children.mjs);
 *   - the services ApplicationSet's directories, applied to charts/, give exactly the
 *     service charts -- every library chart excluded, none missing, none extra -- and
 *     each has the values.yaml it starts from;
 *   - every value file its template names exists, from every chart's directory;
 *   - the release is named for its directory, sync is automated, prune and self-heal on,
 *     and a missing template key is an error rather than an empty string;
 *   - no file under infra/argocd that ArgoCD reads carries key material. The deploy key
 *     lives outside the repository, and bootstrap.mjs reads it from there.
 *
 * Reads files and `git remote`, and renders EKS's root with helm:
 *   node scripts/verify-argocd.mjs [root]
 */

import { execFileSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, posix, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { rootChildren } from './lib/argocd-children.mjs';

const HERE = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const ROOT = process.argv[2] ? resolve(process.argv[2]) : HERE;
/** The commit a Helm root is rendered at: what Argo CD resolved main to, for the run. */
const REV = '0123456789abcdef0123456789abcdef01234567';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/** github.com/owner/repo, from an ssh or https remote. */
function repoId(url) {
  const m = /github\.com[:/]([^/]+)\/(.+?)(?:\.git)?\/?$/.exec(String(url).trim());
  return m ? `github.com/${m[1].toLowerCase()}/${m[2].toLowerCase()}` : null;
}

/** Files under a directory, recursively. */
function walk(dir) {
  const out = [];
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) out.push(...walk(p));
    else out.push(p);
  }
  return out;
}

/** A glob segment as ArgoCD's git directory generator matches it: * within one segment. */
const segmentMatch = (pattern, name) => new RegExp(`^${pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '[^/]*')}$`).test(name);

/** Every root, one per cluster: kind's, which bootstrap.mjs applies, and EKS's, which up.mjs --gitops applies. */
const ROOTS = ['infra/argocd/root.json', 'infra/argocd/eks/root.json'];

function main() {
  const origin = execFileSync('git', ['remote', 'get-url', 'origin'], { cwd: HERE, encoding: 'utf8' }).trim();
  const repo = repoId(origin);
  check(Boolean(repo), `CONTROL: this repository is read from git's origin (${repo})`);
  for (const rel of ROOTS) judgeRoot(join(ROOT, ...rel.split('/')), rel, repo);
  credentials();
}

function judgeRoot(rootFile, rel, repo) {
  const rootApp = JSON.parse(readFileSync(rootFile, 'utf8'));
  console.log('');
  console.log(`  the root Application: ${rel}`);
  const src = rootApp.spec?.source ?? {};
  check(repoId(src.repoURL) === repo, `it reads this repository (${src.repoURL})`);
  check(src.targetRevision === 'main', `at main, the branch that is merged to (${src.targetRevision})`);
  const appsDir = join(ROOT, ...String(src.path ?? '').split('/'));
  check(Boolean(src.path) && existsSync(appsDir) && statSync(appsDir).isDirectory(), `from ${src.path}, a directory that exists`);
  const auto = rootApp.spec?.syncPolicy?.automated;
  check(auto?.prune === true && auto?.selfHeal === true, 'with automated sync that prunes and self-heals');

  // A Helm root pins its children to the commit it resolved main to; a directory root's
  // children name main themselves. Either way, main is what they run.
  const pinned = Boolean(src.helm);
  const childRev = pinned ? REV : 'main';
  const at = pinned ? `the commit the root resolved main to (${REV.slice(0, 7)} for this run)` : 'main';
  if (pinned) check((src.helm.parameters ?? []).some((p) => p.name === 'revision' && p.value === '$ARGOCD_APP_REVISION'), 'a Helm chart, handed the commit it resolved main to: revision=$ARGOCD_APP_REVISION');
  const rendered = rootChildren(ROOT, rootApp, REV);
  if (rendered.error) check(false, `${src.path} did not render: ${rendered.error}`);
  const manifests = rendered.files;
  check(manifests.length > 0, `CONTROL: ${manifests.length} manifest(s) under ${src.path}${pinned ? ', rendered at a commit' : ''}: ${manifests.map((x) => `${x.f} (${x.m.kind})`).join(', ')}`);

  // What charts/ holds, judged the way the charts judge themselves.
  const chartsDir = join(ROOT, 'charts');
  const chartDirs = readdirSync(chartsDir, { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(chartsDir, d.name, 'Chart.yaml')))
    .map((d) => d.name)
    .sort();
  const libraries = chartDirs.filter((c) => /^type:\s*library\s*$/m.test(readFileSync(join(chartsDir, c, 'Chart.yaml'), 'utf8')));
  const services = chartDirs.filter((c) => !libraries.includes(c));
  check(services.length > 0 && libraries.length > 0, `CONTROL: charts/ holds ${services.length} service chart(s) and the librar(ies) ${libraries.join(', ')}`);

  const sets = manifests.filter((x) => x.m.kind === 'ApplicationSet');
  check(sets.length > 0, 'CONTROL: there is an ApplicationSet to evaluate');
  for (const { f, m } of manifests) {
    console.log('');
    console.log(`  ${src.path}/${f}: ${m.kind} ${m.metadata?.name}`);
    if (m.kind === 'Application') {
      check(repoId(m.spec?.source?.repoURL) === repo && m.spec?.source?.targetRevision === childRev, `it reads this repository at ${at} (${m.spec?.source?.targetRevision})`);
      continue;
    }
    if (m.kind !== 'ApplicationSet') {
      check(false, `${m.kind} is neither an Application nor an ApplicationSet`);
      continue;
    }
    const gen = m.spec?.generators?.[0]?.git;
    check(Boolean(gen) && repoId(gen.repoURL) === repo && gen.revision === childRev, `its git generator reads this repository at ${at} (${gen?.repoURL} @ ${gen?.revision})`);
    const dirs = gen?.directories ?? [];
    const includes = dirs.filter((d) => !d.exclude).map((d) => d.path);
    const excludes = dirs.filter((d) => d.exclude).map((d) => d.path);
    // Evaluate the generator against the directory: one segment under charts/.
    const generated = chartDirs.filter((c) => {
      const path = `charts/${c}`;
      const inc = includes.some((p) => p.split('/').length === 2 && p.startsWith('charts/') && segmentMatch(p.split('/')[1], c));
      const exc = excludes.some((p) => p === path || (p.startsWith('charts/') && segmentMatch(p.split('/')[1], c)));
      return inc && !exc;
    });
    const missing = services.filter((s) => !generated.includes(s));
    const extra = generated.filter((g) => !services.includes(g));
    check(missing.length === 0, missing.length === 0 ? `it generates an Application for every service chart (${generated.length})` : `no Application for ${missing.join(', ')}`);
    check(extra.length === 0, extra.length === 0 ? 'and for nothing else: every library chart is excluded' : `it generates an Application for ${extra.join(', ')}, which is not a service`);
    check(generated.every((g) => existsSync(join(chartsDir, g, 'values.yaml'))), 'each generated chart has its values.yaml');

    const t = m.spec?.template?.spec ?? {};
    const tsrc = t.source ?? {};
    check(repoId(tsrc.repoURL) === repo && tsrc.targetRevision === childRev, `each Application reads this repository at ${at} (${tsrc.targetRevision})`);
    check(tsrc.path === '{{.path.path}}' && tsrc.helm?.releaseName === '{{.path.basename}}' && m.spec?.template?.metadata?.name === '{{.path.basename}}', 'from its own directory, as a release and an Application named for it');
    check(m.spec?.goTemplate === true && (m.spec?.goTemplateOptions ?? []).includes('missingkey=error'), 'a missing template key is an error, not an empty string');
    const files = tsrc.helm?.valueFiles ?? [];
    const absent = [];
    for (const g of generated) {
      for (const vf of files) {
        const resolved = posix.normalize(posix.join('charts', g, vf));
        if (resolved.startsWith('..') || !existsSync(join(ROOT, ...resolved.split('/')))) absent.push(`${g}: ${vf}`);
      }
    }
    check(files.length > 0 && absent.length === 0, absent.length === 0 ? `every value file it names exists, from every chart (${files.join(', ')})` : `value files missing: ${absent.join('; ')}`);
    const tauto = t.syncPolicy?.automated;
    check(tauto?.prune === true && tauto?.selfHeal === true, 'with automated sync that prunes and self-heals');
  }

}

function credentials() {
  console.log('');
  console.log('  no credential in git');
  const data = walk(join(ROOT, 'infra', 'argocd')).filter((p) => /\.(json|ya?ml)$/.test(p));
  check(data.length > 0, `CONTROL: ${data.length} manifest file(s) under infra/argocd read`);
  const leaks = data.filter((p) => {
    const text = readFileSync(p, 'utf8');
    return /-----BEGIN [A-Z ]*PRIVATE KEY-----/.test(text) || /"(sshPrivateKey|password|githubAppPrivateKey)"\s*:/.test(text) || /^\s*(sshPrivateKey|password):/m.test(text);
  });
  check(leaks.length === 0, leaks.length === 0 ? 'none of them carries a key or a password' : `key material in ${leaks.map((p) => relative(ROOT, p)).join(', ')}`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('ArgoCD, given root.json, runs exactly the service charts, from main, with no credential in git');
}

main();
