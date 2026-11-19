/**
 * The manifests a root Application makes, as Argo CD makes them: { files, error }, each file
 * { f, m }, for both verifiers that read a root (verify-argocd.mjs, verify-argocd-eks.mjs).
 *
 * Two kinds of root are read here, because the two clusters have one of each:
 *   - a directory (kind's infra/argocd/root.json): every *.json under the root's path, as is;
 *   - a Helm chart (EKS's infra/argocd/eks/root.json, whose children are pinned to the commit
 *     the root resolved): `helm template`, with the root's parameters, and each build variable
 *     in them ($ARGOCD_APP_REVISION and the rest) replaced the way Argo CD replaces it, for
 *     the commit `rev`. Reading a template file as it is would read "{{ .Values.revision }}"
 *     where Argo CD writes a commit, which is the thing a verifier must check.
 * Needs helm on PATH for a Helm root.
 */

import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join, posix } from 'node:path';

/** Argo CD's build environment for an Application at commit `rev` (docs/user-guide/build-environment.md). */
export function buildEnv(app, rev) {
  return {
    ARGOCD_APP_NAME: app.metadata?.name ?? '',
    ARGOCD_APP_NAMESPACE: app.spec?.destination?.namespace ?? '',
    ARGOCD_APP_PROJECT_NAME: app.spec?.project ?? '',
    ARGOCD_APP_REVISION: rev,
    ARGOCD_APP_REVISION_SHORT: rev.slice(0, 7),
    ARGOCD_APP_REVISION_SHORT_8: rev.slice(0, 8),
    ARGOCD_APP_SOURCE_PATH: app.spec?.source?.path ?? '',
    ARGOCD_APP_SOURCE_REPO_URL: app.spec?.source?.repoURL ?? '',
    ARGOCD_APP_SOURCE_TARGET_REVISION: app.spec?.source?.targetRevision ?? '',
  };
}

/** $VAR and ${VAR} replaced from env, $$ left as one $, as Argo CD does for a parameter. */
export function envsubst(value, env) {
  return String(value).replace(/\$\$|\$\{(\w+)\}|\$(\w+)/g, (m, a, b) => (m === '$$' ? '$' : (env[a ?? b] ?? '')));
}

/** helm template's output, one object per document; every template here writes JSON. */
export function parseRendered(stdout) {
  return stdout
    .split(/^---\s*$/m)
    .map((d) => d.split(/\r?\n/).filter((l) => !l.startsWith('# Source:')).join('\n').trim())
    .filter(Boolean)
    .map((d) => JSON.parse(d));
}

export function rootChildren(root, rootApp, rev) {
  const src = rootApp.spec?.source ?? {};
  const dir = join(root, ...String(src.path ?? '').split('/'));
  if (!src.path || !existsSync(dir)) return { files: [], error: `${src.path} does not exist` };
  if (!src.helm) {
    const files = readdirSync(dir).filter((f) => f.endsWith('.json')).map((f) => ({ f, m: JSON.parse(readFileSync(join(dir, f), 'utf8')) }));
    return { files, error: null };
  }
  const env = buildEnv(rootApp, rev);
  const args = ['template', src.helm.releaseName ?? rootApp.metadata?.name ?? 'root', dir];
  for (const f of src.helm.valueFiles ?? []) args.push('-f', join(dir, ...f.split('/')));
  for (const p of src.helm.parameters ?? []) args.push(p.forceString ? '--set-string' : '--set', `${p.name}=${envsubst(p.value, env)}`);
  const r = spawnSync('helm', args, { encoding: 'utf8', maxBuffer: 64 << 20 });
  if (r.error || r.status !== 0) return { files: [], error: (r.stderr || String(r.error?.message ?? '')).trim().slice(-400) };
  const files = [];
  for (const chunk of r.stdout.split(/^---\s*$/m)) {
    const source = /^# Source: (.+)$/m.exec(chunk)?.[1];
    const docs = parseRendered(chunk);
    for (const m of docs) files.push({ f: source ? posix.basename(source.trim()) : '?', m });
  }
  return { files, error: null };
}
