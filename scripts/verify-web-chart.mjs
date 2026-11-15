/**
 * Asserts that infra/web runs on EKS the image apps/web/Dockerfile builds, as that image
 * serves -- and that the installer has everything it needs to build and push it.
 *
 *   - the chart refuses to render without the image's repository and tag, or on latest;
 *   - the container port is the PORT the image's Dockerfile sets, and the Service's; the
 *     pod runs as the user the Dockerfile's runtime stage names (node, uid 1000), non-root,
 *     with a read-only root filesystem, no privilege escalation, no capabilities, no service
 *     links and no API token; readiness renders the page and liveness asks for a file in
 *     apps/web/public, which nitro serves from disk; requests and a memory limit; a preStop
 *     inside the grace period; the Service is ClusterIP;
 *   - the image is built for a Node server: vite.config.ts sets nitro's node-server preset,
 *     without which the build targets Cloudflare and the Dockerfile's CMD finds nothing to
 *     run; the runtime stage copies nitro's output and nothing else; the Dockerfile's Node is
 *     .nvmrc's major and its pnpm the root packageManager's; .dockerignore keeps
 *     node_modules, the build's output and env files out of the context;
 *   - every image infra/eks/up.mjs builds (IMAGES) has a Dockerfile in its context, and
 *     Terraform makes an ECR repository of that name for it to be pushed to.
 *
 * Needs helm on PATH. CI's web job builds the image and serves a page from it.
 *   node scripts/verify-web-chart.mjs          against this tree
 *   node scripts/verify-web-chart.mjs [root]   against another (a red run in a copy)
 */

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

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

/** A chart rendered as JSON objects (render=json), or the error text. */
function render(chartDir, args = []) {
  const r = spawnSync('helm', ['template', 'v', chartDir, '--set', 'render=json', ...args], { encoding: 'utf8', maxBuffer: 64 << 20 });
  if (r.error || r.status !== 0) return { error: (r.stderr || r.stdout || String(r.error?.message ?? '')).trim() };
  return {
    docs: r.stdout
      .split(/^---\s*$/m)
      .map((d) => d.split(/\r?\n/).filter((l) => !l.startsWith('# Source:')).join('\n').trim())
      .filter(Boolean)
      .map((d) => JSON.parse(d)),
  };
}

const read = (p) => (existsSync(p) ? readFileSync(p, 'utf8').replace(/\r\n/g, '\n') : null);

/** The Dockerfile's stages, each { from, lines }. */
function stages(dockerfile) {
  const out = [];
  for (const raw of dockerfile.split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const from = /^FROM\s+(\S+)/i.exec(line);
    if (from) out.push({ from: from[1], lines: [] });
    else out.at(-1)?.lines.push(line);
  }
  return out;
}

async function main() {
  const rootArg = process.argv.slice(2).find((a) => a !== '--');
  const root = rootArg ? resolve(rootArg) : ROOT;
  const chartDir = join(root, 'infra', 'web');
  const webDir = join(root, 'apps', 'web');
  console.log(`  reading ${rootArg ? root : 'this tree'}: infra/web against apps/web`);

  section('the chart refuses an image it cannot name');
  for (const [what, args] of [
    ['no repository', ['--set', 'image.tag=abc1234']],
    ['no tag', ['--set', 'image.repository=r.example/eventhub/web']],
    ['latest', ['--set', 'image.repository=r.example/eventhub/web', '--set', 'image.tag=latest']],
  ]) {
    const r = render(chartDir, args);
    check(Boolean(r.error) && /image\.(repository|tag)/.test(r.error), `with ${what}, it does not render (${r.error ? r.error.split('\n')[0].slice(-110) : 'it rendered'})`);
  }

  const chart = render(chartDir, ['--set', 'image.repository=r.example/eventhub/web', '--set', 'image.tag=abc1234']);
  if (chart.error) {
    check(false, `infra/web did not render: ${chart.error.slice(-500)}`);
    return;
  }
  const deploy = chart.docs.find((d) => d.kind === 'Deployment');
  const svc = chart.docs.find((d) => d.kind === 'Service');
  check(deploy !== undefined && svc !== undefined, `CONTROL: ${chart.docs.length} object(s): ${chart.docs.map((d) => `${d.kind}/${d.metadata.name}`).join(', ')}`);
  if (!deploy || !svc) return;
  const pod = deploy.spec.template.spec;
  const c = pod.containers[0];
  check(c.image === 'r.example/eventhub/web:abc1234', `the image is the repository and tag the installer passes (${c.image})`);

  // ---------------------------------------------------------------------------
  section("the pod runs the image as its Dockerfile builds it");
  const dockerfile = read(join(webDir, 'Dockerfile'));
  check(dockerfile !== null, 'apps/web has a Dockerfile');
  const st = dockerfile ? stages(dockerfile) : [];
  const runtime = st.at(-1);
  check(st.length >= 2 && runtime !== undefined, `CONTROL: ${st.length} stage(s): ${st.map((s) => s.from).join(' -> ')}`);
  const envLine = runtime?.lines.filter((l) => /^ENV\s/i.test(l)).join(' ') ?? '';
  const port = Number(/\bPORT=(\d+)/.exec(envLine)?.[1]);
  const http = (c.ports ?? []).find((p) => p.name === 'http');
  check(port > 0 && http?.containerPort === port, `the container port is the PORT the image sets (${http?.containerPort}; Dockerfile ${Number.isNaN(port) ? 'sets none' : port})`);
  check(/\bHOST=0\.0\.0\.0\b/.test(envLine), 'the image listens on every interface inside the container, so the kubelet and the Service can reach it');
  const served = (svc.spec.ports ?? []).map((p) => p.targetPort);
  check(served.length === 1 && (served[0] === 'http' || served[0] === port) && (svc.spec.type ?? 'ClusterIP') === 'ClusterIP', `the Service is ClusterIP, on that port alone (${svc.spec.type ?? 'ClusterIP'}, ${served.join(', ')})`);
  const user = runtime?.lines.filter((l) => /^USER\s/i.test(l)).map((l) => l.split(/\s+/)[1]).at(-1);
  const sc = pod.securityContext ?? {};
  // The node images' node user is uid 1000; CI's web job asks the built image itself.
  check(user === 'node' && sc.runAsNonRoot === true && sc.runAsUser === 1000, `runs as the runtime stage's user, ${user ?? 'root'} (uid 1000), non-root (runAsNonRoot ${sc.runAsNonRoot}, runAsUser ${sc.runAsUser})`);
  const csc = c.securityContext ?? {};
  check(csc.readOnlyRootFilesystem === true && csc.allowPrivilegeEscalation === false && JSON.stringify(csc.capabilities?.drop) === JSON.stringify(['ALL']), `a read-only root filesystem, no privilege escalation, every capability dropped (${csc.readOnlyRootFilesystem}, ${csc.allowPrivilegeEscalation}, ${JSON.stringify(csc.capabilities?.drop)})`);
  check(pod.enableServiceLinks === false && pod.automountServiceAccountToken === false, `no service links, no Kubernetes API token (${pod.enableServiceLinks}, ${pod.automountServiceAccountToken})`);
  check(c.readinessProbe?.httpGet?.path === '/', `readiness renders the page (${c.readinessProbe?.httpGet?.path})`);
  const live = c.livenessProbe?.httpGet?.path ?? '';
  check(live !== '/' && existsSync(join(webDir, 'public', live.replace(/^\//, ''))) && c.startupProbe?.httpGet?.path === live, `liveness and startup ask for ${live || 'nothing'}, a file in apps/web/public that nitro serves without rendering`);
  const sleep = Number(c.lifecycle?.preStop?.exec?.command?.at(-1));
  check(sleep > 0 && sleep < pod.terminationGracePeriodSeconds, `preStop waits ${sleep} s, inside the ${pod.terminationGracePeriodSeconds} s grace period`);
  const req = c.resources?.requests ?? {};
  const lim = c.resources?.limits ?? {};
  check(Boolean(req.cpu) && Boolean(req.memory) && Boolean(lim.memory), `requests ${req.cpu}/${req.memory}, memory limit ${lim.memory}`);

  // ---------------------------------------------------------------------------
  section('the image is built for a Node server, from the lockfile, with nothing local in it');
  const vite = (read(join(webDir, 'vite.config.ts')) ?? '').replace(/\/\/.*$/gm, '');
  check(/nitro:\s*\{\s*preset:\s*["']node-server["']/.test(vite), "vite.config.ts sets nitro's node-server preset: the shared config's default is Cloudflare, and then there is no .output/server/index.mjs to run");
  const cmd = runtime?.lines.find((l) => /^CMD\s/i.test(l)) ?? '';
  check(/\.output\/server\/index\.mjs/.test(cmd), `the image runs nitro's server (${cmd || 'no CMD'})`);
  const copies = runtime?.lines.filter((l) => /^COPY\s/i.test(l)) ?? [];
  check(copies.length > 0 && copies.every((l) => /--from=\S+/.test(l) && /\/\.output\s+\S*\.output\/?$/.test(l)), `the runtime stage copies nitro's output and nothing else (${copies.join(' | ') || 'no COPY'})`);
  const nvmrc = (read(join(root, '.nvmrc')) ?? '').trim();
  check(nvmrc !== '' && st.every((s) => s.from.startsWith(`node:${nvmrc}-`)), `every stage is on node ${nvmrc}, .nvmrc's (${st.map((s) => s.from).join(', ')})`);
  const pm = /pnpm@([\d.]+)/.exec(JSON.parse(read(join(root, 'package.json')) ?? '{}').packageManager ?? '')?.[1];
  const installed = /pnpm@([\d.]+)/.exec(dockerfile ?? '')?.[1];
  check(pm !== undefined && installed === pm, `pnpm ${installed ?? '(none)'} is the root packageManager's, ${pm ?? '(none)'}`);
  check(/pnpm install --frozen-lockfile --ignore-workspace/.test(dockerfile ?? ''), "it installs from apps/web's own lockfile, frozen, outside the workspace");
  const ignore = (read(join(webDir, '.dockerignore')) ?? '').split('\n').map((l) => l.trim()).filter((l) => l && !l.startsWith('#'));
  for (const rule of ['node_modules', '.output', '.env', '.env.*']) check(ignore.includes(rule), `.dockerignore has ${rule}`);

  // ---------------------------------------------------------------------------
  section('every image the installer builds has a Dockerfile, and an ECR repository to go to');
  const eks = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const images = Object.entries(eks.IMAGES ?? {});
  check(images.length >= 2 && images.some(([, ctx]) => resolve(root, ctx) === webDir), `CONTROL: the installer builds ${images.map(([n, ctx]) => `${n} from ${ctx}`).join(', ') || 'nothing'}, the web app among them`);
  const tf = read(join(root, 'infra', 'terraform', 'envs', 'demo', 'main.tf')) ?? '';
  const ecrRepos = [...tf.matchAll(/module\s+"[^"]+"\s*\{([^}]*)\}/g)]
    .map((m) => m[1])
    .filter((body) => /source\s*=\s*"\.\.\/\.\.\/modules\/ecr"/.test(body))
    .map((body) => /\bname\s*=\s*"([^"]+)"/.exec(body)?.[1])
    .filter(Boolean);
  check(ecrRepos.length > 0, `CONTROL: envs/demo makes ${ecrRepos.length} ECR repository(ies): ${ecrRepos.join(', ')}`);
  for (const [name, ctx] of images) {
    check(existsSync(join(root, ctx, 'Dockerfile')), `${name}: ${ctx}/Dockerfile exists`);
    check(ecrRepos.includes(name), `${name}: Terraform makes its ECR repository`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('infra/web runs the image apps/web/Dockerfile builds, and the installer can build and push every image it names');
}

main().catch((e) => {
  check(false, `the verifier itself failed: ${e.stack || e.message}`);
});
