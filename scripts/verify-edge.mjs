/**
 * Asserts that the EKS cluster's one way in from the internet (infra/edge) reaches the web
 * app on the page's port and Kong on the gateway's, and nothing else; that the installer
 * makes it with the controller Terraform grants a role to, before the Secrets that need its
 * address; and that it takes it off in the order that leaves nothing billing.
 *
 *   - THE LOAD BALANCER. One Service of type LoadBalancer for the AWS Load Balancer
 *     Controller's class: a Network Load Balancer, internet-facing, IP targets, the client's
 *     address kept, across zones, tagged as Terraform tags everything. Plain HTTP on 80 with
 *     no certificate; with one, TLS 1.2 and up on 443 and the gateway's port, and nothing
 *     but an ACM certificate's ARN taken;
 *   - WHAT IT REACHES, from every chart rendered: exactly two pod templates carry its label,
 *     the web app's and Kong's, and each of its two ports names a container port that exactly
 *     one of them has -- the web app's http, Kong's proxy -- at the port that process listens
 *     on. (That Kubernetes leaves a pod out of a port it has no name for was measured on kind:
 *     20 of 20 to the right pod on each port; with Kong given a port named http too, 8 of 20
 *     page requests reached Kong.) The gateway's port is the one the web app calls;
 *   - THE CONTROLLER. Its chart version is the release whose IAM policy Terraform copied, and
 *     its namespace and service account are the ones Terraform's Pod Identity association
 *     names;
 *   - THE ORDER. The installer makes the way in before the Secrets, and makes them with its
 *     address; --down deletes the load balancer, waits for AWS to show nothing left, and only
 *     then removes the controller;
 *   - THE ADDRESS. Without a domain it is the balancer's own through sslip.io, the same
 *     whatever order DNS lists them in, an address Auth takes; and the check from outside
 *     tells a working way in from each way it can fail.
 *
 * Needs helm on PATH and `pnpm build` (Auth's template parser).
 *   node scripts/verify-edge.mjs          against this tree
 *   node scripts/verify-edge.mjs [root]   against another (a red run in a copy)
 */

import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const rootArg = process.argv.slice(2).find((a) => a !== '--');
const root = rootArg ? resolve(rootArg) : ROOT;

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
const sh = (cmd, args) => {
  const r = spawnSync(cmd, args, { encoding: 'utf8', maxBuffer: 64 << 20 });
  return r.error ? { status: -1, stdout: '', stderr: String(r.error.message) } : { status: r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
};
const strip = (src) => src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:'"`\\])\/\/.*$/gm, '$1');

/** A chart rendered as JSON objects (render=json), or the error text. */
function render(chartDir, args = []) {
  const r = sh('helm', ['template', 'v', chartDir, '--set', 'render=json', ...args]);
  if (r.status !== 0) return { error: (r.stderr || r.stdout).trim() };
  return {
    docs: r.stdout
      .split(/^---\s*$/m)
      .map((d) => d.split(/\r?\n/).filter((l) => !l.startsWith('# Source:')).join('\n').trim())
      .filter(Boolean)
      .map((d) => JSON.parse(d)),
  };
}

const ARN = 'arn:aws:acm:ap-southeast-2:123456789012:certificate/0f2a5d6e-1111-2222-3333-444455556666';

async function main() {
  console.log(`  reading ${rootArg ? root : 'this tree'}`);
  const eks = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const pub = await import(pathToFileURL(join(root, 'infra', 'eks', 'public.mjs')).href);
  const appUrl = await import(pathToFileURL(join(root, 'packages', 'config', 'dist', 'app-url.js')).href);
  const edgeDir = join(root, 'infra', 'edge');

  // -------------------------------------------------------------------------
  section('the load balancer: one Network Load Balancer, made by the controller, on two ports');
  const plain = render(edgeDir);
  const svc = plain.docs?.find((d) => d.kind === 'Service');
  check(plain.docs?.length === 1 && svc?.metadata?.name === 'edge', `CONTROL: infra/edge renders one Service, edge${plain.error ? `: ${plain.error.slice(-300)}` : ''}`);
  if (!svc) return;
  const a = svc.metadata.annotations ?? {};
  const P = 'service.beta.kubernetes.io/aws-load-balancer-';
  check(svc.spec.type === 'LoadBalancer' && svc.spec.loadBalancerClass === 'service.k8s.aws/nlb', `type LoadBalancer of class ${svc.spec.loadBalancerClass}: the AWS Load Balancer Controller's Network Load Balancer, never EKS's own Classic one`);
  const providers = readFileSync(join(root, 'infra', 'terraform', 'envs', 'demo', 'providers.tf'), 'utf8');
  const tfTags = ['Project', 'Environment'].map((k) => `${k}=${new RegExp(`${k}\\s*=\\s*"([^"]+)"`).exec(providers)?.[1]}`).join(',');
  const want = {
    [`${P}scheme`]: 'internet-facing',
    [`${P}nlb-target-type`]: 'ip',
    [`${P}target-group-attributes`]: 'preserve_client_ip.enabled=true',
    [`${P}attributes`]: 'load_balancing.cross_zone.enabled=true',
    [`${P}additional-resource-tags`]: tfTags,
  };
  check(JSON.stringify(a, Object.keys(a).sort()) === JSON.stringify(want, Object.keys(want).sort()), `internet-facing, IP targets, the client's own address kept, across zones, and tagged as Terraform tags everything (${tfTags}): exactly these annotations, so no TLS without a certificate`);
  const ports = Object.fromEntries((svc.spec.ports ?? []).map((p) => [p.name, p]));
  check(Object.keys(ports).sort().join() === 'gateway,web' && ports.web.port === 80 && ports.gateway.port === pub.GATEWAY_PORT && ports.web.targetPort === 'http' && ports.gateway.targetPort === 'proxy', `two ports: web ${ports.web?.port} -> ${ports.web?.targetPort}, gateway ${ports.gateway?.port} -> ${ports.gateway?.targetPort}`);
  const tls = render(edgeDir, ['--set', `tls.certificateArn=${ARN}`]).docs?.find((d) => d.kind === 'Service');
  const ta = tls?.metadata?.annotations ?? {};
  const tp = Object.fromEntries((tls?.spec?.ports ?? []).map((p) => [p.name, p.port]));
  check(ta[`${P}ssl-cert`] === ARN && ta[`${P}ssl-ports`] === 'web,gateway' && /^ELBSecurityPolicy-TLS13-1-2-/.test(ta[`${P}ssl-negotiation-policy`] ?? '') && tp.web === 443 && tp.gateway === pub.GATEWAY_PORT, `with a certificate, TLS on both listeners, 1.2 and up (${ta[`${P}ssl-negotiation-policy`] ?? 'none'}): the page on ${tp.web}, the gateway on ${tp.gateway}`);
  const bad = render(edgeDir, ['--set', 'tls.certificateArn=arn:aws:iam::123456789012:server-certificate/x']);
  check(Boolean(bad.error) && /ACM certificate/.test(bad.error), 'and nothing but an ACM certificate is taken');

  // -------------------------------------------------------------------------
  section('what it reaches: the web app and Kong, each on one port, and nothing else');
  const label = Object.entries(svc.spec.selector ?? {});
  check(label.length === 1 && label[0][1] === 'true', `CONTROL: it selects one label, ${label.map(([k, v]) => `${k}=${v}`).join(', ')}`);
  const [key] = label[0] ?? [];
  const pods = [];
  const add = (who, docs) => {
    for (const d of docs ?? []) {
      const spec = d.spec?.template?.spec;
      if (spec) pods.push({ who, kind: d.kind, labels: d.spec.template.metadata?.labels ?? {}, containers: spec.containers ?? [] });
    }
  };
  const charts = eks.allCharts(root);
  for (const chart of charts) {
    const dep = sh('helm', ['dependency', 'build', join(root, 'charts', chart)]);
    if (dep.status !== 0) check(false, `charts/${chart}: helm dependency build failed: ${dep.stderr.trim().slice(-200)}`);
    add(chart, render(join(root, 'charts', chart), ['--set', 'image.tag=abc1234']).docs);
  }
  add('kong', render(join(root, 'infra', 'kong')).docs);
  add('web', render(join(root, 'infra', 'web'), ['--set', 'image.repository=r.example/eventhub/web', '--set', 'image.tag=abc1234']).docs);
  add('stores', render(join(root, 'infra', 'stores')).docs);
  add('setup', render(join(root, 'infra', 'setup'), ['--set', 'image.repository=r.example/eventhub/app', '--set', 'image.tag=abc1234', '--set-json', 'command=["true"]']).docs);
  const whoRendered = new Set(pods.map((p) => p.who));
  check(['kong', 'web', 'stores', 'setup'].every((w) => whoRendered.has(w)) && charts.every((c) => whoRendered.has(c)), `CONTROL: ${pods.length} pod templates from ${whoRendered.size} charts: the ${charts.length} services', the gateway's, the web app's, the stores' and setup's`);
  const selected = pods.filter((p) => p.labels[key] === 'true');
  check(selected.map((p) => p.who).sort().join() === 'kong,web' && selected.every((p) => p.kind === 'Deployment'), `exactly two pod templates carry ${key}: ${selected.map((p) => `${p.who} (${p.kind})`).join(', ') || 'none'}`);
  const named = (p, name) => p.containers.flatMap((c) => c.ports ?? []).filter((port) => port.name === name);
  for (const [port, owner] of [['web', 'web'], ['gateway', 'kong']]) {
    const target = ports[port]?.targetPort;
    const having = selected.filter((p) => named(p, target).length > 0);
    check(having.length === 1 && having[0].who === owner && named(having[0], target).length === 1, `${port} -> ${target}: a port only ${having.map((p) => p.who).join(' and ') || 'nobody'} has${having.length === 1 && having[0].who === owner ? '' : `, not only ${owner}`}`);
  }
  const compose = JSON.parse(sh('docker', ['compose', '-f', join(root, 'docker-compose.yml'), '-p', 'eventhub-rebuild', 'config', '--format', 'json']).stdout || '{}');
  const kongListen = Number(/:(\d+)/.exec(compose.services?.kong?.environment?.KONG_PROXY_LISTEN ?? '')?.[1]);
  const kongPort = named(selected.find((p) => p.who === 'kong') ?? { containers: [] }, 'proxy')[0]?.containerPort;
  check(Number.isFinite(kongListen) && kongPort === kongListen, `Kong's proxy port is ${kongPort}, where compose's KONG_PROXY_LISTEN has it listen (${kongListen})`);
  const dockerfile = readFileSync(join(root, 'apps', 'web', 'Dockerfile'), 'utf8');
  const webListen = Number(/^ENV\s.*\bPORT=(\d+)/m.exec(dockerfile)?.[1]);
  const webPort = named(selected.find((p) => p.who === 'web') ?? { containers: [] }, 'http')[0]?.containerPort;
  check(Number.isFinite(webListen) && webPort === webListen, `the web app's http port is ${webPort}, the PORT its image sets (${webListen})`);
  // Not stripped: the `//` of the template literal's protocol would read as a comment.
  const webEnv = readFileSync(join(root, 'apps', 'web', 'src', 'api', 'env.ts'), 'utf8');
  const apiPort = Number(/\$\{window\.location\.hostname\}:(\d+)/.exec(webEnv)?.[1]);
  check(apiPort === pub.GATEWAY_PORT && ports.gateway.port === apiPort, `the gateway's port is the one the web app calls the API on, at its own host (${apiPort})`);

  // -------------------------------------------------------------------------
  section('the controller: the release Terraform granted a policy for, as the account it bound');
  const c = eks.LB_CONTROLLER;
  const mainTf = readFileSync(join(root, 'infra', 'terraform', 'envs', 'demo', 'main.tf'), 'utf8');
  const policyTag = /iam_policy\.json from kubernetes-sigs\/aws-load-balancer-controller at tag\s+v(\d+\.\d+\.\d+)/.exec(mainTf.replace(/\s*#\s*/g, ' '))?.[1];
  check(Boolean(policyTag) && c.version === policyTag, `chart ${c.version} is the release whose IAM policy envs/demo copied (v${policyTag ?? '?'}): its app version is the chart's, at AWS's chart repo ${c.repo}`);
  const role = /module "aws_load_balancer_controller_role" \{([\s\S]*?)\n\}/.exec(mainTf)?.[1] ?? '';
  const ns = /namespace\s*=\s*"([^"]+)"/.exec(role)?.[1];
  const sa = /service_account\s*=\s*"([^"]+)"/.exec(role)?.[1];
  check(ns === c.namespace && sa === c.serviceAccount, `it runs in ${c.namespace} as ${c.serviceAccount}, the service account Terraform's Pod Identity association names (${ns}/${sa})`);
  check(c.repo === 'https://aws.github.io/eks-charts', 'from AWS\'s own chart repository');

  // -------------------------------------------------------------------------
  section('the order: the way in before the Secrets, and the balancer gone before its controller');
  const src = strip(readFileSync(join(root, 'infra', 'eks', 'up.mjs'), 'utf8'));
  const body = (name) => {
    const at = src.search(new RegExp(`(async )?function ${name}\\(`));
    if (at < 0) return '';
    const next = src.slice(at + 1).search(/\n(async )?function |\nexport /);
    return next < 0 ? src.slice(at) : src.slice(at, at + 1 + next);
  };
  for (const fn of ['main', 'gitops']) {
    const b = body(fn);
    const way = b.indexOf('theWayIn(');
    const creds = b.indexOf('credentialsAndStores(');
    check(way > 0 && creds > way && /credentialsAndStores\(\{[^}]*\btemplate\b[^}]*\}\)/.test(b), `${fn}(): the way in, then the Secrets, made with its address`);
  }
  check(/installSecrets\([\s\S]*?\bexisting, template\)/.test(body('credentialsAndStores')),'and credentialsAndStores hands that address to installSecrets');
  const down = body('down');
  const edgeOff = down.search(/'uninstall',\s*'edge'/);
  const checked = down.indexOf('what.length === 0 &&');
  const ctlOff = down.search(/'uninstall',\s*LB_CONTROLLER\.release/);
  const restOff = down.search(/for \(const name of \['web', 'kong'/);
  check(edgeOff > 0 && checked > edgeOff && ctlOff > checked && restOff > ctlOff, '--down: the load balancer first, then AWS asked until nothing it made is left, and only then the controller, before everything else');

  // -------------------------------------------------------------------------
  section('the address, with no domain: the balancer\'s own, through sslip.io');
  check(pub.DOMAIN_TEMPLATE === null, `CONTROL: no domain is set (DOMAIN_TEMPLATE ${JSON.stringify(pub.DOMAIN_TEMPLATE)})`);
  const s1 = pub.addressFor(['54.66.2.10', '13.54.1.2']);
  const s2 = pub.addressFor(['13.54.1.2', '54.66.2.10']);
  check(s1 === 'http://{tenant}.13-54-1-2.sslip.io' && s1 === s2, `the balancer's first address in numeric order, however DNS lists them: ${s1}`);
  check(pub.addressFor([]) === pub.LAPTOP_TEMPLATE, `with no balancer, the laptop's (${pub.addressFor([])})`);
  let refused = false;
  try {
    pub.addressFor(['13.54.1.2', 'fe80::1']);
  } catch {
    refused = true;
  }
  check(refused, 'anything but IPv4 addresses is refused, not written into a host');
  let authTakes = true;
  try {
    appUrl.parseAppUrlTemplate(s1);
    pub.publicAddress(s1);
  } catch {
    authTakes = false;
  }
  check(authTakes, 'and Auth and the installer both take it as an address');

  const good = {
    origin: 'http://acme.13-54-1-2.sslip.io',
    port: '80',
    page: { status: 200, body: '<html><head><title>EventHub</title></head></html>' },
    config: { status: 200, body: 'window.__EVENTHUB__ = {"domain":"13-54-1-2.sslip.io"};' },
    me: { status: 401, headers: { 'x-kong-upstream-latency': '3' } },
  };
  const allOk = eks.judgeOutside(good, s1).every(([ok]) => ok);
  check(allOk, 'CONTROL: the check from outside passes a working way in');
  const breaks = {
    'the page not served': { ...good, page: { error: 'ECONNREFUSED' } },
    'the domain not told': { ...good, config: { status: 200, body: 'window.__EVENTHUB__ = {"domain":null};' } },
    'Kong answering for itself, not Auth': { ...good, me: { status: 401, headers: {} } },
    'the gateway port closed': { ...good, me: { error: 'ETIMEDOUT' } },
  };
  for (const [what, answers] of Object.entries(breaks)) {
    const failed = eks.judgeOutside(answers, s1).filter(([ok]) => !ok).length;
    check(failed === 1, `and fails exactly one check for ${what} (${failed})`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('the one load balancer reaches the web app and Kong, each on its port, and is made and taken off in order');
}

main().catch((e) => check(false, `the verifier itself failed: ${e.stack || e.message}`));
