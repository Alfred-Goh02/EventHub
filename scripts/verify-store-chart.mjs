/**
 * Asserts that infra/stores runs on EKS the data stores compose runs, as compose runs them,
 * on disks that EBS can give them -- and differs from compose only where it says why.
 *
 * COMPOSE IS THE ONE DESCRIPTION. The chart is written by hand (Helm cannot read compose),
 * so each fact is checked against compose's own resolved configuration
 * (`docker compose config`), never against a list kept here:
 *   - the stores are exactly the hosts the apps' environment names (infra/lib's hostsOf,
 *     the function infra/local/up.mjs bridges with), each a ClusterIP Service of that
 *     name on every port the apps dial, and no Service anywhere is a LoadBalancer or
 *     NodePort;
 *   - compose pins each store image by digest, and the chart's image is exactly compose's
 *     reference, so compose, CI and EKS all run the same bits;
 *   - each environment variable is compose's, value for value. A password is read from a
 *     Secret, never written. The deliberate differences -- an override or an addition --
 *     are declared below with the reason, and a declaration that no longer differs from
 *     compose is a failure too, so the list cannot rot;
 *   - compose's command is the container's args, its container ports are the pod's, and its
 *     stop_grace_period is the pod's terminationGracePeriodSeconds;
 *   - each named volume compose mounts is a claim the StatefulSet mounts at the same path,
 *     on the chart's StorageClass, deleted with the StatefulSet; a store with none has no
 *     claim. A data directory a fresh ext4 disk would break (lost+found) is a subdirectory
 *     of the mount; an image that runs as a user other than root gets that uid as fsGroup;
 *   - each directory compose bind-mounts becomes a Secret mounted at the same path, and
 *     infra/stores/secrets.mjs can make every Secret the chart names, from compose;
 *   - the StorageClass is gp3 on the EBS CSI driver, encrypted, WaitForFirstConsumer,
 *     reclaim Delete; every store pod has the stores' PriorityClass, no service links, no
 *     API token, readiness and liveness probes, requests and a memory limit, and a JVM
 *     heap below that limit.
 *
 * Needs helm and docker on PATH (CI has both, and runs this after starting the stores, so
 * their images are there to ask which user each runs as).
 *   node scripts/verify-store-chart.mjs          against this tree
 *   node scripts/verify-store-chart.mjs [root]   against another: <root>/infra/stores and
 *                                                <root>/docker-compose.yml -- how a planted
 *                                                break is shown red in a copy
 */

import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join, posix, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { hostsOf } from '../infra/lib/compose-hosts.mjs';
import { plan, renderChart } from '../infra/stores/secrets.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const note = (msg) => console.log(`    note  ${msg}`);
const section = (title) => {
  console.log('');
  console.log(`  ${title}`);
};

/**
 * THE DELIBERATE DIFFERENCES FROM COMPOSE, each with its reason. Hand-written because no
 * file says which differences are wanted; checked against what they describe: an override
 * must name a variable compose sets and must differ from it, an addition must name one
 * compose does not set, and every entry must be what the chart actually renders.
 */
const OVERRIDES = {
  opensearch: {
    'bootstrap.memory_lock': { value: 'false', why: 'Kubernetes has no per-pod memlock ulimit and EKS nodes have no swap: the lock would fail and only warn' },
  },
  kafka: {
    KAFKA_CONTROLLER_QUORUM_VOTERS: { value: '1@localhost:29093', why: 'through the kafka Service the broker would wait for itself: a Service routes only to ready pods' },
  },
};
const ADDITIONS = {
  postgres: { PGDATA: 'a subdirectory of the disk: initdb refuses a directory holding lost+found' },
  kafka: {
    KAFKA_LOG_DIRS: 'a subdirectory of the disk, for lost+found',
    KAFKA_HEAP_OPTS: "the image default, written down so it cannot move under the pod's limit",
  },
  'schema-registry': { SCHEMA_REGISTRY_HEAP_OPTS: "the image default, written down so it cannot move under the pod's limit" },
};
/** A data directory these images would initialise at the mount root, where lost+found is. */
const DATA_DIR_UNDER_MOUNT = { postgres: 'PGDATA', kafka: 'KAFKA_LOG_DIRS' };

const SECRETISH = /PASSWORD|SECRET|TOKEN/i;

/** compose's "30s", "1m30s", or a number of seconds. */
function seconds(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === 'number') return value;
  const m = /^(?:(\d+)m)?(?:(\d+)s)?$/.exec(String(value));
  return m ? Number(m[1] ?? 0) * 60 + Number(m[2] ?? 0) : Number.NaN;
}

/** "512Mi", "2Gi", "1G", "512m" (JVM) as bytes. */
function bytes(value) {
  const m = /^(\d+(?:\.\d+)?)\s*([KMGT]i?|[kmgt])?$/.exec(String(value).trim());
  if (!m) return Number.NaN;
  const unit = (m[2] ?? '').toUpperCase().replace('I', '');
  const pow = { '': 0, K: 1, M: 2, G: 3, T: 4 }[unit];
  return Number(m[1]) * 1024 ** pow;
}

function docker(args) {
  const r = spawnSync('docker', args, { encoding: 'utf8' });
  return r.error ? { status: -1, stdout: '', stderr: String(r.error.message) } : { status: r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
}

function main() {
  const rootArg = process.argv.slice(2).find((a) => a !== '--');
  const root = rootArg ? resolve(rootArg) : ROOT;
  const chartDir = join(root, 'infra', 'stores');
  const composeFile = join(root, 'docker-compose.yml');
  console.log(`  reading ${rootArg ? root : 'this tree'}: infra/stores against docker-compose.yml`);
  if (!existsSync(chartDir) || !existsSync(composeFile)) {
    check(false, `${chartDir} and ${composeFile} must both exist`);
    return;
  }

  const composeRun = spawnSync('docker', ['compose', '-f', composeFile, '-p', 'eventhub-rebuild', 'config', '--format', 'json'], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  if (composeRun.status !== 0) {
    check(false, `docker compose config could not read ${composeFile}: ${(composeRun.stderr || '').trim().slice(-400)}`);
    return;
  }
  const compose = JSON.parse(composeRun.stdout);
  const services = compose.services;

  let docs;
  try {
    docs = renderChart(chartDir);
  } catch (e) {
    check(false, `helm template ${chartDir} did not render: ${String(e.stderr || e.message).trim().slice(-600)}`);
    return;
  }

  section('the chart renders');
  const workloads = docs.filter((d) => d.kind === 'StatefulSet' || d.kind === 'Deployment');
  const svcs = docs.filter((d) => d.kind === 'Service');
  check(workloads.length > 0 && svcs.length > 0, `CONTROL: ${docs.length} object(s), ${workloads.length} store workload(s), ${svcs.length} Service(s)`);

  // ---------------------------------------------------------------------------
  section('the stores are exactly the hosts the apps dial, each a ClusterIP Service on every port they dial');
  const names = new Set(Object.keys(services));
  const apps = Object.entries(services).filter(([, s]) => String(s.image ?? '').startsWith('eventhub/app'));
  const pairs = new Map();
  for (const [, s] of apps) {
    for (const { host, port } of hostsOf(s.environment ?? {}, names)) {
      if (apps.some(([n]) => n === host)) continue;
      if (!pairs.has(host)) pairs.set(host, new Set());
      pairs.get(host).add(port);
    }
  }
  check(apps.length > 0 && pairs.size > 0, `CONTROL: ${apps.length} app service(s) in compose name ${pairs.size} store host(s): ${[...pairs].map(([h, p]) => `${h}:${[...p].join(',')}`).join(' ')}`);
  const hosts = [...pairs.keys()].sort();
  const stores = workloads.map((w) => w.metadata.name).sort();
  check(
    JSON.stringify(hosts) === JSON.stringify(stores),
    `the chart runs exactly those stores (chart: ${stores.join(', ')}; compose hosts: ${hosts.join(', ')})`,
  );
  for (const [host, ports] of [...pairs].sort()) {
    const svc = svcs.find((s) => s.metadata.name === host);
    const served = new Set((svc?.spec?.ports ?? []).map((p) => p.port));
    const missing = [...ports].filter((p) => !served.has(p));
    check(
      svc !== undefined && (svc.spec.type ?? 'ClusterIP') === 'ClusterIP' && !svc.spec.clusterIP && missing.length === 0,
      `Service ${host}: ClusterIP on ${[...ports].join(', ')}${svc ? '' : ' -- no such Service'}${missing.length ? ` -- not served: ${missing.join(', ')}` : ''}${svc && (svc.spec.type ?? 'ClusterIP') !== 'ClusterIP' ? ` -- type ${svc.spec.type}` : ''}`,
    );
  }
  const exposed = svcs.filter((s) => ['LoadBalancer', 'NodePort'].includes(s.spec.type));
  check(exposed.length === 0, `no Service is a LoadBalancer or NodePort: with the AWS Load Balancer Controller one would be an internet-facing NLB${exposed.length ? ` -- ${exposed.map((s) => `${s.metadata.name} (${s.spec.type})`).join(', ')}` : ''}`);

  // ---------------------------------------------------------------------------
  section("each store is compose's: image, environment, command, ports and stop grace period");
  const usedOverride = new Set();
  const usedAddition = new Set();
  for (const w of workloads) {
    const name = w.metadata.name;
    const svc = services[name];
    const pod = w.spec.template.spec;
    const c = pod.containers[0];
    if (!svc) {
      check(false, `${name}: compose has no service of that name`);
      continue;
    }
    check(pod.containers.length === 1, `${name}: one container (${pod.containers.length})`);

    // THE SAME REFERENCE, DIGEST AND ALL. The first version pinned the chart and compared
    // its digest with whatever docker had pulled for compose's tag. CI refused it: compose
    // said postgres:16, a tag that moves, and the runner, pulling months after this
    // machine, had different bits under the same name. So compose pins too, and the chart
    // must be exactly compose's reference: one string, one image, wherever it is pulled.
    const digestPinned = (ref) => /@sha256:[0-9a-f]{64}$/.test(String(ref ?? ''));
    check(digestPinned(svc.image), `${name}: compose pins its image by digest, so every machine that pulls it gets the same bits (${svc.image})`);
    check(c.image === svc.image, `${name}: the chart's image is exactly compose's${c.image === svc.image ? '' : ` -- chart ${c.image}, compose ${svc.image}`}`);

    // The environment, variable by variable.
    const env = new Map((c.env ?? []).map((e) => [e.name, e]));
    const composeEnv = svc.environment ?? {};
    for (const [key, raw] of Object.entries(composeEnv)) {
      const want = raw === null ? '' : String(raw);
      const got = env.get(key);
      const override = OVERRIDES[name]?.[key];
      if (SECRETISH.test(key)) {
        const ref = got?.valueFrom?.secretKeyRef;
        check(ref !== undefined && got.value === undefined, `${name}: ${key} is read from a Secret, never written into the chart${ref ? ` (${ref.name}/${ref.key})` : got ? ' -- it is a literal' : ' -- it is missing'}`);
      } else if (override) {
        usedOverride.add(`${name}.${key}`);
        check(got?.value === override.value && override.value !== want, `${name}: ${key} is ${got?.value ?? 'missing'}, the declared override of compose's ${want}: ${override.why}`);
      } else {
        check(got?.value === want, `${name}: ${key} = ${want}, as compose has it${got?.value === want ? '' : ` -- the chart has ${got?.value ?? (got ? 'a reference' : 'nothing')}`}`);
      }
    }
    for (const key of env.keys()) {
      if (key in composeEnv) continue;
      const addition = ADDITIONS[name]?.[key];
      if (addition) usedAddition.add(`${name}.${key}`);
      check(addition !== undefined, `${name}: ${key} is not in compose${addition ? `, a declared addition: ${addition}` : ', and is not declared: say why here, or set it in compose'}`);
    }

    // Command, ports, grace.
    const composeCmd = Array.isArray(svc.command) ? svc.command : svc.command ? String(svc.command).split(/\s+/) : null;
    if (composeCmd) check(JSON.stringify(c.args ?? []) === JSON.stringify(composeCmd), `${name}: args are compose's command (${JSON.stringify(composeCmd)}; chart ${JSON.stringify(c.args ?? [])})`);
    else check(!c.args && !c.command, `${name}: no command or args, as compose sets none`);
    const composePorts = (svc.ports ?? []).map((p) => Number(p.target));
    const podPorts = new Set((c.ports ?? []).map((p) => p.containerPort));
    const notOpen = composePorts.filter((p) => !podPorts.has(p));
    check(notOpen.length === 0, `${name}: the pod declares every port compose publishes (${composePorts.join(', ') || 'none'})${notOpen.length ? ` -- missing ${notOpen.join(', ')}` : ''}`);
    const grace = seconds(svc.stop_grace_period);
    check(grace !== undefined && pod.terminationGracePeriodSeconds === grace, `${name}: terminationGracePeriodSeconds ${pod.terminationGracePeriodSeconds} is compose's stop_grace_period (${svc.stop_grace_period ?? 'unset'})`);
  }
  const staleOverrides = Object.entries(OVERRIDES).flatMap(([s, o]) => Object.keys(o).map((k) => `${s}.${k}`)).filter((k) => !usedOverride.has(k));
  const staleAdditions = Object.entries(ADDITIONS).flatMap(([s, o]) => Object.keys(o).map((k) => `${s}.${k}`)).filter((k) => !usedAddition.has(k));
  check(staleOverrides.length === 0 && staleAdditions.length === 0, `CONTROL: every declared difference from compose is one the chart renders${staleOverrides.length ? ` -- overrides never met: ${staleOverrides.join(', ')}` : ''}${staleAdditions.length ? ` -- additions never met: ${staleAdditions.join(', ')}` : ''}`);

  // ---------------------------------------------------------------------------
  section('each store pod: priority, no service links or API token, probes, resources, a heap inside its limit');
  const priority = docs.find((d) => d.kind === 'PriorityClass');
  check(priority !== undefined && priority.value > 0, `a PriorityClass above the app pods' 0 (${priority ? `${priority.metadata.name} ${priority.value}` : 'none rendered'})`);
  for (const w of workloads) {
    const name = w.metadata.name;
    const pod = w.spec.template.spec;
    const c = pod.containers[0];
    check(pod.priorityClassName === priority?.metadata?.name, `${name}: priorityClassName ${pod.priorityClassName ?? 'unset'}`);
    check(pod.enableServiceLinks === false, `${name}: enableServiceLinks false, so no KAFKA_PORT-style variables reach images that read their environment as configuration (${pod.enableServiceLinks})`);
    check(pod.automountServiceAccountToken === false, `${name}: no Kubernetes API token mounted (${pod.automountServiceAccountToken})`);
    check(Boolean(c.readinessProbe) && Boolean(c.livenessProbe), `${name}: readiness and liveness probes`);
    const req = c.resources?.requests ?? {};
    const lim = c.resources?.limits ?? {};
    check(Boolean(req.cpu) && Boolean(req.memory) && Boolean(lim.memory), `${name}: requests ${req.cpu}/${req.memory}, memory limit ${lim.memory}`);
    for (const e of c.env ?? []) {
      const xmx = /-Xmx(\d+[kmgKMG]?)/.exec(e.value ?? '')?.[1];
      if (!xmx) continue;
      check(bytes(xmx) < bytes(lim.memory), `${name}: ${e.name}'s heap (-Xmx${xmx}) is inside the memory limit (${lim.memory}), with room left for what a JVM holds off-heap`);
    }
  }

  // ---------------------------------------------------------------------------
  section('disks: every named volume compose mounts is a claim on gp3, gone with its StatefulSet');
  const sc = docs.find((d) => d.kind === 'StorageClass');
  check(
    sc !== undefined &&
      sc.provisioner === 'ebs.csi.aws.com' &&
      sc.parameters?.type === 'gp3' &&
      sc.parameters?.encrypted === 'true' &&
      sc.volumeBindingMode === 'WaitForFirstConsumer' &&
      sc.reclaimPolicy === 'Delete',
    `StorageClass ${sc?.metadata?.name ?? '(none)'}: ebs.csi.aws.com, gp3, encrypted, WaitForFirstConsumer, reclaim Delete (${sc ? `${sc.provisioner}, ${sc.parameters?.type}, encrypted=${sc.parameters?.encrypted}, ${sc.volumeBindingMode}, ${sc.reclaimPolicy}` : 'not rendered'})`,
  );
  let claims = 0;
  for (const w of workloads) {
    const name = w.metadata.name;
    const svc = services[name] ?? {};
    const c = w.spec.template.spec.containers[0];
    const named = (svc.volumes ?? []).filter((v) => v.type === 'volume');
    const templates = w.spec.volumeClaimTemplates ?? [];
    if (named.length === 0) {
      check(templates.length === 0, `${name}: compose mounts no named volume, and the chart claims no disk (${templates.length})`);
      continue;
    }
    for (const v of named) {
      const mount = (c.volumeMounts ?? []).find((m) => m.mountPath === v.target);
      const claim = templates.find((t) => t.metadata.name === mount?.name);
      claims += claim ? 1 : 0;
      check(
        w.kind === 'StatefulSet' && claim !== undefined && claim.spec.storageClassName === sc?.metadata?.name,
        `${name}: compose's ${v.source} at ${v.target} is a claim on ${sc?.metadata?.name} mounted there (${w.kind}, ${claim ? `claim ${claim.metadata.name} on ${claim.spec.storageClassName}` : 'no claim at that path'})`,
      );
    }
    const keep = w.spec.persistentVolumeClaimRetentionPolicy ?? {};
    check(keep.whenDeleted === 'Delete' && keep.whenScaled === 'Delete', `${name}: its claim goes with the StatefulSet, so no volume is left to bill (whenDeleted ${keep.whenDeleted ?? 'Retain'}, whenScaled ${keep.whenScaled ?? 'Retain'})`);

    const dirVar = DATA_DIR_UNDER_MOUNT[name];
    if (dirVar) {
      const dir = (c.env ?? []).find((e) => e.name === dirVar)?.value;
      const under = named.some((v) => dir && posix.normalize(dir).startsWith(`${posix.normalize(v.target)}/`));
      check(under, `${name}: ${dirVar} (${dir ?? 'unset'}) is a subdirectory of the disk, never its root, where a fresh ext4 volume keeps lost+found`);
    }

    const user = docker(['image', 'inspect', svc.image, '--format', '{{.Config.User}}']);
    if (user.status === 0) {
      const u = user.stdout.trim();
      if (u && u !== 'root' && u !== '0') {
        const id = docker(['run', '--rm', '--entrypoint', 'id', svc.image, '-u']);
        const uid = id.status === 0 ? Number(id.stdout.trim()) : Number(u);
        const fsGroup = w.spec.template.spec.securityContext?.fsGroup;
        check(fsGroup === uid, `${name}: the image runs as ${u} (uid ${uid}), and fsGroup ${fsGroup ?? 'unset'} gives it the disk, which EBS hands over owned by root`);
      }
    } else if (process.env.CI) {
      // In CI the stores were started before this step, so a missing image is the step
      // having moved, not a machine that never pulled it: refuse rather than skip quietly.
      check(false, `${name}: ${svc.image} is not on the runner, so who it runs as cannot be checked; this step must run after the stores start`);
    } else {
      note(`${name}: ${svc.image} is not on this machine, so who it runs as is not checked`);
    }
  }
  check(claims > 0, `CONTROL: ${claims} claim(s) matched to compose's named volumes`);

  // ---------------------------------------------------------------------------
  section("init files: each directory compose bind-mounts is a Secret at the same path, and secrets.mjs can make every Secret named");
  for (const w of workloads) {
    const name = w.metadata.name;
    const svc = services[name] ?? {};
    const pod = w.spec.template.spec;
    const c = pod.containers[0];
    for (const b of (svc.volumes ?? []).filter((v) => v.type === 'bind')) {
      const mount = (c.volumeMounts ?? []).find((m) => m.mountPath === b.target);
      const vol = (pod.volumes ?? []).find((v) => v.name === mount?.name);
      check(Boolean(vol?.secret) && mount.readOnly === true, `${name}: compose's bind mount at ${b.target} is a Secret mounted read-only there (${vol?.secret ? vol.secret.secretName : 'not a Secret'})`);
    }
  }
  try {
    const wanted = plan(docs, compose);
    const keys = Object.entries(wanted).map(([s, k]) => `${s}: ${Object.keys(k).join(', ')}`);
    check(keys.length > 0, `infra/stores/secrets.mjs makes every Secret the chart names, from compose: ${keys.join('; ')}`);
  } catch (e) {
    check(false, `infra/stores/secrets.mjs cannot make what the chart names: ${e.message}`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log(`infra/stores runs the ${stores.length} stores compose runs, as compose runs them, on disks EBS can give them`);
}

main();
