/**
 * Asserts that every compose host the kind cluster's services name leads, now, to the
 * compose container it names -- asked from a pod, the way the services ask it.
 *
 * THE BRIDGE WENT STALE WITH NOTHING TO SAY SO. It was a Service and an EndpointSlice per
 * host, holding the container's address on kind's network as infra/local/up.mjs read it.
 * After a Docker Desktop restart on 2026-11-14 every store was back at a different
 * address, one of the old ones now a kind worker's, and four services crash-looped against
 * addresses that led nowhere or somewhere else while ArgoCD showed every app Synced. Each
 * host is now an ExternalName Service naming its container, which Docker's DNS resolves to
 * wherever the container is (infra/local/bridge.mjs). This asks whether that holds:
 *
 *   - each store is on the kind network -- compose recreating one drops it, and then
 *     no design reaches it;
 *   - each host's Service is an ExternalName for that container;
 *   - no EndpointSlice still holds an address for a host, and one that does is named,
 *     with STALE when the address is not the container's;
 *   - from a pod, each host resolves to the container's address, first, and each port
 *     the services name answers there.
 *
 * Which hosts: the ones up.mjs bridges, derived the same way, from compose's resolved
 * environment for every chart installed in the cluster. The probe runs in the image the
 * installed services run.
 *
 * CONTROLS. At least one chart is installed and one host derived, or nothing is checked; a
 * pod answered for every host; and from a pod a container name that does not exist gets
 * ENOTFOUND, so a resolver that answers anything cannot pass the lookups.
 *
 * Needs the compose stack and the kind cluster up (node infra/local/up.mjs, or
 * infra/argocd/bootstrap.mjs). Not in CI, which has no cluster.
 *   node scripts/verify-kind-bridge.mjs
 */

import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { allCharts, bridgesFor, composeConfig, inspectBridges } from '../infra/local/bridge.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const CONTEXT = 'kind-eventhub';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

function main() {
  const deployments = JSON.parse(execFileSync('kubectl', ['--context', CONTEXT, 'get', 'deployments', '-o', 'json'], { encoding: 'utf8' })).items;
  const deployed = new Map(deployments.map((d) => [d.metadata.name, d]));
  const charts = allCharts(ROOT).filter((c) => deployed.has(c));
  check(charts.length > 0, `CONTROL: ${charts.length} chart(s) installed in ${CONTEXT}${charts.length ? `: ${charts.join(', ')}` : ''}`);
  if (!charts.length) return;

  const bridges = bridgesFor(charts, composeConfig(ROOT).services);
  check(bridges.size > 0, `CONTROL: their environments name ${bridges.size} compose host(s): ${[...bridges.keys()].join(', ')}`);
  if (!bridges.size) return;

  const image = deployed.get(charts[0]).spec.template.spec.containers[0].image;
  console.log(`\n  from a pod running ${image}`);
  for (const { ok, msg } of inspectBridges({ context: CONTEXT, image, bridges })) check(ok, msg);
}

main();
console.log(failures ? `\nFAIL: ${failures} check(s) failed` : '\nevery bridge leads to the container it names');
