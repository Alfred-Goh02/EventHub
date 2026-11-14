/**
 * infra/argocd/env/eks-setup.json: compose's setup command, for the setup Application on EKS.
 *
 * WHY A COPY. Argo CD renders infra/setup from git, and compose's setup command lives in
 * docker-compose.yml, which Helm cannot read. So the command is written into a values file
 * Argo CD can pass, and scripts/verify-argocd-eks.mjs fails the moment it differs from what
 * `docker compose config` resolves. This writes it; the verifier checks it.
 *
 *   node infra/argocd/eks/setup-values.mjs           write it
 *   node infra/argocd/eks/setup-values.mjs --check   exit 1 when it differs from compose
 */

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', '..');
export const SETUP_VALUES = join(ROOT, 'infra', 'argocd', 'env', 'eks-setup.json');

/** The file's content for a resolved compose configuration. */
export function setupValues(compose) {
  const command = compose.services?.setup?.command;
  if (!Array.isArray(command) || command.length === 0) throw new Error('compose has no setup command');
  return `${JSON.stringify({ command }, null, 2)}\n`;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const compose = JSON.parse(execFileSync('docker', ['compose', '-p', 'eventhub-rebuild', 'config', '--format', 'json'], { cwd: ROOT, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }));
  const want = setupValues(compose);
  const have = existsSync(SETUP_VALUES) ? readFileSync(SETUP_VALUES, 'utf8').replace(/\r\n/g, '\n') : null;
  if (process.argv.includes('--check')) {
    console.log(have === want ? 'eks-setup.json is compose\'s setup command' : 'eks-setup.json differs from compose\'s setup command: run node infra/argocd/eks/setup-values.mjs');
    process.exit(have === want ? 0 : 1);
  }
  writeFileSync(SETUP_VALUES, want);
  // Read back: a write that did not land would otherwise pass for one that did.
  const back = readFileSync(SETUP_VALUES, 'utf8').replace(/\r\n/g, '\n');
  if (back !== want) {
    console.error('FAIL: eks-setup.json did not read back as written');
    process.exit(1);
  }
  console.log(`wrote ${SETUP_VALUES}: ${JSON.parse(want).command.length} argument(s)`);
}
