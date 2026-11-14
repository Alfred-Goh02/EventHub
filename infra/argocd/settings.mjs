/**
 * What both clusters' Argo CD installs share: the version, pinned, and where the read-only
 * deploy key lives. infra/argocd/bootstrap.mjs (kind) and infra/eks/up.mjs --gitops (EKS)
 * import these, so the two never run different Argo CDs or look for the key in two places.
 */

import { homedir } from 'node:os';
import { join } from 'node:path';

// Pinned: an install manifest from a branch changes under you. Release notes at
// https://github.com/argoproj/argo-cd/releases.
export const ARGOCD_VERSION = 'v3.6.1';
export const ARGOCD_MANIFEST = `https://raw.githubusercontent.com/argoproj/argo-cd/${ARGOCD_VERSION}/manifests/install.yaml`;

/** The read-only deploy key's private half: outside the repository, always. */
export const DEPLOY_KEY = join(homedir(), '.eventhub', 'argocd-deploy-key');
