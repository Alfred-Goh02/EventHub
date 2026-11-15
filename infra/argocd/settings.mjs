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

/**
 * The read:packages token the EKS cluster pulls main's images with, beside the deploy key and
 * for the same reason: a file the owner writes once, which the installer reads and never
 * prints, so whoever runs the installer never has to hold the value. GHCR_TOKEN, when set,
 * is used instead.
 */
export const GHCR_TOKEN_FILE = join(homedir(), '.eventhub', 'ghcr-token');
