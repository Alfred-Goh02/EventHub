/**
 * Builds every image the cluster runs and pushes each to GitHub's container registry,
 * tagged with the commit it was built from. .github/workflows/publish.yml runs it for every
 * commit on main.
 *
 *   node scripts/publish-images.mjs --dry-run    what it would build and push, nothing done
 *   node scripts/publish-images.mjs              build and push (CI: logged in to ghcr.io)
 *
 * WHICH IMAGES: infra/eks/up.mjs's IMAGES, each from its own build context -- the list the
 * installer builds for ECR, never a second one. Each goes to ghcr.io/<owner>/<name>, the same
 * name it has in ECR.
 *
 * WHICH TAG: the commit's first seven hex digits. That is exactly what Argo CD calls
 * $ARGOCD_APP_REVISION_SHORT, so an Application that deploys a commit of main names, by
 * that variable, the image CI built from the same commit. `git rev-parse --short` is not
 * used: it lengthens the SHA when seven digits are ambiguous, and the two would disagree.
 *
 * WHERE FROM: GITHUB_SHA and GITHUB_REPOSITORY_OWNER in CI; locally, HEAD and the owner of
 * origin. The owner is lower-cased, as image names must be.
 */

import { execFileSync } from 'node:child_process';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

/** The commit's first seven hex digits: what Argo CD calls $ARGOCD_APP_REVISION_SHORT. */
export const tagOf = (sha) => {
  if (!/^[0-9a-f]{40}$/.test(sha)) throw new Error(`not a full commit SHA: ${sha}`);
  return sha.slice(0, 7);
};

/** Every image to publish: [{ name, context, ref }], from the installer's IMAGES. */
export async function plan({ root = ROOT, sha, owner }) {
  const { IMAGES } = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const entries = Object.entries(IMAGES ?? {});
  if (entries.length === 0) throw new Error('infra/eks/up.mjs names no IMAGES');
  const tag = tagOf(sha);
  const o = owner.toLowerCase();
  return entries.map(([name, context]) => ({ name, context, ref: `ghcr.io/${o}/${name}:${tag}` }));
}

async function main() {
  const dry = process.argv.slice(2).includes('--dry-run');
  const git = (...args) => execFileSync('git', args, { cwd: ROOT, encoding: 'utf8' }).trim();
  const sha = process.env.GITHUB_SHA || git('rev-parse', 'HEAD');
  const owner = process.env.GITHUB_REPOSITORY_OWNER || /github\.com[:/]([^/]+)\//.exec(git('remote', 'get-url', 'origin'))?.[1];
  if (!owner) throw new Error('no owner: set GITHUB_REPOSITORY_OWNER, or give origin a github.com URL');
  const repo = process.env.GITHUB_REPOSITORY || `${owner}/${/\/([^/]+?)(\.git)?$/.exec(git('remote', 'get-url', 'origin'))?.[1]}`;
  const images = await plan({ sha, owner });
  for (const { name, context, ref } of images) {
    // The source label links the package to this repository, so it inherits its access.
    const build = ['build', '--label', `org.opencontainers.image.source=https://github.com/${repo}`, '--label', `org.opencontainers.image.revision=${sha}`, '-t', ref, context];
    console.log(`  ${name}: docker ${build.join(' ')}`);
    console.log(`  ${name}: docker push ${ref}`);
    if (dry) continue;
    execFileSync('docker', build, { cwd: ROOT, stdio: 'inherit' });
    execFileSync('docker', ['push', ref], { cwd: ROOT, stdio: 'inherit' });
  }
  console.log(dry ? `would publish ${images.length} image(s) for ${sha}` : `published ${images.length} image(s) for ${sha}`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((e) => {
    console.error(`FAIL: ${e.message}`);
    process.exit(1);
  });
}
