/**
 * Asserts that every commit on main publishes the images the cluster runs, to GitHub's
 * container registry, under the tag Argo CD will ask for -- and that nothing else can.
 *
 *   - .github/workflows/publish.yml runs on a push to main and on nothing else: never on a
 *     pull request, whose code is not yet reviewed and whose token must not write packages;
 *   - its token may read the repository and write packages, nothing more, and no stored
 *     secret is used: the run's own GITHUB_TOKEN logs in, through the environment;
 *   - one run per commit, never cancelled by the next: a cancelled run is a commit with no
 *     images, which Argo CD would then fail to deploy;
 *   - it runs scripts/publish-images.mjs for real, not as a dry run;
 *   - that script publishes exactly infra/eks/up.mjs's IMAGES, each from its own context, to
 *     ghcr.io/<owner, lower-cased>/<name>, tagged with the commit's first seven hex digits --
 *     $ARGOCD_APP_REVISION_SHORT -- and refuses anything that is not a full SHA.
 *
 *   node scripts/verify-image-publish.mjs          against this tree
 *   node scripts/verify-image-publish.mjs [root]   against another (a red run in a copy)
 */

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

/** A workflow's top-level block, by key, with its indented lines: comments dropped. */
function block(yml, key) {
  const lines = yml.split('\n');
  const start = lines.findIndex((l) => new RegExp(`^${key}:`).test(l));
  if (start === -1) return null;
  const body = [lines[start].replace(new RegExp(`^${key}:\\s*`), '')];
  for (const l of lines.slice(start + 1)) {
    if (/^\S/.test(l)) break;
    if (!/^\s*#/.test(l) && l.trim()) body.push(l.trim());
  }
  return body.filter(Boolean).join('\n');
}

/** The text of every `run:` in a workflow: a one-line value, or a block's indented lines. */
function runScripts(yml) {
  const lines = yml.split('\n');
  const out = [];
  lines.forEach((l, i) => {
    const m = /^(\s*)(?:-\s+)?run:\s*(.*)$/.exec(l);
    if (!m) return;
    if (!/^[|>][-+]?\s*$/.test(m[2])) {
      out.push(m[2]);
      return;
    }
    const indent = m[1].length;
    const body = [];
    for (const next of lines.slice(i + 1)) {
      if (next.trim() && next.search(/\S/) <= indent) break;
      body.push(next);
    }
    out.push(body.join('\n'));
  });
  return out;
}

async function main() {
  const rootArg = process.argv.slice(2).find((a) => a !== '--');
  const root = rootArg ? resolve(rootArg) : ROOT;
  console.log(`  reading ${rootArg ? root : 'this tree'}`);
  const file = join(root, '.github', 'workflows', 'publish.yml');
  check(existsSync(file), 'CONTROL: .github/workflows/publish.yml exists');
  if (!existsSync(file)) return;
  const yml = readFileSync(file, 'utf8').replace(/\r\n/g, '\n');

  section('when it runs, and with what it may do');
  const on = block(yml, 'on');
  check(on === 'push:\nbranches: [main]', `on a push to main, and nothing else (${JSON.stringify(on)})`);
  const perms = block(yml, 'permissions');
  check(perms === 'contents: read\npackages: write', `its token reads the repository and writes packages, nothing more (${JSON.stringify(perms)})`);
  const conc = block(yml, 'concurrency');
  check(/group: publish-\$\{\{ github\.sha \}\}/.test(conc ?? '') && /cancel-in-progress: false/.test(conc ?? ''), `one run per commit, never cancelled by the next (${JSON.stringify(conc)})`);
  const uses = [...yml.matchAll(/\$\{\{\s*secrets\.([A-Za-z0-9_]+)\s*\}\}/g)].map((m) => m[1]);
  check(uses.length > 0 && uses.every((s) => s === 'GITHUB_TOKEN'), `no stored secret: only the run's own GITHUB_TOKEN (${uses.join(', ') || 'none'})`);
  const runs = runScripts(yml);
  check(runs.length > 0 && !runs.some((s) => /\$\{\{/.test(s)), `no expression is pasted into any of the ${runs.length} run script(s): each value comes in through the environment`);
  check(/^\s*run: node scripts\/publish-images\.mjs\s*$/m.test(yml), 'it runs scripts/publish-images.mjs for real, not as a dry run');

  section('what it publishes, and under which tag');
  const pub = await import(pathToFileURL(join(root, 'scripts', 'publish-images.mjs')).href);
  const { IMAGES } = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const sha = '0123456789abcdef0123456789abcdef01234567';
  const images = await pub.plan({ root, sha, owner: 'Alfred-Goh02' });
  check(images.length > 0, `CONTROL: ${images.length} image(s) planned: ${images.map((i) => i.ref).join(', ')}`);
  check(JSON.stringify(images.map((i) => [i.name, i.context])) === JSON.stringify(Object.entries(IMAGES)), "exactly infra/eks/up.mjs's IMAGES, each from its own context, the list the installer builds");
  check(images.every((i) => i.ref === `ghcr.io/alfred-goh02/${i.name}:0123456`), 'each to ghcr.io/<owner, lower-cased>/<its name>, tagged with the first seven hex digits: $ARGOCD_APP_REVISION_SHORT');
  let refused = 0;
  for (const bad of ['0123456', 'HEAD', `${sha}0`, sha.toUpperCase()]) {
    try {
      pub.tagOf(bad);
    } catch {
      refused += 1;
    }
  }
  check(refused === 4, `a tag is only ever made from a full lower-case SHA (${refused} of 4 malformed refused)`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('every commit on main publishes the images the cluster runs, under the tag Argo CD asks for');
}

main().catch((e) => {
  check(false, `the verifier itself failed: ${e.stack || e.message}`);
});
