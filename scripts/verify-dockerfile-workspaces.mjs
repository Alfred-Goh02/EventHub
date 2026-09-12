/**
 * Asserts the Dockerfile copies a package.json for every workspace package.
 *
 * THE BUG THIS EXISTS FOR, which cost a red build minutes after it was written.
 * The image installs dependencies before copying source, so a source-only change
 * does not re-run the install — and that means every workspace package's
 * manifest has to be copied FIRST, individually:
 *
 *     COPY --chown=node:node services/auth/package.json ./services/auth/
 *     COPY --chown=node:node services/event/package.json ./services/event/
 *     ...
 *
 * A list like that is a maintenance trap, and adding services/live sprang it.
 * pnpm install ran without Live's manifest, so Live's node_modules were never
 * created; `COPY . .` then brought its source in, and the build failed at the
 * far end with a message that names the symptom rather than the cause:
 *
 *     WARN  Local package.json exists, but node_modules missing,
 *           did you mean to install?
 *
 * Nothing in the repository connected that to a missing line in the Dockerfile.
 * The Dockerfile even says "this is a list rather than one COPY" — it explains
 * why the list exists, and could not notice when the list fell behind.
 *
 * WHY NOT ONE GLOBBED COPY AND BE DONE. Docker's COPY does not preserve
 * directory structure from a wildcard the way you would expect, and the
 * workarounds either flatten the paths or copy the whole tree, which defeats the
 * layer caching the list exists to get. The list is the right shape; it just
 * needed something watching it.
 *
 * (Writing that wildcard out literally here would have ended this comment early
 * — a star followed by a slash closes a block comment. It did, once.)
 *
 * Run: node scripts/verify-dockerfile-workspaces.mjs
 */

import { readFileSync, readdirSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/**
 * The workspace globs, from the file that defines them rather than a copy.
 *
 * Only the two shapes this repository uses — `dir/*` and a bare `dir` — because
 * supporting arbitrary globs would mean pulling in a matcher to read four lines.
 * An unrecognised pattern is reported rather than skipped, so a new shape fails
 * loudly instead of silently shrinking what this checks.
 */
function workspacePackages() {
  const yaml = readFileSync(join(ROOT, 'pnpm-workspace.yaml'), 'utf8');
  const patterns = [...yaml.matchAll(/^\s*-\s*["']?([^"'\s#]+)["']?\s*$/gm)].map((m) => m[1]);

  const dirs = [];
  const unknown = [];

  for (const pattern of patterns) {
    if (pattern.endsWith('/*')) {
      const parent = join(ROOT, pattern.slice(0, -2));
      if (!existsSync(parent)) continue;
      for (const name of readdirSync(parent)) {
        const dir = join(parent, name);
        if (statSync(dir).isDirectory() && existsSync(join(dir, 'package.json'))) {
          dirs.push(`${pattern.slice(0, -2)}/${name}`);
        }
      }
    } else if (!pattern.includes('*')) {
      if (existsSync(join(ROOT, pattern, 'package.json'))) dirs.push(pattern);
    } else {
      unknown.push(pattern);
    }
  }

  return { dirs: dirs.sort(), patterns, unknown };
}

function main() {
  const dockerfile = readFileSync(join(ROOT, 'Dockerfile'), 'utf8');
  const { dirs, patterns, unknown } = workspacePackages();

  // -------------------------------------------------------------------------
  console.log('  the parse has to have found something');
  // -------------------------------------------------------------------------
  /**
   * Every assertion below is "the workspace has X, so the Dockerfile must copy
   * X". If the workspace list comes back empty, all of them are vacuously true
   * and this file reports success having compared nothing.
   */
  check(patterns.length > 0, `pnpm-workspace.yaml declares ${patterns.length} pattern(s)`);
  check(dirs.length > 0, `which expand to ${dirs.length} package(s): ${dirs.join(', ')}`);
  check(
    unknown.length === 0,
    unknown.length === 0
      ? 'and every pattern is a shape this check understands'
      : `unrecognised workspace pattern(s): ${unknown.join(', ')} — this check silently ` +
        'covers less than the workspace contains until it learns that shape',
  );

  if (patterns.length === 0 || dirs.length === 0 || unknown.length > 0) {
    console.error('\nFAIL: cannot check the Dockerfile against a workspace it could not read');
    process.exitCode = 1;
    return;
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the Dockerfile copies a manifest for each of them');
  // -------------------------------------------------------------------------
  const copied = new Set(
    [...dockerfile.matchAll(/^COPY\s+(?:--\S+\s+)*([\w./-]+)\/package\.json\s/gm)].map((m) => m[1]),
  );

  check(
    copied.size > 0,
    copied.size > 0
      ? `the Dockerfile copies ${copied.size} workspace manifest(s)`
      : 'the Dockerfile copies no per-package manifests at all — either the install strategy ' +
        'changed or this regex no longer matches it, and either way nothing below is meaningful',
  );

  if (copied.size === 0) {
    console.error('\nFAIL: nothing to compare against');
    process.exitCode = 1;
    return;
  }

  const missing = dirs.filter((d) => !copied.has(d));
  check(
    missing.length === 0,
    missing.length === 0
      ? 'every workspace package has its package.json copied before the install'
      : `${missing.join(', ')} ${missing.length === 1 ? 'is' : 'are'} in the workspace and ` +
        'not copied into the image. pnpm install will not create node_modules for ' +
        `${missing.length === 1 ? 'it' : 'them'}, and the build fails much later with ` +
        '"Local package.json exists, but node_modules missing".',
  );

  // The other direction: a manifest copied for a package that no longer exists
  // is a COPY that fails the build outright, but say which one.
  const stale = [...copied].filter((d) => !dirs.includes(d));
  check(
    stale.length === 0,
    stale.length === 0
      ? 'and nothing is copied for a package the workspace no longer has'
      : `the Dockerfile copies ${stale.join(', ')}, which ${stale.length === 1 ? 'is' : 'are'} ` +
        'not in the workspace',
  );

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the image installs dependencies for every package the workspace declares');
}

main();
