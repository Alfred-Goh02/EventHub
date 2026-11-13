/**
 * Asserts that the Terraform under infra/terraform formats, validates, and keeps the
 * guards the AWS demo environment's cost and security rest on.
 *
 * NOTHING HERE TALKS TO AWS. CI has no AWS credentials and is not meant to: no plan, no
 * apply. So this is what can be known about the Terraform without an account, and each
 * guard below is one line that could be deleted with nothing else noticing until the
 * bill, or the breach, arrived.
 *
 * DERIVED, NOT LISTED. A ROOT is any directory under infra/terraform holding .tf files
 * that is not under modules/; a MODULE is one that is. Every root is initialised without
 * a backend (-backend=false: no bucket, no credentials) against its committed lock file,
 * and validated. Every module must be reached from some root, or no validate ever reads
 * it, and every module source must be a path in this tree: no registry modules.
 *
 * THE STATIC GUARDS read the .tf files with comments stripped (# // and block comments),
 * strings and heredocs kept whole: a comment that names a setting is not a setting, and
 * a "#" inside a string is not a comment.
 *   - every S3 backend sets use_lockfile = true, and names neither a bucket (it is given
 *     at init) nor a DynamoDB table; a root with no backend keeps local state, which
 *     only the root that creates the state bucket may;
 *   - every S3 bucket is versioned, encrypted by default, closed by all four
 *     public-access-block flags, and has a policy denying aws:SecureTransport false; the
 *     state bucket has prevent_destroy;
 *   - exactly one aws_nat_gateway, with no count or for_each on it, and no root reaching
 *     it more than once through module calls;
 *   - no access_key or secret_key attribute in any file, and no AWS access key id in
 *     any file, comments included: a key written in a comment has still leaked;
 *   - every EKS cluster with a public endpoint sets public_access_cidrs, and the
 *     variable it comes from has no 0.0.0.0/0 default at any level it is passed
 *     through, nor in the root's terraform.tfvars.example;
 *   - no IAM OIDC or SAML provider and no Federated trust: the AWS project's SCPs deny
 *     iam:*Provider* on both plans, so IRSA fails at apply. Every role a pod assumes
 *     trusts pods.eks.amazonaws.com with sts:TagSession, pins cluster, namespace and
 *     service account with StringEquals, and has its association; the agent addon runs;
 *   - every node group checks its instance types, in a lifecycle precondition, against
 *     a list its caller reads from AWS (describe-instance-types free-tier-eligible), so
 *     a type the account cannot launch refuses the plan instead of stalling the apply;
 *   - every ECR repository is IMMUTABLE;
 *   - every root's aws provider sets default_tags with Project, and no module configures
 *     a provider of its own (one would not carry the root's tags);
 *   - a monthly COST budget exists, alerting at 50, 80 and 100% actual and 100% forecast;
 *   - CI installs the Terraform version infra/TOOLVERSIONS.md records, through
 *     hashicorp/setup-terraform pinned to a full commit SHA.
 *
 * AND TWO CONTROLS ON EVERY RUN, so a green fmt or validate is a verdict and not a tool
 * that could not have failed: fmt -check must refuse a planted misformat, and validate
 * -json must call a planted reference to an undeclared variable invalid.
 *
 * THEN EACH ROOT'S TERRAFORM TEST, where it has tests/*.tftest.hcl: plans against a mocked
 * provider, so a guard is judged by what a plan does. Every run block must pass, counted
 * against the run blocks in the files; "0 passed" is not a pass.
 *
 * Needs terraform on PATH (CI installs it with hashicorp/setup-terraform); the first run
 * downloads the providers the lock files pin, into TF_PLUGIN_CACHE_DIR (by default
 * eventhub-terraform-plugin-cache in the temp directory), and later runs reuse them. A
 * root's .terraform that a run creates, the run removes: on Windows each is a full copy
 * of the providers, 1.6 GB for the two roots. One that was already there is left alone.
 *   node scripts/verify-terraform.mjs                    everything, against this tree
 *   node scripts/verify-terraform.mjs [root]             the same against another tree
 *   node scripts/verify-terraform.mjs --static [root]    the static guards only, no terraform
 * root is a repository root: <root>/infra/terraform is read, and <root>/infra/TOOLVERSIONS.md
 * and <root>/.github/workflows/ci.yml for the version guard. That is how a planted
 * mutation is shown red in a copy without committing it.
 */

import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

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
const tail = (s, n = 1500) => String(s ?? '').trim().slice(-n);

// ---------------------------------------------------------------------------
// Reading HCL
// ---------------------------------------------------------------------------

/**
 * HCL read two ways, each the same length as the source, so an index in one is an index
 * in the other and a line number is the file's:
 *   code  every comment (# // and block) blanked to spaces, newlines kept
 *   skel  code with the inside of every string and heredoc blanked too, so a brace, an
 *         "=" or a keyword inside one is not read as structure
 * A string's ${ } can hold strings of its own ("${a == "}" ? 1 : 2}"), so strings and
 * interpolations are a stack, not a flag.
 */
function lexHcl(src) {
  const n = src.length;
  const code = src.split('');
  const skel = src.split('');
  const blank = (arr, i) => {
    if (i < n && arr[i] !== '\n' && arr[i] !== '\r') arr[i] = ' ';
  };
  const stack = []; // 'string', or { depth } for an interpolation inside one
  let i = 0;
  while (i < n) {
    const c = src[i];
    const d = src[i + 1];
    const top = stack[stack.length - 1];
    const inside = stack.length > 0;
    if (top === 'string') {
      if (c === '\n') {
        // A quoted string cannot span lines: this one was never closed. Stop reading it as one.
        stack.length = 0;
        i += 1;
      } else if (c === '\\') {
        blank(skel, i);
        if (src[i + 1] !== '\n') blank(skel, i + 1);
        i += 2;
      } else if ((c === '$' || c === '%') && d === c && src[i + 2] === '{') {
        // $${ and %%{ are a literal ${ and %{, not an interpolation.
        blank(skel, i);
        blank(skel, i + 1);
        blank(skel, i + 2);
        i += 3;
      } else if ((c === '$' || c === '%') && d === '{') {
        blank(skel, i);
        blank(skel, i + 1);
        stack.push({ depth: 0 });
        i += 2;
      } else if (c === '"') {
        stack.pop();
        // A nested string's closing quote is still inside the outer string.
        if (stack.length > 0) blank(skel, i);
        i += 1;
      } else {
        blank(skel, i);
        i += 1;
      }
      continue;
    }
    // Code: at the top level, or inside an interpolation.
    if (c === '#' || (c === '/' && d === '/')) {
      while (i < n && src[i] !== '\n') {
        blank(code, i);
        blank(skel, i);
        i += 1;
      }
      continue;
    }
    if (c === '/' && d === '*') {
      const end = src.indexOf('*/', i + 2);
      const stop = end === -1 ? n : end + 2;
      for (; i < stop; i += 1) {
        blank(code, i);
        blank(skel, i);
      }
      continue;
    }
    if (c === '"') {
      if (inside) blank(skel, i);
      stack.push('string');
      i += 1;
      continue;
    }
    if (c === '<' && d === '<' && !inside) {
      const head = /^<<-?([A-Za-z_][\w-]*)[ \t]*\r?\n/.exec(src.slice(i, i + 256));
      if (head) {
        // The body runs to the first line that is the identifier alone, and it is content:
        // a "#" in a heredoc'd policy is not a comment. In skel the body and the closing
        // line are blanked newlines and all, so an attribute's value runs to its end.
        let j = i + head[0].length;
        let end = n;
        while (j < n) {
          const eol = src.indexOf('\n', j);
          const lineEnd = eol === -1 ? n : eol;
          if (src.slice(j, lineEnd).trim() === head[1]) {
            end = lineEnd;
            break;
          }
          j = lineEnd + 1;
        }
        for (let k = i + 2; k < end; k += 1) skel[k] = ' ';
        i = end;
        continue;
      }
    }
    if (top && typeof top === 'object') {
      blank(skel, i);
      if (c === '{') top.depth += 1;
      else if (c === '}') {
        if (top.depth === 0) stack.pop();
        else top.depth -= 1;
      }
    }
    i += 1;
  }
  return { code: code.join(''), skel: skel.join('') };
}

/**
 * The attributes and blocks of a body, at its own depth only. Read from skel, so strings
 * cannot be mistaken for structure; values and labels are taken from code at the same
 * indices. `at` is the offset in the file, for a line number.
 */
function parseBody(code, skel, base, file) {
  const items = [];
  const n = skel.length;
  let i = 0;
  const skipLine = () => {
    while (i < n && skel[i] !== '\n') i += 1;
  };
  while (i < n) {
    while (i < n && /\s/.test(skel[i])) i += 1;
    if (i >= n) break;
    const id = /^[A-Za-z_][\w-]*/.exec(skel.slice(i, i + 256));
    if (!id) {
      skipLine();
      continue;
    }
    const start = i;
    let j = i + id[0].length;
    while (j < n && (skel[j] === ' ' || skel[j] === '\t')) j += 1;
    if (skel[j] === '=' && skel[j + 1] !== '=') {
      let k = j + 1;
      let depth = 0;
      while (k < n) {
        const ch = skel[k];
        if (ch === '{' || ch === '[' || ch === '(') depth += 1;
        else if (ch === '}' || ch === ']' || ch === ')') depth -= 1;
        else if (ch === '\n' && depth <= 0) break;
        k += 1;
      }
      items.push({ kind: 'attr', name: id[0], value: code.slice(j + 1, k).trim(), at: base + start, file });
      i = k;
      continue;
    }
    const labels = [];
    while (j < n && skel[j] !== '{' && skel[j] !== '\n') {
      if (skel[j] === '"') {
        const close = skel.indexOf('"', j + 1);
        if (close === -1) break;
        labels.push(code.slice(j + 1, close));
        j = close + 1;
      } else if (/[A-Za-z_]/.test(skel[j])) {
        const word = /^[A-Za-z_][\w-]*/.exec(skel.slice(j, j + 256))[0];
        labels.push(word);
        j += word.length;
      } else if (skel[j] === ' ' || skel[j] === '\t') {
        j += 1;
      } else {
        break;
      }
    }
    if (skel[j] !== '{') {
      skipLine();
      continue;
    }
    let depth = 0;
    let k = j;
    for (; k < n; k += 1) {
      if (skel[k] === '{') depth += 1;
      else if (skel[k] === '}') {
        depth -= 1;
        if (depth === 0) break;
      }
    }
    items.push({
      kind: 'block',
      type: id[0],
      labels,
      body: parseBody(code.slice(j + 1, k), skel.slice(j + 1, k), base + j + 1, file),
      at: base + start,
      file,
    });
    i = k + 1;
  }
  return items;
}

const blocksOf = (items, type, ...labels) =>
  items.filter((it) => it.kind === 'block' && it.type === type && labels.every((l, x) => it.labels[x] === l));
const attrOf = (items, name) => items.find((it) => it.kind === 'attr' && it.name === name)?.value;
/** The contents of a quoted string literal, or null for anything else: a template is not a literal. */
const literal = (value) => {
  const m = /^"((?:[^"\\]|\\.)*)"$/.exec(value ?? '');
  return m && !/(^|[^$%])[$%]\{/.test(m[1]) ? m[1] : null;
};
const compact = (value) => String(value ?? '').replace(/\s+/g, '');
const lineOf = (file, at) => file.code.slice(0, at).split('\n').length;
const where = (it) => `${it.file.rel}:${lineOf(it.file, it.at)}`;
/** 0.0.0.0/0 or ::/0, as a string literal in an expression. */
const OPEN = /"\s*(0\.0\.0\.0\/0|::\/0)\s*"/;

/** The detector's own control: a comment is not a use, a string is not a comment. */
function lexerControl() {
  const sample = [
    'a = "x # not a comment // nor this" # gone-1',
    'b = "${var.c == "}" ? 1 : 2}" // gone-2',
    '/* gone-3',
    '   access_key = "x" */',
    'c = <<EOT',
    '# kept: a heredoc line',
    'EOT',
    'd = "$${literal} {" # gone-4',
    '',
  ].join('\n');
  const { code, skel } = lexHcl(sample);
  const items = parseBody(code, skel, 0, { rel: 'sample', code });
  const names = items.map((it) => `${it.kind}:${it.name ?? it.type}`).join(' ');
  const problems = [
    code.length !== sample.length || skel.length !== sample.length ? 'the views changed length' : null,
    /gone/.test(code) ? 'a comment survived' : null,
    !code.includes('"x # not a comment // nor this"') ? 'a string lost its "#" or "//"' : null,
    !code.includes('"${var.c == "}" ? 1 : 2}"') ? 'a string inside an interpolation was cut short' : null,
    code.includes('access_key') ? 'a block comment survived' : null,
    !code.includes('# kept: a heredoc line') ? 'a heredoc line was read as a comment' : null,
    names !== 'attr:a attr:b attr:c attr:d' ? `the parse found ${names}` : null,
    attrOf(items, 'd') !== '"$${literal} {"' ? `d read as ${attrOf(items, 'd')}` : null,
  ].filter(Boolean);
  check(
    problems.length === 0,
    `CONTROL: comments are stripped before any guard reads a file, and strings, interpolations and heredocs are kept whole${problems.length ? ` -- ${problems.join('; ')}` : ''}`,
  );
}

// ---------------------------------------------------------------------------
// The tree
// ---------------------------------------------------------------------------

/** Every directory under dir holding Terraform files, skipping .terraform and other dot directories. */
function terraformDirs(dir) {
  const out = [];
  const walk = (d) => {
    const entries = readdirSync(d, { withFileTypes: true });
    if (entries.some((e) => e.isFile() && (e.name.endsWith('.tf') || e.name.endsWith('.tf.json')))) out.push(d);
    for (const e of entries) if (e.isDirectory() && !e.name.startsWith('.')) walk(join(d, e.name));
  };
  walk(dir);
  return out.sort();
}

/** Every file under dir that is not in a dot directory: what the credential scan reads. */
function allFiles(dir) {
  const out = [];
  const walk = (d) => {
    for (const e of readdirSync(d, { withFileTypes: true })) {
      if (e.isDirectory()) {
        if (!e.name.startsWith('.')) walk(join(d, e.name));
      } else if (e.isFile()) {
        out.push(join(d, e.name));
      }
    }
  };
  walk(dir);
  return out.sort();
}

function loadUnits(tfDir) {
  return terraformDirs(tfDir).map((dir) => {
    const rel = relative(tfDir, dir).split(sep).join('/') || '.';
    const kind = rel === 'modules' || rel.startsWith('modules/') ? 'module' : 'root';
    const items = [];
    const jsonFiles = [];
    for (const name of readdirSync(dir).sort()) {
      if (name.endsWith('.tf.json')) jsonFiles.push(name);
      if (!name.endsWith('.tf')) continue;
      const { code, skel } = lexHcl(readFileSync(join(dir, name), 'utf8'));
      items.push(...parseBody(code, skel, 0, { rel: `${rel}/${name}`, code }));
    }
    return { rel, dir, kind, items, jsonFiles };
  });
}

/** A unit's module calls, each resolved to the unit its source names when that is a path in this tree. */
function moduleCalls(unit, units) {
  return blocksOf(unit.items, 'module').map((block) => {
    const source = literal(attrOf(block.body, 'source'));
    const local = source !== null && (source.startsWith('./') || source.startsWith('../'));
    const target = local ? resolve(unit.dir, source) : null;
    return {
      name: block.labels[0],
      block,
      source,
      local,
      unit: target ? units.find((u) => resolve(u.dir) === target) ?? null : null,
      multi: attrOf(block.body, 'count') !== undefined || attrOf(block.body, 'for_each') !== undefined,
    };
  });
}

/** Whether unit reaches target through module calls. */
function reaches(unit, target, units, seen = new Set()) {
  if (unit === target) return true;
  if (seen.has(unit)) return false;
  seen.add(unit);
  return moduleCalls(unit, units).some((c) => c.unit && reaches(c.unit, target, units, seen));
}

/** How many instances of target unit creates: Infinity when a call on the way has count or for_each. */
function instancesOf(unit, target, units, path = []) {
  if (unit === target) return 1;
  if (path.includes(unit)) return 0;
  let total = 0;
  for (const c of moduleCalls(unit, units)) {
    if (!c.unit) continue;
    const n = instancesOf(c.unit, target, units, [...path, unit]);
    if (n > 0) total += c.multi ? Infinity : n;
  }
  return total;
}

const backendsOf = (unit) => blocksOf(unit.items, 'terraform').flatMap((t) => blocksOf(t.body, 'backend'));

/** Provider sources a unit requires, as registry addresses: hashicorp/aws. */
function requiredProviders(unit) {
  const out = new Set();
  for (const t of blocksOf(unit.items, 'terraform')) {
    for (const rp of blocksOf(t.body, 'required_providers')) {
      for (const it of rp.body) {
        if (it.kind !== 'attr') continue;
        const src = /\bsource\s*=\s*"([^"]+)"/.exec(it.value)?.[1];
        if (src) out.add(src.replace(/^registry\.terraform\.io\//, ''));
      }
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// The static guards
// ---------------------------------------------------------------------------

function discoveryChecks(tfDir, units, roots, modules) {
  section('the tree: every root and module, derived from the directory');
  check(
    roots.length > 0 && modules.length > 0,
    `CONTROL: found ${roots.length} root(s) (${roots.map((u) => u.rel).join(', ') || 'none'}) and ${modules.length} module(s) (${modules.map((u) => u.rel).join(', ') || 'none'}) under infra/terraform`,
  );
  const json = units.flatMap((u) => u.jsonFiles.map((f) => `${u.rel}/${f}`));
  check(json.length === 0, `no .tf.json files: these guards read HCL, and configuration they cannot read is configuration they cannot judge${json.length ? ` -- ${json.join(', ')}` : ''}`);

  const calls = units.flatMap((u) => moduleCalls(u, units).map((c) => ({ u, c })));
  const foreign = calls.filter(({ c }) => !c.local || !c.unit);
  check(
    foreign.length === 0,
    `every module source is a path to a module in this tree, not the registry (${calls.length} call(s))${foreign.length ? ` -- ${foreign.map(({ u, c }) => `${u.rel}: module "${c.name}" source = ${c.source ?? '(none)'}`).join('; ')}` : ''}`,
  );
  for (const m of modules) {
    const by = roots.filter((r) => reaches(r, m, units)).map((r) => r.rel);
    check(by.length > 0, by.length ? `${m.rel} is reached from ${by.join(', ')}, so validate reads it` : `${m.rel} is called by no root, so no terraform validate ever reads it`);
  }
  for (const r of roots) {
    const lockFile = join(r.dir, '.terraform.lock.hcl');
    const locked = existsSync(lockFile)
      ? new Set([...readFileSync(lockFile, 'utf8').matchAll(/^provider\s+"registry\.terraform\.io\/([\w-]+\/[\w-]+)"/gm)].map((m) => m[1]))
      : null;
    const needed = new Set([r, ...units.filter((u) => u !== r && reaches(r, u, units))].flatMap((u) => [...requiredProviders(u)]));
    const missing = [...needed].filter((p) => !locked?.has(p));
    check(
      locked !== null && needed.size > 0 && missing.length === 0,
      locked === null
        ? `${r.rel} has no .terraform.lock.hcl: commit the one terraform init writes`
        : `${r.rel}/.terraform.lock.hcl pins every provider the root and its modules require (${[...needed].join(', ') || 'none found'})${missing.length ? ` -- missing ${missing.join(', ')}` : ''}`,
    );
  }
}

function backendChecks(units, roots) {
  section('every state backend is S3, locked by S3 itself, with the bucket given at init');
  let s3 = 0;
  for (const r of roots) {
    const backends = backendsOf(r);
    if (backends.length === 0) {
      const buckets = blocksOf(r.items, 'resource', 'aws_s3_bucket').map((b) => b.labels[1]);
      check(
        buckets.length > 0,
        buckets.length
          ? `${r.rel}: no backend, so local state, which only the root that creates the state bucket may keep (it creates aws_s3_bucket ${buckets.join(', ')})`
          : `${r.rel}: no backend block, so its state would be a local file on whoever applied it. Add backend "s3" with use_lockfile = true, as envs/demo has`,
      );
      continue;
    }
    for (const b of backends) {
      if (b.labels[0] !== 's3') {
        check(false, `${r.rel}: backend "${b.labels[0]}" (${where(b)}): state lives in S3, in the bootstrap bucket`);
        continue;
      }
      s3 += 1;
      const lock = attrOf(b.body, 'use_lockfile');
      check(lock === 'true', `${r.rel}: backend "s3" sets use_lockfile = true, so S3 itself holds the state lock (${lock === undefined ? 'not set' : `use_lockfile = ${lock}`}, ${where(b)})`);
      const table = attrOf(b.body, 'dynamodb_table');
      check(table === undefined, `${r.rel}: and no dynamodb_table, the deprecated lock use_lockfile replaces${table ? ` (dynamodb_table = ${table})` : ''}`);
      const bucket = attrOf(b.body, 'bucket');
      check(bucket === undefined, `${r.rel}: and no bucket: it carries the account id, and is given at init with -backend-config${bucket ? ` (bucket = ${bucket})` : ''}`);
    }
  }
  check(s3 > 0, `CONTROL: ${s3} S3 backend(s) judged`);

  // ONE REGION, WRITTEN IN SEVERAL PLACES. The state bucket is made in bootstrap's region,
  // each S3 backend names that region to find it, and each root's provider runs in its own
  // `region` variable. Nothing ties the copies together: a backend left on the old region
  // fails `init` on the first real apply, which nothing without an account can see. Moving
  // from ap-southeast-1 to ap-southeast-2 touched three of them.
  const regions = [];
  for (const r of roots) {
    const v = blocksOf(r.items, 'variable', 'region')[0];
    if (v) regions.push({ at: `${r.rel} variable "region" default`, value: literal(attrOf(v.body, 'default')) });
    for (const b of backendsOf(r).filter((x) => x.labels[0] === 's3')) {
      regions.push({ at: `${r.rel} backend "s3" region`, value: literal(attrOf(b.body, 'region')) });
    }
  }
  const distinct = [...new Set(regions.map((x) => x.value))];
  check(
    regions.length >= roots.length + s3 && distinct.length === 1 && Boolean(distinct[0]),
    distinct.length === 1 && distinct[0]
      ? `every root's region and every backend's region is ${distinct[0]} (${regions.length} place(s))`
      : `the region is not the same everywhere: ${regions.map((x) => `${x.at} = ${x.value ?? 'not a literal'}`).join('; ')}`,
  );
}

function bucketChecks(units) {
  section('every S3 bucket is versioned, encrypted, private and TLS-only, and the state bucket cannot be destroyed');
  let found = 0;
  for (const u of units) {
    const resources = (type) => blocksOf(u.items, 'resource', type);
    for (const b of resources('aws_s3_bucket')) {
      found += 1;
      const name = b.labels[1];
      const id = `${u.rel}: aws_s3_bucket.${name}`;
      const ref = new RegExp(String.raw`^aws_s3_bucket\.${name}\.(id|bucket)$`);
      const of = (type) => resources(type).filter((r) => ref.test(attrOf(r.body, 'bucket') ?? ''));

      const versioning = of('aws_s3_bucket_versioning');
      const status = versioning.flatMap((v) => blocksOf(v.body, 'versioning_configuration')).map((c) => literal(attrOf(c.body, 'status')));
      check(status.includes('Enabled'), `${id} is versioned: aws_s3_bucket_versioning with status "Enabled" (${status.length ? status.join(', ') : 'no versioning resource for it'})`);

      const algorithms = of('aws_s3_bucket_server_side_encryption_configuration')
        .flatMap((s) => blocksOf(s.body, 'rule'))
        .flatMap((r) => blocksOf(r.body, 'apply_server_side_encryption_by_default'))
        .map((a) => literal(attrOf(a.body, 'sse_algorithm')));
      check(
        algorithms.length > 0 && algorithms.every((a) => ['AES256', 'aws:kms', 'aws:kms:dsse'].includes(a)),
        `${id} is encrypted by default (${algorithms.length ? algorithms.join(', ') : 'no encryption configuration for it'})`,
      );

      const blocks = of('aws_s3_bucket_public_access_block');
      const flags = ['block_public_acls', 'block_public_policy', 'ignore_public_acls', 'restrict_public_buckets'];
      const notTrue = flags.filter((f) => !blocks.some((p) => attrOf(p.body, f) === 'true'));
      check(
        blocks.length > 0 && notTrue.length === 0,
        `${id} has all four public-access-block flags true${blocks.length === 0 ? ' -- no aws_s3_bucket_public_access_block for it' : notTrue.length ? ` -- not true: ${notTrue.join(', ')}` : ''}`,
      );

      const policies = of('aws_s3_bucket_policy');
      const documents = policies
        .map((p) => /^data\.aws_iam_policy_document\.([\w-]+)\.json$/.exec(attrOf(p.body, 'policy') ?? '')?.[1])
        .filter(Boolean)
        .flatMap((doc) => blocksOf(u.items, 'data', 'aws_iam_policy_document', doc));
      const denies = documents.some((doc) =>
        blocksOf(doc.body, 'statement').some(
          (s) =>
            literal(attrOf(s.body, 'effect')) === 'Deny' &&
            blocksOf(s.body, 'condition').some(
              (c) =>
                literal(attrOf(c.body, 'test')) === 'Bool' &&
                literal(attrOf(c.body, 'variable')) === 'aws:SecureTransport' &&
                compact(attrOf(c.body, 'values')) === '["false"]',
            ),
        ),
      );
      check(
        denies,
        `${id} refuses plain HTTP: its bucket policy's aws_iam_policy_document has a Deny on aws:SecureTransport "false" (${policies.length} bucket polic(ies), ${documents.length} document(s) read)`,
      );

      if (u.kind === 'root' && backendsOf(u).length === 0) {
        const kept = blocksOf(b.body, 'lifecycle').some((l) => attrOf(l.body, 'prevent_destroy') === 'true');
        check(kept, `${id} is the state bucket (its root keeps local state), and sets lifecycle prevent_destroy = true`);
      }
    }
  }
  check(found > 0, `CONTROL: ${found} S3 bucket(s) found to judge`);
}

function natChecks(units, roots) {
  section('exactly one NAT gateway');
  const nats = units.flatMap((u) => blocksOf(u.items, 'resource', 'aws_nat_gateway').map((r) => ({ u, r })));
  check(nats.length === 1, `exactly one aws_nat_gateway in the tree (${nats.length}${nats.length ? `: ${nats.map(({ r }) => `${r.labels[1]} at ${where(r)}`).join(', ')}` : ''})`);
  for (const { u, r } of nats) {
    const multi = ['count', 'for_each'].filter((m) => attrOf(r.body, m) !== undefined);
    check(
      multi.length === 0,
      `${u.rel}: aws_nat_gateway.${r.labels[1]} sets neither count nor for_each${multi.length ? ` -- ${multi.map((m) => `${m} = ${attrOf(r.body, m)}`).join(', ')}: one block, many gateways` : ''}`,
    );
  }
  for (const target of [...new Set(nats.map(({ u }) => u))]) {
    const per = roots.map((r) => ({ r, n: instancesOf(r, target, units) })).filter(({ n }) => n > 0);
    check(per.length > 0, `CONTROL: ${target.rel}'s NAT gateway is reached from ${per.map(({ r }) => r.rel).join(', ') || 'no root'}`);
    for (const { r, n } of per) {
      check(n === 1, `${r.rel} creates ${target.rel}'s NAT gateway once (${n === Infinity ? 'a module call on the way has count or for_each' : `${n} time(s)`})`);
    }
  }
}

function credentialChecks(tfDir) {
  section('no static AWS credentials, in any file');
  const files = allFiles(tfDir);
  const hcl = files.filter((f) => /\.(tf|tfvars|tfvars\.example)$/.test(f));
  const attrs = [];
  for (const f of hcl) {
    const { code } = lexHcl(readFileSync(f, 'utf8'));
    for (const m of code.matchAll(/\b(access_key|secret_key)\s*=/g)) {
      attrs.push(`${relative(tfDir, f).split(sep).join('/')}:${code.slice(0, m.index).split('\n').length} (${m[1]})`);
    }
  }
  for (const f of files.filter((x) => x.endsWith('.json'))) {
    const text = readFileSync(f, 'utf8');
    for (const m of text.matchAll(/"(access_key|secret_key)"\s*:/g)) attrs.push(`${relative(tfDir, f).split(sep).join('/')} (${m[1]})`);
  }
  check(hcl.length > 0, `CONTROL: ${files.length} file(s) read, ${hcl.length} of them HCL`);
  check(attrs.length === 0, `no access_key or secret_key attribute: credentials come from the environment (AWS_PROFILE), never from a file${attrs.length ? ` -- ${attrs.join('; ')}` : ''}`);
  // The raw text, comments included: a key written in a comment has leaked all the same.
  const keys = [];
  for (const f of files) {
    const text = readFileSync(f, 'utf8');
    if (text.includes('\0')) continue;
    for (const m of text.matchAll(/\b(AKIA|ASIA)[A-Z0-9]{16}\b/g)) keys.push(`${relative(tfDir, f).split(sep).join('/')}:${text.slice(0, m.index).split('\n').length}`);
  }
  check(keys.length === 0, `no AWS access key id in any file, comments included${keys.length ? ` -- ${keys.join('; ')}` : ''}`);
}

/**
 * Follows the variable a cluster's public CIDRs come from, through every module call that
 * passes it, and the example file a person copies at the top.
 */
function traceOpenDefault(unit, name, units, seen = new Set()) {
  if (seen.has(`${unit.rel}:${name}`)) return;
  seen.add(`${unit.rel}:${name}`);
  const decl = blocksOf(unit.items, 'variable', name)[0];
  check(decl !== undefined, `${unit.rel}: variable "${name}" is declared`);
  if (!decl) return;
  const def = attrOf(decl.body, 'default');
  check(
    def === undefined || !OPEN.test(def),
    `${unit.rel}: variable "${name}" ${def === undefined ? 'has no default, so leaving it out is an error, not an open endpoint' : OPEN.test(def) ? `defaults to ${def}, the whole internet (${where(decl)})` : `defaults to ${def}`}`,
  );
  if (unit.kind === 'root') {
    const example = join(unit.dir, 'terraform.tfvars.example');
    if (existsSync(example)) {
      const { code, skel } = lexHcl(readFileSync(example, 'utf8'));
      const given = attrOf(parseBody(code, skel, 0, { rel: `${unit.rel}/terraform.tfvars.example`, code }), name);
      check(given === undefined || !OPEN.test(given), `${unit.rel}/terraform.tfvars.example gives ${name} ${given === undefined ? 'no value' : `${given}`}${given && OPEN.test(given) ? ', the whole internet, to everyone who copies it' : ''}`);
    }
  }
  for (const caller of units) {
    for (const c of moduleCalls(caller, units).filter((x) => x.unit === unit)) {
      const arg = attrOf(c.block.body, name);
      if (arg === undefined) continue;
      const upstream = /^var\.([\w-]+)$/.exec(arg)?.[1];
      if (upstream) traceOpenDefault(caller, upstream, units, seen);
      else check(!OPEN.test(arg), `${caller.rel}: module "${c.name}" passes ${name} = ${arg}`);
    }
  }
}

function eksChecks(units) {
  section('the EKS public endpoint is restricted, and open to the internet by no default');
  const clusters = units.flatMap((u) => blocksOf(u.items, 'resource', 'aws_eks_cluster').map((r) => ({ u, r })));
  check(clusters.length > 0, `CONTROL: ${clusters.length} aws_eks_cluster(s) found to judge`);
  for (const { u, r } of clusters) {
    const id = `${u.rel}: aws_eks_cluster.${r.labels[1]}`;
    const vpc = blocksOf(r.body, 'vpc_config')[0];
    const pub = vpc ? attrOf(vpc.body, 'endpoint_public_access') : undefined;
    if (pub === 'false') {
      note(`${id} has no public endpoint, so nothing to restrict`);
      continue;
    }
    const cidrs = vpc ? attrOf(vpc.body, 'public_access_cidrs') : undefined;
    check(cidrs !== undefined, `${id} has a public endpoint and sets public_access_cidrs${cidrs === undefined ? ': without it, EKS opens the endpoint to 0.0.0.0/0' : ` = ${cidrs}`}`);
    if (cidrs === undefined) continue;
    const variable = /^var\.([\w-]+)$/.exec(cidrs)?.[1];
    if (variable) traceOpenDefault(u, variable, units);
    else check(!OPEN.test(cidrs), `${id}: public_access_cidrs = ${cidrs}`);
  }
}

/** The string literals in an expression: ["a", "b"] gives a, b. */
const strings = (value) => [...String(value ?? '').matchAll(/"((?:[^"\\]|\\.)*)"/g)].map((m) => m[1]);

/**
 * THE FIRST APPLY MET BOTH OF THESE, and neither can be seen without an account. The
 * project's SCPs deny iam:*Provider* on the free plan and the paid one alike, so the IRSA
 * design failed on iam:CreateOpenIDConnectProvider with the cluster already up. Pod
 * Identity needs no provider; what keeps it as tight as IRSA was is the trust policy,
 * pinned to one service account in one cluster through the session tags it sets.
 */
function podIdentityChecks(units) {
  section('pods get AWS roles through Pod Identity, pinned to one service account in one cluster');
  const providers = units.flatMap((u) =>
    ['aws_iam_openid_connect_provider', 'aws_iam_saml_provider'].flatMap((t) => blocksOf(u.items, 'resource', t).map((r) => `${t}.${r.labels[1]} at ${where(r)}`)),
  );
  check(providers.length === 0, `no IAM identity provider: the AWS project's SCPs deny iam:*Provider* on both plans, so one fails at apply, after the cluster is up${providers.length ? ` -- ${providers.join('; ')}` : ''}`);

  const documents = units.flatMap((u) => blocksOf(u.items, 'data', 'aws_iam_policy_document').map((d) => ({ u, d })));
  const statements = documents.flatMap(({ u, d }) => blocksOf(d.body, 'statement').map((s) => ({ u, d, s })));
  const federated = statements.filter(({ s }) => blocksOf(s.body, 'principals').some((p) => literal(attrOf(p.body, 'type')) === 'Federated'));
  check(
    federated.length === 0,
    `no trust statement names a Federated principal, the shape of an IRSA role${federated.length ? ` -- ${federated.map(({ u, d, s }) => `${u.rel}: aws_iam_policy_document.${d.labels[1]} at ${where(s)}`).join('; ')}` : ''}`,
  );

  const podTrust = statements.filter(({ s }) =>
    blocksOf(s.body, 'principals').some((p) => literal(attrOf(p.body, 'type')) === 'Service' && strings(attrOf(p.body, 'identifiers')).includes('pods.eks.amazonaws.com')),
  );
  check(podTrust.length > 0, `CONTROL: ${podTrust.length} trust statement(s) for pods.eks.amazonaws.com found to judge`);
  const PINNED = ['aws:RequestTag/eks-cluster-arn', 'aws:RequestTag/kubernetes-namespace', 'aws:RequestTag/kubernetes-service-account'];
  for (const { u, d, s } of podTrust) {
    const id = `${u.rel}: aws_iam_policy_document.${d.labels[1]}`;
    const actions = strings(attrOf(s.body, 'actions'));
    const lacking = ['sts:AssumeRole', 'sts:TagSession'].filter((a) => !actions.includes(a));
    check(
      literal(attrOf(s.body, 'effect')) !== 'Deny' && lacking.length === 0,
      `${id} allows sts:AssumeRole and sts:TagSession: Pod Identity tags every session, and a role without TagSession refuses it (${actions.join(', ') || 'no actions'})`,
    );
    const conditions = blocksOf(s.body, 'condition').map((c) => ({ test: literal(attrOf(c.body, 'test')), variable: literal(attrOf(c.body, 'variable')), values: attrOf(c.body, 'values') }));
    // ONE VALUE EACH. A second value is a second namespace, service account or cluster the
    // role is handed to, and StringEquals on a list matches any of it. Found in review: a
    // trust policy widened to admit another account's cluster stayed green while this
    // checked only that each key had some values.
    const single = (values) => /^\[[^,[\]*?]+\]$/.test(compact(values));
    const unpinned = PINNED.filter((key) => !conditions.some((c) => c.test === 'StringEquals' && c.variable === key && single(c.values)));
    const loose = conditions.filter((c) => PINNED.includes(c.variable) && (c.test !== 'StringEquals' || !single(c.values)));
    check(
      unpinned.length === 0 && loose.length === 0,
      `${id} pins the cluster, the namespace and the service account, each with StringEquals to exactly one value, so another association to the role gets nothing${unpinned.length ? ` -- not pinned: ${unpinned.join(', ')}` : ''}${loose.length ? ` -- not one exact value: ${loose.map((c) => `${c.test} ${c.variable} = ${compact(c.values)}`).join(', ')}` : ''}`,
    );
    // A role trusted for Pod Identity is bound by an association; without one it is bound to nothing.
    const roles = blocksOf(u.items, 'resource', 'aws_iam_role').filter((r) => attrOf(r.body, 'assume_role_policy') === `data.aws_iam_policy_document.${d.labels[1]}.json`);
    for (const r of roles) {
      const bound = blocksOf(u.items, 'resource', 'aws_eks_pod_identity_association').filter((a) => attrOf(a.body, 'role_arn') === `aws_iam_role.${r.labels[1]}.arn`);
      check(bound.length > 0, `${u.rel}: aws_iam_role.${r.labels[1]} has the aws_eks_pod_identity_association that binds it (${bound.length})`);
    }
    check(roles.length > 0, `CONTROL: ${id} is the trust policy of ${roles.length} role(s)`);
  }

  // The associations do nothing on a node without the agent that answers the pods.
  const associations = units.flatMap((u) => blocksOf(u.items, 'resource', 'aws_eks_pod_identity_association'));
  const addons = units.flatMap((u) =>
    blocksOf(u.items, 'resource', 'aws_eks_addon').flatMap((a) => {
      const name = attrOf(a.body, 'addon_name');
      return name === 'each.key' || name === 'each.value' ? strings(attrOf(a.body, 'for_each')) : [literal(name)];
    }),
  );
  check(
    associations.length === 0 || addons.includes('eks-pod-identity-agent'),
    `${associations.length} Pod Identity association(s), and the eks-pod-identity-agent addon among the cluster's addons (${addons.filter(Boolean).join(', ') || 'none'})`,
  );
}

/**
 * The first apply asked for t3.medium on a free-plan account. EKS said CREATING for
 * eighteen minutes, until the destroy, while every RunInstances failed "not eligible for
 * Free Tier", and a dry run had said yes. The precondition moves that failure to the plan,
 * and the list it compares against has to come from AWS, or it is a copy that goes stale.
 *
 * These read the files, so they cannot tell a precondition that refuses from one that never
 * can: `!= null`, `>= 0`, free_plan defaulting false, `false ?` in the caller and count = 0
 * on the data source all stayed green here. What the plan does is judged by the root's
 * terraform test (testRoot), which these leave to it.
 */
function nodeTypeChecks(units) {
  section("every node group's instance types are checked at plan time against what the account can launch");
  const groups = units.flatMap((u) => blocksOf(u.items, 'resource', 'aws_eks_node_group').map((r) => ({ u, r })));
  check(groups.length > 0, `CONTROL: ${groups.length} aws_eks_node_group(s) found to judge`);
  for (const { u, r } of groups) {
    const id = `${u.rel}: aws_eks_node_group.${r.labels[1]}`;
    const types = attrOf(r.body, 'instance_types');
    const conditions = blocksOf(r.body, 'lifecycle')
      .flatMap((l) => blocksOf(l.body, 'precondition'))
      .map((p) => attrOf(p.body, 'condition') ?? '');
    const guarding = conditions.filter((c) => c.includes('var.allowed_instance_types') && types !== undefined && c.includes(types));
    check(
      guarding.length > 0,
      `${id} has a lifecycle precondition comparing instance_types (${types ?? 'not set'}) with var.allowed_instance_types${conditions.length ? '' : ' -- no precondition at all'}`,
    );
    for (const caller of units) {
      for (const c of moduleCalls(caller, units).filter((x) => x.unit === u)) {
        const arg = attrOf(c.block.body, 'allowed_instance_types');
        const source = /\bdata\.aws_ec2_instance_types\.([\w-]+)\b/.exec(arg ?? '')?.[1];
        const data = source ? blocksOf(caller.items, 'data', 'aws_ec2_instance_types', source)[0] : undefined;
        const asks = data
          ? blocksOf(data.body, 'filter').some((f) => literal(attrOf(f.body, 'name')) === 'free-tier-eligible' && compact(attrOf(f.body, 'values')) === '["true"]')
          : false;
        check(
          asks,
          `${caller.rel}: module "${c.name}" passes allowed_instance_types from AWS, a data "aws_ec2_instance_types" filtered free-tier-eligible = true (${arg === undefined ? 'not passed, so the precondition checks nothing' : !data ? `${arg}: no such data source` : `data.aws_ec2_instance_types.${source}, ${asks ? 'free-tier-eligible' : 'some other filter'}`})`,
        );
      }
    }
  }
}

function ecrChecks(units) {
  section('every ECR repository has immutable tags');
  const repos = units.flatMap((u) => blocksOf(u.items, 'resource', 'aws_ecr_repository').map((r) => ({ u, r })));
  check(repos.length > 0, `CONTROL: ${repos.length} aws_ecr_repository found to judge`);
  for (const { u, r } of repos) {
    const value = attrOf(r.body, 'image_tag_mutability');
    check(
      literal(value) === 'IMMUTABLE',
      `${u.rel}: aws_ecr_repository.${r.labels[1]} sets image_tag_mutability = "IMMUTABLE", so a git-SHA tag can never be pointed at other bytes (${value ?? 'not set: the default is MUTABLE'})`,
    );
  }
}

function tagChecks(units, roots, modules) {
  section("every root's aws provider tags what it creates with the project");
  for (const r of roots) {
    const providers = blocksOf(r.items, 'provider', 'aws');
    check(providers.length > 0, `${r.rel} configures the aws provider (${providers.length} block(s))`);
    for (const p of providers) {
      const tags = blocksOf(p.body, 'default_tags').map((d) => attrOf(d.body, 'tags') ?? '');
      check(
        tags.some((t) => /(^|[\s{,])"?Project"?\s*=/.test(t)),
        `${r.rel}: provider "aws" (${where(p)}) sets default_tags with Project${tags.length ? '' : ' -- no default_tags block'}`,
      );
    }
  }
  const configured = modules.flatMap((m) => blocksOf(m.items, 'provider').map((p) => `${m.rel}: provider "${p.labels[0]}" at ${where(p)}`));
  check(configured.length === 0, `no module configures a provider of its own, which would not carry the root's default_tags${configured.length ? ` -- ${configured.join('; ')}` : ''}`);
}

function budgetChecks(units) {
  section('a monthly cost budget, with email before the money is gone');
  const all = units.flatMap((u) => blocksOf(u.items, 'resource', 'aws_budgets_budget').map((r) => ({ u, r })));
  const monthly = all.filter(({ r }) => literal(attrOf(r.body, 'budget_type')) === 'COST' && literal(attrOf(r.body, 'time_unit')) === 'MONTHLY');
  check(
    monthly.length > 0,
    `a monthly COST budget exists (${all.length} aws_budgets_budget: ${all.map(({ u, r }) => `${u.rel}: ${r.labels[1]}, ${attrOf(r.body, 'limit_amount')} ${attrOf(r.body, 'limit_unit')}, ${attrOf(r.body, 'budget_type')} ${attrOf(r.body, 'time_unit')}`).join('; ') || 'none'})`,
  );
  const wanted = ['ACTUAL 50', 'ACTUAL 80', 'ACTUAL 100', 'FORECASTED 100'];
  for (const { u, r } of monthly) {
    const alerts = blocksOf(r.body, 'notification')
      .filter(
        (n) =>
          literal(attrOf(n.body, 'threshold_type')) === 'PERCENTAGE' &&
          literal(attrOf(n.body, 'comparison_operator')) === 'GREATER_THAN' &&
          (attrOf(n.body, 'subscriber_email_addresses') !== undefined || attrOf(n.body, 'subscriber_sns_topic_arns') !== undefined),
      )
      .map((n) => `${literal(attrOf(n.body, 'notification_type'))} ${Number(attrOf(n.body, 'threshold'))}`);
    const missing = wanted.filter((w) => !alerts.includes(w));
    check(
      missing.length === 0,
      `${u.rel}: aws_budgets_budget.${r.labels[1]} alerts a subscriber at 50, 80 and 100% actual and 100% forecast (${alerts.join(', ') || 'no alerts'})${missing.length ? ` -- missing ${missing.join(', ')}` : ''}`,
    );
    // Credits netted off -- AWS's default when cost_types is unset -- and a Free Tier
    // account's spend reads $0 until the credits are gone: the budget never alerts.
    const ct = blocksOf(r.body, 'cost_types')[0];
    const credit = ct ? attrOf(ct.body, 'include_credit') : undefined;
    check(
      credit === 'false',
      `${u.rel}: aws_budgets_budget.${r.labels[1]} counts usage before credits (cost_types include_credit = ${credit ?? 'unset, so AWS nets credits off and a credit-funded spend reads $0'})`,
    );
  }
}

function versionChecks(root) {
  section('CI installs the Terraform infra/TOOLVERSIONS.md records, from a pinned action');
  const toolPath = join(root, 'infra', 'TOOLVERSIONS.md');
  const ciPath = join(root, '.github', 'workflows', 'ci.yml');
  const recorded = existsSync(toolPath) ? /^\|\s*Terraform\s*\|\s*([0-9][\w.-]*)\s*\|/m.exec(readFileSync(toolPath, 'utf8'))?.[1] ?? null : null;
  check(recorded !== null, `CONTROL: infra/TOOLVERSIONS.md records Terraform ${recorded ?? '(no Terraform row found)'}`);
  // YAML with its comment lines dropped: a comment naming a version is not one CI installs.
  const ci = existsSync(ciPath)
    ? readFileSync(ciPath, 'utf8')
        .split(/\r?\n/)
        .filter((l) => !l.trim().startsWith('#'))
        .join('\n')
    : '';
  const installs = [...ci.matchAll(/^\s*terraform_version:\s*["']?([0-9][\w.-]*)["']?\s*(#.*)?$/gm)].map((m) => m[1]);
  check(
    installs.length > 0 && installs.every((v) => v === recorded),
    `the Terraform ci.yml installs (${installs.join(', ') || 'no terraform_version found'}) is the one TOOLVERSIONS.md records (${recorded})`,
  );
  const uses = [...ci.matchAll(/^\s*(?:-\s*)?uses:\s*hashicorp\/setup-terraform@(\S+)/gm)].map((m) => m[1]);
  check(
    uses.length > 0 && uses.every((ref) => /^[0-9a-f]{40}$/.test(ref)),
    `ci.yml pins hashicorp/setup-terraform to a full commit SHA, which cannot be moved the way a tag can (${uses.join(', ') || 'no setup-terraform step found'})`,
  );
  return recorded;
}

// ---------------------------------------------------------------------------
// terraform itself
// ---------------------------------------------------------------------------

const TF_ENV = {
  ...process.env,
  TF_IN_AUTOMATION: '1',
  TF_INPUT: '0',
  CHECKPOINT_DISABLE: '1',
  TF_PLUGIN_CACHE_DIR: process.env.TF_PLUGIN_CACHE_DIR || join(tmpdir(), 'eventhub-terraform-plugin-cache'),
};

function terraform(args, cwd) {
  const r = spawnSync('terraform', args, { cwd, env: TF_ENV, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  return { status: r.error ? `not run (${r.error.code ?? r.error.message})` : r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
}

function terraformChecks(tfDir, roots, recorded) {
  section('terraform: fmt, then init and validate in every root');
  mkdirSync(TF_ENV.TF_PLUGIN_CACHE_DIR, { recursive: true });
  const version = terraform(['version', '-json']);
  let running = null;
  try {
    running = JSON.parse(version.stdout).terraform_version;
  } catch {
    running = null;
  }
  check(running !== null, `CONTROL: terraform ${running ?? `did not answer (${version.status}): ${tail(version.stderr)}`} is on PATH`);
  if (running === null) return;
  if (recorded && running !== recorded) note(`terraform ${running} runs here and TOOLVERSIONS.md records ${recorded}: CI runs ${recorded}`);

  const scratch = mkdtempSync(join(tmpdir(), 'verify-terraform-'));
  try {
    // The controls first: each tool must be seen to fail before its pass means anything.
    const fmtDir = join(scratch, 'fmt');
    mkdirSync(fmtDir);
    writeFileSync(join(fmtDir, 'planted.tf'), 'variable "planted" {\ntype=string\n}\n');
    const planted = terraform(['fmt', '-check', '-recursive', '-no-color', '.'], fmtDir);
    check(planted.status === 3 && planted.stdout.includes('planted.tf'), `CONTROL: fmt -check refuses a planted misformat (exit ${planted.status}, listing ${planted.stdout.trim() || 'nothing'})`);

    const validDir = join(scratch, 'validate');
    mkdirSync(validDir);
    writeFileSync(join(validDir, 'main.tf'), 'output "planted" {\n  value = var.not_declared\n}\n');
    const vInit = terraform(['init', '-backend=false', '-input=false', '-no-color'], validDir);
    const vPlanted = terraform(['validate', '-json', '-no-color'], validDir);
    let plantedResult = null;
    try {
      plantedResult = JSON.parse(vPlanted.stdout);
    } catch {
      plantedResult = null;
    }
    const summaries = (plantedResult?.diagnostics ?? []).map((d) => d.summary);
    check(
      vInit.status === 0 && plantedResult?.valid === false && summaries.some((s) => /undeclared input variable/i.test(s)),
      `CONTROL: validate -json calls a planted reference to an undeclared variable invalid (init ${vInit.status}, valid=${plantedResult?.valid}, ${summaries.join('; ') || tail(vPlanted.stderr || vPlanted.stdout, 300)})`,
    );
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }

  const fmt = terraform(['fmt', '-check', '-recursive', '-no-color', '.'], tfDir);
  check(
    fmt.status === 0,
    fmt.status === 0
      ? 'terraform fmt -check -recursive: every file is formatted'
      : `terraform fmt -check -recursive exited ${fmt.status}: run terraform fmt -recursive infra/terraform -- ${(fmt.stdout.trim() || tail(fmt.stderr)).split(/\r?\n/).join(', ')}`,
  );

  for (const r of roots) {
    // A .terraform this run creates, it removes: on Windows each one holds a full copy of
    // the providers (787 MB for aws alone), not a link into the cache. One that was already
    // there is left alone; after a real init it holds the backend configuration.
    const workDir = join(r.dir, '.terraform');
    const created = !existsSync(workDir);
    try {
      validateRoot(r);
    } finally {
      if (created && existsSync(workDir)) {
        rmSync(workDir, { recursive: true, force: true });
        note(`cleanup: removed ${r.rel}/.terraform, which this run created${existsSync(workDir) ? ', and it is still there' : ''}`);
      }
    }
  }
}

function validateRoot(r) {
  // -lockfile=readonly: the providers are the ones the committed lock file pins, with the
  // checksums it records, or init fails. -backend=false: no state, so no bucket and no
  // credentials.
  const init = terraform(['init', '-backend=false', '-input=false', '-no-color', '-lockfile=readonly'], r.dir);
  check(init.status === 0, `${r.rel}: terraform init -backend=false -lockfile=readonly${init.status === 0 ? '' : ` exited ${init.status}: ${tail(init.stderr || init.stdout)}`}`);
  if (init.status !== 0) return;

  const calls = blocksOf(r.items, 'module').map((m) => m.labels[0]);
  if (calls.length) {
    const index = join(r.dir, '.terraform', 'modules', 'modules.json');
    let installed = [];
    try {
      installed = JSON.parse(readFileSync(index, 'utf8')).Modules.map((m) => m.Key).filter(Boolean);
    } catch {
      installed = [];
    }
    check(
      calls.every((c) => installed.includes(c)),
      `CONTROL: ${r.rel}: init read the ${calls.length} module(s) it calls, so validate judges them too (${installed.join(', ') || 'none in .terraform/modules/modules.json'})`,
    );
  }

  const v = terraform(['validate', '-json', '-no-color'], r.dir);
  let result = null;
  try {
    result = JSON.parse(v.stdout);
  } catch {
    result = null;
  }
  if (result === null) {
    check(false, `${r.rel}: terraform validate -json gave no JSON (exit ${v.status}): ${tail(v.stderr || v.stdout)}`);
    return;
  }
  const diagnostics = (result.diagnostics ?? []).map((d) => `${d.severity}: ${d.summary}${d.range ? ` (${d.range.filename}:${d.range.start?.line})` : ''}${d.detail ? ` -- ${d.detail}` : ''}`);
  check(
    result.valid === true && result.error_count === 0,
    `${r.rel}: terraform validate -json: ${result.valid ? 'valid' : 'invalid'}, ${result.error_count} error(s), ${result.warning_count} warning(s)${result.error_count ? ` -- ${diagnostics.filter((d) => d.startsWith('error')).join('; ')}` : ''}`,
  );
  for (const d of diagnostics.filter((x) => x.startsWith('warning'))) note(`${r.rel}: ${d}`);
  testRoot(r);
}

/**
 * A root's terraform tests: plans against a mocked provider, so a guard is judged by what
 * a plan does rather than by what its file says. validate does not evaluate preconditions;
 * five mutations that made the node-type precondition unable to fail passed validate and
 * every static guard. The run count is the control: "0 passed, 0 failed" is not a pass.
 */
function testRoot(r) {
  const testsDir = join(r.dir, 'tests');
  const files = existsSync(testsDir) ? readdirSync(testsDir).filter((f) => f.endsWith('.tftest.hcl')) : [];
  if (files.length === 0) return;
  const expected = files.reduce((n, f) => n + [...lexHcl(readFileSync(join(testsDir, f), 'utf8')).code.matchAll(/^\s*run\s+"[^"]+"\s*\{/gm)].length, 0);
  const t = terraform(['test', '-no-color'], r.dir);
  const out = `${t.stdout}\n${t.stderr}`;
  const summary = /Success! (\d+) passed, (\d+) failed\.|Failure! (\d+) passed, (\d+) failed(?:, (\d+) skipped)?\./.exec(out);
  const passed = Number(summary?.[1] ?? summary?.[3] ?? 0);
  const failed = Number(summary?.[2] ?? summary?.[4] ?? 0) + Number(summary?.[5] ?? 0);
  const failing = [...out.matchAll(/^\s*run "([^"]+)"\.\.\. (fail|skip)/gm)].map((m) => `${m[1]} (${m[2]})`);
  check(
    t.status === 0 && expected > 0 && passed === expected && failed === 0,
    `${r.rel}: terraform test: ${passed} of ${expected} run(s) passed in ${files.join(', ')}${failing.length ? ` -- ${failing.join('; ')}` : ''}${summary ? '' : ` -- no summary (exit ${t.status}): ${tail(out, 600)}`}`,
  );
}

// ---------------------------------------------------------------------------

function main() {
  const args = process.argv.slice(2);
  const staticOnly = args[0] === '--static';
  const rootArg = staticOnly ? args[1] : args[0];
  const root = rootArg ? resolve(rootArg) : ROOT;
  const tfDir = join(root, 'infra', 'terraform');

  console.log(`  reading ${rootArg ? tfDir : 'infra/terraform'}${staticOnly ? ', static guards only' : ''}`);
  if (!existsSync(tfDir)) {
    check(false, `${tfDir} does not exist, so there is nothing to judge`);
    return;
  }

  section('the reader');
  lexerControl();

  const units = loadUnits(tfDir);
  const roots = units.filter((u) => u.kind === 'root');
  const modules = units.filter((u) => u.kind === 'module');

  discoveryChecks(tfDir, units, roots, modules);
  backendChecks(units, roots);
  bucketChecks(units);
  natChecks(units, roots);
  credentialChecks(tfDir);
  eksChecks(units);
  podIdentityChecks(units);
  nodeTypeChecks(units);
  ecrChecks(units);
  tagChecks(units, roots, modules);
  budgetChecks(units);
  const recorded = versionChecks(root);

  if (!staticOnly) terraformChecks(tfDir, roots, recorded);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log(
    staticOnly
      ? `the static guards hold across ${roots.length} root(s) and ${modules.length} module(s)`
      : `${roots.length} root(s) and ${modules.length} module(s) format, validate, and keep every guard`,
  );
}

main();
