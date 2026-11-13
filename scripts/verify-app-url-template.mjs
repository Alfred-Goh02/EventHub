/**
 * Asserts that the base every emailed link is built on names the tenant, is checked
 * before Auth or Notification starts, and is never filled with anything but a slug
 * the gateway would route.
 *
 * THE BUG THIS GUARDS. Every emailed link -- a password reset, an invitation, a reply,
 * a digest -- was built on one tenantless base, PUBLIC_APP_URL, which compose never set,
 * so all of them opened http://localhost:3000. But a tenant is its subdomain: Kong takes
 * the tenant from the Host (infra/kong/kong.yml), and a session is bound to it. A reset
 * asked for on acme.eventhub.localhost, and another on globex.eventhub.localhost for the
 * same address, were measured in Mailpit before the fix: both links on localhost:3000,
 * a host that serves no tenant. Now the base is APP_URL_TEMPLATE, with {tenant} in the
 * host, filled per link (packages/config/src/app-url.ts).
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - The rule is Kong's. The reserved names, the longest label and the pattern are read
 *     out of kong.yml's own Lua, and a corpus of slugs -- each of them on either side of
 *     every part of the rule -- is judged by both. A hand-written copy that fell behind
 *     the gateway would disagree on the slug that changed.
 *   - A slug that fails the rule is never put into a link: every refused slug, and every
 *     value that is not a string, gets no origin at all. Each one that passes gets
 *     exactly its own host, so "no origin" is the rule refusing and not a template that
 *     fills nothing. Among the refused are the ones that would move a link if they were
 *     substituted raw: a dot, a slash, an @, a colon.
 *   - Compose, both .env.examples and the README name the same template. Each is
 *     written by hand, so compose's value is read out of docker-compose.yml, alias and
 *     all, for exactly the two services that build links, and compared.
 *   - Auth and Notification each refuse to start with the template missing -- the
 *     deployment that still sets only PUBLIC_APP_URL -- with the OLD TENANTLESS BASE in
 *     it, and with each malformed shape, and each refusal names the variable and the
 *     reason. A schema that accepted anything would start every one of them. The control
 *     is the same process, from the same example, starting with the example's template.
 *
 * The emails themselves, read from Mailpit with their link hosts checked per tenant, are
 * the email verifiers' (verify-email-delivery, verify-invitation-email, verify-reply-emails,
 * verify-reply-digest, and verify-compose-password-reset for acme and globex in the
 * stack). Production's https rule is verify-env-example's for Auth and
 * verify-notification-preferences' for Notification.
 *
 * Notification is started on consumer groups of this run's own, committed at the end of
 * the topics they read, and they are deleted at the end.
 *
 * Run (with Postgres, Kafka and the Schema Registry up; @eventhub/config, Auth and
 * Notification built; both keypairs generated): node scripts/verify-app-url-template.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';
import { contactsFromTheEnd, notificationGroups } from './lib/notification-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
// The shared module both services import, not a copy of it: a copy would only agree with itself.
const { parseAppUrlTemplate, isTenantSlug, RESERVED_SUBDOMAINS, TENANT_SLUG_MAX_LENGTH } = await import(
  pathToFileURL(join(ROOT, 'packages', 'config', 'dist', 'index.js')).href
);

const RUN = randomBytes(4).toString('hex');
const GROUPS = notificationGroups(`verify-app-url-template-${RUN}`);
const AUTH_PORT = 3771;
const NOTIFICATION_PORT = 3772;

const authEnv = serviceEnv(ROOT, 'auth', { PORT: String(AUTH_PORT), NODE_ENV: 'development' });
const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(NOTIFICATION_PORT),
  NODE_ENV: 'development',
  ...GROUPS.env,
  DISPATCH_START_FROM: 'latest',
  COMMUNITY_START_FROM: 'latest',
  AUTH_PUBLIC_KEY_B64: readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64'),
});

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const show = (v) => (typeof v === 'string' ? JSON.stringify(v.length > 24 ? `${v.slice(0, 10)}…(${v.length})` : v) : Array.isArray(v) ? `[${v.map(show)}]` : String(v));

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

function start(service, env) {
  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', service), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  const proc = { child, code: undefined, log: () => log };
  // 'close', not 'exit': it fires once the output has been read to the end.
  proc.exited = new Promise((done) =>
    child.on('close', (code) => {
      proc.code = code;
      done(code);
    }),
  );
  return proc;
}

/** Whichever comes first: the process exits, or it answers /health. */
async function exitOrListen(proc, port, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (proc.code !== undefined) return { exit: proc.code };
    try {
      if ((await fetch(`http://127.0.0.1:${port}/health`)).ok) return { listening: true };
    } catch {
      // not listening
    }
    await sleep(200);
  }
  return {};
}

/** Kong's own rule, out of kong.yml's Lua: the reserved names, the patterns on the host, and the longest label. */
function kongRule() {
  const yml = readFileSync(join(ROOT, 'infra', 'kong', 'kong.yml'), 'utf8');
  const table = yml.match(/local RESERVED = \{([\s\S]*?)\}/)?.[1] ?? '';
  const reserved = [...table.matchAll(/([a-z0-9-]+)\s*=\s*true/g)].map((m) => m[1]);
  // Lua patterns: %. is a literal dot and %- a literal hyphen; nothing else in these two is special.
  const patterns = [...yml.matchAll(/host:match\("([^"]+)"\)/g)].map((m) => new RegExp(m[1].replace(/%\./g, '\\.').replace(/%-/g, '\\-')));
  const max = Number(yml.match(/#slug > (\d+)/)?.[1] ?? NaN);
  return { reserved, patterns, max };
}

/**
 * What compose gives each service as APP_URL_TEMPLATE, read from docker-compose.yml's
 * own text: a service is a two-space key under `services:`, and an alias is resolved to
 * the anchor it names. Lines may end in CRLF on this checkout, which `\s*$` absorbs.
 */
function composeTemplates() {
  const yml = readFileSync(join(ROOT, 'docker-compose.yml'), 'utf8');
  const anchors = Object.fromEntries([...yml.matchAll(/&([a-z0-9-]+)\s+"([^"]*)"/g)].map((m) => [m[1], m[2]]));
  const services = yml.slice(Math.max(0, yml.search(/^services:\s*$/m)));
  const found = {};
  for (const block of services.split(/^(?=  [a-z][a-z0-9-]*:\s*$)/m)) {
    const name = block.match(/^  ([a-z][a-z0-9-]*):\s*$/m)?.[1];
    const value = block.match(/^\s+APP_URL_TEMPLATE:\s*(.+?)\s*$/m)?.[1];
    if (!name || value === undefined) continue;
    const alias = value.match(/^\*([a-z0-9-]+)$/)?.[1];
    const anchored = value.match(/^&[a-z0-9-]+\s+"([^"]*)"$/)?.[1];
    found[name] = alias !== undefined ? anchors[alias] : (anchored ?? value.replace(/^"(.*)"$/, '$1'));
  }
  return found;
}

async function main() {
  // ---------------------------------------------------------------------------------
  console.log("  the rule is the gateway's, read from kong.yml");
  const kong = kongRule();
  check(
    kong.reserved.length > 0 && kong.patterns.length === 2 && Number.isInteger(kong.max),
    `CONTROL: kong.yml's tenant snippet was read: ${kong.reserved.length} reserved name(s), ${kong.patterns.length} host pattern(s), at most ${kong.max} characters`,
  );
  const ours = [...RESERVED_SUBDOMAINS].sort();
  const theirs = [...kong.reserved].sort();
  check(ours.join(',') === theirs.join(','), `the reserved names are kong.yml's (${ours.join(', ')})`);
  check(TENANT_SLUG_MAX_LENGTH === kong.max, `the longest slug is kong.yml's (${TENANT_SLUG_MAX_LENGTH} / ${kong.max})`);

  /**
   * The slug Kong would hand the services for a host, as its snippet derives it: the
   * Host lower-cased (get_host()), matched, then refused when too long or reserved. A
   * slug is one Kong routes as itself when its own host comes back as exactly it.
   */
  const kongSlugOf = (host) => {
    const lower = host.toLowerCase();
    const slug = kong.patterns.map((p) => lower.match(p)?.[1]).find(Boolean);
    return slug && slug.length <= kong.max && !kong.reserved.includes(slug) ? slug : null;
  };
  const kongRoutes = (slug) => kongSlugOf(`${slug}.eventhub.localhost`) === slug && kongSlugOf(`${slug}.eventhub.com`) === slug;

  const passing = ['acme', 'globex', 'a', '0', '9lives', 'acme-', 'a-b-c', 'xn--bcher-kva', 'wwww', 'status-page', 'a'.repeat(63)];
  const failing = [
    ...RESERVED_SUBDOMAINS,
    'a'.repeat(64),
    '',
    '-acme',
    'Acme',
    'ACME',
    'ac_me',
    'ac.me',
    'acme.evil.example',
    'acme/',
    'evil.example/x',
    'acme:80',
    'acme@evil.example',
    'acme#',
    'acme?x',
    'ácme',
    ' acme',
    'acme ',
    'acme\n',
    '..',
  ];
  check(
    passing.every(kongRoutes) && failing.every((s) => !kongRoutes(s)),
    `CONTROL: by kong.yml's own rule, the ${passing.length} slugs meant to pass do and the ${failing.length} meant to fail do not`,
  );
  const disagree = [...passing, ...failing].filter((s) => isTenantSlug(s) !== kongRoutes(s));
  check(
    disagree.length === 0,
    disagree.length === 0
      ? `and isTenantSlug agrees with the gateway on all ${passing.length + failing.length}`
      : `isTenantSlug disagrees with the gateway on ${disagree.map(show).join(', ')}`,
  );

  // ---------------------------------------------------------------------------------
  console.log('');
  console.log('  a slug that fails the rule is never put into a link');
  const exampleTemplate = authEnv.APP_URL_TEMPLATE;
  check(
    exampleTemplate === 'http://{tenant}.eventhub.localhost:5173' && notificationEnv.APP_URL_TEMPLATE === exampleTemplate,
    `CONTROL: both examples set the local template, the same one (${exampleTemplate} / ${notificationEnv.APP_URL_TEMPLATE})`,
  );
  const local = parseAppUrlTemplate(exampleTemplate);
  const production = parseAppUrlTemplate('https://{tenant}.eventhub.com');
  const wrongHost = passing.filter((s) => local.originFor(s) !== `http://${s}.eventhub.localhost:5173` || production.originFor(s) !== `https://${s}.eventhub.com`);
  check(
    wrongHost.length === 0,
    wrongHost.length === 0
      ? `CONTROL: each of the ${passing.length} passing slugs gets exactly its own host, locally and in production (acme: ${local.originFor('acme')}, ${production.originFor('acme')})`
      : `a passing slug got another host: ${wrongHost.map((s) => `${show(s)} -> ${local.originFor(s)}`).join(', ')}`,
  );
  const notStrings = [null, undefined, 42, {}, ['acme'], { toString: () => 'acme' }];
  const substituted = [...failing, ...notStrings].filter((s) => local.originFor(s) !== null || production.originFor(s) !== null);
  check(
    substituted.length === 0,
    substituted.length === 0
      ? `none of the ${failing.length} failing slugs, nor ${notStrings.length} values that are not strings, gets any origin at all`
      : `put into a link: ${substituted.map((s) => `${show(s)} -> ${local.originFor(s) ?? production.originFor(s)}`).join(', ')}`,
  );

  /**
   * Labels the gateway routes that a URL parser may refuse: `xn--` followed by something
   * that is not valid punycode. Whether it does depends on the Node build -- measured,
   * 26.3.0 (ada 3.4.4) throws ERR_INVALID_URL for each, 26.12.1 builds them -- and the
   * image takes whichever node:26 was current when it was built. originFor threw on the
   * first kind, and in Notification's replies consumer kafkajs treats a TypeError as
   * unrecoverable, so the consumer crashed on the same message for good.
   *
   * So it is asked twice. Once with this Node's own parser: no throw, and each answer is
   * null or exactly its own host. And once with a parser that refuses these labels, as
   * the older one does, which is the case the guard exists for and which this check
   * would otherwise only see on some machines: each must answer null.
   */
  const unbuildable = ['xn--a', 'xn--', 'xn---', 'xn--zz'];
  check(
    unbuildable.every((s) => kongRoutes(s) && isTenantSlug(s)),
    `CONTROL: the gateway routes each of ${unbuildable.map(show).join(', ')}, and isTenantSlug passes it, so a tenant can have one`,
  );
  const ask = () =>
    unbuildable.map((s) => {
      try {
        return { s, local: local.originFor(s), production: production.originFor(s) };
      } catch (err) {
        return { s, threw: err instanceof Error ? `${err.name} ${err.code ?? err.message}` : String(err) };
      }
    });
  const describe = (list) => list.map((a) => `${show(a.s)} -> ${a.threw ? `threw ${a.threw}` : `${a.local} / ${a.production}`}`).join(', ');

  const ownParser = ask();
  const notOwn = ownParser.filter(
    (a) =>
      a.threw ||
      (a.local !== null && a.local !== `http://${a.s}.eventhub.localhost:5173`) ||
      (a.production !== null && a.production !== `https://${a.s}.eventhub.com`),
  );
  check(
    notOwn.length === 0,
    notOwn.length === 0
      ? `with this Node's parser (${process.versions.node}) none of them throws, and each answers null or its own host (${describe(ownParser.slice(0, 1))})`
      : `with this Node's parser (${process.versions.node}): ${describe(notOwn)}`,
  );

  const RealURL = globalThis.URL;
  class RefusingURL extends RealURL {
    constructor(input, base) {
      super(input, base);
      if (unbuildable.some((s) => this.hostname === s || this.hostname.startsWith(`${s}.`))) {
        throw Object.assign(new TypeError('Invalid URL'), { code: 'ERR_INVALID_URL' });
      }
    }
  }
  let refused;
  try {
    globalThis.URL = RefusingURL;
    let probeThrew = false;
    try {
      new URL('http://xn--a.eventhub.localhost:5173');
    } catch {
      probeThrew = true;
    }
    check(probeThrew, 'CONTROL: the stand-in parser refuses these labels, as Node 26.3.0 does');
    refused = ask();
  } finally {
    globalThis.URL = RealURL;
  }
  const notNull = refused.filter((a) => a.threw || a.local !== null || a.production !== null);
  check(
    notNull.length === 0,
    notNull.length === 0
      ? 'and with a parser that refuses them, originFor answers null for each, locally and in production, rather than throwing'
      : `with a parser that refuses them, not answered with null: ${describe(notNull)}`,
  );

  // ---------------------------------------------------------------------------------
  console.log('');
  console.log('  compose, both examples and the README name the same template');
  // Hand-written in four places, so each is checked against the examples rather than trusted.
  const composed = composeTemplates();
  check(
    Object.keys(composed).sort().join(',') === 'auth,notification',
    `CONTROL: docker-compose.yml was read, and sets APP_URL_TEMPLATE for exactly the two services that build links (${Object.keys(composed).sort().join(', ') || 'none'})`,
  );
  check(
    composed.auth === exampleTemplate && composed.notification === exampleTemplate,
    `compose gives both the examples' template (${composed.auth ?? 'nothing'} / ${composed.notification ?? 'nothing'})`,
  );
  const readme = readFileSync(join(ROOT, 'README.md'), 'utf8');
  check(readme.includes(`\`${exampleTemplate}\` in compose`), `and the README names it as compose's (${exampleTemplate})`);

  // ---------------------------------------------------------------------------------
  console.log('');
  console.log('  a missing or malformed template refuses to start, in both services');
  const malformed = [
    ['missing, as in a deployment that still sets only PUBLIC_APP_URL', undefined, 'required'],
    ['the old tenantless base, http://localhost:3000', 'http://localhost:3000', 'must contain {tenant} exactly once'],
    ['empty', '', 'must contain {tenant} exactly once'],
    ['{tenant} twice', 'http://{tenant}.{tenant}.eventhub.localhost:5173', 'must contain {tenant} exactly once'],
    ['{tenant} in the path', 'http://eventhub.localhost:5173/{tenant}', 'as the whole first label of the host'],
    ['{tenant} inside a label', 'http://app-{tenant}.eventhub.localhost:5173', 'as the whole first label of the host'],
    ['{tenant} as the whole host', 'http://{tenant}:5173', 'as the whole first label of the host'],
    ['a path after the origin', 'http://{tenant}.eventhub.localhost:5173/app', 'must be an origin'],
    ['not http or https', 'ftp://{tenant}.eventhub.localhost', 'must be http or https'],
    ['not a URL', '{tenant}.eventhub.localhost', 'is not a URL'],
  ];

  const kafka = new Kafka({ clientId: `verify-app-url-template-${RUN}`, brokers: notificationEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  try {
    // A process already answering would pass the controls below for code this run never started.
    let portsFree = true;
    for (const port of [AUTH_PORT, NOTIFICATION_PORT]) {
      const taken = await fetch(`http://127.0.0.1:${port}/health`).then(() => true, () => false);
      check(!taken, taken ? `something already answers on :${port}; stop it and run again` : `CONTROL: :${port} is free`);
      if (taken) portsFree = false;
    }
    if (!portsFree) return;

    for (const [service, env, port] of [['auth', authEnv, AUTH_PORT], ['notification', notificationEnv, NOTIFICATION_PORT]]) {
      const outcomes = [];
      for (const [what, value, reason] of malformed) {
        const caseEnv = { ...env };
        if (value === undefined) delete caseEnv.APP_URL_TEMPLATE;
        else caseEnv.APP_URL_TEMPLATE = value;
        const proc = start(service, caseEnv);
        const outcome = await exitOrListen(proc, port);
        if (proc.code === undefined) proc.child.kill();
        await proc.exited;
        // The loader's own line, anchored: the stack trace also prints the throwing line
        // of source, whose template text would match a looser pattern and name nothing.
        const named = proc.log().match(/^ {2}APP_URL_TEMPLATE: (.+)$/m)?.[1] ?? '';
        outcomes.push({ what, ok: outcome.exit !== undefined && outcome.exit !== 0 && named.includes(reason), outcome, named });
      }
      const wrong = outcomes.filter((o) => !o.ok);
      check(
        wrong.length === 0,
        wrong.length === 0
          ? `${service} refuses all ${outcomes.length}, each naming APP_URL_TEMPLATE and its own reason: ${outcomes.map((o) => o.what).join('; ')}`
          : `${service} did not refuse as it should: ${wrong
              .map((o) => `${o.what} (${o.outcome.listening ? 'it STARTED' : `exit ${o.outcome.exit ?? 'none'}`}: ${o.named || 'no APP_URL_TEMPLATE line'})`)
              .join('; ')}`,
      );
      const told = outcomes.find((o) => o.what.startsWith('missing'));
      check(
        /replaces PUBLIC_APP_URL/.test(told?.named ?? ''),
        `and a deployment that still sets only PUBLIC_APP_URL is told what replaced it ("${(told?.named ?? '').slice(0, 90)}…")`,
      );

      // The control: the same example, the template as given, and it starts.
      const contacts = service === 'notification' ? await contactsFromTheEnd(admin, ROOT, GROUPS) : null;
      const proc = start(service, env);
      const outcome = await exitOrListen(proc, port, 60_000);
      const own = contacts && outcome.listening ? await contacts.joined() : null;
      if (proc.code === undefined) proc.child.kill();
      await proc.exited;
      const lines = proc.log().trim().split(/\r?\n/);
      const why = lines.find((l) => /^(Error|[A-Za-z]*Error):/.test(l.trim())) ?? lines.slice(0, 3).join(' | ');
      check(
        outcome.listening === true,
        outcome.listening
          ? `CONTROL: with the example's template, the same ${service} starts and answers /health`
          : `CONTROL: with the example's template, ${service} did NOT start (exit ${outcome.exit ?? 'none'}): ${why}`,
      );
      if (own) check(own.ok, own.message);
    }
  } finally {
    const left = await deleteGroupsWhenEmpty(admin, GROUPS.all);
    if (left.length > 0) console.log(`    note  consumer group(s) left to expire: ${left.join(', ')}`);
    await admin.disconnect().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log("every emailed link is built on the tenant's own subdomain, from a template both services check before they start, and only a slug the gateway routes is ever put into it");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
