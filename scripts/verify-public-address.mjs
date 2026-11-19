/**
 * Asserts that the EKS cluster's address is one setting (infra/eks/public.mjs), and that
 * everything naming it follows from it: given a real https domain, Kong serves that domain
 * and no other, the services that name it start in production with it, and nothing still
 * points at eventhub.com, a domain nobody here owns.
 *
 *   - THE ADDRESS. publicAddress() takes http(s)://{tenant}.<domain>[:port] and refuses the
 *     rest; every template it takes, Auth's own parser takes too, and Kong's render step
 *     (infra/kong/render-consumer.lua) agrees with it on every template, taken or refused;
 *   - WHAT IT SETS. NODE_ENV is production exactly when the address is https; the link
 *     template, the unsubscribe host and Live's origin suffix go where they are read, and the
 *     readers are checked against each service's own source, not trusted from a list;
 *   - THE SERVICES TAKE IT. Every module with a production rule (found by reading the
 *     services' source) is loaded with the environment the installer makes for an https
 *     domain, and starts; given an http template in production instead, Auth and
 *     Notification refuse, so the loading is the real check. Live takes the suffix, and
 *     refuses one without its dot;
 *   - KONG RENDERS IT. Unset, or the laptop's domain, the file is rendered as it is written;
 *     with a domain, the tenant hosts, the api host, the CORS origin and the tenant rule name
 *     it, no line that is not a comment still names eventhub.com, and Kong parses the result.
 *     A kong.yml without one of the four places stops Kong starting;
 *   - KONG SERVES IT. Kong runs the rendered file, every upstream an echo server: a tenant of
 *     the domain reaches Auth with its slug, the api host reaches the webhook without one,
 *     eventhub.com is routed nowhere, a bare host is refused, and CORS allows the domain's
 *     tenants on its scheme and nothing that only resembles them. The same requests against
 *     the file as written show the domain unrouted: the render is what serves it.
 *
 * The web app's half -- /config.js, and the real API only on the announced domain -- is its
 * own CI step and its own tests (apps/web/src/test/session.test.ts).
 *
 * Needs docker, Kong's image (compose's reference; the CI step pulls it), and `pnpm build`.
 *   node scripts/verify-public-address.mjs          against this tree
 *   node scripts/verify-public-address.mjs [root]   against another (a red run in a copy)
 */

import { spawnSync } from 'node:child_process';
import { generateKeyPairSync } from 'node:crypto';
import { cpSync, existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const rootArg = process.argv.slice(2).find((a) => a !== '--');
const root = rootArg ? resolve(rootArg) : ROOT;

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
const sh = (cmd, args, opts = {}) => {
  const r = spawnSync(cmd, args, { encoding: 'utf8', maxBuffer: 64 << 20, ...opts });
  return r.error ? { status: -1, stdout: '', stderr: String(r.error.message) } : { status: r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
};
const strip = (src) => src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:'"`\\])\/\/.*$/gm, '$1');
const walk = (dir, out = []) => {
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = join(dir, e.name);
    if (e.isDirectory()) walk(p, out);
    else out.push(p);
  }
  return out;
};

const HTTPS = 'https://{tenant}.example.org';
const TAKEN = [HTTPS, 'http://{tenant}.13-54-1-2.sslip.io', 'http://{tenant}.eventhub.localhost:5173', 'https://{tenant}.a.b.example.co.uk', 'https://{tenant}.example.org:8443', 'https://{tenant}.example.org/'];
const REFUSED = [
  'https://{tenant}.Example.org', 'https://{tenant}.org', 'https://app-{tenant}.example.org', 'https://{tenant}.example.org/app',
  'https://{tenant}.exa%mple.org', 'https://{tenant}.example.org:0', 'https://{tenant}.example.org:99999', 'ftp://{tenant}.example.org',
  'https://{tenant}.-example.org', 'https://{tenant}.example..org', 'https://u@{tenant}.example.org', 'https://{tenant}.example.org?x=1',
  'https://example.org', 'https://{tenant}.{tenant}.example.org',
];

async function main() {
  console.log(`  reading ${rootArg ? root : 'this tree'}`);
  const pub = await import(pathToFileURL(join(root, 'infra', 'eks', 'public.mjs')).href);
  const eks = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const stores = await import(pathToFileURL(join(root, 'infra', 'stores', 'secrets.mjs')).href);
  const appUrl = await import(pathToFileURL(join(root, 'packages', 'config', 'dist', 'app-url.js')).href);

  // -------------------------------------------------------------------------
  section('the address: one template, its parts, and the same verdict everywhere it is read');
  const parts = pub.publicAddress(pub.APP_URL_TEMPLATE);
  check(Boolean(parts.domain), `CONTROL: the cluster's address is ${pub.APP_URL_TEMPLATE}: ${parts.scheme}, ${parts.domain}${parts.port ? `, port ${parts.port}` : ''}`);
  const taken = (t) => {
    try {
      pub.publicAddress(t);
      return true;
    } catch {
      return false;
    }
  };
  const wrongJs = [...TAKEN.filter((t) => !taken(t)), ...REFUSED.filter(taken)];
  check(wrongJs.length === 0, `publicAddress takes ${TAKEN.length} addresses and refuses ${REFUSED.length} that are not one${wrongJs.length ? ` -- wrong about ${wrongJs.join(', ')}` : ''}`);
  const authRefuses = TAKEN.filter((t) => {
    try {
      appUrl.parseAppUrlTemplate(t);
      return false;
    } catch {
      return true;
    }
  });
  check(authRefuses.length === 0, `Auth's own parser takes every one of them, so no address the installer accepts stops Auth starting${authRefuses.length ? ` -- refused: ${authRefuses.join(', ')}` : ''}`);
  const h = pub.publicAddress(HTTPS);
  check(h.scheme === 'https' && h.domain === 'example.org' && h.port === '', `${HTTPS} is https, example.org, the scheme's own port`);

  // -------------------------------------------------------------------------
  section('what the address sets, and where');
  const composeRun = sh('docker', ['compose', '-f', join(root, 'docker-compose.yml'), '-p', 'eventhub-rebuild', 'config', '--format', 'json']);
  if (composeRun.status !== 0) {
    check(false, `docker compose config: ${composeRun.stderr.trim().slice(-400)}`);
    return;
  }
  const compose = JSON.parse(composeRun.stdout);
  const charts = eks.allCharts(root);
  const envOf = (chart) => eks.envOf(compose, chart);
  const prod = pub.publicEnvironment(HTTPS, charts, envOf);
  const dev = pub.publicEnvironment('http://{tenant}.eventhub.localhost:5173', charts, envOf);
  const nodeEnvs = (set) => [...new Set(Object.entries(set).filter(([n]) => n.endsWith('-env') && n !== 'kong-env' && n !== 'web-env').map(([, v]) => v.NODE_ENV))];
  const withNodeEnv = charts.filter((c) => 'NODE_ENV' in envOf(c));
  check(withNodeEnv.length > 0 && withNodeEnv.every((c) => prod[`${c}-env`]?.NODE_ENV === 'production'), `on https, NODE_ENV is production for every service compose gives one (${withNodeEnv.length} of ${charts.length})`);
  check(JSON.stringify(nodeEnvs(dev)) === '["development"]', `on http, development for every one (${nodeEnvs(dev).join(', ')})`);
  check(prod['notification-env']?.NOTIFICATION_PUBLIC_URL === 'https://api.example.org:8000', `the unsubscribe link is on the api host, through the gateway's port (${prod['notification-env']?.NOTIFICATION_PUBLIC_URL})`);
  check(prod['live-env']?.ALLOWED_ORIGIN_SUFFIX === '.example.org', `Live admits the domain's subdomains, from a label boundary (${prod['live-env']?.ALLOWED_ORIGIN_SUFFIX})`);
  check(prod['kong-env']?.APP_URL_TEMPLATE === HTTPS && prod['web-env']?.APP_URL_TEMPLATE === HTTPS, 'Kong and the web app are given the template itself');

  // The readers, from the services' source: a key goes to exactly the services that read it.
  const sources = {};
  for (const svc of readdirSync(join(root, 'services'))) {
    const src = join(root, 'services', svc, 'src');
    if (!existsSync(src)) continue;
    sources[svc] = walk(src).filter((f) => f.endsWith('.ts') && !/\.test\.ts$/.test(f)).map((f) => strip(readFileSync(f, 'utf8'))).join('\n');
  }
  check(Object.keys(sources).length >= charts.length / 2, `CONTROL: ${Object.keys(sources).length} services' source read`);
  for (const key of ['APP_URL_TEMPLATE', 'NOTIFICATION_PUBLIC_URL', 'ALLOWED_ORIGIN_SUFFIX']) {
    const given = charts.filter((c) => prod[`${c}-env`]?.[key] !== undefined).sort();
    const readers = Object.keys(sources).filter((svc) => new RegExp(`\\b${key}\\b`).test(sources[svc])).sort();
    check(given.length > 0 && JSON.stringify(given) === JSON.stringify(readers), `${key} goes to ${given.join(', ') || 'nobody'}, exactly the services whose code reads it (${readers.join(', ') || 'none'})`);
  }

  // -------------------------------------------------------------------------
  section('the services take what the installer makes for an https domain, in production');
  const storeDocs = stores.renderChart(join(root, 'infra', 'stores'));
  const made = eks.installSecrets(compose, charts, storeDocs, {}, HTTPS).secrets;
  const guards = [];
  for (const svc of Object.keys(sources)) {
    const src = join(root, 'services', svc, 'src');
    for (const f of walk(src).filter((x) => x.endsWith('.ts') && !/\.test\.ts$/.test(x))) {
      if (/NODE_ENV'?\]?\s*===\s*'production'/.test(strip(readFileSync(f, 'utf8')))) guards.push({ svc, file: relative(src, f) });
    }
  }
  check(guards.length > 0, `CONTROL: ${guards.length} module(s) with a production rule, read from the source: ${guards.map((g) => `${g.svc}/${g.file.split(sep).join('/')}`).join(', ')}`);
  const load = (svc, file, env) => {
    const dist = join(root, 'services', svc, 'dist', file.replace(/\.ts$/, '.js'));
    if (!existsSync(dist)) return { status: -1, stderr: `${relative(root, dist)} is not built (pnpm build)` };
    const base = Object.fromEntries(['PATH', 'Path', 'SystemRoot', 'TEMP', 'TMP', 'HOME', 'USERPROFILE'].filter((k) => process.env[k] !== undefined).map((k) => [k, process.env[k]]));
    return sh(process.execPath, ['--input-type=module', '-e', `await import(${JSON.stringify(pathToFileURL(dist).href)})`], { cwd: join(root, 'services', svc), env: { ...base, ...env } });
  };
  for (const g of guards) {
    const env = made[`${g.svc}-env`];
    const r = env ? load(g.svc, g.file, env) : { status: -1, stderr: `no ${g.svc}-env made` };
    check(r.status === 0 && env?.NODE_ENV === 'production', `${g.svc}/${g.file.split(sep).join('/')} loads in production with ${g.svc}-env${r.status === 0 ? '' : `: ${(r.stderr || r.stdout).trim().split('\n').filter((l) => /Error|refusing|must/.test(l))[0] ?? r.stderr.trim().slice(-200)}`}`);
  }
  for (const svc of ['auth', 'notification']) {
    const g = guards.find((x) => x.svc === svc && /env\.ts$/.test(x.file));
    const r = g ? load(svc, g.file, { ...made[`${svc}-env`], APP_URL_TEMPLATE: 'http://{tenant}.example.org' }) : { status: 0, stderr: '' };
    check(g !== undefined && r.status !== 0 && /refusing to start: .*https in production/.test(r.stderr), `and ${svc} refuses an http template in production, so the load above ran its rule (${g ? (r.status === 0 ? 'it started' : 'refused') : 'no env module with a rule'})`);
  }
  const live = load('live', 'env.ts', made['live-env']);
  check(live.status === 0, `live/env.ts takes ${made['live-env']?.ALLOWED_ORIGIN_SUFFIX}${live.status === 0 ? '' : `: ${live.stderr.trim().slice(-200)}`}`);
  const liveBad = load('live', 'env.ts', { ...made['live-env'], ALLOWED_ORIGIN_SUFFIX: 'example.org' });
  check(liveBad.status !== 0 && /must start with a dot/.test(liveBad.stderr), 'and refuses a suffix without its dot, so it was checked');

  // -------------------------------------------------------------------------
  section("Kong's render step puts the domain in kong.yml's four places, and agrees on every address");
  const image = compose.services.kong?.image;
  if (!image || sh('docker', ['image', 'inspect', image]).status !== 0) {
    check(false, `Kong's image (${image ?? 'not in compose'}) is not here: docker pull it first`);
    return;
  }
  // One scratch directory: the public key Kong is given, and the files it renders.
  const scratch = mkdtempSync(join(tmpdir(), 'verify-public-'));
  const { publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  writeFileSync(join(scratch, 'public.pem'), publicKey.export({ type: 'spki', format: 'pem' }));
  const kongDir = join(root, 'infra', 'kong');
  const written = readFileSync(join(kongDir, 'kong.yml'), 'utf8');
  /** Runs the render step in Kong's image: { status, stderr, text }. */
  const renderWith = (template, dir = kongDir, parse = false) => {
    const outFile = join(scratch, `kong-${Math.random().toString(36).slice(2)}.yml`);
    const r = sh('docker', [
      'run', '--rm', '-e', 'KONG_DATABASE=off', '-e', 'AUTH_PUBLIC_KEY_FILE=/keys/public.pem', '-e', 'JWT_ISSUER=https://auth.eventhub.local',
      '-e', `KONG_DECLARATIVE_CONFIG=/out/${outFile.split(sep).pop()}`, ...(template === undefined ? [] : ['-e', `APP_URL_TEMPLATE=${template}`]),
      '-v', `${dir}:/kong/declarative:ro`, '-v', `${scratch}:/keys:ro`, '-v', `${scratch}:/out`, '--entrypoint', 'sh', image,
      '-c', `resty /kong/declarative/render-consumer.lua${parse ? ' && kong config parse "$KONG_DECLARATIVE_CONFIG" >/dev/null && echo PARSED' : ''}`,
    ]);
    const text = existsSync(outFile) ? readFileSync(outFile, 'utf8') : '';
    rmSync(outFile, { force: true });
    return { ...r, text };
  };
  try {
    const asWritten = renderWith(undefined, kongDir, true);
    const body = written.replace(/\n*$/, '\n');
    check(asWritten.status === 0 && asWritten.text.startsWith(body) && /PARSED/.test(asWritten.stdout), `unset, as compose runs it, kong.yml is rendered as written, and parses (${asWritten.status === 0 ? `${asWritten.text.length} bytes` : asWritten.stderr.trim().slice(-200)})`);
    const laptop = renderWith('http://{tenant}.eventhub.localhost:5173');
    check(laptop.status === 0 && laptop.text === asWritten.text, "the laptop's address renders exactly the same file: eventhub.localhost is always served");

    const real = renderWith(HTTPS, kongDir, true);
    const t = real.text;
    const code = t.split('\n').filter((l) => !/^\s*#/.test(l)).join('\n');
    check(real.status === 0 && /PARSED/.test(real.stdout), `with ${HTTPS} it renders, and Kong parses it${real.status === 0 ? '' : `: ${real.stderr.trim().slice(-200)}`}`);
    check(/hosts: &tenant-hosts \["\*\.eventhub\.localhost", "\*\.example\.org"\]/.test(t), 'the tenant hosts are *.eventhub.localhost and *.example.org');
    check(/hosts: &api-hosts \["api\.eventhub\.localhost", "api\.example\.org"\]/.test(t), 'the api host is api.example.org');
    check(t.includes("'https://[a-z0-9][a-z0-9-]*\\.example\\.org'"), 'the CORS origin is the domain\'s tenants, on https, on its own port');
    check(t.includes('host:match("^([a-z0-9][a-z0-9%-]*)%.example%.org$")'), 'the tenant rule takes the slug from the domain, as a Lua pattern');
    check(code.length > 0 && !/eventhub(\.|\\\.|%\.)com/.test(code), 'and no line that is not a comment names eventhub.com');
    const sslip = renderWith('http://{tenant}.13-54-1-2.sslip.io');
    check(sslip.status === 0 && sslip.text.includes('%.13%-54%-1%-2%.sslip%.io$') && sslip.text.includes("'http://[a-z0-9][a-z0-9-]*\\.13-54-1-2\\.sslip\\.io'"), "a domain with hyphens is escaped for Lua (%-), and an http address is allowed on http only");
    const port = renderWith('https://{tenant}.example.org:8443');
    check(port.status === 0 && port.text.includes("\\.example\\.org:8443'"), 'an address on its own port is allowed on that port');

    const kongTakes = (tpl) => renderWith(tpl).status === 0;
    const disagree = [...TAKEN.filter((x) => !kongTakes(x)), ...REFUSED.filter((x) => kongTakes(x))];
    check(disagree.length === 0, `Kong takes and refuses exactly what publicAddress does, all ${TAKEN.length + REFUSED.length}${disagree.length ? ` -- disagrees on ${disagree.join(', ')}` : ''}`);

    const moved = join(scratch, 'moved');
    cpSync(kongDir, moved, { recursive: true });
    writeFileSync(join(moved, 'kong.yml'), written.replace("        - 'https://[a-z0-9][a-z0-9-]*\\.eventhub\\.com'\n", '').replace("        - 'https://[a-z0-9][a-z0-9-]*\\.eventhub\\.com'\r\n", ''));
    const gone = renderWith(HTTPS, moved);
    check(readFileSync(join(moved, 'kong.yml'), 'utf8') !== written && gone.status !== 0 && /no CORS origin/.test(gone.stderr), `a kong.yml without the CORS origin stops Kong starting (${gone.stderr.trim().split('\n').pop()})`);
    // A fifth place naming eventhub.com, which the render does not know: a route added later.
    const fifth = written.replace('paths: ["/search"]', 'paths: ["/search"]\n      - name: search-old\n        hosts: ["search.eventhub.com"]\n        paths: ["/old-search"]');
    writeFileSync(join(moved, 'kong.yml'), fifth);
    const left = renderWith(HTTPS, moved);
    check(fifth !== written && left.status !== 0 && /still names eventhub\.com/.test(left.stderr), `and so does one that names eventhub.com somewhere the render does not replace (${left.stderr.trim().split('\n').pop()})`);

    // -----------------------------------------------------------------------
    section('Kong serves the domain it was given, and nothing that resembles it');
    const lib = await import(pathToFileURL(join(root, 'scripts', 'lib', 'kong.mjs')).href);
    const echo = await lib.startEcho();
    /** The rendered file with every upstream the echo server, named by its service. */
    const toEcho = (text) => {
      let n = 0;
      const changed = text.replace(/^(\s+url: )http:\/\/([a-z-]+):\d+\s*$/gm, (_, lead, svc) => {
        n += 1;
        return `${lead}http://host.docker.internal:${echo.port}/__upstream/${svc}`;
      });
      return { changed, n };
    };
    const services = (written.match(/^\s+url: http:\/\//gm) ?? []).length;
    const serve = async (name, text) => {
      const { changed, n } = toEcho(text);
      writeFileSync(join(scratch, `${name}.yml`), changed);
      sh('docker', ['rm', '-f', name]);
      const r = sh('docker', ['run', '-d', '--rm', '--name', name, '-e', 'KONG_DATABASE=off', '-e', `KONG_DECLARATIVE_CONFIG=/out/${name}.yml`,
        '-e', 'KONG_PROXY_LISTEN=0.0.0.0:8000', '-e', 'KONG_NGINX_WORKER_PROCESSES=1', '-p', '127.0.0.1::8000',
        '--add-host', 'host.docker.internal:host-gateway', '--add-host', 'redis:127.0.0.1', '-v', `${scratch}:/out:ro`, image]);
      if (r.status !== 0) return { n, error: r.stderr.trim().slice(-300) };
      const proxy = `http://127.0.0.1:${sh('docker', ['port', name, '8000/tcp']).stdout.trim().split('\n')[0].split(':').pop()}`;
      const up = await lib.waitFor(async () => (await lib.send(proxy, '/auth/x', { host: 'acme.eventhub.localhost' })).status === 200, 60_000, 500);
      return { n, proxy, up, stop: () => sh('docker', ['rm', '-f', name]) };
    };
    const node = await serve('verify-public-kong', t);
    check(node.n === services && services > 0 && Boolean(node.up), `CONTROL: Kong runs the rendered file with all ${node.n} of ${services} upstreams on the echo server, and a laptop tenant reaches it${node.error ? `: ${node.error}` : ''}`);
    if (node.up) {
      try {
        echo.seen.length = 0;
        const tenant = await lib.send(node.proxy, '/auth/me', { host: 'acme.example.org' });
        const got = echo.seen.at(-1);
        check(tenant.status === 200 && got?.url === '/__upstream/auth/me' && got?.headers['x-tenant-slug'] === 'acme', `acme.example.org/auth/me reaches Auth as /me, for tenant ${got?.headers['x-tenant-slug'] ?? 'none'} (${tenant.status})`);
        echo.seen.length = 0;
        const forged = await lib.send(node.proxy, '/auth/me', { host: 'acme.example.org', headers: { 'x-tenant-slug': 'globex' } });
        check(forged.status === 200 && echo.seen.at(-1)?.headers['x-tenant-slug'] === 'acme', 'a tenant header the client sends is replaced by the one the host names');
        echo.seen.length = 0;
        const hook = await lib.send(node.proxy, '/webhooks/payments', { host: 'api.example.org', method: 'POST' });
        check(hook.status === 200 && echo.seen.at(-1)?.url === '/__upstream/ticketing/webhooks/payments' && echo.seen.at(-1)?.headers['x-tenant-slug'] === undefined, `api.example.org reaches the payment webhook, with no tenant (${hook.status})`);
        const old = await lib.send(node.proxy, '/auth/me', { host: 'acme.eventhub.com' });
        check(old.status === 404 && old.headers['x-kong-upstream-latency'] === undefined, `acme.eventhub.com is routed nowhere: Kong itself answers ${old.status}`);
        const bare = await lib.send(node.proxy, '/auth/me', { host: 'www.example.org' });
        check(bare.status === 400 && /tenant subdomain required/.test(bare.text), `a reserved name on the domain is refused at the edge (${bare.status})`);
        const two = await lib.send(node.proxy, '/auth/me', { host: 'a.b.example.org' });
        check(two.status === 404 || two.status === 400, `a second label is never a tenant (${two.status})`);
        const preflight = (origin) => lib.send(node.proxy, '/auth/login', { host: 'acme.example.org', method: 'OPTIONS', headers: { origin, 'access-control-request-method': 'POST' } });
        const ok = await preflight('https://acme.example.org');
        check(ok.headers['access-control-allow-origin'] === 'https://acme.example.org', `CORS allows https://acme.example.org (${ok.headers['access-control-allow-origin'] ?? 'no allow-origin'})`);
        for (const origin of ['http://acme.example.org', 'https://acme.example.org.evil.net', 'https://acme.evilexample.org', 'https://acme.eventhub.com']) {
          const r = await preflight(origin);
          check(r.headers['access-control-allow-origin'] !== origin, `and not ${origin} (${r.headers['access-control-allow-origin'] ?? 'no allow-origin'})`);
        }
      } finally {
        node.stop?.();
      }
    }
    const plain = await serve('verify-public-kong-written', asWritten.text);
    if (plain.up) {
      try {
        const r = await lib.send(plain.proxy, '/auth/me', { host: 'acme.example.org' });
        check(r.status === 404, `CONTROL: the same request to kong.yml as written is routed nowhere (${r.status}), so the render is what serves the domain`);
      } finally {
        plain.stop?.();
      }
    } else {
      check(false, `CONTROL: kong.yml as written did not run: ${plain.error ?? 'no answer'}`);
    }
    await echo.close();
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log(`the cluster's address is one line in infra/eks/public.mjs, and Kong, the services and the web app all take it from there`);
}

main().catch((e) => check(false, `the verifier itself failed: ${e.stack || e.message}`));
