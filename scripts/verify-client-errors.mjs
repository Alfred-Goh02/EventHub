/**
 * Asserts that a request body the client got wrong answers 4xx from every HTTP service,
 * and is not logged as a server fault.
 *
 * WHAT WAS BROKEN. Every service ended with its own error middleware that mapped every
 * error to 500 and logged it as "unhandled error". express.json() runs before any route,
 * so a malformed body (`--data '{'`) and one past its 100 kB limit both reached that
 * handler. Measured against the running stack: Auth's /login, Event's /events,
 * Ticketing's /reservations, Community's /spaces, Analytics' /analytics/funnel, Search's
 * /search and Live's /live all answered 500 {"error":"internal error"} to both. The
 * gateway admits bodies up to 1 MB, so anyone could produce a 500, and a line in the
 * error log, on demand. body-parser raises those as http-errors with status 400 or 413
 * and `expose: true`; the handler now in @eventhub/http answers them as such.
 *
 * WHAT THIS DOES, FOR EVERY HTTP SERVICE: boots it from its own .env.example on a spare
 * port, and asserts that a malformed body answers 400, a 200 kB body answers 413, and
 * neither is logged as "unhandled error".
 *
 * THE SERVICES ARE READ FROM services/, not listed here. A service is booted when its
 * source calls express.json(): that is the thing that turns a body into this error. One
 * that creates an Express app without it (Analytics' ingest probes), or has no Express
 * app at all (the relay's node:http metrics listener), cannot be faulted by a body, and
 * is named as skipped with that reason. A body parser anywhere but src/index.ts is a
 * process this does not know how to boot, and fails rather than going unchecked.
 *
 * THE CONTROLS, and what each would look like without it:
 *   - The shared handler is first driven in this process, over real HTTP, by errors it
 *     must still answer 500 and log: a thrown Error, a 4xx without `expose`, and a 5xx
 *     with it. That is the "a real fault is still a 500" control. No service has a route
 *     that faults on demand without either a client-input bug or breaking a dependency
 *     the whole stack shares, so it is provoked here, through the same code every
 *     service mounts, rather than through a service.
 *   - Each port is proved free before it is used, and the probe that proves it is shown
 *     to report a port this process holds as taken. Each service must then answer
 *     /health naming ITSELF, from the process this started: a leftover process on the
 *     port would otherwise answer for it.
 *   - The same path with a well-formed body answers neither 400, 413 nor 500, so the
 *     400 is the body's doing and not the path's. The path is /health: express.json()
 *     is mounted app-wide before every route in each service, so the body is read
 *     whatever the path, and /health is the one path they all have and none takes a POST.
 *   - "Not logged as unhandled error" is only worth something if the run reads the
 *     service's log at all, and reads it after the line would have been written. So each
 *     service's own "client error" lines for the two requests must appear first, and the
 *     absence is checked after them.
 *
 * KAFKA. A service booted from its example would join the deployed consumer groups the
 * example names, and take partitions and messages from the real consumers for as long as
 * it ran: scripts/verify-env-example.mjs measured exactly that. So, as there, every key
 * that names a group is rewritten to one of this run's, every key that says where a new
 * group starts is set to `latest`, and Notification's contact-card group, which has no
 * such setting, is committed at the end of auth.user-contact before it boots. While each
 * service runs, every group the cluster lists is watched, and the check fails if one that
 * is neither this run's nor another verifier's (a name carrying a run's hex tag) gains a
 * member. Every group this run created is deleted at the end, and the count DELETED is
 * reported, not only the count left.
 *
 * Run (with Postgres, Redis, Kafka, the Schema Registry, OpenSearch and ClickHouse up,
 * migrations applied, topics created, both keypairs generated, and every service built):
 *   node scripts/verify-client-errors.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { createServer, connect } from 'node:net';
import { createRequire } from 'node:module';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { serviceEnv } from './lib/service-env.mjs';
import { commitAtEnd, deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';
import { CONTACTS_TOPIC } from './lib/notification-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const RUN = randomBytes(4).toString('hex');
/** A prefix, so the groups Notification derives from its configured name carry it too. */
const RUN_PREFIX = `verify-client-errors-${RUN}-`;

/** Spare ports, one per service, away from the ranges the other verifiers use. */
const PORT_BASE = 3951;

/** Past express.json()'s 100 kB default, and inside the gateway's 1 MB. Well-formed JSON, so only its size is wrong. */
const LARGE_BODY = JSON.stringify({ pad: 'x'.repeat(200 * 1024) });
const PROBE_PATH = '/health';

/** The example keys that name a consumer group, and the ones that say where a new group starts. */
const isGroupKey = (key) => /(^|_)GROUP(_|$)/.test(key);
const isStartKey = (key) => /(^|_)START_FROM$/.test(key);
/**
 * Group keys whose consumer has no start setting and begins a new group at the start of
 * its topics: committed at the end of them before the service boots. Checked below
 * against the examples, so a renamed key is reported rather than silently replaying.
 */
const START_AT_END = { CONTACTS_CONSUMER_GROUP: [CONTACTS_TOPIC] };

/** Neither this run's group nor another verifier's: a deployed group, or one written into the code. */
const isReal = (groupId) => !groupId.startsWith(RUN_PREFIX) && !/[0-9a-f]{8}/.test(groupId);

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** POST `body` as JSON. Resolves to { status, body } or { error }, never rejects. */
async function post(port, path, body, headers = {}) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body,
    });
    return { status: res.status, body: await res.text() };
  } catch (err) {
    return { error: err.cause?.code ?? err.cause?.message ?? err.message };
  }
}

const shown = (r) => (r.error ? `no answer (${r.error})` : `${r.status} ${r.body.replace(/\s+/g, ' ').slice(0, 60)}`);

// ---------------------------------------------------------------------------------------
// Which services, derived from services/
// ---------------------------------------------------------------------------------------

/**
 * TypeScript source with its comments removed, strings kept. A scan that reads comments
 * finds uses that are not uses: several of these files explain express.json() in prose.
 */
function stripComments(src) {
  let out = '';
  for (let i = 0; i < src.length; ) {
    const c = src[i];
    const n = src[i + 1];
    if (c === '/' && n === '/') {
      while (i < src.length && src[i] !== '\n') i++;
    } else if (c === '/' && n === '*') {
      const end = src.indexOf('*/', i + 2);
      i = end === -1 ? src.length : end + 2;
    } else if (c === "'" || c === '"' || c === '`') {
      let j = i + 1;
      while (j < src.length && src[j] !== c) j += src[j] === '\\' ? 2 : 1;
      out += src.slice(i, j + 1);
      i = j + 1;
    } else {
      out += c;
      i++;
    }
  }
  return out;
}

function sourceFiles(dir) {
  const files = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== 'generated' && entry.name !== 'node_modules') files.push(...sourceFiles(path));
    } else if (entry.name.endsWith('.ts') && !entry.name.endsWith('.d.ts')) {
      files.push(path);
    }
  }
  return files;
}

/**
 * Each service, and what it is: booted (its body parser is in src/index.ts), skipped with
 * a reason, or unknown (a body parser this cannot boot), from its source alone.
 */
function classify() {
  const names = readdirSync(join(ROOT, 'services'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(ROOT, 'services', d.name, 'package.json')))
    .map((d) => d.name)
    .sort();
  return names.map((name) => {
    const src = join(ROOT, 'services', name, 'src');
    const code = existsSync(src) ? sourceFiles(src).map((f) => [relative(join(ROOT, 'services', name), f).replaceAll('\\', '/'), stripComments(readFileSync(f, 'utf8'))]) : [];
    const parsers = code.filter(([, c]) => /\bexpress\.json\s*\(/.test(c)).map(([f]) => f);
    const apps = code.filter(([, c]) => /\bexpress\s*\(\s*\)/.test(c)).map(([f]) => f);
    const bodiless = apps.filter((f) => !parsers.includes(f));
    if (parsers.length === 0) {
      return {
        name,
        skip: apps.length
          ? `its Express app${apps.length > 1 ? 's' : ''} (${apps.join(', ')}) read${apps.length > 1 ? '' : 's'} no body: no express.json(), so a body cannot fault it`
          : 'it has no Express app and parses no request body (the relay listens with node:http for its metrics only)',
      };
    }
    const unknown = parsers.filter((f) => f !== 'src/index.ts');
    return { name, unknown, bodiless };
  });
}

// ---------------------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------------------

/** Null when nothing answers on `port` at either loopback and it can be bound; otherwise why not. */
async function portTaken(port) {
  for (const host of ['127.0.0.1', '::1']) {
    const answered = await new Promise((done) => {
      const socket = connect({ port, host });
      socket.setTimeout(1_000, () => {
        socket.destroy();
        done(false);
      });
      socket.once('connect', () => {
        socket.destroy();
        done(true);
      });
      socket.once('error', () => done(false));
    });
    if (answered) return `${host}:${port} accepts connections`;
  }
  const bindError = await new Promise((done) => {
    const server = createServer();
    server.once('error', (err) => done(err.code ?? String(err)));
    server.listen({ port }, () => server.close(() => done(null)));
  });
  return bindError ? `:${port} cannot be bound (${bindError})` : null;
}

// ---------------------------------------------------------------------------------------
// Kafka
// ---------------------------------------------------------------------------------------

/** Every group the cluster lists, with its members, as id to (memberId to "clientId@host"). */
async function allMembers(admin) {
  const ids = (await admin.listGroups()).groups.map((g) => g.groupId);
  if (ids.length === 0) return new Map();
  const { groups } = await admin.describeGroups(ids);
  return new Map(groups.map((g) => [g.groupId, new Map(g.members.map((m) => [m.memberId, `${m.clientId}@${m.clientHost}`]))]));
}

const newcomers = (before, now, g) => [...(now.get(g) ?? new Map())].filter(([id]) => !before.get(g)?.has(id)).map(([, who]) => who);
const gainedMember = (before, now, ids) => ids.filter((g) => newcomers(before, now, g).length > 0);

// ---------------------------------------------------------------------------------------
// Part 1: the shared handler, in this process
// ---------------------------------------------------------------------------------------

async function sharedHandler() {
  console.log('  the shared handler, driven in this process over HTTP');
  console.log('');
  const express = createRequire(join(ROOT, 'packages', 'http', 'package.json'))('express');
  const { errorHandler } = await import(pathToFileURL(join(ROOT, 'packages', 'http', 'dist', 'index.js')).href);

  const lines = [];
  const logger = {
    error: (obj, msg) => lines.push({ level: 'error', obj, msg }),
    info: (obj, msg) => lines.push({ level: 'info', obj, msg }),
  };

  const app = express();
  // Express's own final handler prints every error it is handed to stderr unless the
  // app's env is 'test'; the delegated case below would otherwise print a stack.
  app.set('env', 'test');
  app.use(express.json());
  const thrown = new Error('a genuine fault');
  app.post('/throw', () => {
    throw thrown;
  });
  app.post('/unexposed-4xx', (_req, _res, next) => next(Object.assign(new Error('a 4xx nobody exposed'), { status: 404 })));
  app.post('/exposed-5xx', (_req, _res, next) => next(Object.assign(new Error('a 5xx marked exposed'), { status: 503, expose: true })));
  app.post('/after-headers', (_req, res, next) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.write('partial');
    next(new Error('failed after the headers went'));
  });
  app.use(errorHandler(logger));

  const server = await new Promise((done) => {
    const s = app.listen(0, '127.0.0.1', () => done(s));
  });
  const port = server.address().port;

  try {
    const cases = [
      { path: '/throw', body: '{}', status: 500, error: 'internal error', unhandled: true, what: 'a thrown Error' },
      { path: '/unexposed-4xx', body: '{}', status: 500, error: 'internal error', unhandled: true, what: 'an error with status 404 and no expose' },
      { path: '/exposed-5xx', body: '{}', status: 500, error: 'internal error', unhandled: true, what: 'an error with status 503 and expose: true' },
      { path: '/throw', body: '{', status: 400, error: 'malformed JSON body', unhandled: false, what: 'a malformed body' },
      { path: '/throw', body: LARGE_BODY, status: 413, error: 'request body too large', unhandled: false, what: 'a 200 kB body' },
      {
        path: '/throw',
        body: '{}',
        headers: { 'Content-Encoding': 'compress' },
        status: 415,
        error: 'unsupported content encoding',
        unhandled: false,
        what: 'a body in an encoding body-parser does not read',
      },
    ];
    for (const c of cases) {
      const before = lines.length;
      const r = await post(port, c.path, c.body, c.headers);
      const logged = lines.slice(before);
      const unhandled = logged.filter((l) => l.level === 'error' && l.msg === 'unhandled error');
      let error = null;
      try {
        error = JSON.parse(r.body ?? '').error;
      } catch {
        error = null;
      }
      check(
        r.status === c.status && error === c.error && unhandled.length === (c.unhandled ? 1 : 0),
        `${c.what} answers ${c.status} "${c.error}" and is ${c.unhandled ? '' : 'NOT '}logged as unhandled error ` +
          `(got ${shown(r)}; ${unhandled.length} unhandled line(s): ${logged.map((l) => `${l.level} ${l.msg}`).join(', ') || 'none'})`,
      );
      if (c.path === '/throw' && c.status === 500) {
        check(unhandled[0]?.obj?.err === thrown, 'CONTROL: the unhandled line carries the error that was thrown, not a copy or a message');
      }
    }

    // Once the headers are sent there is no status left to choose: the error must go on to
    // Express, which drops the connection, rather than be answered (Express would throw on
    // a second writeHead) or logged here as if it had been handled.
    const before = lines.length;
    const late = await fetch(`http://127.0.0.1:${port}/after-headers`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
      .then(async (res) => {
        try {
          return { status: res.status, body: await res.text() };
        } catch (err) {
          return { status: res.status, cut: err.cause?.code ?? err.message };
        }
      })
      .catch((err) => ({ error: err.cause?.code ?? err.message }));
    check(
      late.status === 200 && late.cut !== undefined && lines.length === before,
      `an error after the headers went is passed on to Express, which cuts the response, and is not logged here ` +
        `(status ${late.status ?? 'none'}, ${late.cut ? `body cut: ${late.cut}` : `body ${JSON.stringify(late.body ?? late.error)}`}, ${lines.length - before} line(s) logged)`,
    );
  } finally {
    await new Promise((done) => server.close(done));
  }
  console.log('');
}

// ---------------------------------------------------------------------------------------
// Part 2: every HTTP service, booted
// ---------------------------------------------------------------------------------------

/** pino lines in `text`, parsed; a line that is not JSON is kept as { raw }. */
const logLines = (text) =>
  text
    .split(/\r?\n/)
    .filter((l) => l.trim())
    .map((l) => {
      try {
        return JSON.parse(l);
      } catch {
        return { raw: l };
      }
    });

function start(name, env) {
  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', name), env, stdio: ['ignore', 'pipe', 'pipe'] });
  const proc = { child, output: '', code: undefined };
  child.stdout.on('data', (d) => (proc.output += d));
  child.stderr.on('data', (d) => (proc.output += d));
  proc.closed = new Promise((done) =>
    child.on('close', (code) => {
      proc.code = code;
      done(code);
    }),
  );
  return proc;
}

async function stop(proc) {
  if (proc.code === undefined) proc.child.kill();
  await Promise.race([proc.closed, sleep(10_000)]);
}

/** /health from the process this started: it answers 200 and names `name`, and the child is still running. */
async function waitUntilUp(proc, port, name, timeoutMs = 45_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline && proc.code === undefined) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      const body = await res.json().catch(() => null);
      if (res.status === 200 && body?.service === name && proc.code === undefined) return true;
    } catch {
      // not listening yet
    }
    await sleep(300);
  }
  return false;
}

async function bootAndProbe(svc, port, publicKeyB64, kafka, results) {
  const example = serviceEnv(ROOT, svc.name);
  const overrides = { PORT: String(port) };
  if (example['AUTH_PUBLIC_KEY_B64'] === '') overrides['AUTH_PUBLIC_KEY_B64'] = publicKeyB64;
  const groups = [];
  for (const key of Object.keys(example)) {
    if (isGroupKey(key)) {
      overrides[key] = `${RUN_PREFIX}${example[key]}`;
      groups.push(overrides[key]);
    } else if (isStartKey(key)) {
      overrides[key] = 'latest';
    }
  }
  for (const [key, topics] of Object.entries(START_AT_END)) {
    if (key in overrides) await commitAtEnd(kafka.admin, overrides[key], topics);
  }
  const env = serviceEnv(ROOT, svc.name, overrides);
  for (const g of groups) kafka.configured.add(g);

  console.log(`  ${svc.name} on :${port}${groups.length ? ` (consumer groups: ${groups.map((g) => g.slice(RUN_PREFIX.length)).join(', ')}, as this run's)` : ''}`);
  const before = await kafka.allMembers();
  const proc = start(svc.name, env);
  kafka.started.push(proc);
  try {
    const up = await waitUntilUp(proc, port, svc.name);
    const head = proc.output.trim().split(/\r?\n/).slice(0, 6).join('\n          ');
    check(up, up ? `CONTROL: ${svc.name} answers /health on :${port} naming itself, from the process this run started` : `${svc.name} never came up on :${port} (${proc.code === undefined ? 'still running' : `exited ${proc.code}`}):\n          ${head}`);
    if (!up) {
      results.push({ name: svc.name, malformed: 'down', large: 'down' });
      return;
    }
    const listening = logLines(proc.output).some((l) => typeof l.msg === 'string' && l.msg.includes(`listening on port ${port}`));
    check(listening, `CONTROL: its log is being read: its "listening on port ${port}" line is in what this run captured`);

    const from = proc.output.length;
    const malformed = await post(port, PROBE_PATH, '{');
    const large = await post(port, PROBE_PATH, LARGE_BODY);
    const wellFormed = await post(port, PROBE_PATH, '{}');
    results.push({ name: svc.name, malformed: malformed.status ?? malformed.error, large: large.status ?? large.error });

    const errorOf = (r) => {
      try {
        return JSON.parse(r.body).error;
      } catch {
        return undefined;
      }
    };
    check(malformed.status === 400 && errorOf(malformed) === 'malformed JSON body', `a malformed JSON body answers 400 "malformed JSON body" (got ${shown(malformed)})`);
    check(large.status === 413 && errorOf(large) === 'request body too large', `a 200 kB body answers 413 "request body too large" (got ${shown(large)})`);
    check(
      wellFormed.status !== undefined && ![400, 413, 500].includes(wellFormed.status),
      `CONTROL: the same path with a well-formed body answers neither 400, 413 nor 500, so the 400 is the body's doing (got ${shown(wellFormed)})`,
    );

    // The service's own lines for the two requests first, so the absence below is read
    // after the point where an "unhandled error" would have been written.
    const deadline = Date.now() + 5_000;
    let since = [];
    for (;;) {
      since = logLines(proc.output.slice(from));
      const statuses = since.filter((l) => l.msg === 'client error').map((l) => l.status);
      if ((statuses.includes(400) && statuses.includes(413)) || Date.now() >= deadline) break;
      await sleep(100);
    }
    await sleep(300);
    since = logLines(proc.output.slice(from));
    const clientLines = since.filter((l) => l.msg === 'client error');
    const unhandled = since.filter((l) => l.msg === 'unhandled error' || (l.raw ?? '').includes('unhandled error'));
    check(
      clientLines.some((l) => l.status === 400 && l.level === 'info') && clientLines.some((l) => l.status === 413 && l.level === 'info'),
      `CONTROL: the log shows both requests reached the handler, at info (${clientLines.map((l) => `${l.level} ${l.status} ${l.type}`).join(', ') || 'no "client error" line'})`,
    );
    check(
      unhandled.length === 0,
      unhandled.length === 0
        ? 'neither is logged as "unhandled error"'
        : `${unhandled.length} line(s) logged as "unhandled error": ${unhandled.map((l) => `${l.level ?? ''} ${l.err?.type ?? l.raw ?? ''}`.trim()).join('; ')}`,
    );

    /**
     * NOT A MEMBER OF ANY REAL GROUP, read while the process is alive. It is given until
     * each of its groups has a member and the set of groups gaining members stops
     * growing, three seconds at least, so its consumers have joined SOMEWHERE before this
     * looks. A process with no consumer (Analytics' HTTP side, whose example names the
     * ingest's group) waits the time out.
     */
    const started = Date.now();
    let now = before;
    let settled = '';
    while (Date.now() < started + 15_000) {
      now = await kafka.allMembers();
      const moving = gainedMember(before, now, [...now.keys()]).sort().join(',');
      if (gainedMember(before, now, groups).length === groups.length && moving === settled && Date.now() - started >= 3_000) break;
      settled = moving;
      await sleep(1_000);
    }
    const ours = gainedMember(before, now, [...now.keys()].filter((g) => g.startsWith(RUN_PREFIX))).sort();
    for (const g of ours) kafka.joined.add(g);
    const real = gainedMember(before, now, [...now.keys()].filter(isReal)).sort();
    check(
      real.length === 0,
      real.length === 0
        ? `it joined no real consumer group (${ours.length ? `it consumed as this run's ${ours.map((g) => g.slice(RUN_PREFIX.length)).join(', ')}` : 'it joined no group'})`
        : `it JOINED ${real.map((g) => `${g} (as ${newcomers(before, now, g).join(', ')})`).join(', ')}, taking partitions and messages from the real consumers`,
    );
  } finally {
    await stop(proc);
    console.log('');
  }
}

async function main() {
  console.log('  a request body the client got wrong is the client\'s fault, in every HTTP service');
  console.log('');

  await sharedHandler();

  console.log('  the services, from services/');
  console.log('');
  const classified = classify();
  const booted = classified.filter((s) => !s.skip);
  check(booted.length > 0, `services/ holds ${classified.length} service(s); ${booted.length} parse JSON bodies: ${booted.map((s) => s.name).join(', ')}`);
  for (const s of classified.filter((c) => c.skip)) console.log(`          skipped ${s.name}: ${s.skip}`);
  for (const s of booted.filter((c) => c.bodiless.length)) console.log(`          ${s.name} also runs ${s.bodiless.join(', ')}, an Express app with no express.json(): a body cannot fault it`);
  const unknown = booted.filter((s) => s.unknown.length);
  check(
    unknown.length === 0,
    unknown.length === 0
      ? 'and each one parses them in src/index.ts, the entrypoint this boots'
      : `${unknown.map((s) => `${s.name} (${s.unknown.join(', ')})`).join(', ')} parse${unknown.length === 1 ? 's' : ''} bodies outside src/index.ts, which this does not boot`,
  );
  console.log('');

  const keyPath = join(ROOT, 'services', 'auth', 'keys', 'public.pem');
  if (!existsSync(keyPath)) {
    check(false, `${relative(ROOT, keyPath)} is missing: generate the keypairs first`);
    return;
  }
  const publicKeyB64 = readFileSync(keyPath).toString('base64');

  console.log('  ports');
  console.log('');
  const holder = createServer();
  await new Promise((done) => holder.listen({ port: 0 }, done));
  const held = holder.address().port;
  const seen = await portTaken(held);
  await new Promise((done) => holder.close(done));
  check(seen !== null, `CONTROL: the free-port probe reports a port this process holds as taken (:${held}: ${seen ?? 'reported FREE'})`);
  const ports = booted.map((_, i) => PORT_BASE + i);
  const taken = (await Promise.all(ports.map(portTaken))).filter(Boolean);
  check(taken.length === 0, taken.length === 0 ? `:${ports[0]}-${ports[ports.length - 1]} are free` : `not free: ${taken.join('; ')} — stop what holds them, then rerun`);
  console.log('');
  if (taken.length > 0 || seen === null) return;

  const examples = booted.map((s) => serviceEnv(ROOT, s.name));
  const brokers = examples.find((e) => e['KAFKA_BROKERS'])?.['KAFKA_BROKERS'];
  const stale = Object.keys(START_AT_END).filter((k) => !examples.some((e) => k in e));
  check(
    Boolean(brokers) && stale.length === 0,
    `the examples name the brokers (${brokers ?? 'none'}), and every group key committed at the end of its topics before boot is set by one ` +
      `(${stale.length ? `NOT: ${stale.join(', ')}` : Object.keys(START_AT_END).join(', ')})`,
  );
  console.log('');

  const admin = new Kafka({ clientId: `verify-client-errors-${RUN}`, brokers: (brokers ?? '').split(','), logLevel: logLevel.NOTHING }).admin();
  await admin.connect();
  const kafka = { admin, allMembers: () => allMembers(admin), configured: new Set(), joined: new Set(), started: [] };
  const results = [];

  try {
    for (const [i, svc] of booted.entries()) await bootAndProbe(svc, ports[i], publicKeyB64, kafka, results);

    console.log('  across the run');
    console.log('');
    for (const r of results) console.log(`          ${r.name.padEnd(13)} malformed -> ${r.malformed}   200 kB -> ${r.large}`);
    console.log('');
    const joined = [...kafka.joined].sort();
    const unseen = [...kafka.configured].filter((g) => !kafka.joined.has(g)).sort();
    check(
      joined.length > 0,
      `CONTROL: ${joined.length} of this run's groups gained a member while their service ran, so a join to a real group would have been seen` +
        `${unseen.length ? ` (configured and never joined, by a process with no consumer for it: ${unseen.map((g) => g.slice(RUN_PREFIX.length)).join(', ')})` : ''}`,
    );
  } finally {
    for (const proc of kafka.started) await stop(proc);
    const alive = kafka.started.filter((p) => p.code === undefined).map((p) => p.child.pid);
    console.log('');
    check(
      kafka.started.length > 0 && alive.length === 0,
      `cleanup: every one of the ${kafka.started.length} service process(es) this run started has exited${alive.length ? `, but NOT pid ${alive.join(', ')}` : ''}`,
    );
    /**
     * Deleted, and reported by what was DELETED: "0 left" is also what a cleanup that
     * found nothing prints. A killed consumer stays a member until its session times out,
     * so each group is retried until it can go.
     */
    const ours = (await admin.listGroups()).groups.map((g) => g.groupId).filter((g) => g.startsWith(RUN_PREFIX)).sort();
    const left = await deleteGroupsWhenEmpty(admin, ours, { timeoutMs: 90_000 });
    const deleted = ours.filter((g) => !left.includes(g));
    check(
      deleted.length > 0 && left.length === 0,
      `cleanup: deleted this run's ${deleted.length} consumer group(s)${deleted.length ? ` (${deleted.map((g) => g.slice(RUN_PREFIX.length)).join(', ')})` : ''}, ` +
        `${left.length} left${left.length ? ` (${left.join(', ')})` : ''}`,
    );
    await admin.disconnect().catch(() => {});
  }
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(() => {
    console.log('');
    if (process.exitCode) {
      console.error(`FAIL: ${failures} assertion(s) failed`);
    } else {
      console.log('every HTTP service answers a malformed body 400 and an oversized one 413, and logs neither as a fault');
    }
    // kafkajs can hold the event loop open after disconnect.
    process.exit(process.exitCode ?? 0);
  });
