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
 * TEXT POSTGRES CANNOT STORE, the same shape of bug one layer in. Postgres TEXT cannot
 * hold U+0000 (SQLSTATE 22021), and nothing tested errorHandler can make marks that
 * refusal as the client's, so measured through the gateway as a tenant admin: Event's
 * POST /events with a NUL in the title and GET /events/search?q=a%00b, and Ticketing's
 * POST /reservations with a NUL in the event id, answered 500 "internal error".
 * Community's POST /spaces with a NUL in the name answered 409 "a space with that slug
 * already exists" for a slug nobody had, because it read Prisma's P2010 -- ANY failed
 * raw query -- as a duplicate. A lone surrogate fails nowhere: the driver stores U+FFFD
 * in its place, silently. @eventhub/http's refuseUnstorableText now refuses both, in the
 * body and the query string, with 400 and fixed text; Community's create now answers 409
 * only for Postgres's 23505.
 *
 * WHAT THIS DOES, FOR EVERY HTTP SERVICE: boots it from its own .env.example on a spare
 * port, and asserts that a malformed body answers 400, a 200 kB body answers 413, a NUL
 * or a lone surrogate in a JSON string and a NUL in the query string answer 400 with
 * fixed text, and none of them is logged as "unhandled error". Then, on the three
 * services where such text was measured reaching Postgres, the routes it reached, with
 * tokens minted here: see ROUTE_PROBES.
 *
 * THE SERVICES ARE READ FROM services/, not listed here. A service is booted when its
 * source calls express.json(): that is the thing that turns a body into this error. One
 * that creates an Express app without it (Analytics' ingest probes), or has no Express
 * app at all (the relay's node:http metrics listener), cannot be faulted by a body, and
 * is named as skipped with that reason. A body parser anywhere but src/index.ts is a
 * process this does not know how to boot, and fails rather than going unchecked. So does
 * a query string read where no body parser is: refuseUnstorableText checks req.query as
 * well, and a service reading one without express.json() would be skipped by the rule
 * above while still taking text to a store.
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
 *     service's own "client error" lines for the refused requests must appear first, and
 *     the absence is checked after them.
 *   - The lone surrogate is sent as the JSON escape `\ud800`, and the run asserts the
 *     body it sends really contains that escape. Sent as raw bytes, the surrogate would
 *     be decoded as U+FFFD before any check saw it, and the probe would pass with the
 *     refusal deleted.
 *   - Storable text is let through: a query string and a body with accented letters and
 *     an emoji -- a surrogate PAIR, which a careless well-formedness test refuses -- reach
 *     the route, in this process and in every service.
 *   - Each route probe first shows its token reaching the store-backed route it uses,
 *     so a refusal is the text's doing and not a token the service would not accept.
 *     Community's duplicate check is shown both ways: a create that fails for a reason
 *     other than a duplicate must not answer 409, the same slug then creates (so the 409
 *     would have been false), and a REAL duplicate still answers 409.
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
 * DATABASE. Every route probe acts in a tenant made up for this run. The only rows it can
 * leave are Community's space and membership from the duplicate check, deleted at the end
 * as the owner with that tenant set -- the context they were written in -- and reported
 * by the count deleted. The refused writes leave nothing, by construction or by rollback.
 *
 * Run (with Postgres, Redis, Kafka, the Schema Registry, OpenSearch and ClickHouse up,
 * migrations applied, topics created, both keypairs generated, and every service built):
 *   node scripts/verify-client-errors.mjs
 */

import { spawn } from 'node:child_process';
import { createHmac, createPrivateKey, randomBytes, sign } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { createServer, connect } from 'node:net';
import { createRequire } from 'node:module';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
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
/**
 * Where a URL that does not percent-decode reaches Express's parameter decoding before
 * any authentication. Hand-written: which routes have parameters ahead of their auth is
 * a fact about each router. Event's /events/:id is the one measured answering 500.
 */
const URL_PROBES = {
  event: { bad: '/events/%E0%A4%A', fine: '/events/evt_probe' },
};

/** The fixed answers refuseUnstorableText's two refusals are given, from @eventhub/http's CLIENT_ERRORS. */
const UNSTORABLE = 'request text cannot be stored: it contains a NUL character or a lone surrogate';
const UNCHECKABLE = 'request has too many values to check';
/** A NUL in a JSON string. JSON.stringify writes it as the escape \u0000, which JSON.parse turns back into U+0000. */
const NUL_BODY = JSON.stringify({ name: 'a\u0000b' });
/**
 * A lone surrogate in a JSON string, which JSON.stringify keeps as the escape \ud800.
 * It must go as the escape: as raw text, fetch would encode it as UTF-8, which has no
 * surrogates, so it would arrive as U+FFFD and test nothing. Asserted before use.
 */
const SURROGATE_BODY = JSON.stringify({ name: '\ud800x' });
/** Storable text that a careless check refuses: accented letters, CJK, and an emoji, which is a surrogate PAIR. */
const STORABLE_BODY = JSON.stringify({ name: 'café 中 😀' });
/** The tenant every route probe acts in: made up for this run, so nothing it reads or writes is anyone else's. */
const TENANT = `tenant_vce_${RUN}`;

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

/** Any request; a JSON body when `body` is given. Resolves to { status, body } or { error }, never rejects. */
async function send(port, method, path, { body, headers = {} } = {}) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, {
      method,
      headers: body === undefined ? headers : { 'Content-Type': 'application/json', ...headers },
      body,
    });
    return { status: res.status, body: await res.text() };
  } catch (err) {
    return { error: err.cause?.code ?? err.cause?.message ?? err.message };
  }
}

const shown = (r) => (r.error ? `no answer (${r.error})` : `${r.status} ${r.body.replace(/\s+/g, ' ').slice(0, 60)}`);

/** The `error` field of a JSON answer, or undefined. */
const errorOf = (r) => {
  try {
    return JSON.parse(r.body).error;
  } catch {
    return undefined;
  }
};

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
    /**
     * Reads a query string or a body. A service that does with no express.json() is one
     * this does not boot, so its query strings would go unchecked: it fails rather than
     * being skipped. So does an Express app with no body parser that reads one itself.
     */
    const readers = code.filter(([, c]) => /\breq\.(query|body)\b/.test(c)).map(([f]) => f);
    if (parsers.length === 0) {
      if (readers.length > 0) return { name, unknown: readers, bodiless, readsWithoutParser: true };
      return {
        name,
        skip: apps.length
          ? `its Express app${apps.length > 1 ? 's' : ''} (${apps.join(', ')}) read${apps.length > 1 ? '' : 's'} no body and no query string: no express.json() and no req.query, so neither can fault it`
          : 'it has no Express app and parses no request body (the relay listens with node:http for its metrics only)',
      };
    }
    const unknown = [...parsers.filter((f) => f !== 'src/index.ts'), ...bodiless.filter((f) => readers.includes(f))];
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
  const { errorHandler, refuseUnstorableText } = await import(pathToFileURL(join(ROOT, 'packages', 'http', 'dist', 'index.js')).href);

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
  // Mounted as every service mounts it: directly after the body parser.
  app.use(refuseUnstorableText());
  // Counts the requests that got past it, so a refusal is shown to stop the request
  // and a pass is shown to reach the route.
  let reached = 0;
  app.all('/echo', (_req, res) => {
    reached += 1;
    res.json({ reached: true });
  });
  const thrown = new Error('a genuine fault');
  app.post('/throw', () => {
    throw thrown;
  });
  app.post('/unexposed-4xx', (_req, _res, next) => next(Object.assign(new Error('a 4xx nobody exposed'), { status: 404 })));
  app.post('/exposed-5xx', (_req, _res, next) => next(Object.assign(new Error('a 5xx marked exposed'), { status: 503, expose: true })));
  // A route with a parameter, for Express's own URIError, and a plain unexposed 400 to
  // show that error is let through by what it is, not by its status.
  app.post('/item/:id', () => {
    throw thrown;
  });
  app.post('/unexposed-400', (_req, _res, next) => next(Object.assign(new Error('a 400 nobody exposed'), { status: 400 })));
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
      { path: '/unexposed-400', body: '{}', status: 500, error: 'internal error', unhandled: true, what: "a plain Error with status 400 and no expose (only Express's URIError is let through)" },
      { path: '/item/%E0%A4%A', body: '{}', status: 400, error: 'malformed URL', unhandled: false, what: 'a route parameter that is not valid percent-encoding' },
      { path: '/item/abc', body: '{}', status: 500, error: 'internal error', unhandled: true, what: 'CONTROL: the same route with a parameter that decodes, which reaches the handler' },
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

    // Text Postgres cannot store, anywhere in the body or the query string, keys included.
    // The NUL that comes first in its array is the last value the walk reaches, after
    // nine thousand clean ones: a walk that stopped early would let it through.
    console.log('');
    const textCases = [
      { what: 'a NUL in a string nested in an object in an array', body: JSON.stringify({ a: { b: ['fine', 'x\u0000y'] } }), type: 'text.unstorable' },
      { what: 'a NUL in an object KEY', body: JSON.stringify({ 'a\u0000b': 1 }), type: 'text.unstorable' },
      { what: 'a NUL in a key of an object inside an array', body: JSON.stringify([{ ok: 1 }, { 'k\u0000': 2 }]), type: 'text.unstorable' },
      { what: 'a NUL behind 9 000 clean strings, the last value the walk reaches', body: JSON.stringify(['x\u0000', ...Array(9_000).fill('ok')]), type: 'text.unstorable' },
      { what: 'a lone high surrogate, sent as the escape \\ud800, in an array', body: JSON.stringify(['\ud800']), type: 'text.unstorable' },
      { what: 'a lone low surrogate in a key', body: JSON.stringify({ '\udc00': 1 }), type: 'text.unstorable' },
      { what: 'a surrogate pair in the wrong order', body: JSON.stringify({ t: '\udc00\ud800' }), type: 'text.unstorable' },
      { what: 'a NUL in a query value', method: 'GET', path: '/echo?q=a%00b', type: 'text.unstorable' },
      { what: 'a NUL in a query KEY', method: 'GET', path: '/echo?a%00b=1', type: 'text.unstorable' },
      { what: 'a NUL nested in the query, q[x][y]', method: 'GET', path: '/echo?q%5Bx%5D%5By%5D=%00', type: 'text.unstorable' },
      { what: 'a clean body with a NUL in the query', path: '/echo?q=%00', body: '{"fine":true}', type: 'text.unstorable' },
      { what: '10 001 values, one past the bound', body: JSON.stringify(Array(10_001).fill(0)), type: 'text.uncheckable' },
      { what: 'a body nested 49 000 arrays deep, which a recursive walk would answer with a stack overflow', body: '['.repeat(49_000) + ']'.repeat(49_000), type: 'text.uncheckable' },
      { what: 'CONTROL: accented letters, CJK and an emoji (a surrogate PAIR), in values and keys', body: JSON.stringify({ t: 'café 😀', 'kéy': ['中'] }), type: null },
      { what: 'CONTROL: the six characters \\u0000 as text, which are not a NUL', body: JSON.stringify({ t: '\\u0000' }), type: null },
      { what: 'CONTROL: accented text in a query value and key', method: 'GET', path: '/echo?q=caf%C3%A9&n%C3%A9=1', type: null },
      { what: 'CONTROL: 10 000 values, exactly the bound', body: JSON.stringify(Array(10_000).fill(0)), type: null },
    ];
    for (const c of textCases) {
      const linesBefore = lines.length;
      const reachedBefore = reached;
      const r = await send(port, c.method ?? 'POST', c.path ?? '/echo', c.body === undefined ? {} : { body: c.body });
      const logged = lines.slice(linesBefore);
      const unhandled = logged.filter((l) => l.msg === 'unhandled error');
      const went = reached > reachedBefore;
      if (c.type === null) {
        check(
          r.status === 200 && went && logged.length === 0,
          `${c.what}: reaches the route (got ${shown(r)}; ${went ? 'reached' : 'NOT reached'}; ${logged.length} line(s) logged)`,
        );
        continue;
      }
      const answer = c.type === 'text.unstorable' ? UNSTORABLE : UNCHECKABLE;
      const client = logged.filter((l) => l.level === 'info' && l.msg === 'client error' && l.obj?.status === 400 && l.obj?.type === c.type);
      // The line carries what errorHandler logs for every client error, and nothing of the text.
      const bare = client.length === 1 && Object.keys(client[0].obj).sort().join(',') === 'method,status,type';
      check(
        r.status === 400 && errorOf(r) === answer && !went && bare && unhandled.length === 0,
        `${c.what}: 400 "${answer}", never reaching the route, logged once at info as a ${c.type} client error and not as unhandled ` +
          `(got ${shown(r)}; ${went ? 'REACHED the route' : 'not reached'}; logged: ${logged.map((l) => `${l.level} ${l.msg} ${JSON.stringify(l.obj)}`).join('; ') || 'nothing'})`,
      );
    }
  } finally {
    await new Promise((done) => server.close(done));
  }
  console.log('');
}

// ---------------------------------------------------------------------------------------
// The routes where unstorable text was measured reaching Postgres
// ---------------------------------------------------------------------------------------

let signingKey = null;
/** A genuine access token from Auth's private key, for this run's tenant, with `env`'s issuer and audience. */
function mint(env, sub, role) {
  signingKey ??= createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
  const b64url = (v) => Buffer.from(v).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64url(JSON.stringify({ sub, tenantId: TENANT, role, iss: env.JWT_ISSUER, aud: env.JWT_AUDIENCE, iat: now, exp: now + 600 }));
  return `Bearer ${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), signingKey))}`;
}

/**
 * Event: both measured 500s. The token is first shown to reach Postgres through the
 * search route, and to pass POST /events' role check, by a body its schema refuses: so a
 * 400 below is the text's doing, and in a run without the refusal the 500 is reachable.
 */
async function probeEvent({ port, env }) {
  const headers = { Authorization: mint(env, `u_vce_${RUN}`, 'ORGANIZER') };
  const search = await send(port, 'GET', '/events/search?q=probe', { headers });
  check(search.status === 200, `CONTROL: GET /events/search?q=probe with this run's token reaches Postgres and answers 200 (got ${shown(search)})`);
  const schema = await send(port, 'POST', '/events', { headers, body: JSON.stringify({ title: 'no capacity' }) });
  check(schema.status === 400 && errorOf(schema) !== UNSTORABLE, `CONTROL: POST /events with this token passes its role check and reaches the schema, which refuses a body with no capacity (got ${shown(schema)})`);

  const nulSearch = await send(port, 'GET', '/events/search?q=a%00b', { headers });
  check(nulSearch.status === 400 && errorOf(nulSearch) === UNSTORABLE, `GET /events/search?q=a%00b answers 400 with the fixed text; measured before: 500 (got ${shown(nulSearch)})`);
  const body = JSON.stringify({ title: 'probe a\u0000b', capacity: 10, startsAt: '2030-01-01T10:00:00Z', endsAt: '2030-01-01T12:00:00Z' });
  const nulTitle = await send(port, 'POST', '/events', { headers, body });
  check(nulTitle.status === 400 && errorOf(nulTitle) === UNSTORABLE, `POST /events with a NUL in the title answers 400 with the fixed text; measured before: 500 (got ${shown(nulTitle)})`);
}

/**
 * Ticketing: the measured reservation, and the webhook, which is not exempt. The webhook
 * is driven with signatures made here from the example's secret: a clean signed body
 * must still be accepted -- refuseUnstorableText leaves the raw bytes the signature is
 * checked over alone -- and a wrongly signed one still refused, before the signed NUL is
 * asked about.
 */
async function probeTicketing({ port, env }) {
  const headers = { Authorization: mint(env, `u_vce_${RUN}`, 'ATTENDEE') };
  const whoami = await send(port, 'GET', '/whoami', { headers });
  let tenant = null;
  try {
    tenant = JSON.parse(whoami.body ?? '').tenantId;
  } catch {
    tenant = null;
  }
  check(whoami.status === 200 && tenant === TENANT, `CONTROL: this run's token is accepted, as tenant ${TENANT} (got ${shown(whoami)})`);
  const items = [{ ticketTypeId: 'tt_probe', quantity: 1 }];
  const keyless = await send(port, 'POST', '/reservations', { headers, body: JSON.stringify({ eventId: 'evt_probe', items }) });
  check(
    keyless.status === 400 && errorOf(keyless) === 'Idempotency-Key header is required',
    `CONTROL: POST /reservations with it reaches the route's own checks, which want an Idempotency-Key (got ${shown(keyless)})`,
  );
  const nulReservation = await send(port, 'POST', '/reservations', {
    headers: { ...headers, 'Idempotency-Key': `vce-${RUN}` },
    body: JSON.stringify({ eventId: 'evt_a\u0000b', items }),
  });
  check(
    nulReservation.status === 400 && errorOf(nulReservation) === UNSTORABLE,
    `POST /reservations with a NUL in the event id answers 400 with the fixed text; measured before: 500 (got ${shown(nulReservation)})`,
  );

  const signature = (raw, secret = env.PAYMENT_WEBHOOK_SECRET) => {
    const t = Math.floor(Date.now() / 1000);
    return { 'X-Payment-Signature': `t=${t},v1=${createHmac('sha256', secret).update(`${t}.${raw}`).digest('hex')}` };
  };
  const clean = JSON.stringify({ paymentId: `pay_vce_${RUN}`, type: 'charge.failed' });
  const accepted = await send(port, 'POST', '/webhooks/payments', { headers: signature(clean), body: clean });
  let ignored = null;
  try {
    ignored = JSON.parse(accepted.body ?? '').ignored;
  } catch {
    ignored = null;
  }
  check(
    accepted.status === 200 && ignored === 'unknown payment',
    `CONTROL: a correctly signed webhook still verifies over the raw body, and is looked up (got ${shown(accepted)})`,
  );
  const forged = await send(port, 'POST', '/webhooks/payments', { headers: signature(clean, 'not-the-secret'), body: clean });
  check(forged.status === 400 && errorOf(forged) === 'signature mismatch', `CONTROL: a wrongly signed one is still refused by the signature check (got ${shown(forged)})`);
  const nul = JSON.stringify({ paymentId: `pay_vce_${RUN}\u0000`, type: 'charge.failed' });
  const signedNul = await send(port, 'POST', '/webhooks/payments', { headers: signature(nul), body: nul });
  check(
    signedNul.status === 400 && errorOf(signedNul) === UNSTORABLE,
    `a correctly signed webhook with a NUL in paymentId answers 400, "do not retry", with the fixed text (got ${shown(signedNul)})`,
  );
}

/**
 * Community: the measured 409, and the reason for it. A create that fails for a reason
 * other than a duplicate must not answer "a space with that slug already exists"; the
 * failure here is a NUL in the token's subject, which no body check can see and Postgres
 * refuses (22021) on the membership insert, so it reaches the catch whatever the
 * middleware does. The same slug must then create, so the 409 would have been false, and
 * creating it again is the REAL duplicate, which must still answer 409.
 */
async function probeCommunity({ port, env }) {
  const slug = `vce-${RUN}`;
  const asOwner = { Authorization: mint(env, `u_vce_owner_${RUN}`, 'ATTENDEE') };
  const asNamer = { Authorization: mint(env, `u_vce_namer_${RUN}`, 'ATTENDEE') };
  const asUnstorable = { Authorization: mint(env, `u_vce_${RUN}\u0000`, 'ATTENDEE') };
  const space = (headers, s, name) => send(port, 'POST', '/spaces', { headers, body: JSON.stringify({ slug: s, name }) });
  try {
    const nulName = await space(asNamer, `vce-nul-${RUN}`, 'a\u0000b');
    check(
      nulName.status === 400 && errorOf(nulName) === UNSTORABLE,
      `POST /spaces with a NUL in the name answers 400 with the fixed text; measured before: 409 "a space with that slug already exists" (got ${shown(nulName)})`,
    );
    const failed = await space(asUnstorable, slug, 'a space');
    check(
      failed.status === 500 && errorOf(failed) === 'internal error',
      `a create whose insert fails for a reason other than a duplicate (Postgres's 22021) answers 500 "internal error", not 409 (got ${shown(failed)})`,
    );
    const created = await space(asOwner, slug, 'a space');
    check(created.status === 201, `CONTROL: the same slug then creates, so a 409 for it would have been false (got ${shown(created)})`);
    const duplicate = await space(asOwner, slug, 'a space');
    check(
      duplicate.status === 409 && errorOf(duplicate) === 'a space with that slug already exists',
      `CONTROL: a REAL duplicate still answers 409 "a space with that slug already exists" (got ${shown(duplicate)})`,
    );
  } finally {
    // As the owner, with this run's tenant set: the context the rows were written in.
    // FORCE row security binds the owner, so without the tenant this would delete
    // nothing, and the delete count is what shows it did not.
    const db = new pg.Client({ connectionString: env.DATABASE_URL });
    try {
      await db.connect();
      await db.query('BEGIN');
      await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
      const members = await db.query(`DELETE FROM "Membership" WHERE "tenantId" = $1`, [TENANT]);
      const spaces = await db.query(`DELETE FROM "CommunitySpace" WHERE "tenantId" = $1`, [TENANT]);
      const left = await db.query(
        `SELECT (SELECT count(*) FROM "Membership" WHERE "tenantId" = $1) + (SELECT count(*) FROM "CommunitySpace" WHERE "tenantId" = $1) AS n`,
        [TENANT],
      );
      await db.query('COMMIT');
      check(
        spaces.rowCount > 0 && members.rowCount > 0 && Number(left.rows[0].n) === 0,
        `cleanup: deleted ${spaces.rowCount} space(s) and ${members.rowCount} membership(s) in ${TENANT}, ${left.rows[0].n} left`,
      );
    } catch (err) {
      await db.query('ROLLBACK').catch(() => {});
      check(false, `cleanup of ${TENANT} in Community failed: ${err.message}`);
    } finally {
      await db.end().catch(() => {});
    }
  }
}

/** Hand-written, like URL_PROBES: which route took such text to a store is a fact about each router. */
const ROUTE_PROBES = { event: probeEvent, ticketing: probeTicketing, community: probeCommunity };

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
    const nul = await post(port, PROBE_PATH, NUL_BODY);
    const surrogate = await post(port, PROBE_PATH, SURROGATE_BODY);
    const nulQuery = await send(port, 'GET', `${PROBE_PATH}?q=a%00b`);
    const storable = await post(port, PROBE_PATH, STORABLE_BODY);
    const storableQuery = await send(port, 'GET', `${PROBE_PATH}?q=caf%C3%A9`);
    const at = (r) => r.status ?? r.error;
    results.push({ name: svc.name, malformed: at(malformed), large: at(large), nul: at(nul), surrogate: at(surrogate), query: at(nulQuery) });

    check(malformed.status === 400 && errorOf(malformed) === 'malformed JSON body', `a malformed JSON body answers 400 "malformed JSON body" (got ${shown(malformed)})`);
    check(large.status === 413 && errorOf(large) === 'request body too large', `a 200 kB body answers 413 "request body too large" (got ${shown(large)})`);
    check(
      wellFormed.status !== undefined && ![400, 413, 500].includes(wellFormed.status),
      `CONTROL: the same path with a well-formed body answers neither 400, 413 nor 500, so the 400 is the body's doing (got ${shown(wellFormed)})`,
    );
    check(nul.status === 400 && errorOf(nul) === UNSTORABLE, `a NUL in a JSON string answers 400 with the fixed text (got ${shown(nul)})`);
    check(surrogate.status === 400 && errorOf(surrogate) === UNSTORABLE, `a lone surrogate in a JSON string answers 400 with the fixed text (got ${shown(surrogate)})`);
    check(nulQuery.status === 400 && errorOf(nulQuery) === UNSTORABLE, `a NUL in the query string answers 400 with the fixed text (got ${shown(nulQuery)})`);
    check(
      storable.status !== undefined && ![400, 413, 500].includes(storable.status),
      `CONTROL: a body of accented letters, CJK and an emoji is let through, answering as '{}' does (got ${shown(storable)})`,
    );
    let storableService = null;
    try {
      storableService = JSON.parse(storableQuery.body ?? '').service;
    } catch {
      storableService = null;
    }
    check(
      storableQuery.status === 200 && storableService === svc.name,
      `CONTROL: the same GET with an accented query string reaches /health and is answered by ${svc.name} (got ${shown(storableQuery)})`,
    );

    // A URL Express cannot percent-decode, on a service whose parameter route decodes it
    // before any authentication: measured, Event answered 500 to it with no token. The
    // control is the same route with a parameter that decodes, which reaches requireAuth.
    if (URL_PROBES[svc.name]) {
      const get = (path) =>
        fetch(`http://127.0.0.1:${port}${path}`).then(
          async (r) => ({ status: r.status, body: await r.text() }),
          (err) => ({ error: err.cause?.code ?? err.message }),
        );
      const bad = await get(URL_PROBES[svc.name].bad);
      const fine = await get(URL_PROBES[svc.name].fine);
      check(bad.status === 400 && errorOf(bad) === 'malformed URL', `${URL_PROBES[svc.name].bad} answers 400 "malformed URL" (got ${shown(bad)})`);
      check(fine.status === 401, `CONTROL: ${URL_PROBES[svc.name].fine} reaches the route and its requireAuth (got ${shown(fine)})`);
    }

    // The service's own lines for the refused requests first, so the absence below is
    // read after the point where an "unhandled error" would have been written. Five:
    // the malformed body, the large one, and the three with unstorable text.
    const expected = { 'entity.parse.failed': 1, 'entity.too.large': 1, 'text.unstorable': 3 };
    const tally = (lines) => {
      const seen = {};
      for (const l of lines) if (l.msg === 'client error' && l.level === 'info') seen[l.type] = (seen[l.type] ?? 0) + 1;
      return seen;
    };
    const complete = (seen) => Object.entries(expected).every(([type, n]) => (seen[type] ?? 0) >= n);
    const deadline = Date.now() + 5_000;
    let since = [];
    for (;;) {
      since = logLines(proc.output.slice(from));
      if (complete(tally(since)) || Date.now() >= deadline) break;
      await sleep(100);
    }
    await sleep(300);
    since = logLines(proc.output.slice(from));
    const clientLines = since.filter((l) => l.msg === 'client error');
    const unhandled = since.filter((l) => l.msg === 'unhandled error' || (l.raw ?? '').includes('unhandled error'));
    check(
      complete(tally(since)),
      `CONTROL: the log shows all five refusals reached the handler, at info, by type (${clientLines.map((l) => `${l.level} ${l.status} ${l.type}`).join(', ') || 'no "client error" line'})`,
    );
    check(
      unhandled.length === 0,
      unhandled.length === 0
        ? 'none is logged as "unhandled error"'
        : `${unhandled.length} line(s) logged as "unhandled error": ${unhandled.map((l) => `${l.level ?? ''} ${l.err?.type ?? l.raw ?? ''}`.trim()).join('; ')}`,
    );
    // The routes where unstorable text was measured reaching a store.
    if (ROUTE_PROBES[svc.name]) await ROUTE_PROBES[svc.name]({ port, env, proc });

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
  const candidates = classified.filter((s) => !s.skip);
  const booted = candidates.filter((s) => !s.readsWithoutParser);
  check(booted.length > 0, `services/ holds ${classified.length} service(s); ${booted.length} parse JSON bodies: ${booted.map((s) => s.name).join(', ')}`);
  for (const s of classified.filter((c) => c.skip)) console.log(`          skipped ${s.name}: ${s.skip}`);
  for (const s of booted.filter((c) => c.bodiless.length)) console.log(`          ${s.name} also runs ${s.bodiless.join(', ')}, an Express app with no express.json() that reads no req.query or req.body: neither can fault it`);
  const unknown = candidates.filter((s) => s.unknown.length);
  check(
    unknown.length === 0,
    unknown.length === 0
      ? 'and each one parses them in src/index.ts, the entrypoint this boots, and no Express app without a body parser reads req.query or req.body'
      : `${unknown.map((s) => `${s.name} (${s.unknown.join(', ')})`).join(', ')}: a body parser outside src/index.ts, or req.query/req.body read by an app with no ` +
          'body parser -- a process or a route this does not boot or probe',
  );
  console.log('');

  // The fixtures, before anything depends on them: each must be able to show the failure.
  const sentNul = JSON.parse(NUL_BODY).name;
  const sentSurrogate = JSON.parse(SURROGATE_BODY).name;
  check(
    NUL_BODY.includes('\\u0000') && sentNul.includes('\u0000') && SURROGATE_BODY.includes('\\ud800') && !sentSurrogate.isWellFormed() && JSON.parse(STORABLE_BODY).name.isWellFormed(),
    `CONTROL: the bodies sent are ${NUL_BODY} and ${SURROGATE_BODY}, escapes as written, which parse to a NUL and to a lone surrogate; the storable one parses well-formed`,
  );
  console.log('');

  const keyPath = join(ROOT, 'services', 'auth', 'keys', 'public.pem');
  const signingPath = join(ROOT, 'services', 'auth', 'keys', 'private.pem');
  if (!existsSync(keyPath) || !existsSync(signingPath)) {
    check(false, `${relative(ROOT, keyPath)} or ${relative(ROOT, signingPath)} is missing: generate the keypairs first`);
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
    for (const r of results) {
      console.log(
        `          ${r.name.padEnd(13)} malformed -> ${r.malformed}   200 kB -> ${r.large}   NUL -> ${r.nul ?? '-'}   lone surrogate -> ${r.surrogate ?? '-'}   NUL in query -> ${r.query ?? '-'}`,
      );
    }
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
      console.log(
        'every HTTP service answers a malformed body 400, an oversized one 413, and text Postgres cannot store 400, and logs none as a fault; ' +
          "Community's create answers 409 only for a real duplicate",
      );
    }
    // kafkajs can hold the event loop open after disconnect.
    process.exit(process.exitCode ?? 0);
  });
