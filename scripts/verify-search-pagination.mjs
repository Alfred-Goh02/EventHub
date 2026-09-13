/**
 * Asserts that paging through GET /search with nextCursor returns every result
 * exactly once, holds still while the index changes, and cannot be steered into
 * another tenant's data.
 *
 *   COMPLETE      50 events at 10 a page come back as exactly 50 distinct ids,
 *                 including ten that share one start time across a page boundary
 *                 — where a sort with no unique tiebreaker skips or repeats
 *   STABLE        an event indexed mid-traversal, sorting into a later page, does
 *                 not appear in it: the Point-in-Time is a snapshot
 *   BOUND         a cursor used by another tenant, with another query, or with one
 *                 byte changed, is refused
 *   BACKSTOPPED   a VALIDLY SIGNED cursor for tenant B carrying tenant A's PIT —
 *                 what a leaked signing secret would allow — still reads nothing
 *                 of A's, because every query also filters on the caller's tenant
 *   RELEASED      a traversal that reaches its end leaves no PIT open, and a page
 *                 whose PIT has gone is a 410, not a 500
 *
 * The backstop case imports the service's own encodeCursor rather than copying
 * the format, for the reason #71 established.
 *
 * WATCHED FAIL, in three runs against a deliberately broken service:
 *
 *   eventId tiebreaker removed        45 ids of 50 — the tie lost five rows
 *   PIT never released                2 PITs open after traversals that ended
 *   cursor's tenant check removed     tenant B's use of A's cursor → 200
 *   the tenant term filter removed    B read 10 of A's events inside A's PIT.
 *                                     Run alone: combined with the tiebreaker
 *                                     mutation it failed with a 500 instead —
 *                                     the right colour for the wrong reason
 *   no PIT opened                     the event indexed mid-traversal appeared
 *                                     in the later pages
 *
 * That last run also caught this file miscounting, fixed below: the backstop
 * reported tenant B's own events as tenant A's.
 *
 * Run (after pnpm build and bootstrap:index): node scripts/verify-search-pagination.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { searchServiceEnv } from './lib/search-service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const ISSUER = process.env['JWT_ISSUER'] ?? 'https://auth.eventhub.local';
const AUDIENCE = process.env['JWT_AUDIENCE'] ?? 'eventhub-api';
const SECRET = `verify-pagination-secret-${randomUUID()}`;
const PORT = 3935;

/**
 * The service's own configuration, set in THIS process too, because the backstop
 * case imports the service's cursor module and that module reads it at load.
 */
const serviceEnv = searchServiceEnv(ROOT, {
  PORT: String(PORT),
  OPENSEARCH_NODE: process.env['OPENSEARCH_NODE'],
  KAFKA_BROKERS: process.env['KAFKA_BROKERS'],
  SCHEMA_REGISTRY_URL: process.env['SCHEMA_REGISTRY_URL'],
  JWT_ISSUER: ISSUER,
  JWT_AUDIENCE: AUDIENCE,
  EVENT_SERVICE_URL: process.env['EVENT_URL'],
  SEARCH_CURSOR_SECRET: SECRET,
});
Object.assign(process.env, serviceEnv);

const mod = (rel) => import(pathToFileURL(join(ROOT, 'services', 'search', 'dist', rel)).href);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    // HERE, not only at the end: a `return` inside the try below skips the
    // end-of-run gate. Two sibling verifiers printed FAIL and exited 0 that way.
    process.exitCode = 1;
  }
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);
const T1 = `t_page_a_${run}`;
const T2 = `t_page_b_${run}`;
const GROUP = `verify_search_pagination_${run}`;

const b64url = (v) => Buffer.from(v).toString('base64url');
const realKey = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
function token(tenantId) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({ sub: `u_page_${run}`, tenantId, role: 'ATTENDEE', iss: ISSUER, aud: AUDIENCE, iat: now, exp: now + 900 }),
  );
  return `${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), realKey))}`;
}

/**
 * 50 events a day apart, except that events 5 to 14 all start at the SAME
 * moment. At 10 a page that tie straddles the boundary between page one and page
 * two — the one place a sort without a unique tiebreaker loses or repeats rows.
 */
function fixtures() {
  const docs = [];
  for (let i = 0; i < 50; i++) {
    const day = i >= 5 && i < 15 ? 5 : i;
    const startsAt = new Date(Date.UTC(2028, 0, 1 + day, 18)).toISOString();
    docs.push({
      tenantId: T1,
      eventId: `page_${run}_${String(i).padStart(2, '0')}`,
      slug: `page-${run}-${i}`,
      title: `Page ${run} event ${i}`,
      category: 'music',
      city: 'berlin',
      status: 'published',
      startsAt,
      endsAt: startsAt,
      capacity: 10,
      updatedAt: new Date().toISOString(),
      docVersion: 1,
    });
  }
  for (let i = 0; i < 5; i++) {
    const startsAt = new Date(Date.UTC(2028, 0, 1 + i, 18)).toISOString();
    docs.push({ ...docs[i], tenantId: T2, eventId: `page_${run}_b${i}`, slug: `page-${run}-b${i}`, title: `Page ${run} other ${i}`, startsAt, endsAt: startsAt });
  }
  return docs;
}

async function main() {
  const { os } = await mod('opensearch.js');
  const { INDEX_NAME, READ_ALIAS, WRITE_ALIAS, tenantAlias } = await mod('index-definition.js');
  const { encodeCursor } = await mod('cursor.js');

  const holdersOf = async (alias) => {
    const r = await os.indices.getAlias({ name: alias }, { ignore: [404] });
    return r.statusCode === 404 ? [] : Object.keys(r.body);
  };
  const openPits = async () => ((await os.getAllPits()).body.pits ?? []).length;

  const read = await holdersOf(READ_ALIAS);
  check(read.length === 1 && read[0] === INDEX_NAME, `${READ_ALIAS} resolves to [${read.join(', ')}]`);
  if (failures > 0) {
    process.exitCode = 1;
    return;
  }

  let child = null;
  let wrote = false;

  const api = async (path, bearer) => {
    const res = await fetch(`http://127.0.0.1:${PORT}${path}`, { headers: { Authorization: `Bearer ${bearer}` } });
    const text = await res.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
    return { status: res.status, body };
  };

  try {
    const docs = fixtures();
    const { body: bulk } = await os.bulk({
      refresh: 'wait_for',
      require_alias: true,
      body: docs.flatMap((d) => [{ index: { _index: WRITE_ALIAS, _id: d.eventId, routing: d.tenantId } }, d]),
    });
    wrote = true;
    check(!bulk.errors, `indexed ${docs.length} fixtures: 50 for tenant A, 5 for tenant B`);

    child = spawn(process.execPath, ['dist/index.js'], {
      cwd: join(ROOT, 'services', 'search'),
      env: {
        ...serviceEnv,
        LOG_LEVEL: 'warn',
        KAFKA_CONSUMER_GROUP: GROUP,
        SEARCH_INDEXER_START_FROM: 'latest',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    child.stdout.on('data', (c) => (output += c));
    child.stderr.on('data', (c) => (output += c));

    let up = false;
    for (let i = 0; i < 60 && !up; i++) {
      up = await fetch(`http://127.0.0.1:${PORT}/health`).then((r) => r.ok).catch(() => false);
      if (!up) await sleep(500);
    }
    check(up, up ? 'Search is up' : `Search did not start:\n${output}`);
    if (!up) return;

    const t1 = token(T1);
    const t2 = token(T2);
    const pitsBefore = await openPits();

    async function traverse(query, bearer, onPage) {
      const seen = [];
      let cursor = null;
      let pages = 0;
      let first = null;
      do {
        const path = `/search?${query}${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
        const r = await api(path, bearer);
        if (r.status !== 200) return { seen, pages, error: r };
        pages += 1;
        if (!first) first = r.body;
        seen.push(...r.body.hits.map((h) => h.eventId));
        if (onPage) await onPage(pages, r.body);
        cursor = r.body.nextCursor;
      } while (cursor && pages < 20);
      return { seen, pages, first };
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  complete: every result once, including a tie across a page boundary');
    // -----------------------------------------------------------------------
    const browse = await traverse('limit=10', t1);
    check(browse.first?.total === 50, `CONTROL: the search matches ${browse.first?.total} events`);
    check(
      browse.seen.length === 50 && new Set(browse.seen).size === 50,
      `paging by nextCursor returned ${browse.seen.length} ids, ${new Set(browse.seen).size} distinct, in ${browse.pages} pages`,
    );
    const tenantAId = new RegExp(`^page_${run}_\\d\\d$`);
    check(browse.seen.every((x) => tenantAId.test(x)), "and every one is tenant A's");
    const textual = await traverse(`q=${encodeURIComponent(`page ${run}`)}&limit=10`, t1);
    check(
      textual.seen.length === 50 && new Set(textual.seen).size === 50,
      `with a text query too, where every score ties: ${new Set(textual.seen).size} distinct of ${textual.seen.length}`,
    );

    const pitsAfter = await openPits();
    check(pitsAfter === pitsBefore, `a traversal that reached its end left no PIT open (${pitsBefore} before, ${pitsAfter} after)`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  stable: an event indexed mid-traversal does not appear in later pages');
    // -----------------------------------------------------------------------
    const late = { ...docs[49], eventId: `page_${run}_late`, slug: `page-${run}-late`, title: `Page ${run} late arrival`, startsAt: '2028-06-01T18:00:00.000Z', endsAt: '2028-06-01T18:00:00.000Z' };
    const stable = await traverse('limit=10', t1, async (page) => {
      if (page === 2) {
        await os.index({ index: WRITE_ALIAS, id: late.eventId, routing: T1, body: late, refresh: 'wait_for' });
      }
    });
    check(
      !stable.seen.includes(late.eventId) && stable.seen.length === 50,
      `the event indexed during page 2 is absent from pages 3 to 5 (${stable.seen.length} ids, late one present: ${stable.seen.includes(late.eventId)})`,
    );
    const fresh = await api('/search?limit=100', t1);
    check(fresh.body.total === 51, `CONTROL: a fresh search does see it (${fresh.body.total})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  bound: a cursor belongs to one tenant, one query, and is not editable');
    // -----------------------------------------------------------------------
    const page1 = await api('/search?limit=10', t1);
    const page2 = await api(`/search?limit=10&cursor=${encodeURIComponent(page1.body.nextCursor)}`, t1);
    const c2 = page2.body.nextCursor;
    check(page2.status === 200 && typeof c2 === 'string', 'CONTROL: tenant A continues its own traversal');

    const foreign = await api(`/search?limit=10&cursor=${encodeURIComponent(c2)}`, t2);
    check(foreign.status === 400, `tenant B presenting tenant A's cursor → ${foreign.status}`);
    const otherQuery = await api(`/search?limit=10&city=hamburg&cursor=${encodeURIComponent(c2)}`, t1);
    check(otherQuery.status === 400, `the same cursor with a different filter → ${otherQuery.status}`);
    const [payload, sig] = c2.split('.');
    const flipped = `${payload.slice(0, -2)}${payload.slice(-2, -1) === 'A' ? 'B' : 'A'}${payload.slice(-1)}.${sig}`;
    const tampered = await api(`/search?limit=10&cursor=${encodeURIComponent(flipped)}`, t1);
    check(tampered.status === 400, `the cursor with one character changed → ${tampered.status}`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log("  backstopped: a validly signed cursor carrying another tenant's PIT reads nothing of theirs");
    // -----------------------------------------------------------------------
    const pitOfA = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')).p;
    check(typeof pitOfA === 'string', "CONTROL: tenant A's page-2 cursor carries a PIT");
    const { queryHash } = await mod('cursor.js');
    const hashB = queryHash({ q: null, category: null, city: null, includePast: false, limit: 10 });
    const forged = encodeCursor({ t: T2, h: hashB, a: [0, ''], p: pitOfA });
    const leak = await api(`/search?limit=10&cursor=${encodeURIComponent(forged)}`, t2);
    // Only A's ids count. The first version counted every hit, and a run with no
    // PIT at all reported tenant B's own five events as "5 of A's events".
    const leakedIds = (leak.body?.hits ?? []).map((h) => h.eventId).filter((x) => tenantAId.test(x));
    check(
      leak.status === 200 && leakedIds.length === 0,
      `tenant B searching inside tenant A's PIT got ${leak.status} with ${leakedIds.length} of A's events`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  released: a page whose PIT has gone is a 410, not a 500');
    // -----------------------------------------------------------------------
    await os.deletePit({ body: { pit_id: [pitOfA] } }).catch(() => {});
    const expired = await api(`/search?limit=10&cursor=${encodeURIComponent(c2)}`, t1);
    check(expired.status === 410, `the cursor after its PIT was released → ${expired.status} ${JSON.stringify(expired.body)}`);
  } finally {
    if (child) {
      child.kill('SIGTERM');
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
    if (wrote) {
      const { body: del } = await os.deleteByQuery({
        index: INDEX_NAME,
        refresh: true,
        body: { query: { terms: { tenantId: [T1, T2] } } },
      });
      console.log(`\n    cleanup: removed ${del.deleted} document(s)`);
      const actions = [];
      for (const t of [T1, T2]) {
        for (const index of await holdersOf(tenantAlias(t))) actions.push({ remove: { index, alias: tenantAlias(t) } });
      }
      if (actions.length > 0) await os.indices.updateAliases({ body: { actions } });
    }
    const admin = new Kafka({ clientId: 'verify-search-pagination', brokers: serviceEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING }).admin();
    await admin.connect().then(() => admin.deleteGroups([GROUP])).catch(() => {});
    await admin.disconnect().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('paging returns everything once, holds still, and cannot be steered into another tenant');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
