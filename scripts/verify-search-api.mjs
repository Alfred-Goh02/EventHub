/**
 * Asserts what GET /search promises, over HTTP, against the real service.
 *
 *   WHO                the tenant is the signed claim and nothing else: no token,
 *                      a forged one, alg none, an expired or wrong-audience token
 *                      are all 401, and X-Tenant-ID changes nothing
 *   WHAT               public search shows published, upcoming events: never a
 *                      draft, never a deleted one, and the past only when asked
 *   HOW WELL           a typo still finds the event, a title match outranks a
 *                      description match, and with no text the order is by date
 *   FACETS             counts that add up to the total, follow the filters, and
 *                      say they are exact
 *   WITHOUT OPENSEARCH the Event service answers instead, capped at 50, tenant-
 *                      scoped, published only, with a literal "%" matching only
 *                      a literal "%"
 *
 * THE includePast=false CASE IS NOT PADDING. z.coerce.boolean() is Boolean(v),
 * and Boolean("false") is true — the most natural way to write that parameter
 * would include the past exactly when a client asked it not to.
 *
 * WATCHED FAIL, in three runs against a deliberately broken service:
 *
 *   X-Tenant-ID read before the claim      the header check went red
 *   includePast via z.coerce.boolean()     the past came back for "false"
 *   fuzziness removed                      "jaz festvl" found nothing
 *   the fallback's LIMIT raised            56 rows where the cap is 50
 *   the fallback's escaping removed        "%" matched 20 titles, not 1
 *   the published filter removed           drafts and deleted events, and
 *                                          every facet count, went red
 *   the field boosts removed               the boost check went red — but only
 *                                          on the THIRD run. The first fixture
 *                                          put "jazz" once in a description, and
 *                                          BM25's preference for short fields
 *                                          ranked the title match first with no
 *                                          boosts at all. The check had been
 *                                          true of an engine without the thing
 *                                          it claimed to test.
 *
 * Tokens are minted here with Auth's private key, the one generate-keys writes,
 * rather than by logging in: this is about what Search does with a token, and
 * minting makes the hostile ones — forged, expired, alg none — possible to write.
 *
 * Needs: pnpm build, bootstrap:index, the Event service on EVENT_URL running this
 * branch's build (for the degraded half), and services/auth/keys.
 *
 * Run: node scripts/verify-search-api.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
import { searchServiceEnv } from './lib/search-service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

process.env['OPENSEARCH_NODE'] ??= 'http://127.0.0.1:9200';
process.env['KAFKA_BROKERS'] ??= '127.0.0.1:9092';
process.env['SCHEMA_REGISTRY_URL'] ??= 'http://127.0.0.1:8081';

const ISSUER = process.env['JWT_ISSUER'] ?? 'https://auth.eventhub.local';
const AUDIENCE = process.env['JWT_AUDIENCE'] ?? 'eventhub-api';
const EVENT_URL = process.env['EVENT_URL'] ?? 'http://127.0.0.1:3002';
const EVENT_OWNER_URL =
  process.env['EVENT_DATABASE_URL'] ??
  'postgresql://eventhub_event_owner:eventhub_event_owner@localhost:5432/eventhub_event';
const PORT_OK = 3925;
const PORT_DOWN = 3926;

const mod = (rel) => import(pathToFileURL(join(ROOT, 'services', 'search', 'dist', rel)).href);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    // HERE, not only at the end. A `return` inside the try below runs the
    // finally and then skips the end-of-run gate, and this file once printed
    // FAIL and exited 0 — a red run CI would have called green.
    process.exitCode = 1;
  }
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);
const T1 = `t_api_a_${run}`;
const T2 = `t_api_b_${run}`;
const id = (name) => `api_${run}_${name}`;

// ---------------------------------------------------------------------------
// tokens
// ---------------------------------------------------------------------------
const b64url = (v) => Buffer.from(v).toString('base64url');
const realKey = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
const { privateKey: forgedKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });

function token(tenantId, { key = realKey, alg = 'RS256', ...over } = {}) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg, typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({ sub: `u_api_${run}`, tenantId, role: 'ATTENDEE', iss: ISSUER, aud: AUDIENCE, iat: now, exp: now + 600, ...over }),
  );
  if (alg === 'none') return `${header}.${payload}.`;
  return `${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), key))}`;
}

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------
function doc(tenantId, name, over) {
  return {
    tenantId,
    eventId: id(name),
    slug: id(name).replace(/_/g, '-'),
    title: 'Untitled',
    category: 'music',
    city: 'berlin',
    status: 'published',
    startsAt: '2027-06-01T18:00:00.000Z',
    endsAt: '2027-06-01T23:00:00.000Z',
    capacity: 100,
    updatedAt: new Date().toISOString(),
    docVersion: 1,
    ...over,
  };
}

const FIXTURES = [
  doc(T1, 'fest', { title: 'Berlin Jazz Festival', description: 'Three nights of music' }),
  /**
   * "jazz" TWICE in the description, and on purpose. With it once, the title
   * match outranked this document with the boosts removed entirely: BM25 already
   * favours a hit in a short field, so the assertion about boosts was true of an
   * engine with no boosts at all, and a run with them deleted stayed green. Twice,
   * in a field where the word is rarer, this document wins on raw BM25 — so the
   * festival coming first is something title^3 has to earn.
   */
  doc(T1, 'desc', { title: 'Summer Gathering', description: 'Live jazz and more jazz in the park', city: 'hamburg', startsAt: '2027-06-02T18:00:00.000Z', endsAt: '2027-06-02T23:00:00.000Z' }),
  doc(T1, 'tech', { title: 'Node Conference', description: 'Talks', category: 'tech', startsAt: '2027-07-01T09:00:00.000Z', endsAt: '2027-07-01T17:00:00.000Z' }),
  doc(T1, 'draft', { title: 'Jazz Draft Night', status: 'draft', startsAt: '2027-06-03T18:00:00.000Z', endsAt: '2027-06-03T23:00:00.000Z' }),
  doc(T1, 'deleted', { title: 'Jazz Deleted Night', status: 'deleted', startsAt: '2027-06-04T18:00:00.000Z', endsAt: '2027-06-04T23:00:00.000Z' }),
  doc(T1, 'past', { title: 'Jazz Past Night', startsAt: '2020-01-01T18:00:00.000Z', endsAt: '2020-01-01T23:00:00.000Z' }),
  doc(T2, 'other', { title: 'Jazz Night Elsewhere', startsAt: '2027-06-05T18:00:00.000Z', endsAt: '2027-06-05T23:00:00.000Z' }),
];

async function waitFor(fn, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn().catch(() => null);
    if (v) return v;
    await sleep(300);
  }
  return null;
}

async function main() {
  const { os } = await mod('opensearch.js');
  const { INDEX_NAME, READ_ALIAS, WRITE_ALIAS, tenantAlias } = await mod('index-definition.js');

  const holdersOf = async (alias) => {
    const r = await os.indices.getAlias({ name: alias }, { ignore: [404] });
    return r.statusCode === 404 ? [] : Object.keys(r.body);
  };

  // -------------------------------------------------------------------------
  console.log('  what this needs has to be there before anything is written');
  // -------------------------------------------------------------------------
  const read = await holdersOf(READ_ALIAS);
  check(read.length === 1 && read[0] === INDEX_NAME, `${READ_ALIAS} resolves to [${read.join(', ')}]`);
  const eventUp = await fetch(`${EVENT_URL}/health`).then((r) => r.ok).catch(() => false);
  check(eventUp, `the Event service answers at ${EVENT_URL}`);
  if (failures > 0) {
    console.error('\nFAIL: run bootstrap:index and start the Event service first');
    process.exitCode = 1;
    return;
  }

  const children = [];
  const db = new pg.Client({ connectionString: EVENT_OWNER_URL, application_name: 'verify-search-api' });
  let wroteIndex = false;
  let wroteEvents = false;

  // Search's own .env.example plus these. See scripts/lib/search-service-env.mjs
  // for the three times a hand-written list here fell behind the service.
  const baseEnv = searchServiceEnv(ROOT, {
    LOG_LEVEL: 'warn',
    KAFKA_BROKERS: process.env['KAFKA_BROKERS'],
    SCHEMA_REGISTRY_URL: process.env['SCHEMA_REGISTRY_URL'],
    SEARCH_INDEXER_START_FROM: 'latest',
    JWT_ISSUER: ISSUER,
    JWT_AUDIENCE: AUDIENCE,
    EVENT_SERVICE_URL: EVENT_URL,
  });

  function start(port, opensearchNode, group) {
    const child = spawn(process.execPath, ['dist/index.js'], {
      cwd: join(ROOT, 'services', 'search'),
      env: { ...baseEnv, PORT: String(port), OPENSEARCH_NODE: opensearchNode, KAFKA_CONSUMER_GROUP: group },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.output = '';
    child.stdout.on('data', (c) => (child.output += c));
    child.stderr.on('data', (c) => (child.output += c));
    children.push(child);
    return child;
  }

  async function api(port, path, { bearer, headers = {} } = {}) {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, {
      headers: { ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}), ...headers },
    });
    const text = await res.text();
    let body = null;
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
    return { status: res.status, headers: res.headers, body };
  }

  const GROUPS = [`verify_search_api_${run}`, `verify_search_api_down_${run}`];

  try {
    const { body: bulk } = await os.bulk({
      refresh: 'wait_for',
      require_alias: true,
      body: FIXTURES.flatMap((d) => [{ index: { _index: WRITE_ALIAS, _id: d.eventId, routing: d.tenantId } }, d]),
    });
    wroteIndex = true;
    check(!bulk.errors, bulk.errors ? `fixtures failed: ${JSON.stringify(bulk.items[0])}` : `indexed ${FIXTURES.length} fixtures`);

    const ok = start(PORT_OK, process.env['OPENSEARCH_NODE'], GROUPS[0]);
    const down = start(PORT_DOWN, 'http://127.0.0.1:9299', GROUPS[1]);
    const upOk = await waitFor(async () => (await api(PORT_OK, '/health')).status === 200);
    const upDown = await waitFor(async () => (await api(PORT_DOWN, '/health')).status === 200);
    check(Boolean(upOk && upDown), upOk && upDown ? 'both Search instances are up' : `Search did not start:\n${ok.output}${down.output}`);
    if (!upOk || !upDown) return;

    const t1 = token(T1);
    const ids = async (path, bearer = t1, headers) =>
      ((await api(PORT_OK, path, { bearer, headers })).body.hits ?? []).map((h) => h.eventId);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  who: the tenant is the signed claim, and nothing else');
    // -----------------------------------------------------------------------
    const control = await api(PORT_OK, '/search?q=jazz', { bearer: t1 });
    check(control.status === 200, `CONTROL: a genuine token is accepted (${control.status})`);
    for (const [label, bearer] of [
      ['no token', undefined],
      ['a token signed with a different key', token(T1, { key: forgedKey })],
      ['a token with alg none', token(T1, { alg: 'none' })],
      ['an expired token', token(T1, { exp: Math.floor(Date.now() / 1000) - 60 })],
      ['a token for another audience', token(T1, { aud: 'someone-else' })],
    ]) {
      const r = await api(PORT_OK, '/search?q=jazz', { bearer });
      check(r.status === 401, `${label} → ${r.status}`);
    }

    const mine = await ids('/search?q=jazz');
    const withHeader = await ids('/search?q=jazz', t1, { 'X-Tenant-ID': T2 });
    check(
      mine.includes(id('fest')) && !mine.includes(id('other')),
      `tenant A's search finds its own events and not B's: [${mine.join(', ')}]`,
    );
    check(
      JSON.stringify(withHeader) === JSON.stringify(mine),
      'X-Tenant-ID naming tenant B changes nothing',
    );
    const theirs = await ids('/search?q=jazz', token(T2));
    check(theirs.length === 1 && theirs[0] === id('other'), `tenant B's token finds [${theirs.join(', ')}]`);
    const tenantParam = await api(PORT_OK, `/search?q=jazz&tenantId=${T2}`, { bearer: t1 });
    check(tenantParam.status === 400, `?tenantId=... is refused rather than ignored (${tenantParam.status})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  what: published and upcoming, the past only when asked');
    // -----------------------------------------------------------------------
    const hidden = [id('draft'), id('deleted')];
    check(
      !mine.some((x) => hidden.includes(x)) && !mine.includes(id('past')),
      'no draft, no deleted event, no past event by default',
    );
    const withPast = await ids('/search?q=jazz&includePast=true');
    check(
      withPast.includes(id('past')) && !withPast.some((x) => hidden.includes(x)),
      `includePast=true adds the past event and still no draft or deleted one: [${withPast.join(', ')}]`,
    );
    const explicitFalse = await ids('/search?q=jazz&includePast=false');
    check(!explicitFalse.includes(id('past')), 'includePast=false — the STRING "false" — excludes the past');

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  how well: typos, boosts, and a real order without text');
    // -----------------------------------------------------------------------
    const typo = await ids('/search?q=jaz%20festvl');
    check(typo[0] === id('fest'), `"jaz festvl" finds the festival first: [${typo.join(', ')}]`);
    check(
      mine.indexOf(id('fest')) !== -1 && mine.indexOf(id('desc')) !== -1 && mine.indexOf(id('fest')) < mine.indexOf(id('desc')),
      'a title match for "jazz" outranks a description match',
    );
    const browse = await ids('/search');
    check(
      JSON.stringify(browse) === JSON.stringify([id('fest'), id('desc'), id('tech')]),
      `with no text, events come back soonest first: [${browse.join(', ')}]`,
    );
    const highlighted = (await api(PORT_OK, '/search?q=jazz', { bearer: t1 })).body.hits.find((h) => h.eventId === id('fest'));
    check(/<em>/.test(JSON.stringify(highlighted?.highlight ?? {})), `the match is highlighted: ${JSON.stringify(highlighted?.highlight)}`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  facets: counts that add up, follow the filters, and say they are exact');
    // -----------------------------------------------------------------------
    const all = (await api(PORT_OK, '/search', { bearer: t1 })).body;
    const count = (facet, value) => all.facets[facet].find((b) => b.value === value)?.count;
    const categorySum = all.facets.category.reduce((n, b) => n + b.count, 0);
    check(
      all.total === 3 && count('category', 'music') === 2 && count('category', 'tech') === 1 && categorySum === all.total,
      `category facet music=${count('category', 'music')} tech=${count('category', 'tech')} sums to total ${all.total}`,
    );
    check(all.facetsExact === true, `facetsExact is ${all.facetsExact}`);
    const berlin = (await api(PORT_OK, '/search?city=berlin', { bearer: t1 })).body;
    const bCount = (value) => berlin.facets.category.find((b) => b.value === value)?.count;
    check(
      berlin.total === 2 && bCount('music') === 1 && bCount('tech') === 1,
      `filtered to Berlin, the facets move with it: total ${berlin.total}, music=${bCount('music')} tech=${bCount('tech')}`,
    );
    const capital = (await api(PORT_OK, '/search?city=Berlin', { bearer: t1 })).body;
    check(capital.total === 2, `city=Berlin finds the same ${capital.total} — the keyword normalizer applies to the query too`);

    const tooMany = await api(PORT_OK, '/search?limit=1000', { bearer: t1 });
    check(tooMany.status === 400, `limit=1000 is refused (${tooMany.status})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  without OpenSearch: the Event service answers, capped and scoped');
    // -----------------------------------------------------------------------
    const ready = await api(PORT_DOWN, '/ready');
    const health = await api(PORT_DOWN, '/health');
    check(ready.status === 503 && health.status === 200, `with the cluster unreachable /ready is ${ready.status} and /health is ${health.status}`);

    await db.connect();
    await db.query('BEGIN');
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [T1]);
    const rows = [];
    for (let i = 0; i < 55; i++) rows.push([id(`deg_${i}`), `Degraded ${run} ${i}`, 'PUBLISHED', '2027-08-01T10:00:00Z', null]);
    rows.push([id('deg_draft'), `Degraded ${run} draft`, 'DRAFT', '2027-08-01T10:00:00Z', null]);
    rows.push([id('deg_deleted'), `Degraded ${run} deleted`, 'PUBLISHED', '2027-08-01T10:00:00Z', new Date()]);
    rows.push([id('deg_past'), `Degraded ${run} past`, 'PUBLISHED', '2020-08-01T10:00:00Z', null]);
    rows.push([id('deg_pct'), `Degraded ${run} 100% off`, 'PUBLISHED', '2027-08-02T10:00:00Z', null]);
    for (const [rid, title, status, startsAt, deletedAt] of rows) {
      await db.query(
        `INSERT INTO "Event" (id, "tenantId", "organiserId", title, slug, status, capacity, "startsAt", "endsAt", "deletedAt", "updatedAt")
         VALUES ($1, $2, 'u_verify', $3, $4, $5::"EventStatus", 10, $6::timestamptz, $6::timestamptz + interval '2 hours', $7, now())`,
        [rid, T1, title, rid.replace(/_/g, '-'), status, startsAt, deletedAt],
      );
    }
    await db.query('COMMIT');
    wroteEvents = true;

    const q = encodeURIComponent(`Degraded ${run}`);
    const healthy = await api(PORT_OK, `/search?q=${q}&limit=100`, { bearer: t1 });
    check(
      healthy.status === 200 && healthy.headers.get('x-search-degraded') === null && healthy.body.degraded === false,
      'CONTROL: the healthy instance answers the same query from OpenSearch, not degraded',
    );

    const degraded = await api(PORT_DOWN, `/search?q=${q}&limit=100`, { bearer: t1 });
    const dIds = (degraded.body.hits ?? []).map((h) => h.eventId);
    check(
      degraded.status === 200 && degraded.headers.get('x-search-degraded') === 'true' && degraded.body.degraded === true,
      `the unreachable instance answers ${degraded.status}, X-Search-Degraded=${degraded.headers.get('x-search-degraded')}`,
    );
    check(dIds.length === 50 && degraded.body.capped === true, `capped at 50 of 56 matching published upcoming events (got ${dIds.length}, capped=${degraded.body.capped})`);
    check(
      ![id('deg_draft'), id('deg_deleted'), id('deg_past')].some((x) => dIds.includes(x)),
      'and none of them is the draft, the deleted or the past event',
    );

    const pct = await api(PORT_DOWN, `/search?q=${encodeURIComponent('%')}`, { bearer: t1 });
    const pctIds = (pct.body.hits ?? []).map((h) => h.eventId);
    check(
      pctIds.length === 1 && pctIds[0] === id('deg_pct'),
      `q="%" matches only the title with a literal "%" (${pctIds.length} hit(s)) — the pattern is escaped`,
    );
    const otherTenant = await api(PORT_DOWN, `/search?q=${q}`, { bearer: token(T2) });
    check(
      otherTenant.status === 200 && (otherTenant.body.hits ?? []).length === 0,
      `tenant B's token on the degraded path finds ${(otherTenant.body.hits ?? []).length} of tenant A's events`,
    );
  } finally {
    for (const child of children) {
      child.kill('SIGTERM');
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
    if (wroteEvents) {
      await db.query('BEGIN');
      await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [T1]);
      const { rowCount } = await db.query(`DELETE FROM "Event" WHERE id LIKE $1`, [`api_${run}_%`]);
      await db.query('COMMIT');
      console.log(`\n    cleanup: removed ${rowCount} Event row(s)`);
    }
    await db.end().catch(() => {});
    if (wroteIndex) {
      const { body: del } = await os.deleteByQuery({
        index: INDEX_NAME,
        refresh: true,
        body: { query: { terms: { tenantId: [T1, T2] } } },
      });
      console.log(`    cleanup: removed ${del.deleted} document(s)`);
      const actions = [];
      for (const t of [T1, T2]) {
        for (const index of await holdersOf(tenantAlias(t))) actions.push({ remove: { index, alias: tenantAlias(t) } });
      }
      if (actions.length > 0) await os.indices.updateAliases({ body: { actions } });
    }
    const admin = new Kafka({ clientId: 'verify-search-api', brokers: process.env['KAFKA_BROKERS'].split(','), logLevel: logLevel.NOTHING }).admin();
    await admin.connect().then(() => admin.deleteGroups(GROUPS)).catch(() => {});
    await admin.disconnect().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('search answers only for the signed tenant, shows what public search should, and degrades rather than breaks');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
