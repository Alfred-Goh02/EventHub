/**
 * Asserts what the Analytics read API promises, over HTTP, against the real service
 * and a real ClickHouse.
 *
 *   WHO          the tenant is the signed claim and nothing else: no token, a
 *                forged one, alg none, an expired or wrong-audience token are 401;
 *                an attendee is 403; X-Tenant-ID changes nothing; ?tenantId is a 400
 *   ISOLATION    every route, asked by one tenant about another tenant's event —
 *                and about an event id BOTH tenants use — answers only with the
 *                caller's own rows. The routes are read from the router itself, and
 *                a route with no probe here is a failure.
 *   RIGHT NUMBERS  people are counted once across days, not once per day; money
 *                stays per currency; reads merge rows a background merge has not
 *                collapsed yet; the range is whole UTC days, both ends inclusive
 *   REFUSALS     a day that does not exist, a reversed or over-long range, an id
 *                that cannot be an event, are all 400 with the field named
 *   OPERATIONS   answers are Cache-Control: private, no-store; a store that cannot
 *                be reached is a 503 that says so; /metrics counts each outcome
 *
 * THE FIXTURE IS BUILT TO SHOW EACH FAILURE, per the rule in CLAUDE.md:
 *
 *   an event id both tenants use       a query filtering on the event and not the
 *                                      tenant would return the SUM, not zero
 *   the same buyer on two days         summing each day's distinct count says 2
 *   USD and EUR                        one merged total would say 7000
 *   a purchase 40 days ago             the default range would include it if it
 *                                      were not whole days back from today
 *   merges stopped, rows inserted apart the rollup holds several rows per key, so
 *                                      a read that did not merge would be wrong
 *
 * WATCHED FAIL. This file passed every check on its first run, which proves
 * nothing until each check has been seen red. Two runs against a broken API:
 *
 *   run 1, 12 red
 *     the funnel's tenant filter dropped     tenant A read 53 tickets under the
 *                                            shared id, and B's own event
 *     the organiser-or-admin guard dropped   an attendee's token got a 200
 *     currencies merged                      "EUR+USD": 57000 as one number
 *     the calendar-day check dropped         2026-02-30 answered 200
 *     Cache-Control without no-store         "private"
 *     a route added with no probe            NO ISOLATION PROBE for /analytics/extra
 *   run 2, 10 red
 *     the overview's tenant filter dropped   A's overview listed B's event: 69 tickets
 *     .strict() removed                      ?tenantId= answered 200
 *     active users summed across hours       4, where 3 people took part
 *     a rollup read with max, not sum        3 seats reserved where 5 were
 *     an unreachable store not recognised    a 500 "internal error", counted failed
 *
 * Tokens are minted here with Auth's private key, as verify-search-api.mjs does,
 * so the hostile ones are possible to write.
 *
 * Run (after pnpm build and generate-keys, with ClickHouse up):
 *   node scripts/verify-analytics-api.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { ADMIN, APP, CH_URL, ch, createScratchDatabase, dropScratchDatabase, json, migrate } from './lib/clickhouse.mjs';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3917;
const PORT_DOWN = 3918;
const PORT_WRONG_PASSWORD = 3919;

const mod = (rel) => import(pathToFileURL(join(ROOT, 'services', 'analytics', 'dist', rel)).href);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);
const DB = `analytics_api_${run}`;
const TA = `t_api_a_${run}`;
const TB = `t_api_b_${run}`;
const SHARED = `evt_shared_${run}`;
const A_ONLY = `evt_a_${run}`;
const B_ONLY = `evt_b_${run}`;

const DAY_MS = 86_400_000;
const isoDay = (t) => new Date(t).toISOString().slice(0, 10);
const TODAY = isoDay(Date.now());
const YESTERDAY = isoDay(Date.now() - DAY_MS);
const OLD = isoDay(Date.now() - 40 * DAY_MS);

// ---------------------------------------------------------------------------
// tokens
// ---------------------------------------------------------------------------
const example = serviceEnv(ROOT, 'analytics');
const ISSUER = example['JWT_ISSUER'];
const AUDIENCE = example['JWT_AUDIENCE'];
const b64url = (v) => Buffer.from(v).toString('base64url');
const realKey = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
const { privateKey: forgedKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });

function token(tenantId, { key = realKey, alg = 'RS256', role = 'ORGANIZER', ...over } = {}) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg, typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({ sub: `u_api_${run}`, tenantId, role, iss: ISSUER, aud: AUDIENCE, iat: now, exp: now + 600, ...over }),
  );
  if (alg === 'none') return `${header}.${payload}.`;
  return `${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), key))}`;
}

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------
let seq = 0;
function row(tenant, event, day, activity, over = {}) {
  seq += 1;
  return {
    tenant_id: tenant,
    activity,
    occurred_at: `${day} ${over.time ?? '10:15:00'}.000`,
    message_id: `api_${run}_${seq}`,
    line: 0,
    event_id: event,
    ticket_type_id: over.type ?? '',
    user_id: over.user ?? 'u0',
    quantity: over.qty ?? 0,
    amount_cents: over.cents ?? 0,
    currency: over.currency ?? '',
    source_topic: 'verify',
    kafka_partition: 0,
    kafka_offset: seq,
    props: {},
  };
}

const FIXTURE = [
  // Tenant A, the shared event id.
  row(TA, SHARED, YESTERDAY, 'ticket_reserved', { user: 'u1', type: 'tt_ga', qty: 2 }),
  row(TA, SHARED, YESTERDAY, 'ticket_purchased', { user: 'u1', type: 'tt_ga', qty: 2, cents: 4000, currency: 'USD' }),
  row(TA, SHARED, TODAY, 'ticket_reserved', { user: 'u1', type: 'tt_vip', qty: 1 }),
  row(TA, SHARED, TODAY, 'ticket_reserved', { user: 'u2', type: 'tt_ga', qty: 3 }),
  row(TA, SHARED, TODAY, 'reservation_expired', { user: 'u2', type: 'tt_ga', qty: 3 }),
  row(TA, SHARED, TODAY, 'ticket_purchased', { user: 'u1', type: 'tt_vip', qty: 1, cents: 3000, currency: 'EUR' }),
  row(TA, SHARED, OLD, 'ticket_purchased', { user: 'u9', type: 'tt_ga', qty: 5, cents: 10000, currency: 'USD' }),
  row(TA, SHARED, TODAY, 'question_asked', { user: 'u1', time: '10:05:00' }),
  row(TA, SHARED, TODAY, 'question_upvoted', { user: 'u2', time: '10:20:00' }),
  row(TA, SHARED, TODAY, 'question_upvoted', { user: 'u3', time: '11:40:00' }),
  row(TA, SHARED, TODAY, 'question_upvoted', { user: 'u1', time: '11:45:00' }),
  // Tenant A, a second event, selling more than the shared one.
  row(TA, A_ONLY, TODAY, 'ticket_purchased', { user: 'u4', type: 'tt_ga', qty: 7, cents: 700, currency: 'USD' }),
  // Tenant B, under the SAME shared id, and its own event.
  row(TB, SHARED, TODAY, 'ticket_reserved', { user: 'b1', type: 'tt_ga', qty: 50 }),
  row(TB, SHARED, TODAY, 'ticket_purchased', { user: 'b1', type: 'tt_ga', qty: 50, cents: 50000, currency: 'USD' }),
  row(TB, SHARED, TODAY, 'question_asked', { user: 'b1', time: '10:30:00' }),
  row(TB, SHARED, TODAY, 'question_upvoted', { user: 'b2', time: '10:31:00' }),
  row(TB, B_ONLY, TODAY, 'ticket_purchased', { user: 'b2', type: 'tt_ga', qty: 9, cents: 900, currency: 'USD' }),
];

async function waitForHttp(port, path, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}${path}`);
      if (res.ok) return true;
    } catch {
      // not listening yet
    }
    await sleep(250);
  }
  return false;
}

function startApi(port, overrides) {
  let output = '';
  const child = spawn(process.execPath, ['dist/index.js'], {
    cwd: join(ROOT, 'services', 'analytics'),
    // Analytics' own .env.example plus these. See scripts/lib/service-env.mjs.
    env: serviceEnv(ROOT, 'analytics', {
      LOG_LEVEL: 'fatal',
      PORT: String(port),
      CLICKHOUSE_URL: CH_URL,
      CLICKHOUSE_DATABASE: DB,
      AUTH_PUBLIC_KEY_B64: readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64'),
      ...overrides,
    }),
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout.on('data', (d) => (output += d));
  child.stderr.on('data', (d) => (output += d));
  return { child, log: () => output };
}

async function stop(proc) {
  if (!proc || proc.child.exitCode !== null) return;
  proc.child.kill('SIGTERM');
  await new Promise((r) => (proc.child.exitCode !== null ? r() : proc.child.once('exit', r)));
}

async function main() {
  const ping = await fetch(`${CH_URL}/ping`).then((r) => r.text()).catch(() => '');
  check(ping.trim() === 'Ok.', `ClickHouse answers at ${CH_URL}`);
  if (failures > 0) return;

  let api = null;
  let down = null;
  let wrongPassword = null;

  try {
    await createScratchDatabase(DB);
    const migrated = migrate(ROOT, DB);
    check(migrated.code === 0 && migrated.applied > 0, `CONTROL: ${DB} migrated: exit ${migrated.code}, applied ${migrated.applied}`);
    if (failures > 0) return;

    // One insert per row with merges stopped, so every rollup key has as many
    // physical rows as it had inserts, and a read that forgot to merge shows it.
    await ch(ADMIN, 'SYSTEM STOP MERGES ticket_funnel_daily', { database: DB });
    await ch(ADMIN, 'SYSTEM STOP MERGES engagement_hourly', { database: DB });
    for (const r of FIXTURE) {
      const ins = await ch(APP, 'INSERT INTO activity_events', {
        database: DB,
        rows: [r],
        settings: { insert_deduplication_token: r.message_id, deduplicate_blocks_in_dependent_materialized_views: 1 },
      });
      if (!ins.ok) throw new Error(`fixture insert failed: ${ins.text}`);
    }
    const [physical] = await json(
      ADMIN,
      `SELECT count() AS n, uniqExact(tuple(event_id, day, ticket_type_id, currency)) AS keys
         FROM ticket_funnel_daily WHERE tenant_id = '${TA}' AND event_id = '${SHARED}'`,
      DB,
    );
    check(
      Number(physical.n) > Number(physical.keys),
      `CONTROL: the rollup holds ${physical.n} unmerged rows for ${physical.keys} keys, so every answer below had to merge`,
    );

    api = startApi(PORT, {});
    const up = await waitForHttp(PORT, '/ready');
    check(up, `the API answers /ready on :${PORT} against ${DB}`);
    if (!up) {
      console.error(api.log());
      return;
    }

    const get = async (path, { auth = token(TA), headers = {} } = {}) => {
      const res = await fetch(`http://127.0.0.1:${PORT}${path}`, {
        headers: { ...(auth ? { Authorization: `Bearer ${auth}` } : {}), ...headers },
      });
      const text = await res.text();
      let body = null;
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
      return { status: res.status, body, cache: res.headers.get('cache-control') };
    };
    const funnel = (event, q = '', opts) => get(`/analytics/events/${event}/funnel${q}`, opts);
    const engagement = (event, q = '', opts) => get(`/analytics/events/${event}/engagement${q}`, opts);
    const overview = (q = '', opts) => get(`/analytics/overview${q}`, opts);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  who: the tenant is the signed claim and nothing else');
    // -----------------------------------------------------------------------
    const control = await funnel(SHARED);
    check(control.status === 200, `CONTROL: an organiser's own token is answered (${control.status})`);
    for (const [label, auth, want] of [
      ['no token', null, 401],
      ['a token signed with another key', token(TA, { key: forgedKey }), 401],
      ['alg none', token(TA, { alg: 'none' }), 401],
      ['an expired token', token(TA, { exp: Math.floor(Date.now() / 1000) - 60 }), 401],
      ['a token for another audience', token(TA, { aud: 'somebody-else' }), 401],
      ["an attendee's token", token(TA, { role: 'ATTENDEE' }), 403],
    ]) {
      const r = await funnel(SHARED, '', { auth });
      check(r.status === want, `${label}: ${r.status} (want ${want})`);
    }
    // X-Tenant-Slug is not ignored: one that disagrees with the token is a 403, the
    // shared middleware's rule, asserted in verify-token-tenant.mjs.
    const spoofed = await funnel(SHARED, '', { headers: { 'X-Tenant-ID': TB } });
    check(
      spoofed.status === 200 && JSON.stringify(spoofed.body.totals) === JSON.stringify(control.body.totals),
      `X-Tenant-ID naming tenant B changes nothing: ${spoofed.body?.totals?.ticketsSold} tickets, as without it`,
    );
    const asked = await funnel(SHARED, `?tenantId=${TB}`);
    check(asked.status === 400, `?tenantId=... is refused, not ignored (${asked.status})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log("  isolation: every route answers only with the caller's own rows");
    // -----------------------------------------------------------------------
    const zeroFunnel = (b) =>
      b?.totals?.ticketsSold === 0 && b.totals.seatsReserved === 0 && b.totals.reservers === 0 &&
      b.totals.gross.length === 0 && b.days.length === 0 && b.byTicketType.length === 0;

    const PROBES = {
      '/analytics/events/:eventId/funnel': async () => {
        const other = await funnel(B_ONLY);
        check(other.status === 200 && zeroFunnel(other.body), `funnel: tenant A asking for tenant B's event gets zeros, not a 403 and not B's numbers`);
        const shared = await funnel(SHARED);
        check(
          shared.body?.totals?.ticketsSold === 3 && JSON.stringify(shared.body.totals.gross) === JSON.stringify([{ currency: 'EUR', cents: 3000 }, { currency: 'USD', cents: 4000 }]),
          `funnel: under the id both tenants use, A gets its own 3 tickets, not 53 (${shared.body?.totals?.ticketsSold}, ${JSON.stringify(shared.body?.totals?.gross)})`,
        );
        const theirs = await funnel(B_ONLY, '', { auth: token(TB) });
        check(theirs.body?.totals?.ticketsSold === 9, `CONTROL: tenant B's own token reads B's event (${theirs.body?.totals?.ticketsSold} tickets)`);
      },
      '/analytics/events/:eventId/engagement': async () => {
        const other = await engagement(B_ONLY);
        check(
          other.status === 200 && other.body?.totals?.questionsAsked === 0 && other.body.totals.upvotes === 0 && other.body.hours.length === 0,
          `engagement: tenant A asking for tenant B's event gets zeros`,
        );
        const shared = await engagement(SHARED);
        check(
          shared.body?.totals?.upvotes === 3 && shared.body.totals.questionsAsked === 1,
          `engagement: under the shared id, A gets its own 1 question and 3 upvotes, not 2 and 4 (${JSON.stringify(shared.body?.totals)})`,
        );
        const theirs = await engagement(SHARED, '', { auth: token(TB) });
        check(theirs.body?.totals?.upvotes === 1, `CONTROL: tenant B's token reads B's engagement under the same id (${theirs.body?.totals?.upvotes} upvote)`);
      },
      '/analytics/overview': async () => {
        const mine = await overview();
        const ids = (mine.body?.events ?? []).map((e) => e.eventId).sort();
        check(
          JSON.stringify(ids) === JSON.stringify([A_ONLY, SHARED].sort()) && mine.body.totals.ticketsSold === 10,
          `overview: A sees its two events and 10 tickets, nothing of B's (${JSON.stringify(ids)}, ${mine.body?.totals?.ticketsSold})`,
        );
        const theirs = await overview('', { auth: token(TB) });
        check(theirs.body?.totals?.ticketsSold === 59, `CONTROL: tenant B's overview is B's own 59 tickets (${theirs.body?.totals?.ticketsSold})`);
      },
    };

    const { createAnalyticsRouter } = await mod('routes/analytics.js');
    const declared = createAnalyticsRouter({ ch: null, guards: [] })
      .stack.filter((layer) => layer.route)
      .map((layer) => layer.route.path)
      .sort();
    const probed = Object.keys(PROBES).sort();
    check(declared.length > 0, `CONTROL: the router declares ${declared.length} route(s): ${declared.join(', ')}`);
    const unprobed = declared.filter((p) => !probed.includes(p));
    const stale = probed.filter((p) => !declared.includes(p));
    check(unprobed.length === 0, unprobed.length === 0 ? 'and every one has an isolation probe' : `NO ISOLATION PROBE for ${unprobed.join(', ')}`);
    check(stale.length === 0, stale.length === 0 ? 'and no probe names a route that is gone' : `probes for routes that no longer exist: ${stale.join(', ')}`);
    for (const path of declared) if (PROBES[path]) await PROBES[path]();

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the numbers: people once, money per currency, whole days');
    // -----------------------------------------------------------------------
    const f = (await funnel(SHARED)).body;
    check(
      f.totals.reservers === 2 && f.totals.purchasers === 1,
      `a buyer on two days is one purchaser, and two reservers are two: ${f.totals.purchasers} and ${f.totals.reservers} (summing each day's count would say ${f.days.reduce((n, d) => n + d.purchasers, 0)} and ${f.days.reduce((n, d) => n + d.reservers, 0)})`,
    );
    check(
      f.totals.seatsReserved === 6 && f.totals.seatsExpired === 3 && f.totals.ticketsSold === 3,
      `seats: ${f.totals.seatsReserved} reserved, ${f.totals.seatsExpired} expired, ${f.totals.ticketsSold} sold (want 6, 3, 3)`,
    );
    check(
      JSON.stringify(f.byTicketType) ===
        JSON.stringify([
          { ticketTypeId: 'tt_ga', seatsReserved: 5, seatsReleased: 0, seatsExpired: 3, ticketsSold: 2 },
          { ticketTypeId: 'tt_vip', seatsReserved: 1, seatsReleased: 0, seatsExpired: 0, ticketsSold: 1 },
        ]),
      `per ticket type: ${JSON.stringify(f.byTicketType)}`,
    );
    check(
      JSON.stringify(f.days.map((d) => [d.day, d.ticketsSold, d.gross])) ===
        JSON.stringify([
          [YESTERDAY, 2, [{ currency: 'USD', cents: 4000 }]],
          [TODAY, 1, [{ currency: 'EUR', cents: 3000 }]],
        ]),
      `per day, in order, each with its own currency: ${JSON.stringify(f.days.map((d) => [d.day, d.ticketsSold, d.gross]))}`,
    );
    check(f.from === isoDay(Date.now() - 29 * DAY_MS) && f.to === TODAY, `the default range is the last 30 days, today included: ${f.from} to ${f.to}`);

    const wide = (await funnel(SHARED, `?from=${OLD}&to=${TODAY}`)).body;
    check(
      wide.totals.ticketsSold === 8 && JSON.stringify(wide.totals.gross) === JSON.stringify([{ currency: 'EUR', cents: 3000 }, { currency: 'USD', cents: 14000 }]),
      `CONTROL: a range reaching back 40 days includes the old purchase the default left out: ${wide.totals.ticketsSold} tickets, ${JSON.stringify(wide.totals.gross)}`,
    );
    const oneDay = (await funnel(SHARED, `?from=${OLD}&to=${OLD}`)).body;
    check(oneDay.totals.ticketsSold === 5 && oneDay.days.length === 1, `from and to on the same day is that whole day: ${oneDay.totals.ticketsSold} tickets over ${oneDay.days.length} day`);

    const e = (await engagement(SHARED)).body;
    check(
      JSON.stringify(e.hours) ===
        JSON.stringify([
          { hour: `${TODAY}T10:00:00Z`, questionsAsked: 1, upvotes: 1, activeUsers: 2 },
          { hour: `${TODAY}T11:00:00Z`, questionsAsked: 0, upvotes: 2, activeUsers: 2 },
        ]) && e.totals.activeUsers === 3,
      `engagement by UTC hour, and ${e.totals.activeUsers} active users in all (want 3; summing each hour's count says ${e.hours.reduce((n, h) => n + h.activeUsers, 0)}): ${JSON.stringify(e.hours)}`,
    );

    const o = (await overview()).body;
    check(
      JSON.stringify(o.events.map((x) => [x.eventId, x.ticketsSold])) === JSON.stringify([[A_ONLY, 7], [SHARED, 3]]) && o.truncated === false,
      `overview ranks by tickets sold: ${JSON.stringify(o.events.map((x) => [x.eventId, x.ticketsSold]))}`,
    );
    check(
      JSON.stringify(o.totals.gross) === JSON.stringify([{ currency: 'EUR', cents: 3000 }, { currency: 'USD', cents: 4700 }]) && o.totals.purchasers === 2,
      `overview totals keep currencies apart and count people once: ${JSON.stringify(o.totals)}`,
    );
    const top = (await overview('?limit=1')).body;
    check(top.events.length === 1 && top.events[0].eventId === A_ONLY && top.truncated === true, `limit=1 returns the top event and says the list was cut (${top.truncated})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  refusals name what was wrong');
    // -----------------------------------------------------------------------
    for (const [label, path, pattern] of [
      ['a day that does not exist', `/analytics/events/${SHARED}/funnel?from=2026-02-30`, /from: not a calendar date/],
      ['a range that ends before it starts', `/analytics/events/${SHARED}/funnel?from=${TODAY}&to=${OLD}`, /from is after to/],
      ['a range longer than a year', `/analytics/events/${SHARED}/funnel?from=2024-01-01&to=2025-06-01`, /at most 366 days/],
      ['an id that cannot be an event', `/analytics/events/${encodeURIComponent("x' OR 1=1")}/funnel`, /not an event id/],
      ['a limit of zero', '/analytics/overview?limit=0', /limit/],
    ]) {
      const r = await get(path);
      const said = Array.isArray(r.body?.error) ? r.body.error.join('; ') : JSON.stringify(r.body);
      check(r.status === 400 && pattern.test(said), `${label}: ${r.status} — ${said.slice(0, 90)}`);
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  operations: no caching, an honest 503, and counts by outcome');
    // -----------------------------------------------------------------------
    check(control.cache === 'private, no-store', `answers carry Cache-Control: ${control.cache}`);

    const metrics = await fetch(`http://127.0.0.1:${PORT}/metrics`).then((r) => r.json());
    check(
      metrics.queries?.funnel?.servedTotal > 0 && metrics.queries.funnel.rejectedTotal >= 4 && metrics.queries.funnel.durationMs.samples > 0,
      `/metrics counts the funnel's outcomes: ${JSON.stringify({ served: metrics.queries?.funnel?.servedTotal, rejected: metrics.queries?.funnel?.rejectedTotal })}`,
    );

    down = startApi(PORT_DOWN, { CLICKHOUSE_URL: 'http://127.0.0.1:1' });
    const listening = await waitForHttp(PORT_DOWN, '/health');
    check(listening, `a second API pointed at a ClickHouse that is not there still listens on :${PORT_DOWN}`);
    const readyDown = await fetch(`http://127.0.0.1:${PORT_DOWN}/ready`).then((r) => r.status).catch(() => 0);
    check(readyDown === 503, `and its /ready says 503 (${readyDown})`);
    const unreachable = await fetch(`http://127.0.0.1:${PORT_DOWN}/analytics/events/${SHARED}/funnel`, {
      headers: { Authorization: `Bearer ${token(TA)}` },
    });
    const unreachableBody = await unreachable.json().catch(() => null);
    check(
      unreachable.status === 503 && unreachableBody?.error === 'analytics store unavailable',
      `a query it cannot run is a 503 that says why, not a 500 or a hang: ${unreachable.status} ${JSON.stringify(unreachableBody)}`,
    );
    const downMetrics = await fetch(`http://127.0.0.1:${PORT_DOWN}/metrics`).then((r) => r.json());
    check(downMetrics.queries?.funnel?.unavailableTotal === 1, `and it is counted as unavailable, not failed (${JSON.stringify(downMetrics.queries?.funnel)})`);

    // ClickHouse answers GET /ping without authenticating, so a readiness check
    // built on a plain ping said ready with a wrong password (found by the review
    // of the ingest, whose /ready had the same line).
    wrongPassword = startApi(PORT_WRONG_PASSWORD, { CLICKHOUSE_PASSWORD: 'not-the-password' });
    const wrongPasswordListening = await waitForHttp(PORT_WRONG_PASSWORD, '/health');
    const wrongPasswordReady = await fetch(`http://127.0.0.1:${PORT_WRONG_PASSWORD}/ready`).then((r) => r.status).catch(() => 0);
    check(
      wrongPasswordListening && wrongPasswordReady === 503,
      `an API given a wrong ClickHouse password is listening but not ready (${wrongPasswordReady})`,
    );
  } finally {
    await stop(api);
    await stop(down);
    await stop(wrongPassword);
    const gone = await dropScratchDatabase(DB);
    console.log(`\n    cleanup: scratch database ${gone ? 'dropped' : 'STILL PRESENT'}`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log("the read API answers only the token's tenant, with numbers that add up, and says so when it cannot");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
