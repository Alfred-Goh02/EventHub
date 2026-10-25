/**
 * Asserts that GET /events?upcoming=true leaves out the events that have ended,
 * and only those, by the DATABASE's clock.
 *
 * The attendee app lists events soonest first, and without this it opened on
 * events that were over. A filter the browser fed with a `from` time would use
 * the browser's clock; one fed with `new Date()` would use Event's. An event's
 * times were written on the database's, so that is the clock to ask.
 *
 * THE FIXTURE CAN SHOW THE FAILURE. Event runs here with its clock an HOUR FAST
 * (scripts/lib/skewed-clock.mjs). Four published events, timed by SQL against
 * now(), in the order the list sorts them:
 *
 *   festival   4 h ago  to 6 h ahead    listed
 *   ended      3 h ago  to 30 min ago   left out
 *   underway   1 h ago  to 30 min ahead listed: someone arriving late wants it
 *   later      2 h ahead to 5 h ahead   listed
 *
 * By Event's own clock "underway" ended half an hour ago, so a filter on
 * `new Date()` leaves it out. So does one on startsAt rather than endsAt. Both
 * are measured red, as is a route that ignores the parameter, which lists
 * "ended".
 *
 * "festival" is there for the cursor. Walked one page at a time, a route that
 * applied the filter to the first page only stayed green while "ended" sorted
 * before everything upcoming: no page after a cursor could contain it. Sorted
 * between two upcoming events, it lands on the second page. Measured red.
 *
 * CONTROLS. Without the parameter all four are listed, oldest first, so the
 * filter is opt-in and the organiser's full history is unchanged; a route that
 * listed nothing would fail there.
 *
 * This run's own tenant, so the list holds exactly what this run inserted.
 *
 * Starts its own Event on :3988. Needs Postgres up and event built:
 *   node scripts/verify-upcoming-events.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomBytes, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_upcoming_${RUN}`;
const PORT = 3988;
const BASE = `http://127.0.0.1:${PORT}`;

const SKEW = `--import=${pathToFileURL(join(ROOT, 'scripts', 'lib', 'skewed-clock.mjs')).href}`;
const SKEW_LINE = 'clock skewed by +3600s for this process';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const PUBLIC_B64 = readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64');
const PRIVATE = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
const b64url = (v) => Buffer.from(v).toString('base64url');
/** Good for two hours: Event checks exp on its clock, an hour fast. */
function token(env) {
  const now = Math.floor(Date.now() / 1000);
  const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const body = b64url(
    JSON.stringify({
      sub: `user_upcoming_${RUN}`, tenantId: TENANT, tenantSlug: TENANT.replace(/_/g, '-'), role: 'ATTENDEE',
      iss: env.JWT_ISSUER, aud: env.JWT_AUDIENCE, iat: now, exp: now + 7200,
    }),
  );
  return `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), PRIVATE))}`;
}

/** Statements inside this run's tenant, the context the service reads in. */
async function inTenant(db, fn) {
  await db.query('BEGIN');
  try {
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
    const out = await fn();
    await db.query('COMMIT');
    return out;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** Each event by name, its window relative to the database's now(). */
const FIXTURE = [
  { name: 'festival', starts: '-4 hours', ends: '6 hours' },
  { name: 'ended', starts: '-3 hours', ends: '-30 minutes' },
  { name: 'underway', starts: '-1 hour', ends: '30 minutes' },
  { name: 'later', starts: '2 hours', ends: '5 hours' },
];

async function main() {
  console.log(`  run ${RUN}, tenant ${TENANT}`);
  const env = serviceEnv(ROOT, 'event', { PORT: String(PORT), AUTH_PUBLIC_KEY_B64: PUBLIC_B64, NODE_OPTIONS: SKEW });
  const db = new pg.Client({ connectionString: env.APP_DATABASE_URL, application_name: 'verify-upcoming' });
  db.on('error', () => {});
  await db.connect();

  const ids = {};
  let child = null;
  try {
    // Refuse rather than test whatever already answers there.
    const taken = await fetch(`${BASE}/health`).then(() => true, () => false);
    if (taken) {
      check(false, `something already answers on :${PORT}; stop it before running this`);
      return;
    }

    await inTenant(db, async () => {
      for (const e of FIXTURE) {
        const id = `evt_upcoming_${e.name}_${RUN}`;
        await db.query(
          `INSERT INTO "Event" (id, "tenantId", "organiserId", title, slug, status, capacity, "startsAt", "endsAt", "updatedAt")
           VALUES ($1, $2, $3, $4, $4, 'PUBLISHED', 10, now() + $5::interval, now() + $6::interval, now())`,
          [id, TENANT, `user_upcoming_owner_${RUN}`, `${e.name}-${RUN}`, e.starts, e.ends],
        );
        ids[e.name] = id;
      }
    });
    const named = new Map(Object.entries(ids).map(([name, id]) => [id, name]));

    // The fixture shows the failure only if an hour-fast clock calls "underway" over.
    const margin = (
      await inTenant(db, () =>
        db.query(`SELECT extract(epoch FROM "endsAt" - now())::float8 AS s FROM "Event" WHERE id = $1`, [ids.underway]),
      )
    ).rows[0]?.s;
    check(
      typeof margin === 'number' && margin > 0 && margin < 3600,
      `CONTROL: "underway" ends ${typeof margin === 'number' ? Math.round(margin) : '?'} s from the database's now(), inside the hour Event's clock runs ahead, so by Event's clock it has ended`,
    );

    child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', 'event'), env, stdio: ['ignore', 'pipe', 'pipe'] });
    let log = '';
    child.stdout.on('data', (d) => (log += d));
    child.stderr.on('data', (d) => (log += d));
    let up = false;
    for (let i = 0; i < 80 && !up && child.exitCode === null; i += 1) {
      const r = await fetch(`${BASE}/health`).then((x) => x.json(), () => null);
      up = r?.service === 'event';
      if (!up) await sleep(500);
    }
    check(up, up ? `CONTROL: this run's own Event answers on :${PORT}` : `Event never came up:\n${log.slice(-1500)}`);
    if (!up) return;
    check(log.includes(SKEW_LINE), "CONTROL: its clock runs an hour fast, so a filter on it cannot pass for the database's");

    const auth = { Authorization: `Bearer ${token(env)}` };
    const get = async (qs) => {
      const res = await fetch(`${BASE}/events${qs}`, { headers: auth });
      return { status: res.status, body: await res.json().catch(() => null) };
    };
    const names = (body) => (body?.data ?? []).map((e) => named.get(e.id) ?? `other:${e.id}`);

    const all = await get('');
    check(
      all.status === 200 && JSON.stringify(names(all.body)) === '["festival","ended","underway","later"]',
      `CONTROL: without ?upcoming all four are listed, oldest first (${all.status} ${JSON.stringify(names(all.body))})`,
    );

    const upcoming = await get('?upcoming=true');
    const listed = names(upcoming.body);
    check(upcoming.status === 200, `CONTROL: ?upcoming=true answers 200 (${upcoming.status})`);
    check(!listed.includes('ended'), `an event that ended half an hour ago is left out (${JSON.stringify(listed)})`);
    check(
      listed.includes('underway'),
      `an event under way, ending in half an hour by the database's clock, is listed (${JSON.stringify(listed)}; by Event's own clock it has ended)`,
    );
    check(
      listed.includes('later') && listed.includes('festival'),
      `CONTROL: an event that has not started, and one that started before "ended" and runs on, are listed (${JSON.stringify(listed)})`,
    );

    // One page at a time: the cursor and the filter together. "ended" sorts
    // between "festival" and "underway", so it would be the second page.
    const pages = [];
    let cursor = null;
    for (let i = 0; i < 6; i += 1) {
      const page = await get(`?upcoming=true&limit=1${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`);
      pages.push(names(page.body));
      cursor = page.body?.nextCursor ?? null;
      if (!cursor) break;
    }
    check(
      JSON.stringify(pages) === '[["festival"],["underway"],["later"]]',
      `walked a page at a time, ?upcoming=true holds on every page after the first: three pages, then none (${JSON.stringify(pages)})`,
    );

    const bad = await get('?upcoming=yes');
    check(bad.status === 400, `CONTROL: ?upcoming takes true or false, and anything else is a 400 (${bad.status})`);
  } catch (err) {
    check(false, `stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    if (child && child.exitCode === null) {
      child.kill();
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
    // Deleted in this run's tenant, the context they were written in: FORCE row
    // security would otherwise let the DELETE match nothing.
    const deleted = await inTenant(db, () => db.query(`DELETE FROM "Event" WHERE "tenantId" = $1`, [TENANT]))
      .then((r) => r.rowCount, (err) => err.message);
    const left = await inTenant(db, () => db.query(`SELECT count(*)::int AS n FROM "Event" WHERE "tenantId" = $1`, [TENANT]))
      .then((r) => r.rows[0].n, () => -1);
    const inserted = Object.keys(ids).length;
    if (inserted) check(deleted === inserted && left === 0, `cleanup: deleted ${deleted} of ${inserted} event(s); ${left} left`);
    await db.end();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log("?upcoming=true leaves out what has ended, by the database's clock, whatever Event's says");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
