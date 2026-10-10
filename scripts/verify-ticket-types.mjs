/**
 * Asserts the inventory editor: an organiser can put tickets on sale, only for their own
 * event, never past its capacity, and edit them without colliding with buyers.
 *
 * Nothing created ticket types before it -- every reservation verifier inserted its own
 * -- so through the API nobody could sell or buy a ticket. The last check here is that
 * gap closed: a ticket type made through the API, bought through the API.
 *
 *   - Who: the event's organiser and an ADMIN may create; another organiser gets 404
 *     (whether a colleague's event exists is not theirs to learn), an attendee 403.
 *   - What: not on a cancelled or deleted event (409); not a second type of the same
 *     name, in any case (409); not past the event's capacity (409, with what is
 *     allocated); nothing malformed (400).
 *   - THE RACE: eight creates at once, each fitting on its own, three of them filling
 *     the capacity. Exactly three may succeed. Every write locks the event first; a
 *     check that only read the other totals would let more through (write skew).
 *   - Who sees: an attendee lists a PUBLISHED event's types and not a draft's; the
 *     organiser lists their draft's; another tenant sees nothing.
 *   - Editing is optimistic on `revision`: no If-Match is 428, a stale one 412 and
 *     nothing changes. A reservation in between does NOT make the form stale -- every
 *     sale bumps `version`, which is why the editor does not use it. A total below what
 *     is held is 409. A price change leaves the price a hold was taken at alone.
 *   - End to end: a reservation of an API-made type is taken, and `remaining` falls.
 *
 * Starts its own Ticketing on a consumer group of this run's, from the end of the topic,
 * and seeds the event projections it needs directly: this is about the editor, not the
 * projection. Needs Postgres and Kafka up, and Ticketing built:
 *   node scripts/verify-ticket-types.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomBytes, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3961;
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_tt_${RUN}`;
const OTHER_TENANT = `tenant_tt_other_${RUN}`;
const GROUP = `verify-ticket-types-${RUN}`;
const OWNER = `user_tt_owner_${RUN}`;
const COLLEAGUE = `user_tt_colleague_${RUN}`;
const BUYER = `user_tt_buyer_${RUN}`;
const EVENTS = {
  open: `evt_tt_open_${RUN}`, // published, capacity 100, OWNER's
  race: `evt_tt_race_${RUN}`, // published, capacity 10, OWNER's
  draft: `evt_tt_draft_${RUN}`, // draft, OWNER's
  cancelled: `evt_tt_cancelled_${RUN}`,
  deleted: `evt_tt_deleted_${RUN}`,
  shrunk: `evt_tt_shrunk_${RUN}`, // draft, capacity 100, lowered to 20 after its tickets exist
  edits: `evt_tt_edits_${RUN}`, // published, capacity 100: two edits race
  priced: `evt_tt_priced_${RUN}`, // published: a price edit races a reservation
};

const env = serviceEnv(ROOT, 'ticketing', {
  PORT: String(PORT),
  AUTH_PUBLIC_KEY_B64: readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64'),
  KAFKA_CONSUMER_GROUP: GROUP,
  PROJECTION_START_FROM: 'latest',
});
const PRIVATE = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const b64url = (v) => Buffer.from(v).toString('base64url');
function token(userId, role, tenantId = TENANT) {
  const now = Math.floor(Date.now() / 1000);
  const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const body = b64url(JSON.stringify({ sub: userId, tenantId, tenantSlug: tenantId.replace(/_/g, '-'), role, iss: env.JWT_ISSUER, aud: env.JWT_AUDIENCE, iat: now, exp: now + 900 }));
  return `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), PRIVATE))}`;
}
const AS = {
  owner: token(OWNER, 'ORGANIZER'),
  colleague: token(COLLEAGUE, 'ORGANIZER'),
  admin: token(`user_tt_admin_${RUN}`, 'ADMIN'),
  attendee: token(BUYER, 'ATTENDEE'),
  otherTenant: token(OWNER, 'ADMIN', OTHER_TENANT),
};

async function api(method, path, { as, body, headers = {} } = {}) {
  const res = await fetch(`http://127.0.0.1:${PORT}${path}`, {
    method,
    headers: { ...(as ? { Authorization: `Bearer ${as}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}), ...headers },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {}
  return { status: res.status, json, text };
}
const create = (as, body) => api('POST', '/ticket-types', { as, body });

/** Statements as the runtime role inside this run's tenant, the context the service writes in. */
async function inTenant(db, tenantId, fn) {
  await db.query('BEGIN');
  try {
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
    const out = await fn();
    await db.query('COMMIT');
    return out;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

async function main() {
  const taken = await fetch(`http://127.0.0.1:${PORT}/health`).then(() => true, () => false);
  if (taken) {
    console.error(`FAIL: something already answers on :${PORT}. Stop it before running this.`);
    process.exitCode = 1;
    return;
  }

  const db = new pg.Client({ connectionString: env.APP_DATABASE_URL });
  await db.connect();
  const kafka = new Kafka({ clientId: `verify-ticket-types-${RUN}`, brokers: env.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  let child = null;
  let output = '';
  try {
    await inTenant(db, TENANT, async () => {
      const rows = [
        [EVENTS.open, 'PUBLISHED', 100, false],
        [EVENTS.race, 'PUBLISHED', 10, false],
        [EVENTS.draft, 'DRAFT', 50, false],
        [EVENTS.cancelled, 'CANCELLED', 50, false],
        [EVENTS.deleted, 'PUBLISHED', 50, true],
        [EVENTS.shrunk, 'DRAFT', 100, false],
        [EVENTS.edits, 'PUBLISHED', 100, false],
        [EVENTS.priced, 'PUBLISHED', 10, false],
      ];
      for (const [id, status, capacity, deleted] of rows) {
        await db.query(
          `INSERT INTO "EventProjection" (id, "tenantId", title, slug, status, capacity, "startsAt", "endsAt", "organiserId", deleted, "sourceVersion", "updatedAt")
           VALUES ($1, $2, $1, $1, $3, $4, now() + interval '30 days', now() + interval '31 days', $5, $6, 1, now())`,
          [id, TENANT, status, capacity, OWNER, deleted],
        );
      }
    });

    child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', 'ticketing'), env, stdio: ['ignore', 'pipe', 'pipe'] });
    child.stdout.on('data', (d) => (output += d));
    child.stderr.on('data', (d) => (output += d));
    let up = false;
    for (let i = 0; i < 60 && !up; i += 1) {
      const r = await fetch(`http://127.0.0.1:${PORT}/health`).then((x) => x.json(), () => null);
      up = r?.service === 'ticketing';
      if (!up) await sleep(500);
    }
    check(up, up ? `CONTROL: a Ticketing of this run's answers on :${PORT}, on consumer group ${GROUP}` : `Ticketing never came up:\n${output.slice(-1500)}`);
    if (!up) return;

    console.log('');
    console.log('  who may create');
    const general = await create(AS.owner, { eventId: EVENTS.open, name: 'General', priceCents: 2500, total: 40 });
    check(
      general.status === 201 && general.json?.total === 40 && general.json?.remaining === 40 && general.json?.held === 0 && general.json?.revision === 1,
      `the event's organiser creates one: 201, all 40 remaining, revision 1 (${general.status} ${general.text.slice(0, 90)})`,
    );
    const byAdmin = await create(AS.admin, { eventId: EVENTS.open, name: 'VIP', priceCents: 9000, total: 10 });
    check(byAdmin.status === 201, `an ADMIN creates one on the organiser's event (${byAdmin.status})`);
    const byColleague = await create(AS.colleague, { eventId: EVENTS.open, name: 'Sneaky', priceCents: 0, total: 1 });
    check(byColleague.status === 404, `another organiser is told there is no such event (${byColleague.status})`);
    const byAttendee = await create(AS.attendee, { eventId: EVENTS.open, name: 'Free', priceCents: 0, total: 1 });
    check(byAttendee.status === 403, `an attendee may not create one (${byAttendee.status})`);
    const byOtherTenant = await create(AS.otherTenant, { eventId: EVENTS.open, name: 'Elsewhere', priceCents: 0, total: 1 });
    check(byOtherTenant.status === 404, `an ADMIN of another tenant finds no such event (${byOtherTenant.status})`);

    console.log('');
    console.log('  what may be created');
    for (const [eventId, what] of [[EVENTS.cancelled, 'a cancelled event'], [EVENTS.deleted, 'a deleted event']]) {
      const r = await create(AS.owner, { eventId, name: 'General', priceCents: 100, total: 1 });
      check(r.status === 409, `not on ${what} (${r.status} ${r.text.slice(0, 70)})`);
    }
    const unknown = await create(AS.owner, { eventId: `evt_tt_nowhere_${RUN}`, name: 'General', priceCents: 100, total: 1 });
    check(unknown.status === 404, `not on an event this service has never heard of (${unknown.status})`);
    const dup = await create(AS.owner, { eventId: EVENTS.open, name: '  general ', priceCents: 100, total: 1 });
    check(dup.status === 409, `not a second "General", whatever its case or spacing (${dup.status} ${dup.text.slice(0, 70)})`);
    const over = await create(AS.owner, { eventId: EVENTS.open, name: 'Standing', priceCents: 1500, total: 51 });
    check(over.status === 409 && over.json?.capacity === 100 && over.json?.allocated === 50, `not past the capacity: 50 of 100 allocated, 51 more refused (${over.status} ${JSON.stringify(over.json)})`);
    const fits = await create(AS.owner, { eventId: EVENTS.open, name: 'Standing', priceCents: 1500, total: 50 });
    check(fits.status === 201, `CONTROL: exactly the 50 left is accepted (${fits.status})`);
    for (const [body, what] of [
      [{ eventId: EVENTS.open, name: 'Neg', priceCents: -1, total: 1 }, 'a negative price'],
      [{ eventId: EVENTS.open, name: 'Zero', priceCents: 0, total: 0 }, 'a total of zero'],
      [{ eventId: EVENTS.open, name: 'Frac', priceCents: 10.5, total: 1 }, 'a fraction of a cent'],
      [{ eventId: EVENTS.open, name: '', priceCents: 1, total: 1 }, 'an empty name'],
      [{ eventId: EVENTS.open, name: 'Extra', priceCents: 1, total: 1, sold: 5 }, 'a field it does not take (sold)'],
    ]) {
      const r = await create(AS.owner, body);
      check(r.status === 400, `${what} is refused with 400 (${r.status})`);
    }

    console.log('');
    console.log('  eight at once, room for three');
    const racers = await Promise.all(
      Array.from({ length: 8 }, (_, i) => create(AS.owner, { eventId: EVENTS.race, name: `Racer ${i}`, priceCents: 100, total: 3 })),
    );
    const won = racers.filter((r) => r.status === 201).length;
    const lost = racers.filter((r) => r.status === 409).length;
    const raceRows = await inTenant(db, TENANT, () => db.query(`SELECT coalesce(sum(total), 0)::int AS total, count(*)::int AS n FROM "TicketType" WHERE "eventId" = $1`, [EVENTS.race]));
    check(
      won === 3 && lost === 5 && raceRows.rows[0].total === 9,
      `exactly three succeed and the event holds 9 of its 10 (${won} created, ${lost} refused; stored ${raceRows.rows[0].n} types, ${raceRows.rows[0].total} tickets)`,
    );

    console.log('');
    console.log('  who sees them');
    const asAttendee = await api('GET', `/ticket-types?eventId=${EVENTS.open}`, { as: AS.attendee });
    check(asAttendee.status === 200 && asAttendee.json?.data?.length === 3, `an attendee lists a published event's three types (${asAttendee.status}, ${asAttendee.json?.data?.length})`);
    const draftType = await create(AS.owner, { eventId: EVENTS.draft, name: 'Early', priceCents: 500, total: 5 });
    check(draftType.status === 201, `CONTROL: the organiser can set up a draft's tickets (${draftType.status})`);
    const draftAttendee = await api('GET', `/ticket-types?eventId=${EVENTS.draft}`, { as: AS.attendee });
    check(draftAttendee.status === 404, `an attendee cannot see a draft's (${draftAttendee.status})`);
    const draftOwner = await api('GET', `/ticket-types?eventId=${EVENTS.draft}`, { as: AS.owner });
    check(draftOwner.status === 200 && draftOwner.json?.data?.length === 1, `its organiser can (${draftOwner.status})`);
    const otherTenantList = await api('GET', `/ticket-types?eventId=${EVENTS.open}`, { as: AS.otherTenant });
    check(otherTenantList.status === 404, `another tenant sees nothing (${otherTenantList.status})`);

    console.log('');
    console.log('  editing, optimistically');
    const id = general.json?.id;
    const patch = (body, headers, as = AS.owner) => api('PATCH', `/ticket-types/${id}`, { as, body, headers });
    const noMatch = await patch({ priceCents: 3000 });
    check(noMatch.status === 428, `without If-Match: 428 (${noMatch.status})`);
    const stale = await patch({ priceCents: 3000 }, { 'If-Match': '7' });
    check(stale.status === 412 && stale.json?.revision === 1, `a stale revision: 412, and it says which is current (${stale.status} ${stale.text.slice(0, 80)})`);
    const colleagueEdit = await patch({ priceCents: 1 }, { 'If-Match': '1' }, AS.colleague);
    check(colleagueEdit.status === 404, `another organiser cannot edit it (${colleagueEdit.status})`);

    // A hold taken now, through the API, at the current price.
    const reserved = await api('POST', '/reservations', {
      as: AS.attendee,
      headers: { 'Idempotency-Key': `tt-${RUN}-1` },
      body: { eventId: EVENTS.open, items: [{ ticketTypeId: id, quantity: 2 }] },
    });
    check(reserved.status === 201, `END TO END: an attendee reserves 2 of the API-made type (${reserved.status} ${reserved.text.slice(0, 80)})`);
    const afterHold = await api('GET', `/ticket-types?eventId=${EVENTS.open}`, { as: AS.attendee });
    const heldRow = afterHold.json?.data?.find((t) => t.id === id);
    check(heldRow?.remaining === 38 && heldRow?.held === 2 && heldRow?.revision === 1, `remaining falls to 38 and held is 2, and the revision is still 1 (${JSON.stringify(heldRow)})`);

    // The form was read at revision 1, before the sale: it is not stale.
    const repriced = await patch({ priceCents: 3000, name: 'General admission' }, { 'If-Match': '"1"' });
    check(
      repriced.status === 200 && repriced.json?.revision === 2 && repriced.json?.priceCents === 3000 && repriced.json?.remaining === 38,
      `an edit from before the sale still applies: 200, revision 2, remaining unchanged (${repriced.status} ${repriced.text.slice(0, 120)})`,
    );
    const lockedPrice = await inTenant(db, TENANT, () =>
      db.query(`SELECT "unitPriceCents" FROM "ReservationItem" WHERE "ticketTypeId" = $1`, [id]),
    );
    check(lockedPrice.rows[0]?.unitPriceCents === 2500, `the hold keeps the price it was taken at (${lockedPrice.rows[0]?.unitPriceCents} cents)`);
    const again = await patch({ priceCents: 1 }, { 'If-Match': '1' });
    check(again.status === 412, `the same form submitted again is now stale (${again.status})`);
    const belowHeld = await patch({ total: 1 }, { 'If-Match': '2' });
    check(belowHeld.status === 409 && belowHeld.json?.held === 2, `a total below what is held is refused (${belowHeld.status} ${belowHeld.text.slice(0, 80)})`);
    const rename = await patch({ name: 'vip' }, { 'If-Match': '2' });
    check(rename.status === 409, `renaming it to a sibling's name is refused (${rename.status})`);
    const tooBig = await patch({ total: 41 }, { 'If-Match': '2' });
    check(tooBig.status === 409, `growing it past the capacity is refused (${tooBig.status})`);
    const shrink = await patch({ total: 30 }, { 'If-Match': '2' });
    check(shrink.status === 200 && shrink.json?.total === 30 && shrink.json?.remaining === 28 && shrink.json?.held === 2, `shrinking it keeps the hold: total 30, 28 remaining (${shrink.status} ${shrink.text.slice(0, 100)})`);
    const counts = await inTenant(db, TENANT, () => db.query(`SELECT total, remaining, held, sold FROM "TicketType" WHERE id = $1`, [id]));
    const c = counts.rows[0];
    check(c && c.remaining + c.held + c.sold === c.total, `and the counts still add up (${c?.remaining} + ${c?.held} + ${c?.sold} = ${c?.total})`);

    console.log('');
    console.log("  a draft's capacity lowered after its tickets were set up");
    // Event lets a draft's capacity change; the projection then carries it. Made here
    // the way the projection consumer would, then the event is published.
    const half = await create(AS.owner, { eventId: EVENTS.shrunk, name: 'Half A', priceCents: 1000, total: 50 });
    const otherHalf = await create(AS.owner, { eventId: EVENTS.shrunk, name: 'Half B', priceCents: 1000, total: 50 });
    check(half.status === 201 && otherHalf.status === 201, `CONTROL: 50 + 50 on a draft of capacity 100 (${half.status}, ${otherHalf.status})`);
    await inTenant(db, TENANT, () => db.query(`UPDATE "EventProjection" SET capacity = 20, status = 'PUBLISHED', "sourceVersion" = "sourceVersion" + 1 WHERE id = $1`, [EVENTS.shrunk]));
    const reserveShrunk = (key) => api('POST', '/reservations', { as: AS.attendee, headers: { 'Idempotency-Key': `tt-${RUN}-${key}` }, body: { eventId: EVENTS.shrunk, items: [{ ticketTypeId: half.json?.id, quantity: 1 }] } });
    const overSold = await reserveShrunk('shrunk-1');
    check(overSold.status === 409 && overSold.json?.capacity === 20 && overSold.json?.allocated === 100, `with 100 tickets on a capacity of 20, nothing is sold (${overSold.status} ${overSold.text.slice(0, 90)})`);
    const shrunkPatch = (tt, body, rev) => api('PATCH', `/ticket-types/${tt}`, { as: AS.owner, body, headers: { 'If-Match': String(rev) } });
    const priceOnly = await shrunkPatch(half.json?.id, { priceCents: 1200 }, 1);
    check(priceOnly.status === 200, `the organiser can still change a price, which adds no tickets (${priceOnly.status} ${priceOnly.text.slice(0, 80)})`);
    const grow = await shrunkPatch(half.json?.id, { total: 51 }, 2);
    check(grow.status === 409, `but cannot add tickets (${grow.status})`);
    const reduceA = await shrunkPatch(half.json?.id, { total: 1 }, 2);
    const reduceB = await shrunkPatch(otherHalf.json?.id, { total: 19 }, 1);
    check(reduceA.status === 200 && reduceB.status === 200, `and can reduce them, even while still over (${reduceA.status}, ${reduceB.status})`);
    const repaired = await reserveShrunk('shrunk-2');
    check(repaired.status === 201, `at 1 + 19 = 20, it sells again (${repaired.status} ${repaired.text.slice(0, 60)})`);

    console.log('');
    console.log('  two edits at once, room for one');
    const ea = await create(AS.owner, { eventId: EVENTS.edits, name: 'Edit A', priceCents: 100, total: 30 });
    const eb = await create(AS.owner, { eventId: EVENTS.edits, name: 'Edit B', priceCents: 100, total: 30 });
    const raced = await Promise.all([
      api('PATCH', `/ticket-types/${ea.json?.id}`, { as: AS.owner, body: { total: 70 }, headers: { 'If-Match': '1' } }),
      api('PATCH', `/ticket-types/${eb.json?.id}`, { as: AS.owner, body: { total: 70 }, headers: { 'If-Match': '1' } }),
    ]);
    const editTotals = await inTenant(db, TENANT, () => db.query(`SELECT coalesce(sum(total), 0)::int AS total FROM "TicketType" WHERE "eventId" = $1`, [EVENTS.edits]));
    check(
      raced.filter((r) => r.status === 200).length === 1 && raced.filter((r) => r.status === 409).length === 1 && editTotals.rows[0].total === 100,
      `each fits alone, together they do not: one 200, one 409, 100 of 100 allocated (${raced.map((r) => r.status).join(', ')}; ${editTotals.rows[0].total})`,
    );

    console.log('');
    console.log('  a price edit that lands while a reservation waits for the row');
    const pt = await create(AS.owner, { eventId: EVENTS.priced, name: 'Priced', priceCents: 2500, total: 5 });
    // An editor's write, held open: it changes the price and keeps the row locked. The
    // reservation reads the old price without a lock, then waits on this one.
    const editor = new pg.Client({ connectionString: env.APP_DATABASE_URL });
    await editor.connect();
    await editor.query('BEGIN');
    await editor.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
    await editor.query(`UPDATE "TicketType" SET "priceCents" = 9999, revision = revision + 1, version = version + 1 WHERE id = $1`, [pt.json?.id]);
    const waiting = api('POST', '/reservations', { as: AS.attendee, headers: { 'Idempotency-Key': `tt-${RUN}-priced` }, body: { eventId: EVENTS.priced, items: [{ ticketTypeId: pt.json?.id, quantity: 1 }] } });
    await sleep(1_000);
    await editor.query('COMMIT');
    await editor.end();
    const pricedHold = await waiting;
    const pricedItem = await inTenant(db, TENANT, () => db.query(`SELECT "unitPriceCents" FROM "ReservationItem" WHERE "ticketTypeId" = $1`, [pt.json?.id]));
    check(
      pricedHold.status === 201 && pricedItem.rows[0]?.unitPriceCents === 9999,
      `the hold, committed after the price changed, is at the new price (${pricedHold.status}; ${pricedItem.rows[0]?.unitPriceCents ?? 'no item'} cents)`,
    );

    console.log('');
    console.log('  text a database would refuse or change');
    const nul = await create(AS.owner, { eventId: EVENTS.open, name: 'Nul\u0000name', priceCents: 1, total: 1 });
    check(nul.status === 400, `a name with a NUL is a 400, not the 500 Postgres would give (${nul.status})`);
    const lone = await create(AS.owner, { eventId: EVENTS.open, name: '\uD800VIP', priceCents: 1, total: 1 });
    check(lone.status === 400, `a name with a lone surrogate is a 400, not stored as U+FFFD (${lone.status})`);
    const nulQuery = await api('GET', '/ticket-types?eventId=%00', { as: AS.attendee });
    check(nulQuery.status === 400, `an eventId of NUL is a 400 for an attendee too, not a 500 (${nulQuery.status})`);
    const nulPath = await api('PATCH', '/ticket-types/%00', { as: AS.owner, body: { priceCents: 1 }, headers: { 'If-Match': '1' } });
    check(nulPath.status === 404, `a ticket type id of NUL is not found (${nulPath.status})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    if (child) {
      child.kill();
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
    // In this run's tenant, as the role that wrote them, and counted.
    const deleted = await inTenant(db, TENANT, async () => {
      const n = {};
      for (const [table, where] of [
        ['ReservationItem', `"tenantId" = $1`],
        ['Reservation', `"tenantId" = $1`],
        ['TicketType', `"tenantId" = $1`],
        ['EventProjection', `"tenantId" = $1`],
        // What a reservation writes beside itself: its idempotency key and its
        // tickets.reserved outbox row.
        ['IdempotencyKey', `"tenantId" = $1`],
        ['OutboxMessage', `"tenantId" = $1`],
      ]) {
        n[table] = (await db.query(`DELETE FROM "${table}" WHERE ${where}`, [TENANT])).rowCount;
      }
      return n;
    }).catch((err) => ({ error: err.message }));
    const left = await inTenant(db, TENANT, () => db.query(`SELECT (SELECT count(*) FROM "TicketType" WHERE "tenantId" = $1)::int + (SELECT count(*) FROM "EventProjection" WHERE "tenantId" = $1)::int AS n`, [TENANT])).catch(() => ({ rows: [{ n: -1 }] }));
    check(
      !deleted.error && deleted.EventProjection === Object.keys(EVENTS).length && left.rows[0].n === 0,
      `cleanup: deleted ${Object.entries(deleted).map(([t, n]) => `${n} ${t}`).join(', ')}; ${left.rows[0].n} types and events left`,
    );
    const groupLeft = await deleteGroupsWhenEmpty(admin, [GROUP]).catch((err) => [`${GROUP} (${err.message})`]);
    if (Array.isArray(groupLeft) && groupLeft.length) console.log(`    note  consumer group left to expire: ${groupLeft.join(', ')}`);
    await admin.disconnect();
    await db.end();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('an organiser puts tickets on sale for their own event, within its capacity, and edits them without colliding with buyers');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
