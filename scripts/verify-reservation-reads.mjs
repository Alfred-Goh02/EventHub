/**
 * Asserts what a buyer can see of what they bought: GET /reservations/:id and
 * GET /reservations.
 *
 * Nothing answered either before. After POST /reservations and POST .../pay a buyer had
 * no way to ask again -- not even after /pay said 202, "being confirmed", which is the
 * case the payment design is built around.
 *
 *   - One reservation through its life: a hold with its countdown, then paid and
 *     confirmed; a declined card (and the LATEST of two attempts shown); a provider
 *     that did not answer, PENDING_PAYMENT with the attempt UNKNOWN.
 *   - Whose: another buyer, the event's own organiser and another tenant's ADMIN are
 *     all told it does not exist; an ADMIN of the tenant may read it (the control that
 *     it exists). The list is always the caller's own, an ADMIN's too.
 *   - Paging, newest first, on (createdAt, id). A hold taken BETWEEN pages must not
 *     push a reservation onto the next page again, which an offset would. Two
 *     reservations made in the same millisecond must both appear, which a cursor on
 *     createdAt alone would not do.
 *   - The countdown is Postgres's. The service runs an hour fast
 *     (scripts/lib/skewed-clock.mjs); a hold made by SQL to lapse in five minutes must
 *     say about 300 seconds, where the process's clock would say it lapsed long ago.
 *   - Nothing malformed is a 500: cursors, limits, statuses, ids.
 *
 * What was bought stays behind, because nothing may delete a payment; it is left settled
 * and with counters that agree with it (see the cleanup). Starts its own Ticketing on a
 * consumer group of this run's and seeds the event projection it needs. Needs Postgres
 * and Kafka up, and Ticketing built:
 *   node scripts/verify-reservation-reads.mjs
 */

import { spawn } from 'node:child_process';
import { createHmac, createPrivateKey, randomBytes, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3963;
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_rr_${RUN}`;
const OTHER_TENANT = `tenant_rr_other_${RUN}`;
const GROUP = `verify-reservation-reads-${RUN}`;
const EVENT = `evt_rr_${RUN}`;
const OWNER = `user_rr_owner_${RUN}`;
const BUYER = `user_rr_buyer_${RUN}`;
const OTHER = `user_rr_other_${RUN}`;
const CLOCK = `user_rr_clock_${RUN}`;
const TIES = [`rr_tie_a_${RUN}`, `rr_tie_b_${RUN}`];
const LAPSING = `rr_lapsing_${RUN}`;

const env = serviceEnv(ROOT, 'ticketing', {
  PORT: String(PORT),
  AUTH_PUBLIC_KEY_B64: readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64'),
  KAFKA_CONSUMER_GROUP: GROUP,
  PROJECTION_START_FROM: 'latest',
  // An hour fast, so a countdown taken from this process's clock is visible.
  NODE_OPTIONS: `--import=${pathToFileURL(join(ROOT, 'scripts', 'lib', 'skewed-clock.mjs')).href}`,
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
  // Two hours: the service checks expiry on a clock an hour fast.
  const body = b64url(JSON.stringify({ sub: userId, tenantId, tenantSlug: tenantId.replace(/_/g, '-'), role, iss: env.JWT_ISSUER, aud: env.JWT_AUDIENCE, iat: now, exp: now + 7200 }));
  return `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), PRIVATE))}`;
}
const AS = {
  owner: token(OWNER, 'ORGANIZER'),
  buyer: token(BUYER, 'ATTENDEE'),
  other: token(OTHER, 'ATTENDEE'),
  clock: token(CLOCK, 'ATTENDEE'),
  admin: token(`user_rr_admin_${RUN}`, 'ADMIN'),
  otherTenant: token(`user_rr_admin_${RUN}`, 'ADMIN', OTHER_TENANT),
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
/**
 * The provider's answer for an attempt it never answered at the time, signed as the
 * provider signs it. The signature's timestamp must be fresh by the SERVICE's clock,
 * which scripts/lib/skewed-clock.mjs sets an hour fast.
 */
async function providerSays(paymentId) {
  const raw = JSON.stringify({ paymentId, type: 'charge.succeeded' });
  const t = Math.floor(Date.now() / 1000) + 3600;
  const v1 = createHmac('sha256', env.PAYMENT_WEBHOOK_SECRET).update(`${t}.${raw}`).digest('hex');
  const res = await fetch(`http://127.0.0.1:${PORT}/webhooks/payments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': `t=${t},v1=${v1}` },
    body: raw,
  });
  return { ok: res.ok, status: res.status };
}
let fixtures = 0;
let keys = 0;
const reserve =(as, ticketTypeId, quantity = 1) =>
  api('POST', '/reservations', { as, headers: { 'Idempotency-Key': `rr-${RUN}-${(keys += 1)}` }, body: { eventId: EVENT, items: [{ ticketTypeId, quantity }] } });
const read = (id, as = AS.buyer) => api('GET', `/reservations/${id}`, { as });
const list = (query, as = AS.buyer) => api('GET', `/reservations${query}`, { as });
const ids = (r) => (r.json?.data ?? []).map((x) => x.id);

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
  const kafka = new Kafka({ clientId: `verify-reservation-reads-${RUN}`, brokers: env.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();
  /** Postgres's own rendering of a column, to compare with what the API says. */
  const rendered = (column, id) =>
    inTenant(db, TENANT, () => db.query(`SELECT to_char(r."${column}", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS v FROM "Reservation" r WHERE r.id = $1`, [id])).then((r) => r.rows[0]?.v ?? null);

  let child = null;
  let output = '';
  try {
    await inTenant(db, TENANT, () =>
      db.query(
        `INSERT INTO "EventProjection" (id, "tenantId", title, slug, status, capacity, "startsAt", "endsAt", "organiserId", deleted, "sourceVersion", "updatedAt")
         VALUES ($1, $2, $3, $1, 'PUBLISHED', 100, now() + interval '30 days', now() + interval '31 days', $4, false, 1, now())`,
        [EVENT, TENANT, `Reads ${RUN}`, OWNER],
      ),
    );

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
    check(output.includes('clock skewed by +3600s'), 'CONTROL: its clock is an hour fast');

    // The fake provider decides by the amount's last two digits: 00 pays, 01 is
    // declined, 02 does not answer.
    const types = {};
    for (const [key, name, priceCents] of [['paid', 'Paid', 2500], ['declined', 'Declined', 2501], ['unknown', 'Unanswered', 2502]]) {
      const r = await api('POST', '/ticket-types', { as: AS.owner, body: { eventId: EVENT, name, priceCents, total: 20 } });
      types[key] = r.json?.id;
    }
    check(Object.values(types).every(Boolean), `CONTROL: the organiser put three ticket types on sale (${JSON.stringify(types)})`);

    console.log('');
    console.log('  one reservation, from hold to paid');
    const a = await reserve(AS.buyer, types.paid, 2);
    const A = a.json?.id;
    const held = await read(A);
    const h = held.json;
    check(
      held.status === 200 && h?.status === 'HELD' && h?.totalCents === 5000 && h?.currency === 'SGD' && h?.payment === null &&
        h?.items?.length === 1 && h.items[0].name === 'Paid' && h.items[0].quantity === 2 && h.items[0].unitPriceCents === 2500,
      `the buyer reads their hold: HELD, 2 x 2500 = 5000 SGD, no payment yet (${held.status} ${held.text.slice(0, 160)})`,
    );
    check(h?.event?.id === EVENT && h?.event?.title === `Reads ${RUN}` && typeof h?.event?.startsAt === 'string', `with the event it is for (${JSON.stringify(h?.event)})`);
    check(Number.isInteger(h?.holdSecondsLeft) && h.holdSecondsLeft > 0, `and a countdown, while it is a hold (${h?.holdSecondsLeft} s)`);
    const expiresInDb = await rendered('expiresAt', A);
    check(h?.holdExpiresAt === expiresInDb, `its deadline is the one stored (${h?.holdExpiresAt} = ${expiresInDb})`);

    const paid = await api('POST', `/reservations/${A}/pay`, { as: AS.buyer });
    check(paid.status === 200, `CONTROL: it is paid (${paid.status} ${paid.text.slice(0, 80)})`);
    const confirmed = (await read(A)).json;
    check(
      confirmed?.status === 'CONFIRMED' && confirmed?.payment?.status === 'SUCCEEDED' && confirmed?.payment?.amountCents === 5000 &&
        confirmed?.payment?.id === paid.json?.paymentId,
      `read again: CONFIRMED, by the payment that settled it (${JSON.stringify(confirmed?.payment)})`,
    );
    check(confirmed?.holdSecondsLeft === null && confirmed?.holdExpiresAt === null, `and no countdown once it is not a hold (${confirmed?.holdSecondsLeft}, ${confirmed?.holdExpiresAt})`);
    const confirmedInDb = await rendered('confirmedAt', A);
    check(confirmed?.confirmedAt !== null && confirmed?.confirmedAt === confirmedInDb, `confirmedAt is the one stored (${confirmed?.confirmedAt} = ${confirmedInDb})`);
    const repriced = await api('PATCH', `/ticket-types/${types.paid}`, { as: AS.owner, body: { priceCents: 2600 }, headers: { 'If-Match': '1' } });
    const afterRepricing = (await read(A)).json;
    check(
      repriced.status === 200 && afterRepricing?.items?.[0]?.unitPriceCents === 2500 && afterRepricing?.totalCents === 5000,
      `the organiser reprices the type to 2600, and the buyer still sees what they paid: 2500 each, 5000 (${repriced.status}; ${afterRepricing?.items?.[0]?.unitPriceCents}, ${afterRepricing?.totalCents})`,
    );

    console.log('');
    console.log('  a declined card, twice; a provider that never answered');
    const b = await reserve(AS.buyer, types.declined);
    const B = b.json?.id;
    const firstTry = await api('POST', `/reservations/${B}/pay`, { as: AS.buyer });
    const secondTry = await api('POST', `/reservations/${B}/pay`, { as: AS.buyer });
    check(firstTry.status === 402 && secondTry.status === 402 && firstTry.json?.paymentId !== secondTry.json?.paymentId, `CONTROL: two attempts, both declined (${firstTry.status}, ${secondTry.status})`);
    const declined = (await read(B)).json;
    check(
      declined?.status === 'HELD' && declined?.payment?.status === 'FAILED' && declined?.payment?.id === secondTry.json?.paymentId,
      `still a hold, showing the LATEST attempt, declined (${declined?.status}; shows ${declined?.payment?.id === firstTry.json?.paymentId ? 'the first' : declined?.payment?.id === secondTry.json?.paymentId ? 'the second' : 'neither'})`,
    );
    const c = await reserve(AS.buyer, types.unknown);
    const C = c.json?.id;
    const unanswered = await api('POST', `/reservations/${C}/pay`, { as: AS.buyer });
    check(unanswered.status === 202, `CONTROL: the provider does not answer, and /pay says 202 (${unanswered.status})`);
    const pending = (await read(C)).json;
    check(
      pending?.status === 'PENDING_PAYMENT' && pending?.payment?.status === 'UNKNOWN' && pending?.payment?.id === unanswered.json?.paymentId && pending?.holdSecondsLeft === null,
      `the buyer can now ask how it went: PENDING_PAYMENT, attempt UNKNOWN, no countdown (${pending?.status}, ${pending?.payment?.status}, ${pending?.holdSecondsLeft})`,
    );
    const answered = await providerSays(unanswered.json?.paymentId);
    const settledLater = (await read(C)).json;
    check(
      answered.ok && settledLater?.status === 'CONFIRMED' && settledLater?.payment?.status === 'SUCCEEDED' && settledLater?.payment?.id === unanswered.json?.paymentId,
      `and once the provider's answer arrives, it reads CONFIRMED (${answered.status}; ${settledLater?.status}, ${settledLater?.payment?.status})`,
    );

    console.log('');
    console.log('  whose it is');
    const d = await reserve(AS.other, types.paid);
    const D = d.json?.id;
    check(Boolean(D), `CONTROL: another buyer has a reservation of their own (${d.status})`);
    const byOther = await read(A, AS.other);
    check(byOther.status === 404, `another buyer is told it does not exist (${byOther.status})`);
    const byOrganiser = await read(A, AS.owner);
    check(byOrganiser.status === 404, `so is the event's own organiser (${byOrganiser.status})`);
    const byOtherTenant = await read(A, AS.otherTenant);
    check(byOtherTenant.status === 404, `and another tenant's ADMIN (${byOtherTenant.status})`);
    const byAdmin = await read(A, AS.admin);
    check(byAdmin.status === 200 && byAdmin.json?.id === A, `CONTROL: an ADMIN of the tenant reads it, so it does exist (${byAdmin.status})`);
    const otherList = await list('', AS.other);
    check(otherList.status === 200 && JSON.stringify(ids(otherList)) === JSON.stringify([D]), `the other buyer's list is theirs alone (${JSON.stringify(ids(otherList))})`);
    const adminList = await list('', AS.admin);
    check(adminList.status === 200 && ids(adminList).length === 0, `an ADMIN's list is their own too, and they bought nothing (${ids(adminList).length})`);

    console.log('');
    console.log('  paging, newest first');
    const first = await list('?limit=2');
    check(JSON.stringify(ids(first)) === JSON.stringify([C, B]) && typeof first.json?.nextCursor === 'string', `page 1: the two newest, and a cursor (${JSON.stringify(ids(first))})`);
    const e = await reserve(AS.buyer, types.paid);
    const E = e.json?.id;
    const second = await list(`?limit=2&cursor=${first.json?.nextCursor}`);
    check(
      JSON.stringify(ids(second)) === JSON.stringify([A]) && second.json?.nextCursor === null,
      `a hold taken between pages does not push one back onto page 2: [A], and the end (${JSON.stringify(ids(second))}, next ${second.json?.nextCursor})`,
    );
    const everything = await list('?limit=50');
    check(JSON.stringify(ids(everything)) === JSON.stringify([E, C, B, A]), `all of them, the new hold first (${ids(everything).length} of 4, in order: ${JSON.stringify(ids(everything)) === JSON.stringify([E, C, B, A])})`);
    const onlyConfirmed = await list('?status=CONFIRMED');
    check(JSON.stringify(ids(onlyConfirmed)) === JSON.stringify([C, A]), `?status=CONFIRMED: the two that were paid (${JSON.stringify(ids(onlyConfirmed))})`);
    const onlyHeld = await list('?status=HELD');
    check(JSON.stringify(ids(onlyHeld)) === JSON.stringify([E, B]), `?status=HELD: the new one and the declined one (${JSON.stringify(ids(onlyHeld))})`);

    // Two made in the same millisecond, older than the rest. Lapsed, so the sweeper
    // has nothing to do with them.
    await inTenant(db, TENANT, async () => {
      for (const id of TIES) {
        await db.query(
          `INSERT INTO "Reservation" (id, "tenantId", "eventId", "userId", status, "expiresAt", "createdAt", "updatedAt")
           VALUES ($1, $2, $3, $4, 'EXPIRED', '2020-01-01 00:10:00', '2020-01-01 00:00:00.000', now())`,
          [id, TENANT, EVENT, BUYER],
        );
        fixtures += 1;
      }
    });
    const walked = [];
    let cursor = '';
    for (let page = 0; page < 10; page += 1) {
      const r = await list(`?limit=1${cursor ? `&cursor=${cursor}` : ''}`);
      walked.push(...ids(r));
      if (!r.json?.nextCursor) break;
      cursor = r.json.nextCursor;
    }
    const expected = [E, C, B, A, TIES[1], TIES[0]];
    check(JSON.stringify(walked) === JSON.stringify(expected), `one at a time, every one exactly once, both of the same millisecond included (${walked.length} of ${expected.length}: ${walked.map((w) => (TIES.includes(w) ? w.slice(0, 8) : '.')).join(' ')})`);

    console.log('');
    console.log("  the countdown is the database's");
    // Made by SQL to lapse five minutes from now on Postgres's clock. The service's own
    // clock is an hour fast, so it would call this lapsed long ago.
    await inTenant(db, TENANT, () =>
      db.query(
        `INSERT INTO "Reservation" (id, "tenantId", "eventId", "userId", status, "expiresAt", "updatedAt")
         VALUES ($1, $2, $3, $4, 'HELD', now() + interval '5 minutes', now())`,
        [LAPSING, TENANT, EVENT, CLOCK],
      ),
    );
    fixtures += 1;
    const lapsing = (await read(LAPSING, AS.clock)).json;
    check(
      Number.isInteger(lapsing?.holdSecondsLeft) && lapsing.holdSecondsLeft > 280 && lapsing.holdSecondsLeft <= 300,
      `a hold with five minutes left says so, about 300 s (${lapsing?.holdSecondsLeft} s)`,
    );

    console.log('');
    console.log('  nothing malformed is a 500');
    const impossible = Buffer.from(JSON.stringify({ at: '2026-02-30T00:00:00.000', id: A })).toString('base64url');
    const nulId = Buffer.from(JSON.stringify({ at: '2026-01-01T00:00:00.000', id: 'a\u0000b' })).toString('base64url');
    for (const [query, what] of [
      ['?cursor=garbage', 'a cursor this list never issued'],
      [`?cursor=${impossible}`, 'a cursor on 30 February'],
      [`?cursor=${nulId}`, 'a cursor naming an id with a NUL'],
      [`?cursor=${first.json?.nextCursor}&cursor=${first.json?.nextCursor}`, 'two cursors'],
      ['?limit=0', 'a limit of 0'],
      ['?limit=51', 'a limit past 50'],
      ['?limit=ten', 'a limit that is not a number'],
      ['?status=SOLD', 'a status that does not exist'],
    ]) {
      const r = await list(query);
      check(r.status === 400, `${what}: 400 (${r.status})`);
    }
    for (const [id, what] of [['%00', 'a NUL'], ['x'.repeat(65), 'an id too long to be one'], ['a%2Fb', 'a slash']]) {
      const r = await read(id);
      check(r.status === 404, `a reservation id of ${what}: 404 (${r.status})`);
    }
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    /**
     * WHAT WAS BOUGHT STAYS. A payment is never deleted: the runtime role has no grant
     * for it, and the owner matches no policy on it, so under FORCE it sees nothing to
     * delete. Every payment verifier leaves its purchases behind; this one leaves them
     * SETTLED and CONSISTENT, so nothing else is disturbed by them -- no attempt a
     * reconciler pass in another verifier would go and settle, and counters that agree
     * with their reservations, which verify-inventory-reconciliation asks of every
     * tenant. What this run made by SQL, with no money on it, goes.
     */
    const unsettled = await inTenant(db, TENANT, () => db.query(`SELECT id FROM "Payment" WHERE "tenantId" = $1 AND "settledAt" IS NULL`, [TENANT]))
      .then((r) => r.rows.map((p) => p.id), () => []);
    if (child?.exitCode === null) for (const id of unsettled) await providerSays(id).catch(() => {});
    if (child) {
      child.kill();
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
    const end = await inTenant(db, TENANT, async () => {
      const removed = (await db.query(`DELETE FROM "Reservation" WHERE "tenantId" = $1 AND id = ANY($2)`, [TENANT, [...TIES, LAPSING]])).rowCount;
      const open = (await db.query(`SELECT count(*)::int AS n FROM "Payment" WHERE "tenantId" = $1 AND "settledAt" IS NULL`, [TENANT])).rows[0].n;
      const counters = (
        await db.query(
          `SELECT t.id, t.held, t.sold,
                  coalesce((SELECT sum(i.quantity) FROM "ReservationItem" i JOIN "Reservation" r ON r."tenantId" = i."tenantId" AND r.id = i."reservationId"
                             WHERE i."ticketTypeId" = t.id AND r.status IN ('HELD', 'PENDING_PAYMENT')), 0)::int AS "explainedHeld",
                  coalesce((SELECT sum(i.quantity) FROM "ReservationItem" i JOIN "Reservation" r ON r."tenantId" = i."tenantId" AND r.id = i."reservationId"
                             WHERE i."ticketTypeId" = t.id AND r.status = 'CONFIRMED'), 0)::int AS "explainedSold"
             FROM "TicketType" t
            WHERE t."tenantId" = $1`,
          [TENANT],
        )
      ).rows;
      const kept = (await db.query(`SELECT (SELECT count(*) FROM "Reservation" WHERE "tenantId" = $1)::int AS r, (SELECT count(*) FROM "Payment" WHERE "tenantId" = $1)::int AS p`, [TENANT])).rows[0];
      return { removed, open, counters, kept };
    }).catch((err) => ({ error: err.message }));
    const agree = end.counters?.filter((c) => c.held === c.explainedHeld && c.sold === c.explainedSold).length;
    check(
      !end.error && end.removed === fixtures && end.open === 0 && end.counters.length > 0 && agree === end.counters.length,
      `cleanup: deleted the ${end.removed} of ${fixtures} reservations made by SQL; kept ${end.kept?.r} reservations and ${end.kept?.p} payments, ${end.open} unsettled, ${agree} of ${end.counters?.length} ticket types agreeing with them${end.error ? ` (${end.error})` : ''}`,
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
  console.log('a buyer can see what they bought, and nobody else can');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
