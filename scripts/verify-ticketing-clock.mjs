/**
 * Asserts that every timestamp Ticketing writes for a hold or a payment is on the
 * database's clock, which is the clock everything that reads them uses.
 *
 * The sweeper reclaims a hold on `"expiresAt" <= now()`. The reconciler selects a
 * payment on `"startedAt"` and `"nextLookupAt"` against `now()`. The webhook and the
 * reconciler settle with `now()`. The reservation route and the pay endpoint stamped
 * those same columns with `new Date()` -- and Prisma fills a `@default(now())` in the
 * client, with `new Date()` too -- so an app server whose clock disagreed with the
 * database's moved every one of them by the disagreement.
 *
 * Ticketing runs here with its clock an HOUR FAST (scripts/lib/skewed-clock.mjs). On a
 * machine whose clocks agree, a stamp from the process lands on the database's
 * millisecond and no check can tell the two apart. Every comparison is made in SQL,
 * the way the readers make it: a timestamp read into JS is compared on a third clock.
 *
 *   - A hold taken through the API expires ten minutes after the database's now(), not
 *     seventy, and the buyer is told the deadline that is stored.
 *   - A paid reservation's paymentStartedAt and confirmedAt, and its payment's
 *     startedAt, nextLookupAt and settledAt, are within seconds of now(), not an hour
 *     ahead.
 *   - A declined payment's settledAt likewise.
 *
 * Starts its own Ticketing on a consumer group of this run's, from the end of the topic,
 * and seeds the event projection it needs directly. Needs Postgres and Kafka up, and
 * Ticketing built:
 *   node scripts/verify-ticketing-clock.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomBytes, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const PORT = 3971;
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_clock_${RUN}`;
const GROUP = `verify-ticketing-clock-${RUN}`;
const BUYER = `user_clock_buyer_${RUN}`;
const EVENT = `evt_clock_${RUN}`;
// The fake provider decides by the amount's last two digits: 00 succeeds, 01 is declined.
const PAID_TYPE = `tt_clock_paid_${RUN}`;
const DECLINED_TYPE = `tt_clock_declined_${RUN}`;

/** The hold's lifetime, as the route sets it. */
const HOLD_S = 600;
/** How far behind the database's now() a stamp may be by the time it is read. The skew is 3600. */
const NEAR_S = 10;

const env = serviceEnv(ROOT, 'ticketing', {
  PORT: String(PORT),
  AUTH_PUBLIC_KEY_B64: readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64'),
  KAFKA_CONSUMER_GROUP: GROUP,
  PROJECTION_START_FROM: 'latest',
  // An hour fast, so a stamp from this process's clock is an hour out.
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
function token(userId, role) {
  const now = Math.floor(Date.now() / 1000);
  const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  // Good for three hours. The service checks exp on ITS clock, an hour fast, where a
  // fifteen-minute token has already expired.
  const body = b64url(JSON.stringify({ sub: userId, tenantId: TENANT, tenantSlug: TENANT.replace(/_/g, '-'), role, iss: env.JWT_ISSUER, aud: env.JWT_AUDIENCE, iat: now, exp: now + 3 * 3600 }));
  return `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), PRIVATE))}`;
}
const AS_BUYER = token(BUYER, 'ATTENDEE');

async function api(method, path, { body, headers = {} } = {}) {
  const res = await fetch(`http://127.0.0.1:${PORT}${path}`, {
    method,
    headers: { Authorization: `Bearer ${AS_BUYER}`, ...(body ? { 'Content-Type': 'application/json' } : {}), ...headers },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {}
  return { status: res.status, json, text };
}
const hold = (ticketTypeId, key) =>
  api('POST', '/reservations', {
    headers: { 'Idempotency-Key': `clock-${RUN}-${key}` },
    body: { eventId: EVENT, items: [{ ticketTypeId, quantity: 1 }] },
  });

/** Statements as the runtime role inside this run's tenant, the context the service writes in. */
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

/**
 * Each stamp as seconds from the database's now(), computed by Postgres. A stamp on the
 * database's clock is a little behind it; one on the process's clock is an hour ahead.
 */
const OFFSETS = `
  SELECT r.status,
         p.status AS "paymentStatus",
         extract(epoch FROM r."expiresAt" - now())::float8 AS "expiresAt",
         extract(epoch FROM r."paymentStartedAt" - now())::float8 AS "paymentStartedAt",
         extract(epoch FROM r."confirmedAt" - now())::float8 AS "confirmedAt",
         extract(epoch FROM p."startedAt" - now())::float8 AS "startedAt",
         extract(epoch FROM p."nextLookupAt" - now())::float8 AS "nextLookupAt",
         extract(epoch FROM p."settledAt" - now())::float8 AS "settledAt"
    FROM "Reservation" r
    LEFT JOIN "Payment" p ON p."reservationId" = r.id
   WHERE r.id = $1`;

const near = (s) => typeof s === 'number' && s > -NEAR_S && s < 1;
const fmt = (s) => (typeof s === 'number' ? `${s >= 0 ? '+' : ''}${s.toFixed(3)} s` : String(s));

async function main() {
  const taken = await fetch(`http://127.0.0.1:${PORT}/health`).then(() => true, () => false);
  if (taken) {
    console.error(`FAIL: something already answers on :${PORT}. Stop it before running this.`);
    process.exitCode = 1;
    return;
  }

  const db = new pg.Client({ connectionString: env.APP_DATABASE_URL });
  await db.connect();
  const kafka = new Kafka({ clientId: `verify-ticketing-clock-${RUN}`, brokers: env.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  let child = null;
  let output = '';
  try {
    await inTenant(db, async () => {
      await db.query(
        `INSERT INTO "EventProjection" (id, "tenantId", title, slug, status, capacity, "startsAt", "endsAt", "organiserId", deleted, "sourceVersion", "updatedAt")
         VALUES ($1, $2, $1, $1, 'PUBLISHED', 100, now() + interval '30 days', now() + interval '31 days', $3, false, 1, now())`,
        [EVENT, TENANT, `user_clock_owner_${RUN}`],
      );
      for (const [id, priceCents] of [[PAID_TYPE, 1000], [DECLINED_TYPE, 1001]]) {
        await db.query(
          `INSERT INTO "TicketType" (id, "tenantId", "eventId", name, "priceCents", total, remaining, held, sold, "updatedAt")
           VALUES ($1, $2, $3, $1, $4, 10, 10, 0, 0, now())`,
          [id, TENANT, EVENT, priceCents],
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
    check(output.includes('clock skewed by +3600s for this process'), "CONTROL: its clock runs an hour fast, so a stamp from it cannot pass for the database's");

    console.log('');
    console.log('  a hold');
    const held = await hold(PAID_TYPE, 'paid');
    check(held.status === 201, `CONTROL: a hold is taken through the API (${held.status} ${held.text.slice(0, 80)})`);
    const told = await inTenant(db, () =>
      db.query(
        `SELECT extract(epoch FROM "expiresAt" - now())::float8 AS "expiresIn",
                abs(extract(epoch FROM "expiresAt" - $2::timestamptz))::float8 AS "apart"
           FROM "Reservation" WHERE id = $1`,
        [held.json?.id ?? '', held.json?.expiresAt ?? null],
      ),
    );
    const expiresIn = told.rows[0]?.expiresIn;
    check(
      typeof expiresIn === 'number' && expiresIn > HOLD_S - NEAR_S && expiresIn <= HOLD_S + 1,
      `it expires ten minutes after the database's now(), the clock the sweeper reclaims by (in ${fmt(expiresIn)}; from the process's clock it would be ${HOLD_S + 3600})`,
    );
    check(told.rows[0]?.apart === 0, `the buyer is told the deadline that is stored (${held.json?.expiresAt}, ${fmt(told.rows[0]?.apart)} apart)`);

    console.log('');
    console.log('  a payment that succeeds');
    const paid = await api('POST', `/reservations/${held.json?.id}/pay`);
    check(paid.status === 200 && paid.json?.status === 'CONFIRMED', `CONTROL: the payment confirms the hold (${paid.status} ${paid.text.slice(0, 80)})`);
    const p = (await inTenant(db, () => db.query(OFFSETS, [held.json?.id ?? '']))).rows[0] ?? {};
    check(p.status === 'CONFIRMED' && p.paymentStatus === 'SUCCEEDED', `CONTROL: stored as CONFIRMED with a SUCCEEDED payment (${p.status}, ${p.paymentStatus})`);
    check(near(p.paymentStartedAt), `the reservation's paymentStartedAt is the database's now() (${fmt(p.paymentStartedAt)})`);
    check(near(p.startedAt), `the payment's startedAt, which the reconciler's grace is measured from, is the database's now() (${fmt(p.startedAt)})`);
    check(near(p.nextLookupAt), `the payment's nextLookupAt, which the reconciler waits for, is the database's now() (${fmt(p.nextLookupAt)})`);
    check(near(p.settledAt), `the payment's settledAt is the database's now(), as the webhook and the reconciler stamp it (${fmt(p.settledAt)})`);
    check(near(p.confirmedAt), `the reservation's confirmedAt is the database's now(), as the webhook and the reconciler stamp it (${fmt(p.confirmedAt)})`);

    console.log('');
    console.log('  a payment that is declined');
    const declinedHold = await hold(DECLINED_TYPE, 'declined');
    const declined = await api('POST', `/reservations/${declinedHold.json?.id}/pay`);
    const d = (await inTenant(db, () => db.query(OFFSETS, [declinedHold.json?.id ?? '']))).rows[0] ?? {};
    check(
      declinedHold.status === 201 && declined.status === 402 && d.status === 'HELD' && d.paymentStatus === 'FAILED',
      `CONTROL: the card is declined and the seats go back to a plain hold (${declinedHold.status}, ${declined.status}; ${d.status}, ${d.paymentStatus})`,
    );
    check(near(d.settledAt), `the declined payment's settledAt is the database's now() (${fmt(d.settledAt)})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    if (child) {
      child.kill();
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
    // In this run's tenant, as the role that wrote them, and counted. Payment rows stay:
    // this role has no DELETE on them, as for every payment verifier. Each is settled, so
    // no reconciler will select it.
    const deleted = await inTenant(db, async () => {
      const n = {};
      for (const table of ['ReservationItem', 'Reservation', 'TicketType', 'EventProjection', 'IdempotencyKey', 'OutboxMessage']) {
        n[table] = (await db.query(`DELETE FROM "${table}" WHERE "tenantId" = $1`, [TENANT])).rowCount;
      }
      return n;
    }).catch((err) => ({ error: err.message }));
    const left = await inTenant(db, () =>
      db.query(
        `SELECT (SELECT count(*) FROM "Reservation" WHERE "tenantId" = $1)::int
              + (SELECT count(*) FROM "TicketType" WHERE "tenantId" = $1)::int
              + (SELECT count(*) FROM "EventProjection" WHERE "tenantId" = $1)::int AS rows,
                (SELECT count(*) FROM "Payment" WHERE "tenantId" = $1)::int AS payments,
                (SELECT count(*) FROM "Payment" WHERE "tenantId" = $1 AND "settledAt" IS NULL)::int AS unsettled`,
        [TENANT],
      ),
    ).then((r) => r.rows[0], () => ({ rows: -1, payments: -1, unsettled: -1 }));
    check(
      !deleted.error && deleted.Reservation === 2 && deleted.EventProjection === 1 && left.rows === 0 && left.unsettled === 0,
      `cleanup: deleted ${deleted.error ?? Object.entries(deleted).map(([t, n]) => `${n} ${t}`).join(', ')}; ${left.rows} reservations, types and events left, and ${left.payments} settled payment(s), ${left.unsettled} unsettled`,
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
  console.log("every hold and payment timestamp Ticketing writes is on the database's clock, the one its readers compare with");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
