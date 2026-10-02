/**
 * Asserts the payment webhook: what it accepts, what it refuses, and — the
 * point of the whole route — where it gets a tenant from.
 *
 * A webhook arrives with no session, no token and no tenant. Every table in this
 * database is FORCE ROW LEVEL SECURITY and every policy reads app.tenant_id, so
 * the ordinary application role sees ZERO rows for a payment that plainly
 * exists. The tempting fix is to read the tenant out of the (signature-verified)
 * payload. This asserts that the route does NOT do that: a body naming a
 * different tenant changes nothing, because the tenant comes from the database.
 *
 * Run: node scripts/verify-payment-webhook.mjs
 */

import { createHmac } from 'node:crypto';
import pg from 'pg';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';
const SECRET = process.env['PAYMENT_WEBHOOK_SECRET'] ?? 'dev_webhook_secret';

const TENANT = 'tenant_acme';
/** The seeded tenant's subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';
const PRICE_UNKNOWN = 1002; // charge() answers "no answer"; see payments/provider.ts

const run = `wh_${Date.now()}`;
const eventId = `${run}_evt`;
const ticketTypeId = `${run}_tt`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const db = new pg.Client({ connectionString: APP_URL });

async function tx(fn) {
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  try {
    const out = await fn();
    await db.query('COMMIT');
    return out;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** Signs exactly the bytes that will be sent, which is the whole trick. */
function post(bodyObject, { secret = SECRET, timestamp = null, signature = null } = {}) {
  const raw = JSON.stringify(bodyObject);
  const t = timestamp ?? Math.floor(Date.now() / 1000);
  const v1 = signature ?? createHmac('sha256', secret).update(`${t}.${raw}`).digest('hex');

  return fetch(`${TICKETING}/webhooks/payments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Payment-Signature': `t=${t},v1=${v1}` },
    body: raw,
  }).then(async (r) => ({ status: r.status, body: await r.json().catch(() => null) }));
}

const paymentState = (reservationId) =>
  tx(async () => {
    const r = await db.query(`SELECT status FROM "Reservation" WHERE id = $1`, [reservationId]);
    const p = await db.query(
      `SELECT id, status, "settledAt", "providerRef" FROM "Payment"
        WHERE "reservationId" = $1 ORDER BY "startedAt"`,
      [reservationId],
    );
    return { status: r.rows[0]?.status, payments: p.rows };
  });

const inventory = () =>
  tx(async () => {
    const { rows } = await db.query(
      `SELECT total, remaining, held, sold FROM "TicketType" WHERE id = $1`,
      [ticketTypeId],
    );
    return rows[0];
  });

async function main() {
  await db.connect();

  await tx(async () => {
    await db.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Webhook','webhook','PUBLISHED',20,now(),now(),'u_test',1,now())`,
      [eventId, TENANT],
    );
    await db.query(
      `INSERT INTO "TicketType"
         (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
       VALUES ($1,$2,$3,'General',$4,10,10,0,0,now())`,
      [ticketTypeId, TENANT, eventId, PRICE_UNKNOWN],
    );
  });

  const loginRes = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await loginRes.json();
  if (!accessToken) fail('could not log in');
  const auth = { Authorization: `Bearer ${accessToken}`, 'Content-Type': 'application/json' };

  const h = await fetch(`${TICKETING}/reservations`, {
    method: 'POST',
    headers: { ...auth, 'Idempotency-Key': `${run}_hold` },
    body: JSON.stringify({ eventId, items: [{ ticketTypeId, quantity: 1 }] }),
  });
  const held = await h.json();
  if (h.status !== 201) fail(`hold: got ${h.status} ${JSON.stringify(held)}`);

  const p = await fetch(`${TICKETING}/reservations/${held.id}/pay`, { method: 'POST', headers: auth });
  if (p.status !== 202) fail(`pay: expected 202 (no answer), got ${p.status}`);

  const stuck = await paymentState(held.id);
  const paymentId = stuck.payments[0].id;
  console.log(`  a charge nobody got an answer to: payment ${paymentId}`);

  // -------------------------------------------------------------------------
  console.log('\n1. what it refuses');
  // -------------------------------------------------------------------------
  const noSig = await fetch(`${TICKETING}/webhooks/payments`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ paymentId, type: 'charge.succeeded' }),
  }).then(async (r) => ({ status: r.status }));
  check(noSig.status === 400, `no signature -> 400, got ${noSig.status}`);

  const wrongSecret = await post({ paymentId, type: 'charge.succeeded' }, { secret: 'not_the_secret' });
  check(wrongSecret.status === 400, `wrong secret -> 400, got ${wrongSecret.status}`);

  const stale = await post(
    { paymentId, type: 'charge.succeeded' },
    { timestamp: Math.floor(Date.now() / 1000) - 3600 },
  );
  check(stale.status === 400, `hour-old timestamp -> 400, got ${stale.status}`);

  // A valid signature over a DIFFERENT body. This is the one a naive
  // implementation gets wrong by signing the re-serialised parsed object.
  const rawA = JSON.stringify({ paymentId, type: 'charge.failed' });
  const t = Math.floor(Date.now() / 1000);
  const sigForA = createHmac('sha256', SECRET).update(`${t}.${rawA}`).digest('hex');
  const swapped = await post({ paymentId, type: 'charge.succeeded' }, { timestamp: t, signature: sigForA });
  check(swapped.status === 400, `signature from another body -> 400, got ${swapped.status}`);

  const afterRefusals = await paymentState(held.id);
  check(afterRefusals.payments[0].settledAt === null, 'nothing was settled by any refused request');
  check(afterRefusals.status === 'PENDING_PAYMENT', `reservation untouched, got ${afterRefusals.status}`);

  // -------------------------------------------------------------------------
  console.log('\n2. a payment id we never issued');
  // -------------------------------------------------------------------------
  // 200, not 404: retrying will not make an unknown id recognisable, and an
  // endless provider retry loop is worse than dropping it.
  const unknown = await post({ paymentId: 'pay_does_not_exist', type: 'charge.succeeded' });
  check(unknown.status === 200, `unknown payment -> 200, got ${unknown.status}`);
  check(unknown.body?.ignored === 'unknown payment', `ignored as unknown, got ${JSON.stringify(unknown.body)}`);

  // -------------------------------------------------------------------------
  console.log('\n3. the tenant comes from the database, not the payload');
  // -------------------------------------------------------------------------
  // A correctly signed body that also names another tenant. If the route trusted
  // the payload, a leaked signing key would be a cross-tenant write primitive.
  // The extra field is simply not read.
  const injected = await post({
    paymentId,
    type: 'charge.succeeded',
    providerRef: 'ch_webhook_1',
    tenantId: 'tenant_globex',
  });
  check(injected.status === 200, `applied, got ${injected.status}`);
  check(injected.body?.result === 'confirmed', `confirmed, got ${JSON.stringify(injected.body)}`);

  const settled = await paymentState(held.id);
  const inv = await inventory();
  check(settled.status === 'CONFIRMED', `reservation CONFIRMED, got ${settled.status}`);
  check(settled.payments[0].status === 'SUCCEEDED', `payment SUCCEEDED, got ${settled.payments[0].status}`);
  check(settled.payments[0].providerRef === 'ch_webhook_1', `provider reference recorded, got ${settled.payments[0].providerRef}`);
  check(inv.held === 0 && inv.sold === 1, `held->sold moved (held=${inv.held} sold=${inv.sold})`);

  // The row still belongs to the tenant that owned it, not the one named in the
  // body — the check that the injected field really was ignored.
  const owner = await tx(async () => {
    const { rows } = await db.query(`SELECT "tenantId" FROM "Payment" WHERE id = $1`, [paymentId]);
    return rows[0]?.tenantId;
  });
  check(owner === TENANT, `payment still belongs to ${TENANT}, got ${owner}`);

  // -------------------------------------------------------------------------
  console.log('\n4. redelivery');
  // -------------------------------------------------------------------------
  // Providers redeliver, sometimes many times. There is no dedupe table: every
  // write in settleAttempt is guarded on state the first delivery already
  // changed. A double held->sold would satisfy every CHECK constraint.
  const again = await post({ paymentId, type: 'charge.succeeded', providerRef: 'ch_webhook_1' });
  const invAgain = await inventory();
  check(again.status === 200, `redelivery -> 200, got ${again.status}`);
  check(again.body?.ignored === 'already settled', `recognised as settled, got ${JSON.stringify(again.body)}`);
  check(invAgain.sold === 1, `sold still 1, got ${invAgain.sold}`);
  check(invAgain.held === 0, `held still 0, got ${invAgain.held}`);
  check(
    invAgain.remaining + invAgain.held + invAgain.sold === invAgain.total,
    'inventory invariant holds',
  );

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('webhook verified: signature-gated, replay-bounded, and the tenant is resolved');
  console.log('from the database rather than taken from the request');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
