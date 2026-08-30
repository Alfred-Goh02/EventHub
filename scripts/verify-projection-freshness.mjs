/**
 * Asserts that "I have never heard of that event" and "I have not caught up yet"
 * get DIFFERENT answers.
 *
 * Ticketing learns about events by consuming a topic, so there is always a
 * window in which an event is real, on sale, and absent from the projection.
 * Answering 404 in that window tells a customer an event does not exist while
 * they are trying to buy a ticket for it — and a client that trusts 404 will not
 * retry.
 *
 * ConsumerCheckpoint was built for this decision and the schema comment has
 * described it since it was written, but projectionLagSeconds() was exported and
 * called by NOTHING. Worse, it measured the wrong thing: wall-clock since the
 * last checkpoint WRITE, which on a healthy idle system reads as hours. Wiring
 * it in as written turned every legitimate 404 into a 503 — measured at 12329
 * seconds of "lag" on a stack that was perfectly up to date.
 *
 * What replaced it needs BOTH halves, and this asserts both:
 *
 *   liveness      the consumer still holds its assignment (heartbeat)
 *   completeness  a fetch came back EMPTY, so the broker has nothing more
 *
 * The second is the one that catches a cold start. kafkajs heartbeats as soon as
 * the group is joined, which is before a compacted topic has been replayed, so
 * liveness alone would claim "current" during exactly the window this exists for.
 *
 * PROJECTION_COLD_START=1 asserts the 503 half. Set it when the caller has just
 * started the service, because that is the only moment the precondition is
 * guaranteed. Without it only the 404 half is checked — and a run that checks
 * one half says so rather than implying it checked both.
 *
 * Run: node scripts/verify-projection-freshness.mjs
 */

const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const TENANT = 'tenant_acme';
const COLD_START = process.env['PROJECTION_COLD_START'] === '1';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await login.json();
  if (!accessToken) fail('could not log in');

  const probe = async (tag) => {
    const r = await fetch(`${TICKETING}/reservations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
        'Idempotency-Key': `freshness_${tag}_${Date.now()}_${Math.random()}`,
      },
      body: JSON.stringify({
        eventId: `no_such_event_${Date.now()}`,
        items: [{ ticketTypeId: 'no_such_type', quantity: 1 }],
      }),
    });
    return {
      status: r.status,
      retryAfter: r.headers.get('retry-after'),
      body: await r.json().catch(() => null),
    };
  };

  // -------------------------------------------------------------------------
  // 1. The 503 half — only assertable on a cold start.
  // -------------------------------------------------------------------------
  if (COLD_START) {
    console.log('\n1. before catching up, a missing row is retryable rather than final');

    const first = await probe('cold');
    check(first.status === 503, `the first probe after startup is 503, got ${first.status}`);
    check(
      first.retryAfter !== null,
      `503 carries Retry-After, got ${first.retryAfter === null ? 'none' : first.retryAfter}`,
    );
    check(
      first.body?.error?.includes('caught up'),
      `and says why, got ${JSON.stringify(first.body)}`,
    );
  } else {
    console.log('\n1. the 503 half is NOT checked in this run');
    console.log('   set PROJECTION_COLD_START=1 immediately after starting the service');
  }

  // -------------------------------------------------------------------------
  console.log('\n2. once caught up, an unknown event is genuinely not found');
  // -------------------------------------------------------------------------
  // Poll rather than assume: on a cold start this is legitimately 503 for a few
  // seconds, and asserting immediately would make the test a race.
  //
  // This half is what would catch the mistake I made building it: measuring
  // idleness instead of lag reported hours of "lag" on a caught-up system, so
  // this would never settle to 404.
  let settled = null;
  let probes = 0;
  for (; probes < 30; probes += 1) {
    const r = await probe(`warm${probes}`);
    if (r.status === 404) { settled = r; break; }
    await sleep(1000);
  }

  if (!settled) {
    fail(
      'the projection never reported itself current within 30s. Either the consumer is ' +
        'not running, or it never observes an empty fetch — in which case EVERY unknown ' +
        'event is answered 503 forever and the 404 path is dead.',
    );
  }

  check(settled.status === 404, `an unknown event is 404 once caught up, got ${settled.status}`);
  check(
    settled.body?.error === 'event not found',
    `with the plain not-found message, got ${JSON.stringify(settled.body)}`,
  );
  console.log(`    --    settled after ${probes + 1} probe(s)`);

  console.log('');
  if (failures > 0) {
    console.error(`${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('projection freshness verified: a missing row is only reported as "not found"');
  console.log('once the consumer is both live and demonstrably caught up');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
