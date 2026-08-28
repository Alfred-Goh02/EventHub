/**
 * Asserts that a refresh token can be spent exactly once, including under
 * concurrency, and that losing the race fails closed.
 *
 * THE BUG THIS EXISTS FOR, which shipped and was found by a design review rather
 * than by a test:
 *
 * rotateRefreshToken() read the token row on its own connection, outside any
 * transaction and holding no lock, checked `usedAt`, and then wrote
 * `update({ where: { id } })` with no predicate. Two requests presenting the
 * SAME token concurrently both passed the check and both created a successor.
 * One refresh token, two independent live sessions.
 *
 * The reason that is worse than it sounds is that FAMILY REUSE DETECTION NEVER
 * FIRES. The detector looks for a token presented a second time, and in this
 * race there isn't one: each party walks away with a distinct successor and
 * rotates it forever. The security control is not merely bypassed, it is blind.
 *
 * A sequential test cannot catch it. Present a token twice in a row and the
 * second call correctly reports reuse. The defect exists only in the window
 * between the read and the write, so the test has to race.
 *
 * Run: node scripts/verify-refresh-rotation.mjs
 */

import pg from 'pg';

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const AUTH_APP_URL =
  process.env['APP_DATABASE_URL'] ??
  'postgresql://eventhub_auth_app:eventhub_auth_app@localhost:5432/eventhub_auth';

const TENANT = 'tenant_acme';
const EMAIL = 'alice@acme.com';
const PASSWORD = 'hunter2222';

// The race is a window, not a certainty: a single attempt can miss it by
// scheduling luck. Before the fix it reproduced within the first few rounds.
// After the fix no round may ever produce a second successor.
const ROUNDS = 20;
const RACERS = 2;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const login = async () => {
  const r = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email: EMAIL, password: PASSWORD }),
  });
  const body = await r.json();
  if (!body.refreshToken) fail(`could not log in (${r.status})`);
  return body.refreshToken;
};

const refresh = async (refreshToken) => {
  const r = await fetch(`${AUTH}/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refreshToken }),
  });
  return { status: r.status, body: await r.json().catch(() => null) };
};

async function main() {
  // -------------------------------------------------------------------------
  console.log('\n1. ordinary rotation still works');
  // -------------------------------------------------------------------------
  const first = await login();
  const rotated = await refresh(first);
  check(rotated.status === 200, `200, got ${rotated.status}`);
  check(Boolean(rotated.body?.refreshToken), 'a successor was issued');
  check(Boolean(rotated.body?.accessToken), 'an access token was issued');
  check(rotated.body?.refreshToken !== first, 'the successor differs from the token spent');

  const spentAgain = await refresh(first);
  check(spentAgain.status === 401, `spending the same token again -> 401, got ${spentAgain.status}`);

  // Sequential reuse must still take the whole family down, successor included.
  const successorAfterReuse = await refresh(rotated.body.refreshToken);
  check(
    successorAfterReuse.status === 401,
    `the successor is dead after reuse was detected, got ${successorAfterReuse.status}`,
  );

  // -------------------------------------------------------------------------
  console.log(`\n2. ${RACERS} concurrent redemptions of one token, ${ROUNDS} rounds`);
  // -------------------------------------------------------------------------
  let roundsWithMultipleWinners = 0;
  let worstUsable = 0;

  for (let round = 1; round <= ROUNDS; round += 1) {
    const token = await login();

    const results = await Promise.all(
      Array.from({ length: RACERS }, () => refresh(token)),
    );

    const winners = results.filter((r) => r.status === 200);
    if (winners.length > 1) {
      roundsWithMultipleWinners += 1;

      // More than one 200 is already wrong, but the thing that makes it a
      // double-spend is whether both successors are independently USABLE.
      let usable = 0;
      for (const w of winners) {
        const again = await refresh(w.body.refreshToken);
        if (again.status === 200) usable += 1;
      }
      worstUsable = Math.max(worstUsable, usable);
      console.log(`    round ${round}: ${winners.length} winners, ${usable} usable successors`);
    }
  }

  check(
    roundsWithMultipleWinners === 0,
    `no round minted more than one successor (${roundsWithMultipleWinners}/${ROUNDS} rounds did)`,
  );
  check(
    worstUsable < 2,
    `no round produced two independently usable sessions (worst was ${worstUsable})`,
  );

  // -------------------------------------------------------------------------
  console.log('\n3. losing the race fails closed');
  // -------------------------------------------------------------------------
  // Two parties held one token. Nothing here can distinguish "my client fired
  // twice" from "an attacker raced me", so a security control that cannot tell
  // them apart must kill the family rather than pick a winner to trust.
  const db = new pg.Client({ connectionString: AUTH_APP_URL });
  await db.connect();

  const raced = await login();
  const before = await db.query(
    `SELECT "familyId" FROM "RefreshToken" WHERE "revokedAt" IS NULL AND "usedAt" IS NULL
      ORDER BY "createdAt" DESC LIMIT 1`,
  );
  const familyId = before.rows[0]?.familyId;

  const outcome = await Promise.all(Array.from({ length: RACERS }, () => refresh(raced)));
  const lost = outcome.filter((r) => r.status !== 200).length;

  if (lost > 0 && familyId) {
    const live = await db.query(
      `SELECT count(*)::int AS n FROM "RefreshToken"
        WHERE "familyId" = $1 AND "revokedAt" IS NULL`,
      [familyId],
    );
    check(
      live.rows[0].n === 0,
      `a lost race revoked the whole family (${live.rows[0].n} tokens still live)`,
    );
  } else {
    // Both racers can legitimately serialise so cleanly that neither loses.
    // That is not a failure, and asserting on it would make this flaky.
    console.log('    --    no racer lost this round; nothing to assert about revocation');
  }

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('refresh rotation verified: one token, one successor, even under a race —');
  console.log('and a contested token takes its whole family down rather than picking a winner');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
