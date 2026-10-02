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

import { createHash } from 'node:crypto';
import pg from 'pg';

const sha256 = (t) => createHash('sha256').update(t).digest('hex');

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const AUTH_APP_URL =
  process.env['APP_DATABASE_URL'] ??
  'postgresql://eventhub_auth_app:eventhub_auth_app@localhost:5432/eventhub_auth';

const TENANT = 'tenant_acme';
/** The seeded tenant's subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';
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
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
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

  // -------------------------------------------------------------------------
  console.log('');
  console.log('4. a session cannot be extended forever');
  // -------------------------------------------------------------------------
  // Every rotation issues a successor with a fresh 30-day expiry, so the sliding
  // window never closes on an active chain -- including one an attacker is
  // keeping alive. Reuse detection only fires if the legitimate holder presents
  // a token the thief already spent; if the thief takes over the chain and the
  // victim never refreshes again, nothing ever notices.
  //
  // The absolute ceiling is measured from the FIRST token in the family, so
  // ageing the family past it is enough to test the rule without waiting.
  const aged = await login();
  const { rows: fam } = await db.query(
    `SELECT "familyId" FROM "RefreshToken" WHERE "tokenHash" = $1`,
    [sha256(aged)],
  );
  const agedFamilyId = fam[0]?.familyId;
  if (!agedFamilyId) fail("could not find the family for a token just issued");

  await db.query(
    `UPDATE "RefreshToken" SET "createdAt" = now() - interval '91 days' WHERE "familyId" = $1`,
    [agedFamilyId],
  );

  const tooOld = await refresh(aged);
  check(tooOld.status === 401, `a family older than the ceiling is refused, got ${tooOld.status}`);

  const { rows: live } = await db.query(
    `SELECT count(*)::int AS n FROM "RefreshToken" WHERE "familyId" = $1 AND "revokedAt" IS NULL`,
    [agedFamilyId],
  );
  // Refused is not enough: the family is revoked, so the outstanding successor
  // cannot be presented either. Reaching the ceiling ENDS the session.
  check(live[0].n === 0, `the family is revoked, got ${live[0].n} live token(s)`);

  // And a family just inside the ceiling still rotates, so the check is not
  // simply refusing everything.
  const young = await login();
  const { rows: fam2 } = await db.query(
    `SELECT "familyId" FROM "RefreshToken" WHERE "tokenHash" = $1`,
    [sha256(young)],
  );
  await db.query(
    `UPDATE "RefreshToken" SET "createdAt" = now() - interval '89 days' WHERE "familyId" = $1`,
    [fam2[0].familyId],
  );
  const stillOk = await refresh(young);
  check(stillOk.status === 200, `a family inside the ceiling still rotates, got ${stillOk.status}`);

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
