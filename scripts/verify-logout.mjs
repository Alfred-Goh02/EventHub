/**
 * Asserts that logging out actually ends the session.
 *
 * WHY THIS EXISTS: /logout had no test at all. Not a weak one — none. Nothing in
 * scripts/ mentioned the route and nothing in ci.yml ran it, which was found by
 * listing every HTTP route in the three services and asking which ones no
 * assertion anywhere referred to. It was the only one.
 *
 * That is a bad route to have unasserted. Its entire job is revocation, it is
 * the control a user reaches for on a shared machine, and every way it can fail
 * fails SILENTLY: it answers 204 and returns nothing, so a logout that revoked
 * nothing is indistinguishable from one that worked. The user believes they are
 * signed out; the refresh token in somebody else's hands still mints sessions
 * for up to thirty days.
 *
 * The implementation was read before this was written and looks right. That is
 * not the point — "I read it and it looked right" is the state /logout was
 * already in. Three specific ways it could be wrong, each of which this file
 * would catch and none of which the 204 would reveal:
 *
 *   revoke the presented ROW rather than the family   the successor stays live,
 *                                                     so logging out mid-session
 *                                                     revokes a token already spent
 *                                                     and leaves the live one alone
 *   revoke by userId rather than familyId             logging out on a laptop
 *                                                     signs you out on your phone
 *   match zero rows and not notice                    the shape this repository
 *                                                     keeps finding: a guarded
 *                                                     updateMany whose count is
 *                                                     discarded. RefreshToken is
 *                                                     RLS-exempt, so it does not
 *                                                     bite here -- but nothing was
 *                                                     checking that it stays exempt
 *
 * Run: node scripts/verify-logout.mjs
 */

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const TENANT = 'tenant_acme';
/** The seeded tenant's subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';
const EMAIL = 'alice@acme.com';
const PASSWORD = 'hunter2222';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

async function login() {
  const r = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email: EMAIL, password: PASSWORD }),
  });
  const body = await r.json().catch(() => null);
  if (!body?.refreshToken) fail(`could not log in (${r.status})`);
  return body.refreshToken;
}

async function refresh(refreshToken) {
  const r = await fetch(`${AUTH}/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refreshToken }),
  });
  return { status: r.status, body: await r.json().catch(() => null) };
}

async function logout(body) {
  const r = await fetch(`${AUTH}/logout`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: r.status, text: await r.text() };
}

async function main() {
  // -------------------------------------------------------------------------
  console.log('\n  1. a session works until you end it (positive control first)');
  // -------------------------------------------------------------------------
  /**
   * The control is the whole reason the assertion below means anything. If
   * /refresh were broken, or the seeded account were gone, every "401 after
   * logout" in this file would be a 401 for reasons that have nothing to do with
   * logging out — and the file would report success while proving nothing.
   */
  const a = await login();
  const beforeLogout = await refresh(a);
  check(beforeLogout.status === 200, `the session refreshes before logout (${beforeLogout.status})`);
  const aSuccessor = beforeLogout.body?.refreshToken;
  check(Boolean(aSuccessor), 'and rotation issued a successor to end');

  const out = await logout({ refreshToken: aSuccessor });
  check(out.status === 204, `logout answers 204 with an empty body (${out.status})`);

  const afterLogout = await refresh(aSuccessor);
  check(
    afterLogout.status === 401,
    afterLogout.status === 401
      ? 'and the token it was given no longer refreshes'
      : `the token still refreshes after logout (${afterLogout.status}) — the user believes ` +
        'they are signed out and the session is alive',
  );

  // -------------------------------------------------------------------------
  console.log('\n  2. the whole family dies, not just the token you handed in');
  // -------------------------------------------------------------------------
  /**
   * Logging out with an ALREADY-SPENT parent, and asserting the LIVE successor
   * dies with it.
   *
   * This is the case a plausible implementation gets wrong: revoke the row whose
   * hash was presented, answer 204, and stop. Everything looks right — logout
   * returned success, and the token that was sent really is dead — while the only
   * token that could still mint a session is untouched.
   *
   * Testing it the other way round proves much less. Logging out with the live
   * token and finding it dead is satisfied by revoking one row.
   */
  const c = await login();
  const rotated = await refresh(c);
  check(rotated.status === 200, `rotated once to create a parent and a child (${rotated.status})`);
  const cSuccessor = rotated.body?.refreshToken;

  const outParent = await logout({ refreshToken: c });
  check(outParent.status === 204, `logging out with the spent parent answers 204 (${outParent.status})`);

  const childAfter = await refresh(cSuccessor);
  check(
    childAfter.status === 401,
    childAfter.status === 401
      ? 'the live successor died with its family'
      : `the live successor still refreshes (${childAfter.status}) — logout revoked one row, ` +
        'not the family, so the session it was meant to end is still running',
  );

  // -------------------------------------------------------------------------
  console.log('\n  3. and only that family — other sessions are untouched');
  // -------------------------------------------------------------------------
  /**
   * The opposite failure, and the one with no security upside to soften it:
   * revoking by userId instead of familyId. Logout would work, in the sense that
   * the session ends, and it would also sign the user out of every other device
   * they own. "Sign out" and "sign out everywhere" are different features and a
   * user chose the first one.
   *
   * Two separate logins are two separate families for the same user, which is
   * exactly the distinction under test.
   */
  const laptop = await login();
  const phone = await login();

  const outLaptop = await logout({ refreshToken: laptop });
  check(outLaptop.status === 204, `one device logs out (${outLaptop.status})`);

  const phoneAfter = await refresh(phone);
  check(
    phoneAfter.status === 200,
    phoneAfter.status === 200
      ? 'the other session is still signed in'
      : `the other session was signed out too (${phoneAfter.status}) — logout revoked by user, ` +
        'not by family, so signing out here signs you out everywhere',
  );

  // -------------------------------------------------------------------------
  console.log('\n  4. and it says nothing about tokens it has never seen');
  // -------------------------------------------------------------------------
  /**
   * A token that does not exist must answer exactly like one that does. A 404 or
   * a 401 here would turn /logout into an oracle for whether a given string is a
   * live refresh token — which is a thing an attacker holding a guess would very
   * much like to check, unauthenticated, for free.
   */
  const unknown = await logout({ refreshToken: 'not-a-real-token-' + Date.now() });
  check(
    unknown.status === out.status && unknown.text === out.text,
    unknown.status === out.status
      ? `an unknown token answers identically (${unknown.status}, same body)`
      : `an unknown token answers ${unknown.status} where a real one answers ${out.status} — ` +
        'that difference tells an attacker their guess was a live token',
  );

  const missing = await logout({});
  check(missing.status === 400, `and a request with no token is a 400, not a crash (${missing.status})`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('logging out ends the session it was asked to end, and only that one');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
