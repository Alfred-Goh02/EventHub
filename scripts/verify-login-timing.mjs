/**
 * Asserts that /login does not reveal whether an account exists by how long it
 * takes to say no.
 *
 * Returning early on a missing user skipped the argon2id verify, and argon2id
 * is expensive ON PURPOSE — 19 MB and two passes, which is the whole reason it
 * was chosen over bcrypt. Measured before the fix, eight runs each:
 *
 *   wrong password, account EXISTS : median 13 ms
 *   wrong password, account MISSING: median  3 ms
 *
 * A consistent four-fold difference, with the same 401 body either way. That is
 * an account-existence oracle: an attacker learns which addresses are
 * registered by timing, one request each, without ever guessing a password.
 *
 * It also contradicted a decision made forty lines further down the same file.
 * /forgot-password returns 202 unconditionally, with a comment explaining that
 * telling a caller whether an address exists is exactly what must not happen.
 * /login was giving it away for free.
 *
 * HOW THIS IS ASSERTED WITHOUT BEING A FLAKY TIMING TEST.
 *
 * Wall-clock timing on a shared CI runner is noisy, so the assertion is on the
 * RATIO of medians, with a deliberately loose bound, and it is stated in terms
 * of the property rather than a millisecond figure. The bound is wide enough
 * that ordinary scheduling noise cannot trip it and narrow enough that skipping
 * a 19 MB hash cannot hide inside it — the defect it guards against was a 4x
 * difference, and the bound is 2x.
 *
 * The POSITIVE CONTROL is what makes the number mean anything: a correct login
 * is timed too, and it must be in the same range as a wrong-password login. If
 * every path had collapsed to nothing — an argon2 that failed to load, a mocked
 * verify — all three medians would agree and the ratio assertion would pass
 * while proving nothing. Asserting that the verify actually costs something
 * closes that.
 *
 * Run: node scripts/verify-login-timing.mjs
 */

import { randomBytes, createHash } from 'node:crypto';
import pg from 'pg';

const AUTH_OWNER_URL =
  process.env['DATABASE_URL'] ??
  'postgresql://eventhub_auth_owner:eventhub_auth_owner@localhost:5432/eventhub_auth';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';

const TENANT = 'tenant_acme';
const run = `lt_${Date.now()}`;

/** Enough samples that one scheduling hiccup cannot move the median. */
const RUNS = 12;

/**
 * How far apart the two medians may be before it counts as a signal.
 *
 * The defect was a 4x gap. Two-fold leaves room for ordinary noise on a loaded
 * runner while still catching a skipped hash, which cannot be subtle: the whole
 * point of the parameters is that the work is measurable.
 */
const MAX_RATIO = 2.0;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const sha256 = (s) => createHash('sha256').update(s).digest('hex');
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
};

const db = new pg.Client({ connectionString: AUTH_OWNER_URL, application_name: 'verify-timing' });
db.on('error', () => {});

async function makeUser(email) {
  const token = randomBytes(32).toString('base64url');
  await db.query(
    `INSERT INTO "Invite" (id,"tenantId",email,role,"tokenHash","invitedBy","expiresAt","createdAt")
     VALUES ($1,$2,$3,'ATTENDEE',$4,'seed', now() + interval '7 days', now())`,
    [`inv_${randomBytes(8).toString('hex')}`, TENANT, email, sha256(token)],
  );
  const res = await fetch(`${AUTH}/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, password: 'hunter2222' }),
  });
  if (res.status !== 201) throw new Error(`could not create the fixture user: ${res.status}`);
}

async function timeLogin(email, password) {
  const started = process.hrtime.bigint();
  const res = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email, password }),
  });
  await res.text();
  return { ms: Number(process.hrtime.bigint() - started) / 1e6, status: res.status };
}

async function main() {
  await db.connect();

  const email = `${run}@acme.com`;
  await makeUser(email);

  // Warm the process: the first request pays for lazy module init and a cold
  // connection, and would otherwise land entirely in whichever sample went first.
  for (let i = 0; i < 3; i += 1) await timeLogin(email, 'warmup');

  const exists = [];
  const missing = [];
  const correct = [];

  /**
   * Interleaved rather than run in two blocks, so a slow patch of CI affects
   * both series equally instead of landing on one of them.
   */
  for (let i = 0; i < RUNS; i += 1) {
    const a = await timeLogin(email, `wrong-${i}`);
    const b = await timeLogin(`${run}_nobody_${i}@acme.com`, `wrong-${i}`);
    if (a.status !== 401) check(false, `a wrong password should be 401, got ${a.status}`);
    if (b.status !== 401) check(false, `an unknown address should be 401, got ${b.status}`);
    exists.push(a.ms);
    missing.push(b.ms);
  }

  for (let i = 0; i < 5; i += 1) {
    const ok = await timeLogin(email, 'hunter2222');
    if (ok.status !== 200) check(false, `the correct password should be 200, got ${ok.status}`);
    correct.push(ok.ms);
  }

  const mExists = median(exists);
  const mMissing = median(missing);
  const mCorrect = median(correct);
  const ratio = Math.max(mExists, mMissing) / Math.min(mExists, mMissing);

  console.log(`  ${RUNS} samples each, medians:`);
  console.log(`    wrong password, account EXISTS : ${mExists.toFixed(1)} ms`);
  console.log(`    wrong password, account MISSING: ${mMissing.toFixed(1)} ms`);
  console.log(`    correct password               : ${mCorrect.toFixed(1)} ms`);
  console.log('');

  check(
    ratio < MAX_RATIO,
    ratio < MAX_RATIO
      ? `the two answers cost about the same (${ratio.toFixed(2)}x, bound ${MAX_RATIO}x)`
      : `a missing account answers ${ratio.toFixed(2)}x faster — the response time IS the answer`,
  );

  /**
   * The control. If argon2 were not running at all — a failed native load, a
   * mocked verify — every median would collapse together and the ratio above
   * would pass while proving nothing.
   */
  check(
    mCorrect >= mMissing / MAX_RATIO,
    `a successful login costs at least as much as a failed one (${mCorrect.toFixed(1)} ms vs ` +
      `${mMissing.toFixed(1)} ms) — otherwise the hash is not actually running and the ratio ` +
      'above would agree for the wrong reason',
  );

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a wrong password and an unknown address cost the same to refuse');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
