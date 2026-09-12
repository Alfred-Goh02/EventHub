/**
 * Asserts that /login stops a password guesser, without locking out CI or
 * telling anyone which accounts exist.
 *
 * /login had no limit at all: 25 wrong passwords gave 25 × 401 and never a
 * 429. That is unbounded online guessing against any account, and each guess
 * costs the SERVER an argon2id verify -- 19 MB and two passes, on purpose.
 *
 * WHY THIS STARTS ITS OWN AUTH INSTANCES. The limiter is per-process memory
 * (see rate-limit.ts for why, and what that costs). A test that exhausted the
 * budget on the shared instance at :3001 would lock every later CI step that
 * logs in from localhost -- the lockout #30 already fixed once. So this spawns
 * private instances on their own ports with deliberately small limits, and the
 * lockouts it provokes die with those processes.
 *
 * Two instances, because the two limits have to be tested apart: the
 * per-account cases need a per-IP budget too large to interfere, and the
 * per-IP case needs a per-account budget too large to interfere.
 *
 * Run: node scripts/verify-login-rate-limit.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const TENANT = 'tenant_acme';
const REAL = { email: 'alice@acme.com', password: 'hunter2222' };
const MAX_FAILURES = 3;
const MAX_PER_IP = 4;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/** An address that exists nowhere, fresh per run so nothing carries over. */
const invented = (label) => `${label}-${randomBytes(4).toString('hex')}@nowhere.example`;

function client(port) {
  return async function login(email, password, tenant = TENANT) {
    const res = await fetch(`http://localhost:${port}/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': tenant },
      body: JSON.stringify({ email, password }),
    });
    return { status: res.status, retryAfter: res.headers.get('retry-after'), body: await res.text() };
  };
}

/**
 * A private auth instance. --env-file-if-exists picks up services/auth/.env on a
 * laptop; in CI that file does not exist and the job's environment supplies the
 * same variables. Variables passed here win over the file either way, which is
 * what keeps PORT and the limits under the test's control.
 */
async function startAuth(port, limits) {
  const child = spawn(process.execPath, ['--env-file-if-exists=.env', 'dist/index.js'], {
    cwd: join(ROOT, 'services', 'auth'),
    env: { ...process.env, PORT: String(port), ...limits },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));

  for (let i = 0; i < 60; i += 1) {
    try {
      const res = await fetch(`http://localhost:${port}/health`);
      if (res.ok) return { child, up: true, log: () => log };
    } catch {
      // not listening yet
    }
    if (child.exitCode !== null) break;
    await new Promise((r) => setTimeout(r, 250));
  }
  return { child, up: false, log: () => log };
}

async function perAccountCases() {
  const port = 3097;
  const auth = await startAuth(port, {
    LOGIN_MAX_FAILURES_PER_ACCOUNT: String(MAX_FAILURES),
    // Far above anything these cases send, so the per-IP limit stays out of it.
    LOGIN_MAX_ATTEMPTS_PER_IP: '1000',
  });
  const login = client(port);

  try {
    check(auth.up, auth.up ? `a private auth instance is listening on :${port}` : 'the private auth instance never came up');
    if (!auth.up) {
      console.error(auth.log());
      return;
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  one account: the guessing stops, even for the right password');
    // -----------------------------------------------------------------------
    /**
     * The positive control is the first attempt: a wrong password must be an
     * ordinary 401. A limiter that refused everything -- or a service that 429s
     * for some unrelated reason -- would pass the lockout assertion below while
     * proving nothing about counting.
     */
    const first = await login(REAL.email, 'wrong-password-1');
    check(first.status === 401, `the first wrong password is an ordinary 401 (got ${first.status})`);

    for (let i = 2; i <= MAX_FAILURES; i += 1) {
      const r = await login(REAL.email, `wrong-password-${i}`);
      check(r.status === 401, `wrong password ${i} of ${MAX_FAILURES} is still a 401 (got ${r.status})`);
    }

    /**
     * The CORRECT password, after the budget is spent. This is the assertion
     * that matters: if the check happened after the argon2 verify, a guesser
     * would still learn when they hit the right one. It has to be refused
     * before the password is even looked at.
     */
    const locked = await login(REAL.email, REAL.password);
    check(
      locked.status === 429,
      `attempt ${MAX_FAILURES + 1} is refused with 429 even with the right password (got ${locked.status})`,
    );
    const seconds = Number(locked.retryAfter);
    check(
      Number.isInteger(seconds) && seconds > 0,
      `and it says when to come back: Retry-After ${locked.retryAfter ?? '(missing)'}`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a successful login wipes the slate');
    // -----------------------------------------------------------------------
    /**
     * Someone who mistypes twice and then gets in should not carry those two
     * failures into their next typo. Same alice@acme.com, but in tenant_globex:
     * a DIFFERENT key, because the key is (tenant, email) -- which is also why
     * the lockout above does not leak into this case.
     *
     * Without the reset, the fifth attempt is refused: the first two failures
     * and the fourth add up to the limit of three.
     */
    const plan = [
      ['wrong', 401],
      ['wrong', 401],
      ['right', 200],
      ['wrong', 401],
      ['wrong', 401],
      ['right', 200],
    ];
    const got = [];
    for (const [kind] of plan) {
      const r = await login(REAL.email, kind === 'right' ? REAL.password : 'wrong-password-x', 'tenant_globex');
      got.push(r.status);
    }
    const want = plan.map(([, status]) => status);
    check(
      JSON.stringify(got) === JSON.stringify(want),
      `wrong, wrong, RIGHT, wrong, wrong, RIGHT answered ${got.join(', ')} (expected ${want.join(', ')})`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  one locked account does not lock its neighbours');
    // -----------------------------------------------------------------------
    /**
     * alice@acme.com is locked in tenant_acme from the first case. Someone else
     * in the SAME tenant, from the SAME address, must still get an ordinary
     * answer. A key that dropped the email -- per tenant, or per IP -- would
     * turn one attacker hammering one account into a lockout of everybody.
     */
    const neighbour = await login(invented('neighbour'), 'whatever');
    check(
      neighbour.status === 401,
      `a different account in the same tenant, same IP, still gets a normal 401 (got ${neighbour.status})`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  an invented address locks exactly like a real one');
    // -----------------------------------------------------------------------
    /**
     * If failures only counted for accounts that exist, an attacker could tell
     * which addresses are registered: real ones lock after three tries, invented
     * ones never do. So an address that exists nowhere must walk the identical
     * path -- three 401s, then a 429 -- with the identical bodies.
     */
    const ghost = invented('ghost');
    const ghostAnswers = [];
    for (let i = 1; i <= MAX_FAILURES + 1; i += 1) ghostAnswers.push(await login(ghost, `guess-${i}`));

    const ghostStatuses = ghostAnswers.map((r) => r.status);
    const realStatuses = [...Array(MAX_FAILURES).fill(401), 429];
    check(
      JSON.stringify(ghostStatuses) === JSON.stringify(realStatuses),
      `an invented address answered ${ghostStatuses.join(', ')} -- the real one answered ${realStatuses.join(', ')}`,
    );
    check(
      ghostAnswers[0].body === first.body && ghostAnswers[MAX_FAILURES].body === locked.body,
      'and with byte-identical bodies, so the response itself gives nothing away',
    );
  } finally {
    auth.child.kill();
  }
}

async function perIpCase() {
  const port = 3096;
  const auth = await startAuth(port, {
    // Far above anything this case sends, so only the per-IP limit can fire.
    LOGIN_MAX_FAILURES_PER_ACCOUNT: '1000',
    LOGIN_MAX_ATTEMPTS_PER_IP: String(MAX_PER_IP),
  });
  const login = client(port);

  try {
    check(auth.up, auth.up ? `a second private instance is listening on :${port}` : 'the second private instance never came up');
    if (!auth.up) {
      console.error(auth.log());
      return;
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  one address walking a list of accounts is stopped too');
    // -----------------------------------------------------------------------
    /**
     * The per-account limit cannot see this attack: every guess is against a
     * DIFFERENT account, so no single account ever reaches its budget. Only a
     * limit keyed on where the requests come from catches one client trying one
     * password against many addresses -- and it bounds how many argon2 verifies
     * one client can make the server run.
     *
     * Positive control: every attempt inside the budget is an ordinary 401.
     */
    const within = [];
    for (let i = 1; i <= MAX_PER_IP; i += 1) within.push((await login(invented(`list${i}`), 'Password1')).status);
    check(
      within.every((s) => s === 401) && within.length === MAX_PER_IP,
      `the first ${MAX_PER_IP} attempts, each against a different account, are ordinary 401s (got ${within.join(', ')})`,
    );

    const over = await login(invented('list-over'), 'Password1');
    check(
      over.status === 429,
      `attempt ${MAX_PER_IP + 1}, against yet another account, is refused with 429 (got ${over.status})`,
    );
    const seconds = Number(over.retryAfter);
    check(
      Number.isInteger(seconds) && seconds > 0,
      `and it says when to come back: Retry-After ${over.retryAfter ?? '(missing)'}`,
    );
  } finally {
    auth.child.kill();
  }
}

async function main() {
  await perAccountCases();
  await perIpCase();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('/login stops a guesser without telling anyone which accounts exist');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
