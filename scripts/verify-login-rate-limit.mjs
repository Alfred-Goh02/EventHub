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
 * Three instances, because the three budgets have to be tested apart: each case
 * needs the budgets it is not testing set far too large to interfere. The third
 * also runs with TRUST_PROXY=1, which is the only way a test can present itself
 * as two different clients -- without it X-Forwarded-For is ignored, every
 * request shares the socket address, and the lockout case cannot be written at
 * all.
 *
 * Run: node scripts/verify-login-rate-limit.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const TENANT = 'tenant_acme';
/** The seeded tenants' subdomains: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const SLUG = { tenant_acme: 'acme', tenant_globex: 'globex' };
function slugOf(tenant) {
  if (!SLUG[tenant]) throw new Error(`no subdomain known for ${tenant}`);
  return SLUG[tenant];
}
const REAL ={ email: 'alice@acme.com', password: 'hunter2222' };
const MAX_FAILURES = 3;
const MAX_PER_IP = 4;

/**
 * For the lockout case, which needs the two per-account limits set APART: a
 * small per-(account, address) budget, and a global per-account budget well
 * above it. See lockoutCase for why those particular numbers.
 */
const LOCKOUT_PER_IP = 3;
const LOCKOUT_GLOBAL = 10;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/** An address that exists nowhere, fresh per run so nothing carries over. */
const invented = (label) => `${label}-${randomBytes(4).toString('hex')}@nowhere.example`;

/**
 * `ip` is sent as X-Forwarded-For, which only means anything on an instance
 * started with TRUST_PROXY=1. On the other instances it is ignored, which is
 * exactly right: without TRUST_PROXY a client cannot choose its own address,
 * and a test that could would be testing nothing.
 */
function client(port) {
  return async function login(email, password, tenant = TENANT, ip) {
    const res = await fetch(`http://localhost:${port}/login`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-Slug': slugOf(tenant),
        ...(ip ? { 'X-Forwarded-For': ip } : {}),
      },
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

/**
 * Asserts that a stranger cannot lock a victim out of their own account.
 *
 * THE HOLE THIS CLOSES. With one global per-account limit and nothing else,
 * anyone who knows an address could spend that account's whole budget from
 * their own machine and the real owner was refused for the rest of the window.
 * Ten wrong passwords, repeated every fifteen minutes, denied service to any
 * account whose email address the attacker could guess -- and email addresses
 * are not secret. The limit meant to protect the account was the attack.
 *
 * THE FIX IS WHERE THE COUNTING HAPPENS. The budget that actually refuses is
 * now keyed on (tenant, email, ADDRESS), so spending it only locks out the
 * address that spent it. The global per-account budget stays as the backstop
 * against one password tried from many machines, but it is set far higher,
 * and -- this is the part that matters -- a request refused by the per-address
 * budget returns BEFORE anything is counted. So one attacker can no longer
 * drive the global counter up at all: they contribute their per-address
 * budget's worth and then stop counting.
 *
 * WHY THESE NUMBERS MAKE THE TEST FAIL BEFORE THE FIX. The attacker sends
 * LOCKOUT_GLOBAL failures, which is exactly what the single old limit allowed,
 * so before the fix the victim's next attempt was a 429 and the assertion
 * below was red. After the fix the attacker is cut off at LOCKOUT_PER_IP, the
 * global counter never passes 3 of its 10, and the victim gets in.
 */
async function lockoutCase() {
  const port = 3095;
  const auth = await startAuth(port, {
    LOGIN_MAX_FAILURES_PER_ACCOUNT: String(LOCKOUT_GLOBAL),
    LOGIN_MAX_FAILURES_PER_ACCOUNT_IP: String(LOCKOUT_PER_IP),
    // Out of the way: this case is about the per-account budgets, and the
    // attacker's addresses each send few enough requests to stay well inside
    // the overall per-address cap anyway.
    LOGIN_MAX_ATTEMPTS_PER_IP: '1000',
    // Without this, every request shares the socket address and the whole case
    // is untestable -- X-Forwarded-For is only honoured behind a declared proxy.
    TRUST_PROXY: '1',
  });
  const login = client(port);

  const ATTACKER = '203.0.113.10';
  const VICTIM = '198.51.100.25';

  try {
    check(
      auth.up,
      auth.up
        ? `a third private instance is listening on :${port}, trusting one proxy hop`
        : 'the third private instance never came up',
    );
    if (!auth.up) {
      console.error(auth.log());
      return;
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  X-Forwarded-For is actually being honoured');
    // -----------------------------------------------------------------------
    /**
     * The control for everything below. If the header were ignored -- a missing
     * TRUST_PROXY, a proxy hop count of the wrong shape -- then the attacker and
     * the victim would share one address, and every assertion in this case would
     * be about a single client. Some of them would still pass, for the wrong
     * reason.
     *
     * So: spend the per-address budget from one address, and show that a
     * DIFFERENT address is unaffected while the first is refused. That can only
     * be true if the two are being told apart.
     */
    const probeAccount = invented('probe');
    for (let i = 1; i <= LOCKOUT_PER_IP; i += 1) await login(probeAccount, `p-${i}`, TENANT, ATTACKER);
    const probeSameIp = await login(probeAccount, 'p-again', TENANT, ATTACKER);
    const probeOtherIp = await login(probeAccount, 'p-again', TENANT, VICTIM);
    check(
      probeSameIp.status === 429 && probeOtherIp.status === 401,
      `the spent address is refused (${probeSameIp.status}) while another is not (${probeOtherIp.status})`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a stranger cannot lock the owner out of their own account');
    // -----------------------------------------------------------------------
    /**
     * Positive control first: the attacker's opening guess is an ordinary 401.
     * An instance refusing everything would pass the lockout assertion while
     * proving nothing about counting.
     */
    const opening = await login(REAL.email, 'attacker-guess-1', TENANT, ATTACKER);
    check(opening.status === 401, `the attacker's first guess is an ordinary 401 (got ${opening.status})`);

    const attackerStatuses = [opening.status];
    for (let i = 2; i <= LOCKOUT_GLOBAL; i += 1) {
      attackerStatuses.push((await login(REAL.email, `attacker-guess-${i}`, TENANT, ATTACKER)).status);
    }

    /**
     * The attacker is cut off at their own budget rather than at the account's.
     * Before the fix every one of these was a 401 until the global budget ran
     * out, which is precisely how the victim got locked out.
     */
    const attackerRefused = attackerStatuses.filter((s) => s === 429).length;
    check(
      attackerRefused === LOCKOUT_GLOBAL - LOCKOUT_PER_IP,
      `of ${LOCKOUT_GLOBAL} guesses from one address, ${attackerRefused} were refused ` +
        `(expected ${LOCKOUT_GLOBAL - LOCKOUT_PER_IP}: cut off after ${LOCKOUT_PER_IP})`,
    );

    /** THE ASSERTION THIS CASE EXISTS FOR. */
    const victim = await login(REAL.email, REAL.password, TENANT, VICTIM);
    check(
      victim.status === 200,
      victim.status === 200
        ? 'and the real owner, from their own address, still signs in'
        : `the real owner is locked out of their own account (${victim.status}) -- a stranger who ` +
          'knows the address has denied them service',
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  but many addresses together still run the account out of budget');
    // -----------------------------------------------------------------------
    /**
     * The backstop, and an honest statement of what remains. One password tried
     * against one account from many machines is a real attack that a per-address
     * budget cannot see, so the global per-account budget stays.
     *
     * Which means the lockout is not gone, it is PRICED: it now costs an
     * attacker LOCKOUT_GLOBAL / LOCKOUT_PER_IP distinct addresses instead of
     * one. That is the trade, and it is deliberate. Raising the global budget
     * raises the price and weakens the backstop; there is no setting that does
     * both.
     *
     * The victim's successful login above cleared the global counter, so this
     * starts from zero.
     */
    const needed = Math.ceil(LOCKOUT_GLOBAL / LOCKOUT_PER_IP);
    for (let n = 0; n < needed; n += 1) {
      for (let i = 1; i <= LOCKOUT_PER_IP; i += 1) {
        await login(REAL.email, `spray-${n}-${i}`, TENANT, `203.0.113.${20 + n}`);
      }
    }
    const sprayed = await login(REAL.email, REAL.password, TENANT, VICTIM);
    check(
      sprayed.status === 429,
      sprayed.status === 429
        ? `${needed} addresses did between them what one no longer can (429), so distributed ` +
          'guessing is still bounded'
        : `${needed} addresses spent more than the account's global budget and it still answered ` +
          `${sprayed.status} -- the backstop against distributed guessing is not there`,
    );
  } finally {
    auth.child.kill();
  }
}

async function main() {
  await perAccountCases();
  await perIpCase();
  await lockoutCase();

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
