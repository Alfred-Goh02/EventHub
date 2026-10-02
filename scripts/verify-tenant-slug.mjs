/**
 * Asserts that Auth takes the tenant of a sign-in from the gateway's X-Tenant-Slug, and
 * from nothing else.
 *
 * The gateway clears every tenant header a client sends and passes the Host's subdomain
 * on as X-Tenant-Slug (verify-compose-gateway.mjs checks that half). This is the other
 * half: what Auth does with it.
 *
 *   - A slug signs in to its own tenant. The seed holds alice@acme.com with one password
 *     in BOTH acme and globex, so the two answers can only differ by the slug. The
 *     token names the slug as well, and so does the one a refresh mints.
 *   - X-Tenant-ID, the header this replaced, is not read: on its own it is a 400, and
 *     beside a slug it changes nothing.
 *   - A slug with no tenant behind it, or one that is not a subdomain at all, is the
 *     same 401 as a wrong password.
 *   - A tenant created a moment after its slug missed signs in AT ONCE. Auth caches the
 *     slug -> tenant lookup; if a miss were cached, this is where it would show. The
 *     fixture can show it: the miss is asked first, through the same Auth, on purpose.
 *
 * Run with Auth up (AUTH_URL, default http://localhost:3001) and the database reachable
 * as Auth's runtime role (APP_DATABASE_URL):
 *   node scripts/verify-tenant-slug.mjs
 */

import { randomBytes } from 'node:crypto';
import pg from 'pg';

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const APP_URL = process.env['APP_DATABASE_URL'] ?? 'postgresql://eventhub_auth_app:eventhub_auth_app@localhost:5432/eventhub_auth';

const ACCOUNT = { email: 'alice@acme.com', password: 'hunter2222' };
const RUN = randomBytes(4).toString('hex');
const FRESH = { id: `tenant_slugcheck_${RUN}`, slug: `slugcheck-${RUN}`, userId: `user_slugcheck_${RUN}` };

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/** POST /login with these headers; the status, the error, and the token's tenant if one came back. */
async function login(headers, account = ACCOUNT) {
  const res = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(account),
  });
  const body = await res.json().catch(() => null);
  const claims = body?.accessToken ? JSON.parse(Buffer.from(body.accessToken.split('.')[1], 'base64url').toString('utf8')) : null;
  return { status: res.status, error: body?.error ?? null, tenant: claims?.tenantId ?? null, slug: claims?.tenantSlug ?? null, refreshToken: body?.refreshToken ?? null };
}

/** POST /refresh; the new access token's tenant and slug. */
async function refresh(refreshToken) {
  const res = await fetch(`${AUTH}/refresh`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ refreshToken }) });
  const body = await res.json().catch(() => null);
  const claims = body?.accessToken ? JSON.parse(Buffer.from(body.accessToken.split('.')[1], 'base64url').toString('utf8')) : null;
  return { status: res.status, tenant: claims?.tenantId ?? null, slug: claims?.tenantSlug ?? null };
}

/** One statement inside a tenant's context, as the runtime role; the rows it returned. */
async function asTenant(db, tenantId, sql, params) {
  await db.query('BEGIN');
  try {
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
    const { rows, rowCount } = await db.query(sql, params);
    await db.query('COMMIT');
    return { rows, rowCount };
  } catch (err) {
    await db.query('ROLLBACK');
    throw err;
  }
}

async function main() {
  console.log('  a slug signs in to its own tenant');
  const acme = await login({ 'X-Tenant-Slug': 'acme' });
  check(acme.status === 200 && acme.tenant === 'tenant_acme', `CONTROL: slug acme is a token for tenant_acme (${acme.status}, ${acme.tenant})`);
  const globex = await login({ 'X-Tenant-Slug': 'globex' });
  check(globex.status === 200 && globex.tenant === 'tenant_globex', `the same account and password with slug globex is a token for tenant_globex (${globex.status}, ${globex.tenant})`);
  if (failures > 0) {
    console.error('\nIs Auth up and seeded? Start it, or set AUTH_URL.');
    return;
  }

  // The token names its tenant's subdomain too, so that every service can refuse it
  // on another tenant's (verify-token-tenant.mjs). From a sign-in and from a refresh.
  check(acme.slug === 'acme' && globex.slug === 'globex', `each token carries its own tenant's slug (${acme.slug}, ${globex.slug})`);
  const refreshed = await refresh(acme.refreshToken);
  check(
    refreshed.status === 200 && refreshed.tenant === 'tenant_acme' && refreshed.slug === 'acme',
    `so does the access token a refresh mints (${refreshed.status}, ${refreshed.tenant}, ${refreshed.slug})`,
  );

  console.log('');
  console.log('  the header it replaced is not read');
  const idOnly = await login({ 'X-Tenant-ID': 'tenant_acme' });
  check(idOnly.status === 400 && /X-Tenant-Slug/.test(idOnly.error ?? ''), `X-Tenant-ID alone is no tenant: 400 (${idOnly.status} ${JSON.stringify(idOnly.error)})`);
  const both = await login({ 'X-Tenant-Slug': 'acme', 'X-Tenant-ID': 'tenant_globex' });
  check(both.status === 200 && both.tenant === 'tenant_acme', `beside slug acme, X-Tenant-ID naming globex changes nothing (${both.status}, ${both.tenant})`);

  console.log('');
  console.log('  a slug that names no tenant is a wrong password');
  const wrong = await login({ 'X-Tenant-Slug': 'acme' }, { ...ACCOUNT, password: 'not-the-password' });
  check(wrong.status === 401, `CONTROL: a wrong password at acme is 401 (${wrong.status} ${JSON.stringify(wrong.error)})`);
  for (const [slug, what] of [[`nobody-${RUN}`, 'no tenant behind it'], ['Not_A.Slug', 'not a subdomain at all'], ['a'.repeat(64), 'longer than a DNS label']]) {
    const r = await login({ 'X-Tenant-Slug': slug });
    check(r.status === 401 && r.error === wrong.error && r.tenant === null, `a slug with ${what} is the same 401 (${r.status} ${JSON.stringify(r.error)})`);
  }

  console.log('');
  console.log('  a tenant created after its slug missed signs in at once');
  const db = new pg.Client({ connectionString: APP_URL, application_name: 'verify-tenant-slug' });
  db.on('error', () => {});
  await db.connect();
  let created = false;
  try {
    const before = await login({ 'X-Tenant-Slug': FRESH.slug });
    check(before.status === 401, `CONTROL: before it exists, slug ${FRESH.slug} is 401 (${before.status})`);

    // The seeded account's own hash, so the new user has a known password without
    // hashing one here.
    const seeded = await asTenant(db, 'tenant_acme', `SELECT "passwordHash" FROM "User" WHERE email = $1`, [ACCOUNT.email]);
    const hash = seeded.rows[0]?.passwordHash ?? '';
    check(hash.startsWith('$argon2'), `CONTROL: read the seeded account's hash (${hash.slice(0, 10)}…)`);

    await db.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [FRESH.id, `Slug check ${RUN}`, FRESH.slug]);
    created = true;
    await asTenant(
      db,
      FRESH.id,
      `INSERT INTO "User" (id, "tenantId", email, "passwordHash", role, "updatedAt") VALUES ($1, $2, $3, $4, 'ATTENDEE', now())`,
      [FRESH.userId, FRESH.id, ACCOUNT.email, hash],
    );

    const after = await login({ 'X-Tenant-Slug': FRESH.slug });
    check(after.status === 200 && after.tenant === FRESH.id, `the moment it exists, slug ${FRESH.slug} signs in to it (${after.status}, ${after.tenant})`);
  } finally {
    if (created) {
      // Inside the tenant, where row security lets the DELETE see the row, and counted:
      // a cleanup that deletes nothing must not pass as one that worked.
      const users = await asTenant(db, FRESH.id, `DELETE FROM "User" WHERE id = $1`, [FRESH.userId]).catch(() => ({ rowCount: 0 }));
      const tenants = await db.query(`DELETE FROM "Tenant" WHERE id = $1`, [FRESH.id]).catch(() => ({ rowCount: 0 }));
      const left = await db.query(`SELECT count(*)::int AS n FROM "Tenant" WHERE id = $1`, [FRESH.id]);
      check(users.rowCount === 1 && tenants.rowCount === 1 && left.rows[0].n === 0, `cleanup: ${users.rowCount} user and ${tenants.rowCount} tenant deleted, ${left.rows[0].n} left`);
    }
    await db.end();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log("a sign-in's tenant is the gateway's slug, and nothing a client adds to it");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
