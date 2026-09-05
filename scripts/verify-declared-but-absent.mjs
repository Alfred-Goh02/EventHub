/**
 * Two things this repository declared and did not have.
 *
 * They are unrelated in mechanism and identical in shape: something is written
 * down, nothing checks it, and the gap only shows up when a person follows the
 * declaration. The same shape has now produced a mechanism documented and never
 * called (projectionLagSeconds), a schema comment describing a control that
 * moved elsewhere (ConsumerCheckpoint), and an .env.example that could not boot
 * its own service.
 *
 * 1. A SEED SCRIPT THAT IS NOT THERE.
 *
 *    Every service's prisma.config.ts declared `seed: "tsx prisma/seed.ts"`.
 *    Only Auth has that file, and only Auth's seed is run by CI — so
 *    `prisma db seed` in Event or Ticketing failed on a missing module, and
 *    nothing anywhere noticed.
 *
 *    The check is a ratchet rather than a fix: whatever a config declares, the
 *    file has to exist. That catches the next one, in whichever service adds a
 *    seed and then moves it.
 *
 * 2. /me ANSWERING 200 WITH A NULL BODY.
 *
 *    `res.json(user)` where the lookup can miss. A client that trusts the 200
 *    and reads .email crashes on null. Measured, by deleting the User row and
 *    keeping the token:
 *
 *      /me while the user exists -> 200 {"id":"...","email":"..."}
 *      /me after deletion        -> 200 "null"
 *
 *    Reachable because access tokens are stateless and live 15 minutes with no
 *    revocation — a deliberate trade — so a deleted user keeps a working
 *    credential for up to a quarter of an hour.
 *
 * Run: node scripts/verify-declared-but-absent.mjs
 */

import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomBytes, createHash } from 'node:crypto';
import pg from 'pg';

const AUTH_OWNER_URL =
  process.env['DATABASE_URL'] ??
  'postgresql://eventhub_auth_owner:eventhub_auth_owner@localhost:5432/eventhub_auth';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';

/**
 * The repository root, resolved from THIS FILE rather than from cwd.
 *
 * The CI step that runs this has working-directory: services/auth, so
 * readdirSync('services') looked for services/auth/services and died with
 * ENOENT. A verifier that scans the tree has to know where the tree is; it
 * cannot borrow that from whoever invoked it.
 */
const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const TENANT = 'tenant_acme';
const run = `dba_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const sha256 = (s) => createHash('sha256').update(s).digest('hex');

// ---------------------------------------------------------------------------
function seedDeclarations() {
  console.log('  every declared seed script must exist');

  const services = readdirSync(join(REPO_ROOT, 'services'), { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name);

  let declared = 0;
  const missing = [];

  for (const svc of services) {
    const config = join(REPO_ROOT, 'services', svc, 'prisma.config.ts');
    if (!existsSync(config)) continue;

    const text = readFileSync(config, 'utf8');
    // `seed: "tsx prisma/seed.ts"` — the path is the last argument of the
    // command, which is how prisma itself resolves it.
    const match = text.match(/seed:\s*"([^"]+)"/);
    if (!match) continue;

    declared += 1;
    const target = match[1].trim().split(/\s+/).pop();
    const resolved = join(REPO_ROOT, 'services', svc, target);
    if (!existsSync(resolved)) missing.push(`${config} -> ${resolved}`);
  }

  check(
    declared > 0,
    `${declared} service(s) declare a seed — the scan is reading real config, not an empty list`,
  );
  check(
    missing.length === 0,
    missing.length === 0
      ? 'every declared seed script is on disk'
      : `declared but absent:\n        ${missing.join('\n        ')}`,
  );
}

// ---------------------------------------------------------------------------
async function meEndpoint() {
  console.log('');
  console.log('  /me must not answer 200 with a null body');

  const db = new pg.Client({ connectionString: AUTH_OWNER_URL, application_name: 'verify-dba' });
  db.on('error', () => {});
  await db.connect();

  const email = `${run}@acme.com`;
  const token = randomBytes(32).toString('base64url');
  await db.query(
    `INSERT INTO "Invite" (id,"tenantId",email,role,"tokenHash","invitedBy","expiresAt","createdAt")
     VALUES ($1,$2,$3,'ATTENDEE',$4,'seed', now() + interval '7 days', now())`,
    [`inv_${randomBytes(8).toString('hex')}`, TENANT, email, sha256(token)],
  );
  await fetch(`${AUTH}/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, password: 'hunter2222' }),
  });

  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email, password: 'hunter2222' }),
  });
  const { accessToken } = await login.json();

  const me = () =>
    fetch(`${AUTH}/me`, { headers: { Authorization: `Bearer ${accessToken}` } }).then(
      async (r) => ({ status: r.status, text: await r.text() }),
    );

  /**
   * The positive control. Without it, an endpoint that answered 401 to
   * everybody would satisfy the assertion below.
   */
  const before = await me();
  check(before.status === 200, `a live user still gets 200, got ${before.status}`);
  check(
    JSON.parse(before.text)?.email === email,
    'and the body is the user, not null',
  );

  // Remove the subject, keep the token. Access tokens are stateless, so it stays
  // valid for its full 15 minutes.
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  await db.query(
    `DELETE FROM "RefreshToken" WHERE "userId" IN (SELECT id FROM "User" WHERE email = $1)`,
    [email],
  );
  const deleted = await db.query(`DELETE FROM "User" WHERE email = $1`, [email]);
  await db.query('COMMIT');
  check(deleted.rowCount === 1, 'the user row was deleted while its token stayed valid');

  const after = await me();
  check(
    after.status === 401,
    after.status === 401
      ? 'the token no longer identifies anyone -> 401'
      : `-> ${after.status} ${after.text} — a client reading .email off this crashes`,
  );
  check(
    after.text !== 'null',
    `and the body is not the string "null" (${after.text.slice(0, 60)})`,
  );

  await db.end();
}

async function main() {
  seedDeclarations();
  await meEndpoint();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('what the repository declares, it has; and a valid token names a real user');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
