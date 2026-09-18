/**
 * Asserts who may act in a space, and how often.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - A private space answers 404 to a stranger, not 403. A 403 confirms the space
 *     exists, which maps a tenant's structure for anyone willing to guess ids. THE
 *     CONTROL is that the same request from a member returns 200, so the 404 is the
 *     rule and not a missing fixture.
 *   - Joining twice adds one member and moves the counter once.
 *   - Space roles are read PER REQUEST and cached, and the cache is dropped by whatever
 *     changes them: a promotion takes effect on the next call, not a minute later. The
 *     cache TTL here is the default sixty seconds, so a run that passes cannot have
 *     been waiting it out.
 *   - A moderator may ban; only an owner may change roles; an owner's own membership is
 *     not editable through that route; and the last owner cannot leave.
 *   - The rate limiter is per user, per tenant, per action, and ATOMIC. The atomicity
 *     check fires more requests at once than the bucket holds and requires exactly the
 *     bucket's worth to pass: a read-then-write limiter lets several through together,
 *     which is precisely what a burst is.
 *   - Another tenant's space is not there at all, over HTTP, with a real token.
 *
 * Tokens are minted here with Auth's private key rather than by logging in, the way
 * scripts/verify-search-api.mjs does: this is about what Community does with a token.
 *
 * Redis keys are left to expire: the bucket's TTL is seconds and the membership cache's
 * is a minute, and every key is scoped by ids unique to this run.
 *
 * Run (with Postgres and Redis up, Community built and migrated):
 *   node scripts/verify-community-membership.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const RUN = randomUUID().replace(/-/g, '').slice(0, 8);
const TENANT = `tenant_cm_${RUN}`;
const OTHER_TENANT = `tenant_cm_other_${RUN}`;
const PORT = 3814;
/** Small on purpose: the limiter's behaviour is the point, not its default budget. */
const JOINS_PER_MINUTE = 3;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  RATE_LIMIT_JOIN_PER_MINUTE: String(JOINS_PER_MINUTE),
  AUTH_PUBLIC_KEY_B64: Buffer.from(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem'), 'utf8')).toString('base64'),
});

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const excerpt = (log) => (log.length <= 2000 ? log : `${log.slice(0, 1500)}\n  [...]\n${log.slice(-500)}`);

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

const b64url = (v) => Buffer.from(v).toString('base64url');
const signingKey = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));

/** A genuine token for one user in one tenant. */
function token(tenantId, userId, role = 'ATTENDEE') {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({
      sub: userId,
      tenantId,
      role,
      iss: communityEnv.JWT_ISSUER,
      aud: communityEnv.JWT_AUDIENCE,
      iat: now,
      exp: now + 600,
    }),
  );
  return `${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), signingKey))}`;
}

async function api(path, { method = 'GET', bearer, body } = {}) {
  const res = await fetch(`http://127.0.0.1:${PORT}${path}`, {
    method,
    headers: {
      ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = { raw: text };
  }
  return { status: res.status, body: parsed, retryAfter: res.headers.get('retry-after') };
}

function start(env) {
  const child = spawn(process.execPath, ['dist/index.js'], {
    cwd: join(ROOT, 'services', 'community'),
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  return { child, log: () => log };
}

async function main() {
  const taken = await fetch(`http://127.0.0.1:${PORT}/health`).then(
    () => true,
    () => false,
  );
  if (taken) {
    console.error(`FAIL: something already answers on :${PORT}. Stop it before running this.`);
    process.exitCode = 1;
    return;
  }

  const owner = new pg.Client({ connectionString: communityEnv.DATABASE_URL });
  await owner.connect();
  const service = start(communityEnv);

  // Users. The same id in both tenants on purpose, so the limiter's key has to carry
  // the tenant for the cross-tenant check to mean anything.
  const OWNER = `u_owner_${RUN}`;
  const MEMBER = `u_member_${RUN}`;
  const EXTRA = `u_extra_${RUN}`;
  const STRANGER = `u_stranger_${RUN}`;
  const BURSTER = `u_burst_${RUN}`;
  const asOwner = token(TENANT, OWNER);
  const asMember = token(TENANT, MEMBER);
  const asExtra = token(TENANT, EXTRA);
  const asStranger = token(TENANT, STRANGER);
  const asBurster = token(TENANT, BURSTER);
  const asOtherTenant = token(OTHER_TENANT, MEMBER);

  try {
    console.log('  the fixture');
    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Community is ready on :${PORT}` : `it never became ready:\n${excerpt(service.log())}`);
    if (failures > 0) return;

    const made = await api('/spaces', { method: 'POST', bearer: asOwner, body: { slug: `public-${RUN}`, name: 'A public space' } });
    check(made.status === 201 && Boolean(made.body?.id), `a space is created by its owner (${made.status})`);
    const publicSpace = made.body?.id;
    const badSlug = await api('/spaces', { method: 'POST', bearer: asOwner, body: { slug: 'Not A Slug', name: 'x' } });
    check(badSlug.status === 400, `CONTROL: a slug that is not a slug is refused (${badSlug.status})`);
    const privateMade = await api('/spaces', {
      method: 'POST',
      bearer: asOwner,
      body: { slug: `private-${RUN}`, name: 'A private space', visibility: 'PRIVATE' },
    });
    const privateSpace = privateMade.body?.id;
    check(privateMade.status === 201 && Boolean(privateSpace), `and a private one (${privateMade.status})`);
    if (failures > 0) return;

    console.log('');
    console.log('  a private space does not admit to existing');
    const ownerSees = await api(`/spaces/${privateSpace}`, { bearer: asOwner });
    check(ownerSees.status === 200, `CONTROL: its owner reads it (${ownerSees.status})`);
    const strangerSees = await api(`/spaces/${privateSpace}`, { bearer: asStranger });
    check(strangerSees.status === 404, `a stranger gets 404, not 403, so its existence is not confirmed (${strangerSees.status})`);
    const strangerJoins = await api(`/spaces/${privateSpace}/join`, { method: 'POST', bearer: asStranger });
    check(strangerJoins.status === 404, `and cannot join it (${strangerJoins.status})`);

    console.log('');
    console.log('  joining is idempotent, and counted once');
    const first = await api(`/spaces/${publicSpace}/join`, { method: 'POST', bearer: asMember });
    const second = await api(`/spaces/${publicSpace}/join`, { method: 'POST', bearer: asMember });
    check(first.status === 200 && first.body?.joined === true, `the first join adds a member (${first.status})`);
    check(second.status === 200 && second.body?.joined === false, `the second is accepted and adds nothing (${second.status})`);
    const counted = await api(`/spaces/${publicSpace}`, { bearer: asOwner });
    check(counted.body?.memberCount === 2, `so the space counts two members, not three (${counted.body?.memberCount})`);

    console.log('');
    console.log('  space roles are read per request, and the cache is dropped by what changes them');
    await api(`/spaces/${publicSpace}/join`, { method: 'POST', bearer: asExtra });
    const beforePromotion = await api(`/spaces/${publicSpace}/members/${EXTRA}`, { method: 'PATCH', bearer: asMember, body: { status: 'BANNED' } });
    check(beforePromotion.status === 403, `a member cannot moderate (${beforePromotion.status})`);
    const promote = await api(`/spaces/${publicSpace}/members/${MEMBER}`, { method: 'PATCH', bearer: asOwner, body: { role: 'MODERATOR' } });
    check(promote.status === 200, `the owner promotes them to moderator (${promote.status})`);
    const afterPromotion = await api(`/spaces/${publicSpace}/members/${EXTRA}`, { method: 'PATCH', bearer: asMember, body: { status: 'BANNED' } });
    check(afterPromotion.status === 200, `and they can moderate on the very next request, not a minute later (${afterPromotion.status})`);
    const bannedReads = await api(`/spaces/${publicSpace}`, { bearer: asExtra });
    check(bannedReads.status === 200, 'CONTROL: a banned member can still read a PUBLIC space, which anyone in the tenant can');
    const bannedModerates = await api(`/spaces/${publicSpace}/members/${OWNER}`, { method: 'PATCH', bearer: asExtra, body: { status: 'BANNED' } });
    check(bannedModerates.status === 403, `but a banned member acts in it no further (${bannedModerates.status})`);

    console.log('');
    console.log('  a moderator may ban, and may not promote');
    const modPromotes = await api(`/spaces/${publicSpace}/members/${EXTRA}`, { method: 'PATCH', bearer: asMember, body: { role: 'MODERATOR' } });
    check(modPromotes.status === 403, `only an owner changes roles (${modPromotes.status})`);
    const touchesOwner = await api(`/spaces/${publicSpace}/members/${OWNER}`, { method: 'PATCH', bearer: asMember, body: { status: 'BANNED' } });
    check(touchesOwner.status === 409, `and an owner's membership is not editable there (${touchesOwner.status})`);
    const ownerLeaves = await api(`/spaces/${publicSpace}/members/me`, { method: 'DELETE', bearer: asOwner });
    check(ownerLeaves.status === 409, `nor can the last owner leave (${ownerLeaves.status})`);
    const memberLeaves = await api(`/spaces/${publicSpace}/members/me`, { method: 'DELETE', bearer: asMember });
    const afterLeaving = await api(`/spaces/${publicSpace}`, { bearer: asOwner });
    check(memberLeaves.status === 204 && afterLeaving.body?.memberCount === 2, `a member can leave, and the count follows (${memberLeaves.status}, ${afterLeaving.body?.memberCount})`);

    console.log('');
    console.log('  the limiter is per user, per tenant, and atomic');
    /** One public space per join, since joining the same one twice is idempotent and cheap. */
    const spaces = [];
    for (let i = 0; i < JOINS_PER_MINUTE + 4; i++) {
      const made = await api('/spaces', { method: 'POST', bearer: asOwner, body: { slug: `rl-${i}-${RUN}`, name: `Space ${i}` } });
      spaces.push(made.body?.id);
    }
    check(spaces.every(Boolean), `CONTROL: ${spaces.length} spaces to join were created`);

    // A FRESH user, with a full bucket: the stranger above already spent a token on the
    // private space it was refused from, because the limiter runs before the handler.
    const SEQUENTIAL = `u_seq_${RUN}`;
    const asSequential = token(TENANT, SEQUENTIAL);
    const sequential = [];
    for (let i = 0; i < JOINS_PER_MINUTE + 1; i++) {
      sequential.push(await api(`/spaces/${spaces[i]}/join`, { method: 'POST', bearer: asSequential }));
    }
    const allowed = sequential.filter((r) => r.status === 200).length;
    const refused = sequential.find((r) => r.status === 429);
    check(allowed === JOINS_PER_MINUTE, `a user gets exactly ${JOINS_PER_MINUTE} joins a minute (${allowed})`);
    check(Boolean(refused) && Number(refused?.retryAfter) >= 1, `and the refusal says when to come back (Retry-After: ${refused?.retryAfter})`);

    const otherUser = await api(`/spaces/${spaces[0]}/join`, { method: 'POST', bearer: asExtra });
    check(otherUser.status === 200, `another user in the same tenant still has their own budget (${otherUser.status})`);

    // Tenant B, with THE USER WHOSE TENANT-A BUDGET IS ALREADY SPENT. Any other user
    // would still have room in a shared bucket, so this check could not fail whether or
    // not the key carries the tenant — which is exactly what happened the first time.
    const otherSpace = await api('/spaces', { method: 'POST', bearer: asOtherTenant, body: { slug: `other-${RUN}`, name: 'Other tenant' } });
    const otherJoin = await api(`/spaces/${otherSpace.body?.id}/join`, { method: 'POST', bearer: token(OTHER_TENANT, SEQUENTIAL) });
    check(otherSpace.status === 201 && otherJoin.status === 200, `and the same user id in another tenant has its own budget (${otherJoin.status})`);

    // ATOMICITY. Fired together, not one after another: a read-then-write limiter lets
    // several of these through, because they all read the same count.
    const burst = await Promise.all(
      Array.from({ length: JOINS_PER_MINUTE + 3 }, (_, i) => api(`/spaces/${spaces[i]}/join`, { method: 'POST', bearer: asBurster })),
    );
    const burstAllowed = burst.filter((r) => r.status === 200).length;
    const burstRefused = burst.filter((r) => r.status === 429).length;
    check(
      burstAllowed === JOINS_PER_MINUTE && burstRefused === 3,
      `${JOINS_PER_MINUTE + 3} requests at once spend the bucket exactly once: ${burstAllowed} through, ${burstRefused} refused`,
    );

    console.log('');
    console.log("  another tenant's space is not there");
    const crossRead = await api(`/spaces/${publicSpace}`, { bearer: asOtherTenant });
    check(crossRead.status === 404, `reading it with another tenant's token is a 404 (${crossRead.status})`);
    const crossJoin = await api(`/spaces/${publicSpace}/join`, { method: 'POST', bearer: asOtherTenant });
    check(crossJoin.status === 404, `and joining it is a 404 (${crossJoin.status})`);
    const unauthenticated = await api(`/spaces/${publicSpace}`);
    check(unauthenticated.status === 401, `CONTROL: with no token at all it is a 401 (${unauthenticated.status})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Community's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    // One statement per call: the driver cannot send parameters with a multi-statement
    // query, and row security needs the tenant set in the same transaction.
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of ['Membership', 'CommunitySpace']) {
        try {
          await owner.query('BEGIN');
          await owner.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
          await owner.query(`DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]);
          await owner.query('COMMIT');
        } catch (err) {
          await owner.query('ROLLBACK').catch(() => {});
          console.error(`  cleanup of ${table} failed: ${err.message}`);
        }
      }
    }
    let left = 0;
    for (const table of ['Membership', 'CommunitySpace']) {
      const rows = await owner
        .query(`SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = ANY($1)`, [[TENANT, OTHER_TENANT]])
        .catch(() => ({ rows: [{ n: -1 }] }));
      left += rows.rows[0].n;
    }
    check(left === 0, `cleanup: this run's spaces and memberships are gone (${left} left)`);
    await owner.end().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a private space stays private, roles take effect on the next request, and the limiter holds under a burst');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
