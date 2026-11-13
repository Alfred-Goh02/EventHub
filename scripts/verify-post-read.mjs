/**
 * Asserts that GET /posts/:postId shows a post to exactly the people its thread shows
 * itself to, and nobody else.
 *
 * Notification's reply and digest emails link to /posts/<postId> on the tenant's web
 * app, and until this route a post could only be reached through its space's list or the
 * feed. A route that hands out one post by id is the easy place to leak one: an id
 * travels in emails, so anyone holding it may ask.
 *
 * WHAT IS ASSERTED, each with what keeps it from passing for the wrong reason:
 *   - a member reads a visible post: 200, the body, the space, canReply true. The
 *     control that the route answers at all, so the refusals below are refusals;
 *   - a tenant member who is not in a PUBLIC space reads it, with canReply false;
 *   - a non-member asking for a post in a PRIVATE space gets 404, not 403. Control: a
 *     member of that space reads the same id, so the 404 came from membership;
 *   - a member banned from the space gets 403;
 *   - a HIDDEN post and a DELETED one are 404 for everyone, their author included;
 *   - another tenant's post is 404 under the same user id. Control: that tenant's own
 *     member reads it, so the id is real;
 *   - an unknown id is 404.
 *
 * Starts its own Community on :3824, on a consumer group of this run's, in two tenants
 * of this run's. Needs Postgres, Redis and Kafka up, and community built:
 *   node scripts/verify-post-read.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const RUN = randomUUID().replace(/-/g, '').slice(0, 8);
const TENANT = `tenant_pr_${RUN}`;
const OTHER_TENANT = `tenant_pr_other_${RUN}`;
const PORT = 3824;
const GROUP = `community-post-read-${RUN}`;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  // A group of this run's: the example's is the real one, and joining it would take
  // messages from compose's Community (see CLAUDE.md).
  FEED_CONSUMER_GROUP: GROUP,
  FEED_START_FROM: 'latest',
  RATE_LIMIT_SPACE_PER_MINUTE: '50',
  RATE_LIMIT_JOIN_PER_MINUTE: '50',
  RATE_LIMIT_POST_PER_MINUTE: '50',
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

const b64url = (v) => Buffer.from(v).toString('base64url');
const signingKey = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
function token(tenantId, userId) {
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64url(
    JSON.stringify({ sub: userId, tenantId, role: 'ATTENDEE', iss: communityEnv.JWT_ISSUER, aud: communityEnv.JWT_AUDIENCE, iat: now, exp: now + 900 }),
  );
  return `${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), signingKey))}`;
}

async function api(path, { method = 'GET', bearer, body } = {}) {
  const res = await fetch(`http://127.0.0.1:${PORT}${path}`, {
    method,
    headers: { ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = { raw: text };
  }
  return { status: res.status, body: parsed };
}

async function inTenant(client, tenant, sql, params = []) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

async function main() {
  const taken = await fetch(`http://127.0.0.1:${PORT}/health`).then(
    () => true,
    () => false,
  );
  if (taken) {
    check(false, `something already answers on :${PORT}; stop it before running this`);
    return;
  }

  const owner = new pg.Client({ connectionString: communityEnv.DATABASE_URL });
  await owner.connect();
  const admin = new Kafka({ clientId: `verify-post-read-${RUN}`, brokers: communityEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING }).admin();
  await admin.connect();

  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', 'community'), env: communityEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));

  const OWNER = `user_pr_owner_${RUN}`;
  const MEMBER = `user_pr_member_${RUN}`;
  const OUTSIDER = `user_pr_outsider_${RUN}`;
  const PRIVATE_MEMBER = `user_pr_private_${RUN}`;
  const BANNED = `user_pr_banned_${RUN}`;
  const as = (u) => token(TENANT, u);

  try {
    let up = false;
    for (let i = 0; i < 120 && !up && child.exitCode === null; i += 1) {
      up = (await api('/health').catch(() => ({ status: 0 }))).status === 200;
      if (!up) await sleep(500);
    }
    check(up, up ? `CONTROL: this run's own Community answers on :${PORT}` : `Community never came up:\n${excerpt(log)}`);
    if (!up) return;

    // The fixture: a public and a private space, owned by OWNER, and four posts.
    const pub = await api('/spaces', { method: 'POST', bearer: as(OWNER), body: { slug: `pub-${RUN}`, name: `Public ${RUN}`, visibility: 'PUBLIC' } });
    const priv = await api('/spaces', { method: 'POST', bearer: as(OWNER), body: { slug: `priv-${RUN}`, name: `Private ${RUN}`, visibility: 'PRIVATE' } });
    check(pub.status === 201 && priv.status === 201, `CONTROL: a public and a private space are made (${pub.status}, ${priv.status})`);
    const PUB = pub.body?.id;
    const PRIV = priv.body?.id;
    const post = async (spaceId, body) => (await api(`/spaces/${spaceId}/posts`, { method: 'POST', bearer: as(OWNER), body: { body } })).body?.id;
    const visible = await post(PUB, `visible ${RUN}`);
    const secret = await post(PRIV, `private ${RUN}`);
    const hidden = await post(PUB, `to be hidden ${RUN}`);
    const deleted = await post(PUB, `to be deleted ${RUN}`);
    check([visible, secret, hidden, deleted].every(Boolean), 'CONTROL: four posts are made');

    const joinedM = await api(`/spaces/${PUB}/join`, { method: 'POST', bearer: as(MEMBER) });
    const joinedB = await api(`/spaces/${PUB}/join`, { method: 'POST', bearer: as(BANNED) });
    const banned = await api(`/spaces/${PUB}/members/${BANNED}`, { method: 'PATCH', bearer: as(OWNER), body: { status: 'BANNED', reason: 'verify' } });
    // A private space cannot be joined; its member is added the way an owner's tooling would.
    await inTenant(
      owner,
      TENANT,
      `INSERT INTO "Membership" (id, "tenantId", "spaceId", "userId", role, status)
       VALUES ($1, $2, $3, $4, 'MEMBER'::"SpaceRole", 'ACTIVE'::"MemberStatus")`,
      [randomUUID(), TENANT, PRIV, PRIVATE_MEMBER],
    );
    const hide = await api(`/posts/${hidden}/moderation`, { method: 'POST', bearer: as(OWNER), body: { action: 'HIDE', reason: 'verify' } });
    const del = await api(`/posts/${deleted}`, { method: 'DELETE', bearer: as(OWNER) });
    check(
      joinedM.status === 200 && joinedB.status === 200 && banned.status === 200 && hide.status < 300 && del.status < 300,
      `CONTROL: a member joins, another joins and is banned, one post is hidden and one deleted (${joinedM.status}, ${joinedB.status}, ${banned.status}, ${hide.status}, ${del.status})`,
    );

    console.log('');
    console.log('  who may read a post');
    const own = await api(`/posts/${visible}`, { bearer: as(OWNER) });
    check(
      own.status === 200 && own.body?.body === `visible ${RUN}` && own.body?.space?.id === PUB && own.body?.space?.name === `Public ${RUN}` && own.body?.canReply === true,
      `CONTROL: its author reads a visible post, with its space, and may reply (${own.status} ${JSON.stringify(own.body?.space ?? own.body)}, canReply ${own.body?.canReply})`,
    );
    const member = await api(`/posts/${visible}`, { bearer: as(MEMBER) });
    check(member.status === 200 && member.body?.canReply === true, `a member of the space reads it and may reply (${member.status}, canReply ${member.body?.canReply})`);
    const outsider = await api(`/posts/${visible}`, { bearer: as(OUTSIDER) });
    check(
      outsider.status === 200 && outsider.body?.canReply === false,
      `someone in the tenant but not in the PUBLIC space reads it, and may not reply (${outsider.status}, canReply ${outsider.body?.canReply})`,
    );
    const ban = await api(`/posts/${visible}`, { bearer: as(BANNED) });
    check(ban.status === 403, `a member banned from the space is refused (${ban.status})`);

    console.log('');
    console.log('  a private space does not admit to existing');
    const insider = await api(`/posts/${secret}`, { bearer: as(PRIVATE_MEMBER) });
    check(insider.status === 200 && insider.body?.body === `private ${RUN}`, `CONTROL: a member of the PRIVATE space reads its post (${insider.status})`);
    const stranger = await api(`/posts/${secret}`, { bearer: as(OUTSIDER) });
    check(stranger.status === 404, `someone not in it gets 404 for the same id, not 403 (${stranger.status})`);

    console.log('');
    console.log('  nothing that is not visible, for anyone');
    const hiddenOwn = await api(`/posts/${hidden}`, { bearer: as(OWNER) });
    const hiddenMember = await api(`/posts/${hidden}`, { bearer: as(MEMBER) });
    check(hiddenOwn.status === 404 && hiddenMember.status === 404, `a HIDDEN post is 404 to its author and to a member (${hiddenOwn.status}, ${hiddenMember.status})`);
    const deletedOwn = await api(`/posts/${deleted}`, { bearer: as(OWNER) });
    check(deletedOwn.status === 404, `a DELETED post is 404 to its author (${deletedOwn.status})`);
    const unknown = await api(`/posts/${randomUUID()}`, { bearer: as(OWNER) });
    check(unknown.status === 404, `an unknown id is 404 (${unknown.status})`);

    console.log('');
    console.log("  another tenant's post, under the same user id");
    const theirs = token(OTHER_TENANT, OWNER);
    const otherSpace = await api('/spaces', { method: 'POST', bearer: theirs, body: { slug: `pub-${RUN}`, name: `Other ${RUN}`, visibility: 'PUBLIC' } });
    const otherPost = otherSpace.body?.id
      ? (await api(`/spaces/${otherSpace.body.id}/posts`, { method: 'POST', bearer: theirs, body: { body: `other tenant ${RUN}` } })).body?.id
      : null;
    const home = otherPost ? await api(`/posts/${otherPost}`, { bearer: theirs }) : { status: 0 };
    check(home.status === 200, `CONTROL: the other tenant's own member reads its post (${home.status})`);
    const across = otherPost ? await api(`/posts/${otherPost}`, { bearer: as(OWNER) }) : { status: 0 };
    check(across.status === 404, `the same user id in this tenant gets 404 for it (${across.status})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.stack : String(err)}`);
    console.error(`  Community's output:\n${excerpt(log)}`);
  } finally {
    if (child.exitCode === null) {
      child.kill();
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
    // As the owner, inside each tenant: FORCE row security binds the owner too.
    const tables = ['FeedEntry', 'Reaction', 'Comment', 'ModerationAction', 'Report', 'Post', 'Membership', 'CommunitySpace', 'OutboxMessage'];
    let removed = 0;
    let left = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        removed += (await inTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      }
      for (const table of tables) {
        left += (await inTenant(owner, tenant, `SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rows: [{ n: 0 }] }))).rows[0].n;
      }
    }
    const groupsLeft = await deleteGroupsWhenEmpty(admin, [GROUP]).catch(() => [GROUP]);
    check(removed > 0 && left === 0, `cleanup: this run's rows are gone (${removed} deleted, ${left} left); consumer group ${groupsLeft.length ? 'left to expire' : 'deleted'}`);
    await Promise.allSettled([owner.end(), admin.disconnect()]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('a post is shown to exactly the people its thread is shown to');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
