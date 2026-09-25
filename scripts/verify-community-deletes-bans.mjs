/**
 * Asserts what an author's delete and a moderator's ban do, and what they refuse.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - Only the author deletes, and a delete is audited once. A second delete finds
 *     nothing. UNHIDE cannot bring a deleted post back: the compare-and-set is asserted
 *     by trying it, not by reading the code.
 *   - Authorship is the permission, not membership: an author who left the space can
 *     still delete what they wrote there. A ban stops it.
 *   - A deleted comment keeps the replies other people wrote. It comes back as a
 *     tombstone — no words, no author, and neither anywhere in the response — with its
 *     replies still nested under it. Without that, the replies would be promoted to the
 *     top (the part-6 bug, from the other side) or removed with it. The control is the
 *     same thread nested beforehand.
 *   - A tombstone with nothing left under it goes, bottom-up: a chain of deleted
 *     comments ending in nothing leaves no trace.
 *   - The post's comment count drops by exactly one for a tombstoned comment — its
 *     replies still count — and the score MOVES with it.
 *   - Everyone acts only on lower ranks: a moderator cannot ban a moderator, or
 *     themselves; an owner can ban a moderator. A ban and an unban are audited once
 *     each, as MEMBER targets with the space they happened in.
 *   - The actor's role is judged on the locked row, not the cache. A moderator demoted
 *     in the database while their cached MODERATOR entry is still live is refused —
 *     and the control shows the cache really was still answering MODERATOR.
 *
 * The feed consumer is off: every post stays unfanned, and the feed's read-time half is
 * enough to watch a deleted post leave it.
 *
 * Scoped to two tenants created for this run.
 *
 * Run (with Postgres and Redis up, and Community built):
 *   node scripts/verify-community-deletes-bans.mjs
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
const TENANT = `tenant_db_${RUN}`;
const OTHER_TENANT = `tenant_db_other_${RUN}`;
const PORT = 3823;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  FEED_CONSUMER_ENABLED: 'false',
  RATE_LIMIT_SPACE_PER_MINUTE: '50',
  RATE_LIMIT_JOIN_PER_MINUTE: '50',
  RATE_LIMIT_POST_PER_MINUTE: '50',
  RATE_LIMIT_COMMENT_PER_MINUTE: '50',
  RATE_LIMIT_REPORT_PER_MINUTE: '50',
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
  return { status: res.status, body: parsed, text };
}

function start(dir, args, env) {
  const child = spawn(process.execPath, args, { cwd: join(ROOT, 'services', dir), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  return { child, log: () => log };
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

/** A nested thread as a compact string, tombstones marked: "c1*[c2[c3]],c4". */
function shape(comments) {
  const walk = (list) =>
    (list ?? []).map((c) => `${c.label ?? c.id}${c.deleted ? '*' : ''}${c.replies?.length ? `[${walk(c.replies)}]` : ''}`).join(',');
  return walk(comments);
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
  const service = start('community', ['dist/index.js'], communityEnv);

  const q = (sql, params = []) => inTenant(owner, TENANT, sql, params).then((r) => r.rows);
  const statusOf = async (table, id) => (await q(`SELECT status::text AS s FROM "${table}" WHERE id = $1`, [id]))[0]?.s;
  const audits = async (targetId) =>
    q(
      `SELECT action::text AS action, "actorId", "targetType"::text AS "targetType", "spaceId", reason
         FROM "ModerationAction" WHERE "targetId" = $1 ORDER BY "createdAt", id`,
      [targetId],
    );
  const memberStatus = async (spaceId, userId) =>
    (await q(`SELECT status::text AS s FROM "Membership" WHERE "spaceId" = $1 AND "userId" = $2`, [spaceId, userId]))[0]?.s;

  try {
    const ready = await waitFor(async () => (await api('/ready')).status === 200, 60_000);
    check(Boolean(ready), `CONTROL: Community is ready on :${PORT}`);
    if (!ready) {
      console.error(`  Community's output:\n${excerpt(service.log())}`);
      return;
    }

    const u = (name) => ({ id: `u_${name}_${RUN}`, t: token(TENANT, `u_${name}_${RUN}`) });
    const A = u('owner');
    const M1 = u('mod1');
    const M2 = u('mod2');
    const U = u('author');
    const R = u('replier');
    const V = u('victim');

    console.log('');
    console.log('  the fixture: one space, an owner, two moderators, three members');
    const S = (await api('/spaces', { method: 'POST', bearer: A.t, body: { slug: `s-${RUN}`, name: 'S' } })).body?.id;
    for (const who of [M1, M2, U, R, V]) await api(`/spaces/${S}/join`, { method: 'POST', bearer: who.t });
    const promoted = [
      (await api(`/spaces/${S}/members/${M1.id}`, { method: 'PATCH', bearer: A.t, body: { role: 'MODERATOR' } })).status,
      (await api(`/spaces/${S}/members/${M2.id}`, { method: 'PATCH', bearer: A.t, body: { role: 'MODERATOR' } })).status,
    ];
    check(Boolean(S) && promoted.every((s) => s === 200), `S exists and M1, M2 moderate it (${promoted.join(', ')})`);

    const post = async (who, body) => (await api(`/spaces/${S}/posts`, { method: 'POST', bearer: who.t, body: { body } })).body?.id;
    const reply = async (who, postId, body, parentId) =>
      api(`/posts/${postId}/comments`, { method: 'POST', bearer: who.t, body: { body, ...(parentId ? { parentId } : {}) } });
    const del = (who, path) => api(path, { method: 'DELETE', bearer: who.t });

    console.log('');
    console.log('  an author deletes their own post, and only they can');
    const P = await post(U, `post by U ${RUN}`);
    const listed = async () => ((await api(`/spaces/${S}/posts`, { bearer: R.t })).body?.posts ?? []).map((p) => p.id);
    const fed = async () => ((await api('/feed?limit=50', { bearer: R.t })).body?.posts ?? []).map((p) => p.id);
    check((await listed()).includes(P) && (await fed()).includes(P), 'CONTROL: the post is in the space list and a reader\'s feed');

    const byOther = await del(R, `/posts/${P}`);
    const byModerator = await del(M1, `/posts/${P}`);
    check(
      byOther.status === 403 && byModerator.status === 403 && (await statusOf('Post', P)) === 'VISIBLE' && (await audits(P)).length === 0,
      `another member and a moderator are both refused, and nothing changed or was audited (${byOther.status}, ${byModerator.status}, still ${await statusOf('Post', P)})`,
    );

    const byAuthor = await del(U, `/posts/${P}`);
    const postTrail = await audits(P);
    check(byAuthor.status === 204 && (await statusOf('Post', P)) === 'DELETED', `the author deletes it (${byAuthor.status}, now ${await statusOf('Post', P)})`);
    check(
      postTrail.length === 1 && postTrail[0].action === 'DELETE' && postTrail[0].actorId === U.id && postTrail[0].targetType === 'POST' && postTrail[0].spaceId === S,
      `audited once: DELETE of a POST by its author, in S (${postTrail.map((t) => `${t.action}/${t.targetType}/${t.actorId === U.id ? 'U' : t.actorId}/${t.spaceId === S ? 'S' : t.spaceId}`).join(', ')})`,
    );
    const gone = [
      (await api(`/posts/${P}/comments`, { bearer: R.t })).status,
      (await reply(R, P, 'too late')).status,
      (await api(`/posts/${P}/reactions`, { method: 'POST', bearer: R.t, body: { kind: 'LIKE' } })).status,
    ];
    check(!(await listed()).includes(P) && !(await fed()).includes(P) && gone.every((s) => s === 404), `it has left the list and the feed; its thread, a comment and a reaction are 404 (${gone.join(', ')})`);
    const again = await del(U, `/posts/${P}`);
    check(again.status === 404 && (await audits(P)).length === 1, `deleting it again finds nothing and audits nothing (${again.status}, ${(await audits(P)).length} row)`);
    const unhide = await api(`/posts/${P}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'UNHIDE' } });
    check(unhide.body?.changed === false && (await statusOf('Post', P)) === 'DELETED', `a moderator's UNHIDE cannot bring it back (${unhide.status}, changed ${unhide.body?.changed}, still ${await statusOf('Post', P)})`);

    // A refusal must not confirm that something in a PRIVATE space exists.
    const X = u('stranger');
    const hiddenSpace = (await api('/spaces', { method: 'POST', bearer: A.t, body: { slug: `p-${RUN}`, name: 'Private', visibility: 'PRIVATE' } })).body?.id;
    const privatePost = (await api(`/spaces/${hiddenSpace}/posts`, { method: 'POST', bearer: A.t, body: { body: 'members only' } })).body?.id;
    const privateComment = (await reply(A, privatePost, 'also members only')).body?.id;
    const strangerDeletes = [(await del(X, `/posts/${privatePost}`)).status, (await del(X, `/comments/${privateComment}`)).status];
    check(
      Boolean(privatePost && privateComment) && strangerDeletes.every((s) => s === 404) && (await statusOf('Post', privatePost)) === 'VISIBLE',
      `someone outside a private space gets 404 for its post and comment, not a 403 that admits they exist (${strangerDeletes.join(', ')})`,
    );

    console.log('');
    console.log('  authorship is the permission, not membership; a ban stops it');
    const leaverPost = await post(R, 'R posts, then leaves');
    const left = await api(`/spaces/${S}/members/me`, { method: 'DELETE', bearer: R.t });
    const leaverDeletes = await del(R, `/posts/${leaverPost}`);
    check(left.status === 204 && leaverDeletes.status === 204, `an author who left the space can still delete what they wrote there (${left.status}, ${leaverDeletes.status})`);
    await api(`/spaces/${S}/join`, { method: 'POST', bearer: R.t });

    const victimPost = await post(V, 'V posts, then is banned');
    const banV = await api(`/spaces/${S}/members/${V.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'BANNED' } });
    const bannedDeletes = await del(V, `/posts/${victimPost}`);
    check(
      banV.status === 200 && bannedDeletes.status === 403 && (await statusOf('Post', victimPost)) === 'VISIBLE',
      `a banned author cannot (${bannedDeletes.status}, still ${await statusOf('Post', victimPost)})`,
    );
    await api(`/spaces/${S}/members/${V.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'ACTIVE' } });

    console.log('');
    console.log('  a deleted comment keeps the replies other people wrote');
    const T = await post(U, 'a thread');
    const secret = `c1 words that must not come back ${RUN}`;
    const c1 = (await reply(U, T, secret)).body?.id;
    const c2 = (await reply(R, T, 'R answers c1', c1)).body?.id;
    const c3 = (await reply(U, T, 'U answers R', c2)).body?.id;
    const c4 = (await reply(U, T, 'c4 alone')).body?.id;
    const label = { [c1]: 'c1', [c2]: 'c2', [c3]: 'c3', [c4]: 'c4' };
    const thread = async () => {
      const r = await api(`/posts/${T}/comments`, { bearer: R.t });
      const relabel = (list) => (list ?? []).map((c) => ({ ...c, label: label[c.id] ?? c.id, replies: relabel(c.replies) }));
      return { ...r.body, comments: relabel(r.body?.comments), text: r.text };
    };
    const before = await thread();
    const counts = async () => (await q(`SELECT "commentCount", "rankScore",
        ln(greatest("reactionCount" + 2 * "commentCount", 1) + 1) + extract(epoch from "createdAt") / 45000.0 AS expected
      FROM "Post" WHERE id = $1`, [T]))[0];
    const countBefore = await counts();
    check(shape(before.comments) === 'c1[c2[c3]],c4' && Number(countBefore.commentCount) === 4, `CONTROL: the thread is ${shape(before.comments)} and counts ${countBefore.commentCount}`);

    const notMine = await del(R, `/comments/${c1}`);
    check(notMine.status === 403 && (await statusOf('Comment', c1)) === 'VISIBLE', `someone else cannot delete c1 (${notMine.status})`);

    const delC1 = await del(U, `/comments/${c1}`);
    const afterC1 = await thread();
    const tomb = afterC1.comments?.[0];
    check(
      delC1.status === 204 && shape(afterC1.comments) === 'c1*[c2[c3]],c4',
      `U deletes c1, and c2 > c3 stay under it — neither removed nor promoted (${delC1.status}: ${shape(afterC1.comments)})`,
    );
    check(
      tomb?.deleted === true && tomb.body === null && tomb.authorId === null && !afterC1.text.includes(secret),
      `c1 is a tombstone: no words and no author, and the words are nowhere in the response (body ${JSON.stringify(tomb?.body)}, author ${JSON.stringify(tomb?.authorId)})`,
    );
    const countAfter = await counts();
    check(
      Number(countAfter.commentCount) === 3 && Number(countAfter.rankScore) < Number(countBefore.rankScore) && Math.abs(Number(countAfter.rankScore) - Number(countAfter.expected)) < 1e-9,
      `the post counts 3, not 4 and not 1 — c2 and c3 still count — and its score moved to the formula (${countBefore.commentCount} -> ${countAfter.commentCount}, ${Number(countBefore.rankScore).toFixed(4)} -> ${Number(countAfter.rankScore).toFixed(4)})`,
    );
    const c1Trail = await audits(c1);
    check(
      c1Trail.length === 1 && c1Trail[0].action === 'DELETE' && c1Trail[0].targetType === 'COMMENT' && c1Trail[0].actorId === U.id && c1Trail[0].spaceId === S,
      `audited once: DELETE of a COMMENT by its author, in S (${c1Trail.map((t) => `${t.action}/${t.targetType}`).join(', ')})`,
    );

    const toTomb = await reply(R, T, 'answering nothing', c1);
    const underTomb = await reply(R, T, 'R answers U again', c2);
    const c5 = underTomb.body?.id;
    label[c5] = 'c5';
    check(toTomb.status === 404 && underTomb.status === 201, `no reply to the tombstone itself (${toTomb.status}), but the replies under it are still answerable (${underTomb.status})`);

    const delC4 = await del(U, `/comments/${c4}`);
    const afterC4 = await thread();
    check(delC4.status === 204 && shape(afterC4.comments) === 'c1*[c2[c3,c5]]', `a deleted comment with nothing under it leaves no tombstone (${shape(afterC4.comments)})`);

    const hideAttempt = await api(`/comments/${c1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'HIDE' } });
    check(hideAttempt.body?.changed === false && (await statusOf('Comment', c1)) === 'DELETED', `a moderator's HIDE does not turn a deletion into something UNHIDE could undo (changed ${hideAttempt.body?.changed})`);

    for (const [who, id] of [[R, c5], [U, c3], [R, c2]]) await del(who, `/comments/${id}`);
    const emptied = await thread();
    const countEmpty = await counts();
    check(
      (emptied.comments ?? []).length === 0 && emptied.count === 0 && Number(countEmpty.commentCount) === 0,
      `once everything under c1 is deleted too, the whole chain goes, bottom-up (${shape(emptied.comments) || 'empty'}, count ${emptied.count}, commentCount ${countEmpty.commentCount})`,
    );

    console.log('');
    console.log('  what a moderator hid, an author cannot reach');
    const H = await post(R, 'a thread with hidden content');
    const h1 = (await reply(R, H, 'to be hidden')).body?.id;
    const h2 = (await reply(U, H, 'U, under it', h1)).body?.id;
    await api(`/comments/${h1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'HIDE' } });
    const underHidden = await del(U, `/comments/${h2}`);
    check(underHidden.status === 404 && (await statusOf('Comment', h2)) === 'VISIBLE', `U's comment inside a hidden subtree is 404 to U, and untouched (${underHidden.status})`);

    console.log('');
    console.log('  everyone acts only on lower ranks');
    const modOnMod = await api(`/spaces/${S}/members/${M2.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'BANNED' } });
    const modOnSelf = await api(`/spaces/${S}/members/${M1.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'BANNED' } });
    check(
      modOnMod.status === 403 && modOnSelf.status === 403 && (await memberStatus(S, M2.id)) === 'ACTIVE' && (await memberStatus(S, M1.id)) === 'ACTIVE' && (await audits(M2.id)).length === 0,
      `a moderator cannot ban another moderator, or themselves (${modOnMod.status}, ${modOnSelf.status}); both still ACTIVE, nothing audited`,
    );
    const ownerOnMod = await api(`/spaces/${S}/members/${M2.id}`, { method: 'PATCH', bearer: A.t, body: { status: 'BANNED', reason: 'owner decision' } });
    check(ownerOnMod.status === 200 && (await memberStatus(S, M2.id)) === 'BANNED', `an owner can ban a moderator (${ownerOnMod.status}, ${await memberStatus(S, M2.id)})`);
    const onOwner = await api(`/spaces/${S}/members/${A.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'BANNED' } });
    check(onOwner.status === 409, `and an owner is protected from a moderator (${onOwner.status})`);

    console.log('');
    console.log('  bans and unbans are audited, once each');
    const vTrailBefore = (await audits(V.id)).length;
    const ban1 = await api(`/spaces/${S}/members/${V.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'BANNED', reason: 'spam' } });
    const ban2 = await api(`/spaces/${S}/members/${V.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'BANNED' } });
    const vPosts = await api(`/spaces/${S}/posts`, { method: 'POST', bearer: V.t, body: { body: 'still here?' } });
    const unban = await api(`/spaces/${S}/members/${V.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'ACTIVE' } });
    const vTrail = (await audits(V.id)).slice(vTrailBefore);
    check(
      ban1.body?.statusChanged === true && ban2.body?.statusChanged === false && unban.body?.statusChanged === true,
      `ban, ban again, unban: changed ${ban1.body?.statusChanged}, ${ban2.body?.statusChanged}, ${unban.body?.statusChanged}`,
    );
    check(vPosts.status === 403, `CONTROL: while banned, V could not post (${vPosts.status})`);
    check(
      vTrail.map((t) => t.action).join(',') === 'BAN,UNBAN' &&
        vTrail.every((t) => t.targetType === 'MEMBER' && t.spaceId === S && t.actorId === M1.id) &&
        vTrail[0]?.reason === 'spam',
      `the trail reads BAN then UNBAN — the second ban wrote nothing — as MEMBER targets in S, by M1, with the reason (${vTrail.map((t) => `${t.action}/${t.targetType}/${t.spaceId === S ? 'S' : t.spaceId}`).join(', ')})`,
    );
    const m2Trail = await audits(M2.id);
    check(m2Trail.length === 1 && m2Trail[0].action === 'BAN' && m2Trail[0].actorId === A.id && m2Trail[0].reason === 'owner decision', `the owner's ban of M2 is in the trail too (${m2Trail.map((t) => t.action).join(', ')})`);

    console.log('');
    console.log('  the role is judged on the locked row, not the cache');
    const warm = await api(`/spaces/${S}/reports`, { bearer: M1.t });
    // Demoted behind the service's back: no forgetMembership, as if the cache delete failed.
    await q(`UPDATE "Membership" SET role = 'MEMBER'::"SpaceRole" WHERE "spaceId" = $1 AND "userId" = $2`, [S, M1.id]);
    const stillCached = await api(`/spaces/${S}/reports`, { bearer: M1.t });
    check(warm.status === 200 && stillCached.status === 200, `CONTROL: after the demotion the cache still answers MODERATOR for M1 (${warm.status}, ${stillCached.status})`);
    const staleBan = await api(`/spaces/${S}/members/${V.id}`, { method: 'PATCH', bearer: M1.t, body: { status: 'BANNED' } });
    check(
      staleBan.status === 403 && (await memberStatus(S, V.id)) === 'ACTIVE',
      `yet M1's ban is refused, because the locked row says MEMBER (${staleBan.status}, V ${await memberStatus(S, V.id)})`,
    );

    console.log('');
    console.log('  another tenant finds none of it');
    const other = token(OTHER_TENANT, A.id); // the owner's user id, in another tenant
    const cross = [
      (await api(`/spaces/${S}/members/${V.id}`, { method: 'PATCH', bearer: other, body: { status: 'BANNED' } })).status,
      (await api(`/posts/${victimPost}`, { method: 'DELETE', bearer: token(OTHER_TENANT, V.id) })).status,
    ];
    check(
      cross.every((s) => s === 404) && (await memberStatus(S, V.id)) === 'ACTIVE' && (await statusOf('Post', victimPost)) === 'VISIBLE',
      `banning and deleting under the same user ids elsewhere are both 404, and change nothing (${cross.join(', ')})`,
    );
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Community's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    const tables = ['ModerationAction', 'Report', 'Reaction', 'Comment', 'FeedEntry', 'Post', 'Membership', 'CommunitySpace', 'OutboxMessage'];
    let deleted = 0;
    let left = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        deleted += (await inTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      }
      for (const table of tables) {
        left += (await inTenant(owner, tenant, `SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rows: [{ n: -1 }] }))).rows[0].n;
      }
    }
    check(deleted > 0 && left === 0, `cleanup: this run's rows are gone (${deleted} deleted, ${left} left)`);
    await owner.end().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('authors delete only their own words, replies outlive them, and everyone acts only on lower ranks, on the locked row, audited once');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
