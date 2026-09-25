/**
 * Asserts what reporting and moderation do, and what they refuse.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - A moderator's role is judged in the TARGET'S space. M1 moderates S1 and is a
 *     plain member of S2; asked to hide a post in S2 — with S1's id in the body, which
 *     is how a route trusting the request would be fooled — the answer is 403, the post
 *     stays visible, and nothing is audited. The control is M1 hiding in S1.
 *   - Every change writes its audit row in the same transaction, and a second HIDE is
 *     a no-op that writes no second row.
 *   - A report attached to an action must be about THAT target and still open. A
 *     mismatched one is refused AND the hide it came with is rolled back — the post is
 *     read back visible afterwards, so a refusal that left the hide in place fails.
 *   - Hiding a comment takes its whole subtree out of the thread. Without the ancestor
 *     check its replies would be promoted to top-level comments; the control is the
 *     same thread nested three deep beforehand.
 *   - Hidden comments stop counting: the post's commentCount and rankScore are
 *     recomputed, and the score is asserted to have MOVED, not just to equal a formula.
 *   - Nothing can be written under hidden content: no reply to a hidden comment or to
 *     anything below one, no comment or reaction on a hidden post.
 *   - A hidden post leaves the space's list, the reader's feed and its own thread, and
 *     comes back on UNHIDE.
 *
 * The feed consumer is switched off here, so every post stays unfanned and the feed
 * serves them from its read-time half — which is enough to see a hidden post leave it,
 * without Kafka.
 *
 * Scoped to two tenants created for this run.
 *
 * Run (with Postgres and Redis up, and Community built):
 *   node scripts/verify-community-moderation.mjs
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
const TENANT = `tenant_md_${RUN}`;
const OTHER_TENANT = `tenant_md_other_${RUN}`;
const PORT = 3822;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  // No consumer: nothing here depends on fan-out, and the feed's read-time half serves
  // every unfanned post, which is all this needs to watch a hidden post leave the feed.
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
  return { status: res.status, body: parsed };
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

/** Every comment id in a nested thread, and the ids at the top level. */
function flatten(comments) {
  const all = [];
  const walk = (list) => list.forEach((c) => (all.push(c.id), walk(c.replies ?? [])));
  walk(comments ?? []);
  return { all, top: (comments ?? []).map((c) => c.id) };
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
  const audits = async (targetId) => q(`SELECT action::text AS action, "actorId", "reportId" FROM "ModerationAction" WHERE "targetId" = $1 ORDER BY "createdAt", id`, [targetId]);

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
    const U = u('member');
    const R = u('reader');
    const X = u('stranger');

    console.log('');
    console.log('  the fixture: two spaces, a moderator of each, and M1 a plain member of the other');
    const S1 = (await api('/spaces', { method: 'POST', bearer: A.t, body: { slug: `s1-${RUN}`, name: 'S1' } })).body?.id;
    const S2 = (await api('/spaces', { method: 'POST', bearer: A.t, body: { slug: `s2-${RUN}`, name: 'S2' } })).body?.id;
    for (const who of [M1, U, R]) await api(`/spaces/${S1}/join`, { method: 'POST', bearer: who.t });
    for (const who of [M1, M2, U]) await api(`/spaces/${S2}/join`, { method: 'POST', bearer: who.t });
    const p1 = await api(`/spaces/${S1}/members/${M1.id}`, { method: 'PATCH', bearer: A.t, body: { role: 'MODERATOR' } });
    const p2 = await api(`/spaces/${S2}/members/${M2.id}`, { method: 'PATCH', bearer: A.t, body: { role: 'MODERATOR' } });
    check(Boolean(S1 && S2) && p1.status === 200 && p2.status === 200, `S1 and S2 exist, M1 moderates S1 and M2 moderates S2 (${p1.status}, ${p2.status})`);

    const post = async (spaceId, body) => (await api(`/spaces/${spaceId}/posts`, { method: 'POST', bearer: U.t, body: { body } })).body?.id;
    const P1 = await post(S1, 'p1');
    const P2 = await post(S1, 'p2');
    const Q1 = await post(S2, 'q1');
    const reply = async (postId, body, parentId) => api(`/posts/${postId}/comments`, { method: 'POST', bearer: U.t, body: { body, ...(parentId ? { parentId } : {}) } });
    const c1 = (await reply(P1, 'c1')).body?.id;
    const c2 = (await reply(P1, 'c2', c1)).body?.id;
    const c3 = (await reply(P1, 'c3', c2)).body?.id;
    const c4 = (await reply(P1, 'c4')).body?.id;
    check([P1, P2, Q1, c1, c2, c3, c4].every(Boolean), 'three posts, and on p1 a thread three deep (c1 > c2 > c3) beside c4');

    console.log('');
    console.log('  reporting');
    const r1 = await api(`/posts/${P1}/reports`, { method: 'POST', bearer: U.t, body: { reason: 'SPAM' } });
    const r1again = await api(`/posts/${P1}/reports`, { method: 'POST', bearer: U.t, body: { reason: 'ABUSE' } });
    const r2 = await api(`/posts/${P1}/reports`, { method: 'POST', bearer: R.t, body: { reason: 'OFF_TOPIC', note: 'nothing to do with the event' } });
    check(r1.status === 201 && r1again.status === 200 && r2.status === 201, `a report is 201, the same reporter again is 200, a second reporter is 201 (${r1.status}, ${r1again.status}, ${r2.status})`);
    const reportRows = await q(`SELECT id, "reporterId" FROM "Report" WHERE "targetId" = $1 ORDER BY "createdAt"`, [P1]);
    check(reportRows.length === 2, `and there are two reports, not three — reporting twice is not louder (${reportRows.length})`);
    const reportByU = reportRows.find((r) => r.reporterId === U.id)?.id;
    const reportByR = reportRows.find((r) => r.reporterId === R.id)?.id;
    const stranger = await api(`/posts/${P1}/reports`, { method: 'POST', bearer: X.t, body: { reason: 'SPAM' } });
    check(stranger.status === 403, `someone who has not joined the space cannot report in it (${stranger.status})`);

    console.log('');
    console.log('  the queue belongs to the space');
    const queue = await api(`/spaces/${S1}/reports`, { bearer: M1.t });
    const queued = (queue.body?.reports ?? []).map((r) => r.id);
    check(queue.status === 200 && queued.includes(reportByU) && queued.includes(reportByR), `M1 sees both reports on p1 in S1's queue (${queue.status}, ${queued.length})`);
    const queueOther = await api(`/spaces/${S2}/reports`, { bearer: M2.t });
    check(queueOther.status === 200 && !(queueOther.body?.reports ?? []).some((r) => r.id === reportByU), `S2's queue does not carry S1's reports (${queueOther.body?.reports?.length})`);
    const queueMember = await api(`/spaces/${S1}/reports`, { bearer: U.t });
    check(queueMember.status === 403, `a member who is not a moderator cannot read the queue (${queueMember.status})`);

    console.log('');
    console.log("  a moderator's role counts in the target's space, and only there");
    const idor = await api(`/posts/${Q1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'HIDE', spaceId: S1 } });
    check(
      idor.status === 403 && (await statusOf('Post', Q1)) === 'VISIBLE' && (await audits(Q1)).length === 0,
      `M1, who moderates S1, cannot hide S2's post even naming S1 in the body: ${idor.status}, still ${await statusOf('Post', Q1)}, ${(await audits(Q1)).length} audit row(s)`,
    );
    const plainMember = await api(`/posts/${P2}/moderation`, { method: 'POST', bearer: U.t, body: { action: 'HIDE' } });
    check(plainMember.status === 403 && (await statusOf('Post', P2)) === 'VISIBLE', `a plain member cannot hide anything (${plainMember.status})`);

    console.log('');
    console.log('  hiding a post, with the report it answers');
    const listed = async () => ((await api(`/spaces/${S1}/posts`, { bearer: R.t })).body?.posts ?? []).map((p) => p.id);
    const fed = async () => ((await api('/feed?limit=50', { bearer: R.t })).body?.posts ?? []).map((p) => p.id);
    const beforeList = await listed();
    const beforeFeed = await fed();
    check(beforeList.includes(P1) && beforeFeed.includes(P1), `CONTROL: p1 is in S1's list and the reader's feed before anything happens`);

    const hide = await api(`/posts/${P1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'HIDE', reason: 'spam', reportId: reportByU } });
    const trail = await audits(P1);
    check(hide.status === 200 && hide.body?.changed === true && (await statusOf('Post', P1)) === 'HIDDEN', `M1 hides p1 (${hide.status}, changed ${hide.body?.changed}, now ${await statusOf('Post', P1)})`);
    check(
      trail.length === 1 && trail[0].action === 'HIDE' && trail[0].actorId === M1.id && trail[0].reportId === reportByU,
      `and it is in the audit trail, by M1, answering U's report (${trail.map((t) => `${t.action}/${t.actorId === M1.id ? 'M1' : t.actorId}`).join(', ')})`,
    );
    const statuses = await q(`SELECT id, status::text AS s FROM "Report" WHERE id = ANY($1)`, [[reportByU, reportByR]]);
    const reportStatus = Object.fromEntries(statuses.map((r) => [r.id, r.s]));
    check(reportStatus[reportByU] === 'ACTIONED' && reportStatus[reportByR] === 'OPEN', `the report it named is ACTIONED and the other stays OPEN (${reportStatus[reportByU]}, ${reportStatus[reportByR]})`);

    check(!(await listed()).includes(P1) && !(await fed()).includes(P1), 'p1 has left the space list and the reader\'s feed');
    const onHidden = [
      await api(`/posts/${P1}/comments`, { bearer: R.t }),
      await api(`/posts/${P1}/comments`, { method: 'POST', bearer: U.t, body: { body: 'keep it alive' } }),
      await api(`/posts/${P1}/reactions`, { method: 'POST', bearer: U.t, body: { kind: 'LIKE' } }),
    ].map((r) => r.status);
    check(onHidden.every((s) => s === 404), `its thread, a new comment and a reaction on it are all 404 (${onHidden.join(', ')})`);

    const hideAgain = await api(`/posts/${P1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'HIDE' } });
    check(hideAgain.status === 200 && hideAgain.body?.changed === false && (await audits(P1)).length === 1, `hiding it again changes nothing and audits nothing (${hideAgain.body?.changed}, ${(await audits(P1)).length} row)`);

    console.log('');
    console.log('  a report has to be about the thing acted on');
    const mismatch = await api(`/posts/${P2}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'HIDE', reportId: reportByR } });
    const p2after = await statusOf('Post', P2);
    const rAfter = (await q(`SELECT status::text AS s FROM "Report" WHERE id = $1`, [reportByR]))[0]?.s;
    check(
      mismatch.status === 400 && p2after === 'VISIBLE' && rAfter === 'OPEN' && (await audits(P2)).length === 0,
      `hiding p2 "answering" a report about p1 is refused, and the hide is rolled back with it: ${mismatch.status}, p2 ${p2after}, report ${rAfter}, ${(await audits(P2)).length} audit row(s)`,
    );

    console.log('');
    console.log('  unhiding');
    const unhide = await api(`/posts/${P1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'UNHIDE' } });
    check(
      unhide.body?.changed === true && (await statusOf('Post', P1)) === 'VISIBLE' && (await audits(P1)).map((a) => a.action).join(',') === 'HIDE,UNHIDE' && (await listed()).includes(P1),
      `UNHIDE brings p1 back, and the trail reads HIDE then UNHIDE (${(await audits(P1)).map((a) => a.action).join(', ')})`,
    );

    console.log('');
    console.log("  hiding a comment takes its subtree with it, and stops it counting");
    const thread = async () => (await api(`/posts/${P1}/comments`, { bearer: R.t })).body;
    const before = flatten((await thread())?.comments);
    check(before.top.join() === [c1, c4].join() && before.all.includes(c3), `CONTROL: the thread is c1 > c2 > c3 beside c4, with c1 and c4 at the top (${before.top.length} top, ${before.all.length} in all)`);
    const scoreBefore = (await q(`SELECT "commentCount", "rankScore" FROM "Post" WHERE id = $1`, [P1]))[0];

    const hideC1 = await api(`/comments/${c1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'HIDE' } });
    const after = flatten((await thread())?.comments);
    check(
      hideC1.body?.changed === true && after.all.join() === c4 && after.top.join() === c4,
      `hiding c1 leaves only c4 — c2 and c3 went with it rather than being promoted (${after.all.length} left: ${after.top.length} at the top)`,
    );
    const scoreAfter = (await q(
      `SELECT "commentCount", "rankScore",
              ln(greatest("reactionCount" + 2 * "commentCount", 1) + 1) + extract(epoch from "createdAt") / 45000.0 AS expected
         FROM "Post" WHERE id = $1`,
      [P1],
    ))[0];
    check(
      Number(scoreBefore.commentCount) === 4 && Number(scoreAfter.commentCount) === 1,
      `the post counts 1 comment now, not 4 and not 3 — the subtree is gone, not just c1 (${scoreBefore.commentCount} -> ${scoreAfter.commentCount})`,
    );
    check(
      Number(scoreAfter.rankScore) < Number(scoreBefore.rankScore) && Math.abs(Number(scoreAfter.rankScore) - Number(scoreAfter.expected)) < 1e-9,
      `and its score MOVED down to the formula over the new count, so hidden spam stops buying rank (${Number(scoreBefore.rankScore).toFixed(4)} -> ${Number(scoreAfter.rankScore).toFixed(4)})`,
    );

    const underHidden = [(await reply(P1, 'into the hidden one', c1)).status, (await reply(P1, 'deeper still', c3)).status];
    const underVisible = (await reply(P1, 'to c4', c4)).status;
    check(
      underHidden.every((s) => s === 404) && underVisible === 201,
      `no reply to a hidden comment or anything under one (${underHidden.join(', ')}), while replying to c4 still works (${underVisible})`,
    );

    const unhideC1 = await api(`/comments/${c1}/moderation`, { method: 'POST', bearer: M1.t, body: { action: 'UNHIDE' } });
    const restored = flatten((await thread())?.comments);
    const recount = (await q(`SELECT "commentCount" FROM "Post" WHERE id = $1`, [P1]))[0].commentCount;
    check(
      unhideC1.body?.changed === true && restored.all.includes(c2) && restored.all.includes(c3) && Number(recount) === 5,
      `UNHIDE puts the subtree back and the count follows: c1-c4 plus the reply to c4 is ${recount}`,
    );

    console.log('');
    console.log('  dismissing a report');
    const dismissByMember = await api(`/reports/${reportByR}`, { method: 'PATCH', bearer: U.t, body: { status: 'DISMISSED' } });
    const dismiss = await api(`/reports/${reportByR}`, { method: 'PATCH', bearer: M1.t, body: { status: 'DISMISSED' } });
    const dismissedStatus = (await q(`SELECT status::text AS s FROM "Report" WHERE id = $1`, [reportByR]))[0]?.s;
    check(dismissByMember.status === 403 && dismiss.body?.changed === true && dismissedStatus === 'DISMISSED', `a member cannot dismiss (${dismissByMember.status}); M1 can (${dismissedStatus})`);

    console.log('');
    console.log('  another tenant finds none of it');
    const other = token(OTHER_TENANT, M1.id); // the same user id, in another tenant
    const cross = [
      (await api(`/posts/${P2}/moderation`, { method: 'POST', bearer: other, body: { action: 'HIDE' } })).status,
      (await api(`/spaces/${S1}/reports`, { bearer: other })).status,
      (await api(`/reports/${reportByU}`, { method: 'PATCH', bearer: other, body: { status: 'DISMISSED' } })).status,
    ];
    check(cross.every((s) => s === 404) && (await statusOf('Post', P2)) === 'VISIBLE', `acting, reading the queue and dismissing are all 404 under the same user id elsewhere (${cross.join(', ')})`);
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Community's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    // ModerationAction is append-only to the APP role; the owner may still delete, which
    // is what lets a verifier clean up after itself.
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
  console.log("a moderator acts only in the target's space, every change is audited once, and hidden content takes its subtree and its rank with it");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
