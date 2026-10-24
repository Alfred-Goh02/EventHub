import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import type { AppUrlTemplate } from '@eventhub/config';
import type { Logger } from '@eventhub/logger';
import type { EmailChannel } from '../channels/types.js';
import { withTenant } from '../db.js';
import { withClient } from '../pool.js';
import { isEnabled } from '../preferences/store.js';
import { signUnsubscribe } from '../preferences/unsubscribe-token.js';
import { deliver } from '../dispatch/consumer.js';
import { PoisonMessage, type ContactCardEnvelope, type OpenedContactCard } from '../dispatch/open-seal.js';
import { loadCard } from '../dispatch/replies.js';
import { closePending, idempotencyKey, type DeliveryKey } from '../dispatch/sent-log.js';
import { renderReplyDigest } from '../dispatch/templates.js';
import { ABANDONED } from '../sweeper/sweep.js';

/**
 * Sends each person's held replies as one digest.
 *
 * WHO IS WAITING is asked across tenants, on the digest's own credential, which can
 * read three columns of the held items and nothing else. EVERYTHING ELSE — which posts,
 * the preference, the card, the send, the delete — is done as the app role inside that
 * person's tenant, exactly as a reply is.
 *
 * ONE PASS AT A TIME PER PERSON. Two passes — two replicas, or `digest:once` run while
 * the hourly pass is going — would otherwise both settle the same digest, and deliver()
 * resumes a PENDING claim rather than refusing it, so both would send. A pass takes a
 * session advisory lock for the person first, and steps over anyone another pass holds.
 *
 * TAKEN, THEN SETTLED. A pass stamps a person's waiting items with a new digest id, in
 * one statement, and that id is the digest's identity in the sent-log. Then it sends,
 * and deletes the items once the digest is settled — sent, suppressed, or refused for
 * good. An item still carrying a digest id when a pass finds it is from a pass that did
 * not finish, and the sent-log says what became of that digest:
 *   - it went, or was suppressed or refused: the items are settled, and deleted;
 *   - it is still PENDING, or was never claimed: that same digest is resumed;
 *   - it failed, deferred by its pass or abandoned by the sweeper: the items go back
 *     to waiting and into this pass's new digest. Nothing is dropped because an earlier
 *     attempt failed — the sweeper ends a PENDING row after an hour, which is about
 *     when the next hourly pass arrives, and an ended row is not a sent one.
 *
 * NOTHING IS LEFT PENDING. A send that fails for a reason that may pass closes its row
 * as deferred and puts the items back to wait, so the next pass sends them under a new
 * id; a digest dropped because the person opted out or has no card closes its row too.
 * Otherwise the sweeper would report a correctly handled digest as a stuck delivery.
 *
 * RE-ASKED AT SEND TIME, because an hour is long enough for things to change: a person
 * who turned replies off is sent nothing and their items are dropped; one with no card
 * that opens, the same. The suppression list is deliver()'s, as for any email. A held
 * reply more than a day old is dropped as stale — a count of old replies is not news,
 * and it bounds what can pile up while the digest is not running.
 *
 * EACH LINK OPENS ON THE RECIPIENT'S TENANT'S SUBDOMAIN, from the slug on their card,
 * as a reply's does; with no slug the gateway would route, the digest goes with its
 * counts and no links, for the reasons dispatch/replies.ts gives.
 */

export const DIGEST_CATEGORY = 'COMMUNITY_REPLY';
const DIGEST_TEMPLATE = 'community-digest';
/** Its own advisory-lock namespace; the reply limit's is 9401. */
const DIGEST_FLUSH_LOCK = 9_402;
/** Past this, a held reply is dropped rather than counted. */
const STALE_AFTER = '1 day';
/** Batches one pass may work through before leaving the rest to the next pass. */
const MAX_ROUNDS = 20;
/** The reason a deferred digest's row is closed with. The next pass reads it. */
const DEFERRED = 'deferred to the next digest';

export interface FlushDeps {
  openCard: (envelope: ContactCardEnvelope) => Promise<OpenedContactCard>;
  channel: EmailChannel;
  logger: Logger;
  unsubscribeSecret: string;
  /** Where /unsubscribe answers: one host for every tenant. */
  publicUrl: string;
  /** APP_URL_TEMPLATE, filled with the recipient's tenant's slug. */
  appUrl: AppUrlTemplate;
}

export interface FlushCounts {
  digestsSent: number;
  /** Digests built with no links, because the recipient's card names no subdomain the gateway routes. */
  withoutLinks: number;
  /** Held items removed, for any reason: sent, suppressed, refused, dropped or stale. */
  itemsSettled: number;
  droppedOptedOut: number;
  droppedNoAddress: number;
  /** Held items older than a day, dropped. */
  droppedStale: number;
  /** Put back to wait for the next pass: the store or the channel failed. */
  deferred: number;
}

const zero = (): FlushCounts => ({ digestsSent: 0, withoutLinks: 0, itemsSettled: 0, droppedOptedOut: 0, droppedNoAddress: 0, droppedStale: 0, deferred: 0 });

const digestKey = (tenantId: string, userId: string, digestId: string): DeliveryKey => ({
  tenantId,
  sourceMessageId: digestId,
  recipientId: userId,
  channel: 'email',
  templateKey: DIGEST_TEMPLATE,
});

/** Who has anything waiting, longest first. The digest credential's whole job. */
async function whoIsWaiting(client: pg.PoolClient, limit: number): Promise<Array<{ tenantId: string; userId: string }>> {
  const { rows } = await client.query<{ tenantId: string; userId: string }>(
    `SELECT i."tenantId", i."userId" FROM "DigestItem" i
      GROUP BY i."tenantId", i."userId"
      ORDER BY min(i."createdAt"), i."tenantId", i."userId"
      LIMIT $1`,
    [limit],
  );
  return rows;
}

export interface PassResult extends FlushCounts {
  /** People found waiting, across every batch of the pass. */
  waiting: number;
  /** People another pass was flushing, stepped over. */
  busyElsewhere: number;
  /** People whose flush failed; their items stay for the next pass. */
  recipientFailures: number;
}

/**
 * One pass: a digest to each person with replies held, longest-waiting first, in
 * batches of `batchSize` until fewer than a batch are waiting or a batch settles
 * nothing. One person's failure is counted and the pass moves on.
 */
export async function flushOnce(
  pool: pg.Pool,
  deps: FlushDeps,
  opts: { batchSize: number; stopping?: () => boolean },
): Promise<PassResult> {
  const result: PassResult = { ...zero(), waiting: 0, busyElsewhere: 0, recipientFailures: 0 };
  const seen = new Set<string>();
  await withClient(pool, deps.logger, 'notification-digest', async (client) => {
    for (let round = 0; round < MAX_ROUNDS; round++) {
      const waiting = await whoIsWaiting(client, opts.batchSize);
      let settledAny = false;
      for (const { tenantId, userId } of waiting) {
        if (opts.stopping?.()) return;
        // Once per pass: a person whose send failed is not retried in the next batch.
        const who = `${tenantId}\u001f${userId}`;
        if (seen.has(who)) continue;
        seen.add(who);
        result.waiting += 1;

        const lock = [DIGEST_FLUSH_LOCK, tenantId, userId];
        const { rows } = await client.query<{ ok: boolean }>('SELECT pg_try_advisory_lock($1::int, hashtext($2 || chr(31) || $3)) AS ok', lock);
        if (!rows[0]?.ok) {
          result.busyElsewhere += 1;
          continue;
        }
        try {
          const c = await flushRecipient(tenantId, userId, deps);
          for (const k of Object.keys(c) as Array<keyof FlushCounts>) result[k] += c[k];
          if (c.itemsSettled > 0) settledAny = true;
        } catch (err) {
          // Their items stay where they are, digest id and all, for the next pass.
          result.recipientFailures += 1;
          deps.logger.error({ err, tenantId, recipientId: userId }, 'digest flush failed for one recipient; next pass');
        } finally {
          await client.query('SELECT pg_advisory_unlock($1::int, hashtext($2 || chr(31) || $3))', lock);
        }
      }
      if (waiting.length < opts.batchSize || !settledAny) return;
    }
  });
  return result;
}

const drop = (tenantId: string, userId: string, digestId: string): Promise<number> =>
  withTenant(tenantId, (tx) => tx.$executeRaw`
    DELETE FROM "DigestItem" WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND "digestId" = ${digestId}`);

/** Back to waiting, for the next digest to take. */
const release = (tenantId: string, userId: string, digestId: string): Promise<number> =>
  withTenant(tenantId, (tx) => tx.$executeRaw`
    UPDATE "DigestItem" SET "digestId" = NULL WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND "digestId" = ${digestId}`);

/**
 * What the sent-log says of a digest a pass did not finish: 'resume' it (never claimed,
 * or still PENDING), 'retry' its items in a new one (it failed without going), or it is
 * 'settled' (it went, was suppressed, or was refused for good).
 */
async function digestState(tenantId: string, userId: string, digestId: string): Promise<'resume' | 'retry' | 'settled'> {
  const rows = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ status: string; lastError: string | null }>>`
      SELECT d.status::text AS status, d."lastError" FROM "NotificationDelivery" d
       WHERE d."tenantId" = ${tenantId} AND d."idempotencyKey" = ${idempotencyKey(digestKey(tenantId, userId, digestId))}`,
  );
  const row = rows[0];
  if (!row || row.status === 'PENDING') return 'resume';
  if (row.status === 'FAILED' && (row.lastError === ABANDONED || (row.lastError ?? '').startsWith(DEFERRED))) return 'retry';
  return 'settled';
}

/** One person's held replies: anything a previous pass left, then everything new. */
export async function flushRecipient(tenantId: string, userId: string, deps: FlushDeps): Promise<FlushCounts> {
  const counts = zero();

  const stale = await withTenant(tenantId, (tx) => tx.$executeRaw`
    DELETE FROM "DigestItem"
     WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND category = ${DIGEST_CATEGORY}
       AND "createdAt" < now() - ${STALE_AFTER}::interval`);
  counts.droppedStale += stale;
  counts.itemsSettled += stale;

  const unsettled = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ digestId: string }>>`
      SELECT DISTINCT i."digestId" FROM "DigestItem" i
       WHERE i."tenantId" = ${tenantId} AND i."userId" = ${userId} AND i.category = ${DIGEST_CATEGORY}
         AND i."digestId" IS NOT NULL`,
  );
  for (const { digestId } of unsettled) {
    const state = await digestState(tenantId, userId, digestId);
    if (state === 'resume') await settle(tenantId, userId, digestId, deps, counts);
    else if (state === 'retry') await release(tenantId, userId, digestId);
    else counts.itemsSettled += await drop(tenantId, userId, digestId);
  }

  const fresh = randomUUID();
  const taken = await withTenant(tenantId, (tx) => tx.$executeRaw`
    UPDATE "DigestItem" SET "digestId" = ${fresh}
     WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND category = ${DIGEST_CATEGORY} AND "digestId" IS NULL`);
  if (taken > 0) await settle(tenantId, userId, fresh, deps, counts);
  return counts;
}

async function settle(tenantId: string, userId: string, digestId: string, deps: FlushDeps, counts: FlushCounts): Promise<void> {
  const ids = { digestId, tenantId, recipientId: userId };
  const key = digestKey(tenantId, userId, digestId);

  const groups = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ postId: string; toComment: boolean; n: number }>>`
      SELECT i."postId", i."toComment", count(*)::int AS n FROM "DigestItem" i
       WHERE i."tenantId" = ${tenantId} AND i."userId" = ${userId} AND i."digestId" = ${digestId}
       GROUP BY i."postId", i."toComment"
       ORDER BY count(*) DESC, i."postId", i."toComment"`,
  );
  if (groups.length === 0) return;

  if (!(await isEnabled(tenantId, userId, DIGEST_CATEGORY, 'EMAIL'))) {
    await closePending(key, 'not sent: opted out');
    counts.itemsSettled += await drop(tenantId, userId, digestId);
    counts.droppedOptedOut += 1;
    deps.logger.info(ids, 'replies are off for this recipient; digest dropped');
    return;
  }

  const card = await loadCard(tenantId, userId);
  let opened: OpenedContactCard | null = null;
  if (card) {
    try {
      opened = await deps.openCard(card);
    } catch (err) {
      if (!(err instanceof PoisonMessage)) throw err;
      deps.logger.error({ ...ids, reason: err.message }, 'the stored contact card does not open; digest dropped');
    }
  }
  if (!opened) {
    await closePending(key, 'not sent: no address');
    counts.itemsSettled += await drop(tenantId, userId, digestId);
    counts.droppedNoAddress += 1;
    if (!card) deps.logger.info(ids, 'no contact card for this recipient; digest dropped');
    return;
  }

  const unsubscribeUrl = `${deps.publicUrl}/unsubscribe?token=${encodeURIComponent(
    signUnsubscribe(deps.unsubscribeSecret, { tenantId, userId, category: DIGEST_CATEGORY }),
  )}`;
  const origin = deps.appUrl.originFor(opened.tenantSlug);
  if (!origin) {
    counts.withoutLinks += 1;
    deps.logger.warn(
      { ...ids, reason: opened.tenantSlug === null ? 'the card carries no slug' : 'the card\'s slug is not a subdomain the gateway routes' },
      'the recipient\'s contact card names no subdomain to link to; the digest goes without its links',
    );
  }
  const outcome = await deliver(
    {
      key,
      category: 'community',
      to: opened.email,
      email: renderReplyDigest({
        groups: groups.map((g) => ({ postUrl: origin ? `${origin}/posts/${encodeURIComponent(g.postId)}` : null, toComment: g.toComment, count: g.n })),
        unsubscribeUrl,
      }),
      headers: {
        'X-EventHub-Message-Id': digestId,
        'List-Unsubscribe': `<${unsubscribeUrl}>`,
        'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
      },
      ids,
      sentMessage: 'reply digest sent',
    },
    deps,
  );

  if (outcome.kind === 'transient') {
    await closePending(key, `${DEFERRED}: ${outcome.error}`);
    await release(tenantId, userId, digestId);
    counts.deferred += 1;
    deps.logger.warn({ ...ids, reason: outcome.error }, 'digest not sent; its replies wait for the next pass');
    return;
  }
  // "Already handled" can also mean the sweeper ended the claim between this pass
  // reading it PENDING and claiming it: that is a digest that did not go.
  if (outcome.kind === 'duplicate' && (await digestState(tenantId, userId, digestId)) === 'retry') {
    await release(tenantId, userId, digestId);
    counts.deferred += 1;
    return;
  }
  if (outcome.kind === 'sent') counts.digestsSent += 1;
  counts.itemsSettled += await drop(tenantId, userId, digestId);
}

/**
 * Refuses to flush on a credential that can do more than find who is waiting.
 *
 * It crosses every tenant, so its privileges are the whole bound on it, and they are
 * read from the CATALOG rather than probed from a list someone wrote: every privilege
 * it holds on every column and every table in the schema must be exactly SELECT on
 * the three columns it reads. A list of probes is only as complete as the list — a
 * grant nobody thought to probe would pass. And it must be a member of no role at all:
 * a membership, even one that does not inherit, is a SET ROLE away from everything the
 * catalog check just proved it cannot do.
 */
export async function assertDigestIsLeastPrivilege(pool: pg.Pool, logger: Logger): Promise<void> {
  await withClient(pool, logger, 'notification-digest', async (client) => {
    const who = await client.query<{ rolsuper: boolean; rolbypassrls: boolean; memberships: number; create: boolean }>(
      `SELECT r.rolsuper, r.rolbypassrls,
              (SELECT count(*)::int FROM pg_auth_members m WHERE m.member = r.oid) AS memberships,
              has_schema_privilege(current_user, 'public', 'CREATE') AS "create"
         FROM pg_roles r WHERE r.rolname = current_user`,
    );
    const role = who.rows[0];
    if (!role) throw new Error('could not read current_user from pg_roles');
    if (role.rolsuper || role.rolbypassrls) {
      throw new Error(
        'refusing to flush digests: the digest credential bypasses row-level security ' +
          `(rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls})`,
      );
    }
    if (role.memberships > 0 || role.create) {
      throw new Error(
        `refusing to flush digests: the digest credential is a member of ${role.memberships} role(s)` +
          `${role.create ? ' and can create objects in the schema' : ''}; it must hold nothing but its own grants`,
      );
    }

    const held = await client.query<{ what: string }>(
      `SELECT format('%s %s(%s)', p.priv, c.relname, a.attname) AS what
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        CROSS JOIN unnest(ARRAY['SELECT', 'INSERT', 'UPDATE', 'REFERENCES']) AS p(priv)
        WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') AND has_column_privilege(current_user, c.oid, a.attnum, p.priv)
       UNION ALL
       SELECT format('%s %s', p.priv, c.relname)
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
        CROSS JOIN unnest(ARRAY['DELETE', 'TRUNCATE', 'TRIGGER']) AS p(priv)
        WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f') AND has_table_privilege(current_user, c.oid, p.priv)
       ORDER BY 1`,
    );
    const expected = ['SELECT DigestItem(createdAt)', 'SELECT DigestItem(tenantId)', 'SELECT DigestItem(userId)'];
    const got = held.rows.map((r) => r.what);
    const extra = got.filter((g) => !expected.includes(g));
    const missing = expected.filter((e) => !got.includes(e));
    if (extra.length > 0 || missing.length > 0) {
      throw new Error(
        'refusing to flush digests: the digest credential should hold SELECT on three columns of "DigestItem" and ' +
          `nothing else (more: ${extra.join(', ') || 'none'}; missing: ${missing.join(', ') || 'none'})`,
      );
    }

    // It must be able to do its one job, or the check above proves nothing but a
    // broken connection.
    await client.query('SELECT i."tenantId", i."userId", i."createdAt" FROM "DigestItem" i LIMIT 1');

    const policies = await client.query<{ cmd: string }>(
      `SELECT cmd FROM pg_policies WHERE schemaname = 'public' AND tablename = 'DigestItem' AND current_user = ANY(roles)`,
    );
    if (policies.rows.length === 0 || policies.rows.some((p) => p.cmd !== 'SELECT')) {
      throw new Error(
        'refusing to flush digests: the digest credential must have row-security policies on "DigestItem" ' +
          `for SELECT only (it has: ${policies.rows.map((p) => p.cmd).join(', ') || 'none'})`,
      );
    }
  });
}
