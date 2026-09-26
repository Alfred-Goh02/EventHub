import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import type { Logger } from '@eventhub/logger';
import type { EmailChannel } from '../channels/types.js';
import { withTenant } from '../db.js';
import { withClient } from '../pool.js';
import { isEnabled } from '../preferences/store.js';
import { signUnsubscribe } from '../preferences/unsubscribe-token.js';
import { deliver } from '../dispatch/consumer.js';
import { PoisonMessage, type ContactCardEnvelope, type OpenedContactCard } from '../dispatch/open-seal.js';
import { loadCard } from '../dispatch/replies.js';
import { renderReplyDigest } from '../dispatch/templates.js';

/**
 * Sends each person's held replies as one digest.
 *
 * WHO IS WAITING is asked across tenants, on the digest's own credential, which can
 * read three columns of the held items and nothing else. EVERYTHING ELSE — which posts,
 * the preference, the card, the send, the delete — is done as the app role inside that
 * person's tenant, exactly as a reply is.
 *
 * TAKEN, THEN SETTLED. A pass first stamps a person's waiting items with a new digest
 * id, in one statement, and that id is the digest's identity in the sent-log. Then it
 * sends, and deletes the items once the digest is settled — sent, a duplicate of one
 * already sent, suppressed, or refused. If the process dies between the two, the items
 * still carry their digest id, and the next pass settles THAT digest before taking
 * anything new: the sent-log says whether it went, so it is never sent twice.
 *
 * RE-ASKED AT SEND TIME, because an hour is long enough for things to change: a person
 * who turned replies off is sent nothing and their items are dropped; one with no card
 * that opens, the same. The suppression list is deliver()'s, as for any email.
 */

export const DIGEST_CATEGORY = 'COMMUNITY_REPLY';
const DIGEST_TEMPLATE = 'community-digest';

export interface FlushDeps {
  openCard: (envelope: ContactCardEnvelope) => Promise<OpenedContactCard>;
  channel: EmailChannel;
  logger: Logger;
  unsubscribeSecret: string;
  publicUrl: string;
  appUrl: string;
}

export interface FlushCounts {
  digestsSent: number;
  itemsSettled: number;
  droppedOptedOut: number;
  droppedNoAddress: number;
  /** Left for the next pass: the store or the channel failed. */
  deferred: number;
}

const zero = (): FlushCounts => ({ digestsSent: 0, itemsSettled: 0, droppedOptedOut: 0, droppedNoAddress: 0, deferred: 0 });

/** Who has anything waiting, longest first. The digest credential's whole job. */
export async function whoIsWaiting(pool: pg.Pool, logger: Logger, limit: number): Promise<Array<{ tenantId: string; userId: string }>> {
  return withClient(pool, logger, 'notification-digest', async (client) => {
    const { rows } = await client.query<{ tenantId: string; userId: string }>(
      `SELECT i."tenantId", i."userId" FROM "DigestItem" i
        GROUP BY i."tenantId", i."userId"
        ORDER BY min(i."createdAt"), i."tenantId", i."userId"
        LIMIT $1`,
      [limit],
    );
    return rows;
  });
}

export interface PassResult extends FlushCounts {
  /** People with replies waiting when the pass began, up to the batch size. */
  waiting: number;
  /** People whose flush failed; their items stay for the next pass. */
  recipientFailures: number;
}

/**
 * One pass: a digest to each person with replies held, up to `batchSize` people,
 * longest-waiting first. One person's failure is counted and the pass moves on.
 */
export async function flushOnce(
  pool: pg.Pool,
  deps: FlushDeps,
  opts: { batchSize: number; stopping?: () => boolean },
): Promise<PassResult> {
  const waiting = await whoIsWaiting(pool, deps.logger, opts.batchSize);
  const result: PassResult = { ...zero(), waiting: waiting.length, recipientFailures: 0 };
  for (const { tenantId, userId } of waiting) {
    if (opts.stopping?.()) break;
    try {
      const c = await flushRecipient(tenantId, userId, deps);
      for (const k of Object.keys(c) as Array<keyof FlushCounts>) result[k] += c[k];
    } catch (err) {
      // Their items stay where they are, digest id and all, for the next pass.
      result.recipientFailures += 1;
      deps.logger.error({ err, tenantId, recipientId: userId }, 'digest flush failed for one recipient; next pass');
    }
  }
  return result;
}

/** One person's held replies: anything a previous pass took and left, then everything new. */
export async function flushRecipient(tenantId: string, userId: string, deps: FlushDeps): Promise<FlushCounts> {
  const counts = zero();
  const unsettled = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ digestId: string }>>`
      SELECT DISTINCT i."digestId" FROM "DigestItem" i
       WHERE i."tenantId" = ${tenantId} AND i."userId" = ${userId} AND i.category = ${DIGEST_CATEGORY}
         AND i."digestId" IS NOT NULL`,
  );
  const fresh = randomUUID();
  const taken = await withTenant(tenantId, (tx) => tx.$executeRaw`
    UPDATE "DigestItem" SET "digestId" = ${fresh}
     WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND category = ${DIGEST_CATEGORY} AND "digestId" IS NULL`);

  for (const digestId of [...unsettled.map((u) => u.digestId), ...(taken > 0 ? [fresh] : [])]) {
    await settle(tenantId, userId, digestId, deps, counts);
  }
  return counts;
}

async function settle(tenantId: string, userId: string, digestId: string, deps: FlushDeps, counts: FlushCounts): Promise<void> {
  const ids = { digestId, tenantId, recipientId: userId };
  const drop = async (): Promise<number> =>
    withTenant(tenantId, (tx) => tx.$executeRaw`
      DELETE FROM "DigestItem" WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND "digestId" = ${digestId}`);

  const groups = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ postId: string; toComment: boolean; n: number }>>`
      SELECT i."postId", i."toComment", count(*)::int AS n FROM "DigestItem" i
       WHERE i."tenantId" = ${tenantId} AND i."userId" = ${userId} AND i."digestId" = ${digestId}
       GROUP BY i."postId", i."toComment"
       ORDER BY count(*) DESC, i."postId", i."toComment"`,
  );
  if (groups.length === 0) return;

  if (!(await isEnabled(tenantId, userId, DIGEST_CATEGORY, 'EMAIL'))) {
    counts.itemsSettled += await drop();
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
    counts.itemsSettled += await drop();
    counts.droppedNoAddress += 1;
    if (!card) deps.logger.info(ids, 'no contact card for this recipient; digest dropped');
    return;
  }

  const unsubscribeUrl = `${deps.publicUrl}/unsubscribe?token=${encodeURIComponent(
    signUnsubscribe(deps.unsubscribeSecret, { tenantId, userId, category: DIGEST_CATEGORY }),
  )}`;
  const outcome = await deliver(
    {
      key: { tenantId, sourceMessageId: digestId, recipientId: userId, channel: 'email', templateKey: DIGEST_TEMPLATE },
      category: 'community',
      to: opened.email,
      email: renderReplyDigest({
        groups: groups.map((g) => ({ postUrl: `${deps.appUrl}/posts/${encodeURIComponent(g.postId)}`, toComment: g.toComment, count: g.n })),
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
    // The items keep their digest id; the next pass settles this same digest.
    counts.deferred += 1;
    deps.logger.warn({ ...ids, reason: outcome.error }, 'digest not sent; trying again next pass');
    return;
  }
  if (outcome.kind === 'sent') counts.digestsSent += 1;
  counts.itemsSettled += await drop();
}

/** Runs statements that must be refused, and rolls back whatever they did if they were not. */
async function refused(client: pg.PoolClient, ...statements: string[]): Promise<boolean> {
  await client.query('BEGIN');
  try {
    for (const sql of statements) await client.query(sql);
    return false;
  } catch {
    return true;
  } finally {
    await client.query('ROLLBACK').catch(() => {});
  }
}

/**
 * Refuses to flush on a credential that can do more than find who is waiting.
 *
 * It crosses every tenant, so its grants and its policy are the whole bound on it, and
 * both are proved here rather than trusted: not a role that bypasses row security; no
 * read of the sent-log, the cards, the suppression list, the preferences, or the held
 * items' posts; no write to the held items at all, with a tenant set so the probe meets
 * the grant and not the policy; and only SELECT policies of its own. Each refusal is a
 * statement that fails on the privilege before it looks at a row, so an empty table
 * cannot make a probe pass — the trap the sweeper's own check fell into once.
 */
export async function assertDigestIsLeastPrivilege(pool: pg.Pool, logger: Logger): Promise<void> {
  await withClient(pool, logger, 'notification-digest', async (client) => {
    const who = await client.query<{ rolsuper: boolean; rolbypassrls: boolean }>(
      'SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user',
    );
    const role = who.rows[0];
    if (!role) throw new Error('could not read current_user from pg_roles');
    if (role.rolsuper || role.rolbypassrls) {
      throw new Error(
        'refusing to flush digests: the digest credential bypasses row-level security ' +
          `(rolsuper=${role.rolsuper}, rolbypassrls=${role.rolbypassrls})`,
      );
    }

    const tenant = `SELECT set_config('app.tenant_id', 'digest_privilege_probe', true)`;
    const mustRefuse: Array<[string, string[]]> = [
      ['read the sent-log', ['SELECT 1 FROM "NotificationDelivery" LIMIT 1']],
      ['read contact cards', ['SELECT 1 FROM "ContactCard" LIMIT 1']],
      ['read the suppression list', ['SELECT 1 FROM "EmailSuppression" LIMIT 1']],
      ['read preferences', ['SELECT 1 FROM "NotificationPreference" LIMIT 1']],
      ['read which post a held reply was on', ['SELECT "postId" FROM "DigestItem" LIMIT 1']],
      [
        'hold a reply',
        [
          tenant,
          `INSERT INTO "DigestItem" ("tenantId", "userId", category, "sourceMessageId", "postId", "toComment")
           VALUES ('digest_privilege_probe', 'probe', 'probe', 'probe', 'probe', false)`,
        ],
      ],
      ['take held replies into a digest', [tenant, `UPDATE "DigestItem" SET "digestId" = 'probe'`]],
      ['delete held replies', [tenant, 'DELETE FROM "DigestItem"']],
    ];
    for (const [what, statements] of mustRefuse) {
      if (!(await refused(client, ...statements))) {
        throw new Error(
          `refusing to flush digests: the digest credential can ${what}. It should read three columns of ` +
            '"DigestItem" and nothing else — it crosses every tenant, so its grants are the whole bound on it.',
        );
      }
    }

    // It must be able to do its one job, or the refusals above prove nothing but a
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
