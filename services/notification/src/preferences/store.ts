import { withTenant } from '../db.js';
import { CATEGORIES, isOptional, type Category, type Channel } from './categories.js';

/**
 * What a user has chosen, read and written under row security in their tenant.
 */

/** Thrown when something asks to turn off a message that cannot be turned off. */
export class NotOptional extends Error {
  override name = 'NotOptional';
}

/**
 * Whether a message in this category may be sent to this user on this channel.
 *
 * A TRANSACTIONAL category is always enabled, and the table is not even read for it:
 * a row that said otherwise — however it got there — must not stop a password reset.
 */
export async function isEnabled(tenantId: string, userId: string, category: Category, channel: Channel): Promise<boolean> {
  if (!isOptional(category)) return true;
  const rows = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ enabled: boolean }>>`
      SELECT enabled FROM "NotificationPreference"
       WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND category = ${category} AND channel = ${channel}`,
  );
  return rows[0]?.enabled ?? CATEGORIES[category].defaultEnabled;
}

/** Every category with the user's current setting, for the preferences page. */
export async function listPreferences(tenantId: string, userId: string, channel: Channel) {
  const rows = await withTenant(tenantId, (tx) =>
    tx.$queryRaw<Array<{ category: string; enabled: boolean }>>`
      SELECT category, enabled FROM "NotificationPreference"
       WHERE "tenantId" = ${tenantId} AND "userId" = ${userId} AND channel = ${channel}`,
  );
  const chosen = new Map(rows.map((r) => [r.category, r.enabled]));
  return (Object.keys(CATEGORIES) as Category[]).map((category) => {
    const optional = isOptional(category);
    return {
      category,
      channel,
      label: CATEGORIES[category].label,
      // A transactional category reads as enabled whatever a row says, as isEnabled does.
      enabled: optional ? (chosen.get(category) ?? CATEGORIES[category].defaultEnabled) : true,
      canTurnOff: optional,
    };
  });
}

/**
 * Records a choice. Refuses a transactional category HERE, not only in the route, so
 * no caller — the unsubscribe link, a future admin tool — can write the row that turns
 * a password reset off.
 *
 * Returns whether the stored value changed, so a repeated unsubscribe reports that it
 * did nothing rather than claiming a change.
 */
export async function setPreference(
  tenantId: string,
  userId: string,
  category: Category,
  channel: Channel,
  enabled: boolean,
): Promise<boolean> {
  if (!isOptional(category)) throw new NotOptional(`${category} cannot be turned off`);
  const written = await withTenant(tenantId, (tx) =>
    tx.$executeRaw`
      INSERT INTO "NotificationPreference" ("tenantId", "userId", category, channel, enabled, "updatedAt")
      VALUES (${tenantId}, ${userId}, ${category}, ${channel}, ${enabled}, now())
      ON CONFLICT ("tenantId", "userId", category, channel) DO UPDATE
         SET enabled = EXCLUDED.enabled, "updatedAt" = now()
       WHERE "NotificationPreference".enabled IS DISTINCT FROM EXCLUDED.enabled`,
  );
  return written === 1;
}
