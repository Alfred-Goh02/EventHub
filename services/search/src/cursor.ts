import { createHash, createHmac, timingSafeEqual } from 'node:crypto';
import { env } from './env.js';

/** A cursor that must not be honoured. The reason is logged, never returned. */
export class CursorError extends Error {}

/**
 * What a cursor carries between pages.
 *
 *   t  the tenant it was issued to
 *   h  a hash of the query it belongs to
 *   a  the last hit's sort values, for search_after
 *   p  the Point-in-Time id, once one has been opened
 */
export interface CursorPayload {
  v: 1;
  t: string;
  h: string;
  a: unknown[];
  p?: string;
}

/**
 * SIGNED, because a cursor is handed to the client and comes back from it.
 *
 * Unsigned, the tenant field is decoration: a client edits it to match its own
 * tenant and keeps someone else's PIT id. And a PIT id is not a secret — it is a
 * structured reference to search contexts, not an unguessable token. With an
 * HMAC over the whole payload, a cursor this service did not issue, or issued to
 * another tenant, or for another query, is refused before anything is searched.
 *
 * The redundant tenantId term filter in every query is the backstop behind this,
 * and scripts/verify-search-pagination.mjs tests it directly: it signs a cursor
 * for tenant B that carries tenant A's PIT, and asserts B reads nothing of A's.
 */
const mac = (payload: string): string =>
  createHmac('sha256', env.SEARCH_CURSOR_SECRET).update(payload).digest('base64url');

/**
 * Binds a cursor to its query. Paging with a cursor from one search and the
 * filters of another would seek to a sort position that means nothing in the
 * new result set — no leak, but silently wrong pages.
 */
export const queryHash = (params: Record<string, unknown>): string =>
  createHash('sha256').update(JSON.stringify(params)).digest('base64url').slice(0, 22);

export function encodeCursor(cursor: Omit<CursorPayload, 'v'>): string {
  const payload = Buffer.from(JSON.stringify({ v: 1, ...cursor })).toString('base64url');
  return `${payload}.${mac(payload)}`;
}

export function decodeCursor(cursor: string, tenantId: string, hash: string): CursorPayload {
  const [payload, signature, extra] = cursor.split('.');
  if (!payload || !signature || extra !== undefined) throw new CursorError('malformed');

  // Constant time, so the signature cannot be discovered a byte at a time.
  const expected = Buffer.from(mac(payload));
  const given = Buffer.from(signature);
  if (expected.length !== given.length || !timingSafeEqual(expected, given)) {
    throw new CursorError('bad signature');
  }

  let decoded: CursorPayload;
  try {
    decoded = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8')) as CursorPayload;
  } catch {
    throw new CursorError('malformed');
  }

  if (decoded.v !== 1 || !Array.isArray(decoded.a)) throw new CursorError('malformed');
  if (decoded.t !== tenantId) throw new CursorError('issued to another tenant');
  if (decoded.h !== hash) throw new CursorError('issued for another query');
  return decoded;
}
