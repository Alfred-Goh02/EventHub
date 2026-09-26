import { createHmac, timingSafeEqual } from 'node:crypto';
import { isCategory, isOptional, type Category } from './categories.js';

/**
 * The token in an unsubscribe link: which tenant, user and category it turns off,
 * signed with a secret only this service holds.
 *
 * WHY A SIGNATURE AND NOT A LOGIN. An unsubscribe must work from the email itself,
 * without signing in — that is the point of it, and the law in more than one place.
 * So the link carries its own authority, and the signature is what stops anyone from
 * writing a link that turns off someone else's email by changing the user id in it.
 *
 * HMAC-SHA256 over the whole payload, compared in constant time. Notification's RSA
 * key is an encryption key (RSA-OAEP), and reusing a key for a second purpose is how
 * a signing oracle gets built, so this has a secret of its own.
 *
 * NO EXPIRY. An unsubscribe link in an email a user kept for a year must still work;
 * what it can do is only ever turn one optional category off for the person it was
 * sent to.
 *
 * Bound to ONE category, and refused for a transactional one, so a token cannot be
 * minted — by a bug here or anywhere — that turns off a password reset.
 */

const VERSION = 'v1';

interface Claims {
  tenantId: string;
  userId: string;
  category: Category;
}

const mac = (secret: string, body: string) => createHmac('sha256', secret).update(`${VERSION}.${body}`).digest();

export function signUnsubscribe(secret: string, claims: Claims): string {
  if (!isOptional(claims.category)) throw new Error(`refusing to mint an unsubscribe token for ${claims.category}`);
  const body = Buffer.from(JSON.stringify({ t: claims.tenantId, u: claims.userId, c: claims.category }), 'utf8').toString('base64url');
  return `${VERSION}.${body}.${mac(secret, body).toString('base64url')}`;
}

/** The claims, or null for anything that is not a token this service signed. */
export function verifyUnsubscribe(secret: string, token: string): Claims | null {
  const parts = token.split('.');
  if (parts.length !== 3 || parts[0] !== VERSION) return null;
  const [, body, signature] = parts as [string, string, string];

  const expected = mac(secret, body);
  const given = Buffer.from(signature, 'base64url');
  // timingSafeEqual throws on a length mismatch, which would itself leak the length.
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(body, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
  const { t, u, c } = (parsed ?? {}) as { t?: unknown; u?: unknown; c?: unknown };
  if (typeof t !== 'string' || typeof u !== 'string' || t.length === 0 || u.length === 0) return null;
  if (!isCategory(c) || !isOptional(c)) return null;
  return { tenantId: t, userId: u, category: c };
}
