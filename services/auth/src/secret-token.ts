import { createHash, randomBytes } from 'node:crypto';

/**
 * The one place this service mints and hashes bearer secrets.
 *
 * Refresh tokens, invites and password resets are the same primitive wearing
 * three different expiry policies: 32 random bytes handed out exactly once, only
 * the SHA-256 hash stored, found later by hashing what the caller presents.
 *
 * It lives in one module because it had started to live in three. tokens.ts and
 * invites.ts each carried a private copy with the same comment, and password
 * reset was about to add a fourth. Three copies of a security primitive is three
 * chances for one of them to drift to a shorter token or a different digest —
 * and every failure mode here returns the same generic error, so drift would be
 * invisible from outside.
 *
 * SHA-256 rather than argon2 is deliberate and is the reason these are NOT
 * passwords: the input is already 256 bits of entropy, so there is nothing to
 * brute-force and a slow hash would only make every lookup slow. Argon2 exists
 * to make weak human-chosen secrets expensive to guess; there is no weak secret
 * here to protect.
 */

/** 32 bytes of CSPRNG output, base64url so it survives a URL and an email. */
export const newSecretToken = (): string => randomBytes(32).toString('base64url');

/**
 * What gets stored. Never the token itself.
 *
 * A database dump therefore yields no usable credential, which is also why a
 * lost invite or reset link must be reissued rather than looked up.
 */
export const hashSecretToken = (token: string): string =>
  createHash('sha256').update(token).digest('hex');
