import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { compactDecrypt, errors, importPKCS8, jwtVerify, type CryptoKey } from 'jose';

/**
 * Opens a sealed email request from Auth: decrypt with Notification's private key,
 * then verify Auth's signature on what was inside.
 *
 * services/auth/src/email-request.ts explains why there are two layers. Encryption
 * keeps the address and the link off Kafka. The signature is what makes a request
 * worth acting on: anyone can encrypt to Notification's public key, so a seal that
 * merely opens proves nothing about who wrote it.
 *
 * THE ALGORITHMS ARE PINNED ON THE READ SIDE TOO. Without the lists below, jose
 * would accept whatever algorithm the message's own header names, and a
 * producer could choose a weaker one.
 */

/** The message is at fault. It will never succeed, so it is recorded and skipped. */
export class PoisonMessage extends Error {
  override name = 'PoisonMessage';
}

/** The link inside has already stopped working, so there is nothing worth sending. */
export class ExpiredRequest extends Error {
  override name = 'ExpiredRequest';
}

export interface EmailRequestEnvelope {
  messageId: string;
  tenantId: string;
  userId: string;
  kind: string;
  expiresAt: string;
  sealed: string;
}

/**
 * The kinds of sealed email this service sends: a CLOSED SET, checked against what Auth
 * signed. Anything else is poison, not a default.
 *
 *   PASSWORD_RESET  the subject is a User.
 *   INVITATION      the subject is an Invite. Its recipient is NOT a user, so nothing
 *                   that is keyed by user -- a contact card, a preference, the digest --
 *                   may be looked up for it.
 */
export const SEALED_KINDS = ['PASSWORD_RESET', 'INVITATION'] as const;
export type SealedKind = (typeof SEALED_KINDS)[number];
const isSealedKind = (value: unknown): value is SealedKind => SEALED_KINDS.includes(value as SealedKind);

/** The roles an invitation can grant, as Auth defines them. The role is printed in the email, so it is a closed set too. */
export const INVITATION_ROLES = ['ADMIN', 'ORGANIZER', 'ATTENDEE'] as const;
export type InvitationRole = (typeof INVITATION_ROLES)[number];
const isInvitationRole = (value: unknown): value is InvitationRole => INVITATION_ROLES.includes(value as InvitationRole);

/**
 * Control characters, CR and LF among them. An address is written into a header, so
 * one carrying a line break is a header injection, however it got signed. Auth's
 * schema refuses them on the way in; this refuses them on the way out as well.
 */
const CONTROL = /[\u0000-\u001f\u007f]/;

interface OpenedCommon {
  tenantId: string;
  /** The subject: a User for a reset, an Invite for an invitation. Named for the envelope's field. */
  userId: string;
  email: string;
  link: string;
  expiresAt: Date;
}

export type OpenedEmailRequest =
  | (OpenedCommon & { kind: 'PASSWORD_RESET' })
  | (OpenedCommon & { kind: 'INVITATION'; role: InvitationRole });

export const EMAIL_REQUEST_AUDIENCE = 'eventhub-notification';
export const EMAIL_REQUEST_TYPE = 'auth-email+jwt';

// From src/dispatch/ and dist/dispatch/ alike, ../../keys is services/notification/keys.
const keyUrl = new URL('../../keys/private.pem', import.meta.url);

/** Refuses to start without the key, rather than consuming requests it can never open. */
export async function loadNotificationPrivateKey(): Promise<CryptoKey> {
  let pem: string;
  try {
    pem = readFileSync(keyUrl, 'utf8');
  } catch (err) {
    throw new Error(
      `refusing to start: Notification's private key is missing at ${fileURLToPath(keyUrl)}. ` +
        'Every sealed request would be unreadable. Generate it with ' +
        '`node services/notification/scripts/generate-keys.mjs`.',
      { cause: err },
    );
  }
  return importPKCS8(pem, 'RSA-OAEP-256');
}

const reason = (err: unknown): string => (err as { code?: string })?.code ?? (err instanceof Error ? err.name : 'unknown');

/** The one decryption, with the algorithms pinned, for every kind of seal. */
async function unseal(sealed: string, privateKey: CryptoKey): Promise<string> {
  try {
    const { plaintext } = await compactDecrypt(sealed, privateKey, {
      keyManagementAlgorithms: ['RSA-OAEP-256'],
      contentEncryptionAlgorithms: ['A256GCM'],
    });
    return new TextDecoder().decode(plaintext);
  } catch (err) {
    throw new PoisonMessage(`the seal did not open (${reason(err)})`);
  }
}

export function createSealOpener(opts: { privateKey: CryptoKey; authPublicKey: CryptoKey; issuer: string }) {
  return async function open(envelope: EmailRequestEnvelope): Promise<OpenedEmailRequest> {
    const plaintext = await unseal(envelope.sealed, opts.privateKey);

    let claims;
    try {
      ({ payload: claims } = await jwtVerify(plaintext, opts.authPublicKey, {
        algorithms: ['RS256'],
        issuer: opts.issuer,
        audience: EMAIL_REQUEST_AUDIENCE,
        typ: EMAIL_REQUEST_TYPE,
        requiredClaims: ['sub', 'jti', 'exp'],
      }));
    } catch (err) {
      // Genuine, but too late: the signature was checked before exp was.
      if (err instanceof errors.JWTExpired) throw new ExpiredRequest('the sealed link has expired');
      throw new PoisonMessage(`Auth's signature did not verify (${reason(err)})`);
    }

    /**
     * The envelope is plaintext anyone can edit; the claims are what Auth signed.
     * They must agree. jti binds a seal to the one message it was made for, so a
     * valid seal copied into another message is refused rather than delivered under
     * a message id the sent-log has never seen.
     */
    if (claims.jti !== envelope.messageId) {
      throw new PoisonMessage('the seal was made for a different message (jti does not match)');
    }
    if (claims.sub !== envelope.userId || claims.tenantId !== envelope.tenantId) {
      throw new PoisonMessage('the seal names a different user or tenant than its envelope');
    }
    // The kind Auth signed, and the envelope's must be the same one: a genuine reset's
    // seal relabelled as an invitation in the plaintext would otherwise be sent with
    // another template, and the reverse.
    if (!isSealedKind(claims.kind) || claims.kind !== envelope.kind) {
      throw new PoisonMessage('not a kind of email this service sends');
    }
    if (typeof claims.email !== 'string' || typeof claims.link !== 'string' || typeof claims.tenantId !== 'string') {
      throw new PoisonMessage('the seal carries no address, no link or no tenant');
    }
    if (claims.email.length === 0 || CONTROL.test(claims.email)) {
      throw new PoisonMessage('the seal carries an address that cannot be put in a header');
    }

    const common: OpenedCommon = {
      tenantId: claims.tenantId,
      userId: claims.sub as string,
      email: claims.email,
      link: claims.link,
      expiresAt: new Date((claims.exp as number) * 1000),
    };
    if (claims.kind === 'PASSWORD_RESET') return { ...common, kind: 'PASSWORD_RESET' };

    if (!isInvitationRole(claims.role)) {
      throw new PoisonMessage('the invitation names no role it could grant');
    }
    return { ...common, kind: 'INVITATION', role: claims.role };
  };
}

/**
 * A contact card's type, pinned here as the email request's is above. The two are
 * sealed to the same key and signed by the same key, so the type is all that tells
 * them apart: a card must never be accepted as a request to send, nor the reverse.
 */
export const CONTACT_CARD_TYPE = 'auth-contact+jwt';

export interface ContactCardEnvelope {
  messageId: string;
  occurredAt: string;
  tenantId: string;
  userId: string;
  sealed: string;
}

export interface OpenedContactCard {
  tenantId: string;
  userId: string;
  email: string;
}

/**
 * Opens a contact card from auth.user-contact and believes it only if Auth signed it
 * for THIS envelope. Same two layers, and the same reasons, as an email request.
 *
 * No exp is required: a card stands until a newer one replaces it.
 */
export function createCardOpener(opts: { privateKey: CryptoKey; authPublicKey: CryptoKey; issuer: string }) {
  return async function openCard(envelope: ContactCardEnvelope): Promise<OpenedContactCard> {
    const plaintext = await unseal(envelope.sealed, opts.privateKey);

    let claims;
    try {
      ({ payload: claims } = await jwtVerify(plaintext, opts.authPublicKey, {
        algorithms: ['RS256'],
        issuer: opts.issuer,
        audience: EMAIL_REQUEST_AUDIENCE,
        typ: CONTACT_CARD_TYPE,
        requiredClaims: ['sub', 'jti', 'iat'],
      }));
    } catch (err) {
      throw new PoisonMessage(`Auth's signature did not verify (${reason(err)})`);
    }

    // The envelope is plaintext anyone can edit; these are what Auth signed.
    if (claims.jti !== envelope.messageId) {
      throw new PoisonMessage('the card was sealed for a different message (jti does not match)');
    }
    if (claims.sub !== envelope.userId || claims.tenantId !== envelope.tenantId) {
      throw new PoisonMessage('the card names a different user or tenant than its envelope');
    }
    // The ordering key. Unsigned, a replayed old card could claim to be the newest.
    if (claims.occurredAt !== envelope.occurredAt) {
      throw new PoisonMessage('the card was sealed at a different time than its envelope says');
    }
    if (typeof claims.email !== 'string' || claims.email.length === 0) {
      throw new PoisonMessage('the card carries no address');
    }
    return { tenantId: claims.tenantId as string, userId: claims.sub as string, email: claims.email };
  };
}
