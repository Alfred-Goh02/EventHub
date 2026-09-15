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

export interface OpenedEmailRequest {
  tenantId: string;
  userId: string;
  kind: 'PASSWORD_RESET';
  email: string;
  link: string;
  expiresAt: Date;
}

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

export function createSealOpener(opts: { privateKey: CryptoKey; authPublicKey: CryptoKey; issuer: string }) {
  return async function open(envelope: EmailRequestEnvelope): Promise<OpenedEmailRequest> {
    let plaintext: Uint8Array;
    try {
      ({ plaintext } = await compactDecrypt(envelope.sealed, opts.privateKey, {
        keyManagementAlgorithms: ['RSA-OAEP-256'],
        contentEncryptionAlgorithms: ['A256GCM'],
      }));
    } catch (err) {
      throw new PoisonMessage(`the seal did not open (${reason(err)})`);
    }

    let claims;
    try {
      ({ payload: claims } = await jwtVerify(new TextDecoder().decode(plaintext), opts.authPublicKey, {
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
    if (claims.kind !== 'PASSWORD_RESET' || envelope.kind !== 'PASSWORD_RESET') {
      throw new PoisonMessage('not a kind of email this service sends');
    }
    if (typeof claims.email !== 'string' || typeof claims.link !== 'string' || typeof claims.tenantId !== 'string') {
      throw new PoisonMessage('the seal carries no address, no link or no tenant');
    }

    return {
      tenantId: claims.tenantId,
      userId: claims.sub as string,
      kind: 'PASSWORD_RESET',
      email: claims.email,
      link: claims.link,
      expiresAt: new Date((claims.exp as number) * 1000),
    };
  };
}
