import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { CompactEncrypt, calculateJwkThumbprint, exportJWK, importSPKI } from 'jose';
import { signEmailRequest } from './jwt.js';

/**
 * Seals what Notification needs to send an email that carries a credential.
 *
 * DECIDED on 2026-09-15: a reset link travels to Notification over Kafka, encrypted
 * to Notification's public key. Two alternatives were rejected:
 *   - the Field Guide's raw token on a short-retention topic, where anyone who can
 *     read the topic can take over the account;
 *   - Notification asking Auth for the link at send time, which makes every email
 *     wait on Auth being up.
 *
 * TWO LAYERS, because each proves a different thing:
 *   JWE, to Notification's key   only Notification can read the address and the
 *                                link: not Kafka, not the outbox table, not a log.
 *   JWS, by Auth's key, inside   Auth wrote it. Encryption says nothing about the
 *                                author, since anyone can encrypt to a public key.
 *                                Without the signature, any process that can produce
 *                                to the topic could have Notification email a link
 *                                of its choosing, from this platform, to any address.
 *
 * The algorithms are fixed here, and the reader must pin them too: RSA-OAEP-256
 * rather than RSA1_5, which is open to padding-oracle attacks, and A256GCM, which
 * authenticates the ciphertext.
 */

export const AUTH_EMAIL_TOPIC = 'auth.email-requested';

const KEY_ALG = 'RSA-OAEP-256';
const CONTENT_ALG = 'A256GCM';

// Resolved relative to THIS FILE, as jwt.ts does: ../keys works from src/ and dist/.
const keyUrl = new URL('../keys/notification-public.pem', import.meta.url);

function readNotificationKey(): string {
  try {
    return readFileSync(keyUrl, 'utf8');
  } catch (err) {
    throw new Error(
      `refusing to start: Notification's public key is missing at ${fileURLToPath(keyUrl)}. ` +
        'A password reset could be issued and never delivered. Generate it with ' +
        '`node services/notification/scripts/generate-keys.mjs`, which copies the public half here.',
      { cause: err },
    );
  }
}

const notificationKey = await importSPKI(readNotificationKey(), KEY_ALG, { extractable: true });

/**
 * The RFC 7638 thumbprint of the key sealed to. Carried as the JWE's kid, so that
 * after a rotation a reader can tell "sealed to a key I do not hold" from "corrupt".
 */
export const notificationKeyId = await calculateJwkThumbprint(await exportJWK(notificationKey));

export type AuthEmailKind = 'PASSWORD_RESET';

export async function sealEmailRequest(params: {
  messageId: string;
  tenantId: string;
  userId: string;
  email: string;
  link: string;
  kind: AuthEmailKind;
  expiresAt: Date;
}): Promise<string> {
  const signed = await signEmailRequest({
    subject: params.userId,
    jti: params.messageId,
    expiresAt: params.expiresAt,
    claims: { tenantId: params.tenantId, email: params.email, link: params.link, kind: params.kind },
  });

  return new CompactEncrypt(new TextEncoder().encode(signed))
    .setProtectedHeader({ alg: KEY_ALG, enc: CONTENT_ALG, cty: 'JWT', kid: notificationKeyId })
    .encrypt(notificationKey);
}
