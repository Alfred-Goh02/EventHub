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
 *
 * AN INVITATION TRAVELS THE SAME WAY, since 2026-10-11. Until then POST /invites
 * handed its link to the dev sink and nothing else, a leftover from before
 * Notification existed: the sink is off unless configured and refuses to boot in
 * production, so in a deployed system nobody new could ever join a tenant. An
 * accept-invite link creates an account at the role it names, so it is as much a
 * credential as a reset link, and gets the same two layers.
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
        'A password reset or an invitation could be issued and never delivered. Generate it with ' +
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

/**
 * Which email. Each is a credential, so each travels sealed.
 *
 *   PASSWORD_RESET  for an existing user; the subject is the User's id.
 *   INVITATION      for somebody who is not a user yet, and may never become one; the
 *                   subject is the Invite's id. Nothing on Notification's side may
 *                   look a subject up as a user: contact cards, preferences and the
 *                   digest are all keyed by user, and an invitee has none of them.
 */
export type AuthEmailKind = 'PASSWORD_RESET' | 'INVITATION';

export async function sealEmailRequest(params: {
  messageId: string;
  tenantId: string;
  /** Whom the email is about: a User for a reset, an Invite for an invitation. */
  subject: string;
  email: string;
  link: string;
  kind: AuthEmailKind;
  /** When the link stops working. The seal expires with it, never after it. */
  expiresAt: Date;
  /** Anything else a kind needs signed: an invitation's role. */
  extra?: Record<string, string>;
}): Promise<string> {
  const signed = await signEmailRequest({
    subject: params.subject,
    jti: params.messageId,
    expiresAt: params.expiresAt,
    // The fixed claims last, so nothing in `extra` can replace the address, the link,
    // the tenant or the kind.
    claims: { ...params.extra, tenantId: params.tenantId, email: params.email, link: params.link, kind: params.kind },
  });

  return sealToNotification(signed);
}

/**
 * Encrypts a JWS Auth has signed to Notification's key, with the algorithms pinned
 * above. The one place that seals anything for Notification, so a contact card and
 * an email request cannot drift onto different algorithms.
 */
export function sealToNotification(signed: string): Promise<string> {
  return new CompactEncrypt(new TextEncoder().encode(signed))
    .setProtectedHeader({ alg: KEY_ALG, enc: CONTENT_ALG, cty: 'JWT', kid: notificationKeyId })
    .encrypt(notificationKey);
}
