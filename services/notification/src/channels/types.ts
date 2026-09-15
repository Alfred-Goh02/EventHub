/**
 * How the dispatcher sends an email, without knowing how it is sent.
 *
 * One implementation today: SMTP, which reaches Mailpit locally. The Field Guide's
 * production path is SES, and it would be a second implementation of this, not a
 * change to the consumer.
 */

export interface OutgoingEmail {
  to: string;
  subject: string;
  text: string;
  html: string;
  headers: Record<string, string>;
}

export interface SendResult {
  /** What the provider calls the message, when it says. Recorded on the delivery row. */
  providerMessageId: string | null;
}

/**
 * The provider refused this message for a reason repeating will not fix, such as an
 * address it rejects outright. Recorded as FAILED, and the partition moves on.
 *
 * Anything else a send throws is taken as transient: the offset is not committed
 * and the message comes back. Getting that wrong one way stalls the partition
 * forever on one bad address; the other way, it drops mail during an outage.
 */
export class PermanentSendError extends Error {
  override name = 'PermanentSendError';
}

export interface EmailChannel {
  send(email: OutgoingEmail): Promise<SendResult>;
  /** Asks the provider whether it will accept mail from this process, for /ready. */
  verify(): Promise<void>;
}
