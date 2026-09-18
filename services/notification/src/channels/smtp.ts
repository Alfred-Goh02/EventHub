import { createTransport } from 'nodemailer';
import { PermanentSendError, type EmailChannel } from './types.js';

/**
 * Email over SMTP, which is Mailpit locally.
 *
 * WHAT COUNTS AS PERMANENT IS NARROW ON PURPOSE. Only a 5xx reply to RCPT TO, the
 * server refusing this particular recipient, is taken as the message's fault.
 * Other 5xx replies can mean the server itself is misconfigured: authentication
 * required, a relay refused, a transaction failed. Calling those permanent would
 * mark every email FAILED during a configuration mistake and send none of them
 * again. Taken as transient, they redeliver, lag grows, and /ready says so.
 */
/**
 * Enhanced status codes (RFC 3463) that say the ADDRESS is bad, rather than that this
 * message was refused: 5.1.1 no such mailbox, 5.1.2 no such domain, 5.1.3 a malformed
 * address, 5.1.6 the mailbox has moved, 5.1.10 the domain accepts no mail (a null MX,
 * RFC 7505).
 *
 * ONLY THESE SUPPRESS. A 5.7.x is a policy decision, often a spam filter, and can be
 * lifted tomorrow; a 550 with no enhanced code says too little to tell the two apart.
 * A wrong suppression cuts a user off from password resets until someone lifts it, so
 * a refusal that does not prove the address is gone is recorded FAILED and no more.
 */
const ADDRESS_DOES_NOT_EXIST = new Set(['5.1.1', '5.1.2', '5.1.3', '5.1.6', '5.1.10']);

/** The enhanced status code at the start of an SMTP reply, when its class agrees with the reply code's. */
export function enhancedStatus(reply: string | undefined): string | null {
  const match = reply?.match(/^([245])\d\d[ -]([245])\.(\d{1,3})\.(\d{1,3})(?=\s|$)/);
  return match && match[1] === match[2] ? `${match[2]}.${match[3]}.${match[4]}` : null;
}

/**
 * Replaces anything shaped like an address in provider text.
 *
 * Real servers repeat the recipient in their replies ("550 5.1.1 <alice@example.com>:
 * Recipient address rejected"), and a send error's text travels a long way from here:
 * into the logs, the sent-log's lastError, and the retry and dead-letter headers on
 * Kafka. None of those may hold an address.
 *
 * THE APOSTROPHE IS PART OF THE LOCAL PART, not a delimiter. Leaving it out of the
 * left-hand class replaced only the tail of mary.o'connor@corp.example and published
 * the name. The other delimiters stay out on both sides, so a reply that quotes or
 * brackets the address still loses all of it.
 */
export function redactAddresses(text: string): string {
  return text.replace(/[^\s<>()[\]",;:]+@[^\s<>()[\]"',;:]+/g, '[address]');
}

export function createSmtpChannel(opts: { host: string; port: number; from: string }): EmailChannel {
  const transport = createTransport({
    host: opts.host,
    port: opts.port,
    // Mailpit speaks plain SMTP. A real provider would need TLS and credentials.
    secure: false,
    // Bounded, so a server that accepts the connection and then stalls cannot hold
    // a partition for the library's default of minutes.
    connectionTimeout: 10_000,
    greetingTimeout: 10_000,
    socketTimeout: 30_000,
  });

  return {
    async send(email) {
      try {
        const info = await transport.sendMail({
          from: opts.from,
          to: email.to,
          subject: email.subject,
          text: email.text,
          html: email.html,
          headers: email.headers,
        });
        return { providerMessageId: typeof info.messageId === 'string' ? info.messageId : null };
      } catch (err) {
        const smtp = err as { responseCode?: number; command?: string; response?: string };
        if (typeof smtp.responseCode === 'number' && smtp.responseCode >= 500 && smtp.command === 'RCPT TO') {
          const status = enhancedStatus(smtp.response);
          throw new PermanentSendError(
            `the server refused the recipient (${smtp.responseCode}${status ? ` ${status}` : ''})`,
            status !== null && ADDRESS_DOES_NOT_EXIST.has(status),
            status,
          );
        }
        if (err instanceof Error) err.message = redactAddresses(err.message);
        throw err;
      }
    },

    async verify() {
      await transport.verify();
    },
  };
}
