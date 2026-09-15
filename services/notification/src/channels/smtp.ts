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
        const smtp = err as { responseCode?: number; command?: string; message?: string };
        if (typeof smtp.responseCode === 'number' && smtp.responseCode >= 500 && smtp.command === 'RCPT TO') {
          throw new PermanentSendError(`the server refused the recipient (${smtp.responseCode})`);
        }
        throw err;
      }
    },

    async verify() {
      await transport.verify();
    },
  };
}
