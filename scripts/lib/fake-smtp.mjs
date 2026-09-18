import net from 'node:net';

/**
 * A minimal SMTP server for verifiers, which can be told to fail.
 *
 * Mailpit accepts everything, which is right for a local inbox and useless for
 * proving what happens when a send fails. This speaks just enough of RFC 5321
 * for nodemailer (greeting, EHLO, MAIL FROM, RCPT TO, DATA, RSET, NOOP, QUIT; no
 * TLS, no AUTH), records every message it accepts, and can:
 *   - be stopped, so connections are refused, the way an outage looks;
 *   - reply to RCPT TO for one address with a code the caller chooses, such as
 *     451 (try again later) or 550 (no such mailbox). `{address}` in a reply is
 *     replaced by the recipient, because real servers repeat it back, and that is
 *     text which must never reach a log.
 */
export function createFakeSmtp() {
  /** @type {Array<{ from: string, to: string[], data: string, at: number }>} */
  const accepted = [];
  /** Lower-cased address -> the full reply line to send for its RCPT TO. */
  const rules = new Map();
  /**
   * Every RCPT TO the server was sent, refused or not. `accepted` alone cannot tell
   * an address that was never tried from one that was tried and refused.
   */
  const rcpts = [];
  const sockets = new Set();
  let server = null;

  const address = (line) => (line.match(/<([^>]*)>/)?.[1] ?? '').trim();

  function session(socket) {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => {});

    let buffer = '';
    let inData = false;
    let envelope = { from: '', to: [] };
    let data = [];
    const reply = (line) => socket.write(`${line}\r\n`);

    reply('220 fake-smtp ready');
    socket.on('data', (chunk) => {
      buffer += chunk.toString('utf8');
      let end;
      while ((end = buffer.indexOf('\r\n')) !== -1) {
        const line = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);

        if (inData) {
          if (line === '.') {
            inData = false;
            accepted.push({ from: envelope.from, to: envelope.to, data: data.join('\r\n'), at: Date.now() });
            envelope = { from: '', to: [] };
            data = [];
            reply('250 2.0.0 queued');
          } else {
            data.push(line.startsWith('..') ? line.slice(1) : line);
          }
          continue;
        }

        const verb = line.slice(0, 4).toUpperCase();
        if (verb === 'EHLO') socket.write('250-fake-smtp\r\n250 8BITMIME\r\n');
        else if (verb === 'HELO') reply('250 fake-smtp');
        else if (verb === 'MAIL') {
          envelope = { from: address(line), to: [] };
          reply('250 2.1.0 ok');
        } else if (verb === 'RCPT') {
          const to = address(line);
          rcpts.push({ to, at: Date.now() });
          const rule = rules.get(to.toLowerCase());
          if (rule) reply(rule.replaceAll('{address}', to));
          else {
            envelope.to.push(to);
            reply('250 2.1.5 ok');
          }
        } else if (verb === 'DATA') {
          if (envelope.to.length === 0) reply('503 5.5.1 no valid recipients');
          else {
            inData = true;
            reply('354 end data with <CR><LF>.<CR><LF>');
          }
        } else if (verb === 'RSET') {
          envelope = { from: '', to: [] };
          reply('250 2.0.0 ok');
        } else if (verb === 'NOOP') reply('250 2.0.0 ok');
        else if (verb === 'QUIT') {
          reply('221 2.0.0 bye');
          socket.end();
        } else reply('502 5.5.2 not implemented');
      }
    });
  }

  return {
    accepted,
    rules,
    /** Messages accepted for one address. */
    to: (addr) => accepted.filter((m) => m.to.some((t) => t.toLowerCase() === addr.toLowerCase())),
    /** Every RCPT TO naming one address, whether it was accepted or refused. */
    tried: (addr) => rcpts.filter((r) => r.to.toLowerCase() === addr.toLowerCase()),
    get listening() {
      return server !== null;
    },
    async start(port) {
      if (server) return;
      const s = net.createServer(session);
      await new Promise((resolve, reject) => {
        s.once('error', reject);
        s.listen(port, '127.0.0.1', resolve);
      });
      server = s;
    },
    /** Refuses new connections and cuts the open ones, as an outage would. */
    async stop() {
      if (!server) return;
      const s = server;
      server = null;
      for (const socket of sockets) socket.destroy();
      await new Promise((resolve) => s.close(() => resolve()));
    },
  };
}
