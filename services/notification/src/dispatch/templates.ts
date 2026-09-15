/**
 * What a password-reset email says.
 *
 * Plain text and simple HTML, rendered here rather than through a template engine.
 * One email does not justify MJML, which the Field Guide proposes, and every
 * dependency that renders HTML is one more place for an unescaped value.
 *
 * The link is escaped wherever it lands in HTML. It comes from a claim Auth signed,
 * so it is not a hostile input today, but escaping is cheaper than deciding, for
 * each future field, whether it can be trusted.
 */

export interface RenderedEmail {
  subject: string;
  text: string;
  html: string;
}

const HTML_ESCAPES: Record<string, string> = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
const escapeHtml = (value: string): string => value.replace(/[&<>"']/g, (c) => HTML_ESCAPES[c]!);

export function renderPasswordReset(params: { link: string; expiresAt: Date }): RenderedEmail {
  const until = `${params.expiresAt.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
  const text = [
    'Someone asked to reset the password for your EventHub account.',
    '',
    'To choose a new password, open this link:',
    params.link,
    '',
    `It works once, and stops working at ${until}.`,
    '',
    'If you did not ask for this, you can ignore this email. Your password has not changed.',
  ].join('\n');

  const link = escapeHtml(params.link);
  const html = [
    '<p>Someone asked to reset the password for your EventHub account.</p>',
    `<p><a href="${link}">Choose a new password</a></p>`,
    `<p>It works once, and stops working at ${escapeHtml(until)}.</p>`,
    `<p>If the button does not work, copy this address into your browser:<br>${link}</p>`,
    '<p>If you did not ask for this, you can ignore this email. Your password has not changed.</p>',
  ].join('\n');

  return { subject: 'Reset your EventHub password', text, html };
}
