/**
 * What each email says.
 *
 * Plain text and simple HTML, rendered here rather than through a template engine.
 * A handful of emails does not justify MJML, which the Field Guide proposes, and every
 * dependency that renders HTML is one more place for an unescaped value.
 *
 * The link is escaped wherever it lands in HTML. It comes from a claim Auth signed,
 * so it is not a hostile input today, but escaping is cheaper than deciding, for
 * each future field, whether it can be trusted.
 */

import type { Role } from '@eventhub/types';

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

/**
 * How the email names each role an invitation can grant.
 *
 * Keyed by @eventhub/types' Role, the list every service checks a token's role against
 * and verify-shared-types holds to Auth's own enum. A Record over that type must name
 * every member, so a role added there does not build here until it has words: the
 * alternative was a hand-copied list nothing compared with anything.
 */
const ROLE_PHRASES: Record<Role, string> = {
  ADMIN: 'an administrator',
  ORGANIZER: 'an organiser',
  ATTENDEE: 'an attendee',
};

/**
 * You have been asked to join.
 *
 * THE LINK AND THE ROLE, AND NOTHING ELSE. Not who sent it and not the organisation's
 * name: Notification knows neither, and each would be text somebody other than this
 * platform chose, in an email that asks the reader to create an account -- the shape
 * of every phishing message. Not the address either, which the reader already knows.
 *
 * No unsubscribe link and no List-Unsubscribe header: an invitation is not a
 * subscription, and the reader has no account to hold a preference. Ignoring it is the
 * way out, and the email says so.
 */
export function renderInvitation(params: {
  link: string;
  role: Role;
  expiresAt: Date;
}): RenderedEmail {
  const until = `${params.expiresAt.toISOString().slice(0, 16).replace('T', ' ')} UTC`;
  const as = ROLE_PHRASES[params.role];
  // Two invitations to one address travel on different partitions and can arrive in
  // either order. Only the newest link works, so the email says so rather than leave
  // the reader to try the one that happened to arrive last.
  const replaces = 'It replaces any earlier invitation sent to this address: only the newest link works.';
  const text = [
    `You have been invited to join EventHub as ${as}.`,
    '',
    'To accept, open this link and choose a password:',
    params.link,
    '',
    `It works once, and stops working at ${until}. ${replaces}`,
    '',
    'If you were not expecting this, you can ignore this email. No account is created unless you open the link.',
  ].join('\n');

  const link = escapeHtml(params.link);
  const html = [
    `<p>You have been invited to join EventHub as ${escapeHtml(as)}.</p>`,
    `<p><a href="${link}">Accept the invitation</a></p>`,
    `<p>It works once, and stops working at ${escapeHtml(until)}. ${escapeHtml(replaces)}</p>`,
    `<p>If the button does not work, copy this address into your browser:<br>${link}</p>`,
    '<p>If you were not expecting this, you can ignore this email. No account is created unless you open the link.</p>',
  ].join('\n');

  return { subject: 'You have been invited to EventHub', text, html };
}

/** The most of a reply an email quotes. The rest is one click away. */
const EXCERPT_CHARS = 280;

/** In place of a post link this email has no host for: see renderCommunityReply. */
const NO_LINK_ONE = 'Sign in to EventHub to read the conversation.';
const NO_LINK_MANY = 'Sign in to EventHub to read them.';

/**
 * Collapsed whitespace, cut at a CODE POINT rather than a UTF-16 unit, so an emoji at
 * the boundary is dropped whole rather than split into a lone surrogate — which some
 * mail clients render as garbage and some SMTP relays refuse.
 */
function excerpt(body: string): string {
  const flat = body.replace(/\s+/g, ' ').trim();
  const points = [...flat];
  return points.length <= EXCERPT_CHARS ? flat : `${points.slice(0, EXCERPT_CHARS).join('').trimEnd()}…`;
}

/**
 * Someone answered what you wrote.
 *
 * THE REPLY IS SOMEONE ELSE'S TEXT, and so hostile input: it is escaped wherever it
 * lands in HTML, and only an excerpt is quoted. No name is shown, because Notification
 * knows user ids and nobody's name — and the reader will see who wrote it on the page.
 *
 * The unsubscribe link is in the body as well as the List-Unsubscribe header: not every
 * client shows the header's button, and the law in more than one place wants it where
 * the reader can see it.
 *
 * NO POST LINK when there is no host to put it on (postUrl null): the recipient's card
 * names no subdomain the gateway routes. The email says where to read the conversation
 * instead of linking somewhere that cannot open it.
 */
export function renderCommunityReply(params: {
  /** True for a reply to a comment; false for a comment on a post. */
  toComment: boolean;
  body: string;
  postUrl: string | null;
  unsubscribeUrl: string;
}): RenderedEmail {
  const what = params.toComment ? 'your comment' : 'your post';
  const quoted = excerpt(params.body);
  const text = [
    `Someone replied to ${what}:`,
    '',
    `  ${quoted}`,
    '',
    ...(params.postUrl ? ['Read the conversation:', params.postUrl] : [NO_LINK_ONE]),
    '',
    'You get these because replies to you are on. To stop them:',
    params.unsubscribeUrl,
  ].join('\n');

  const html = [
    `<p>Someone replied to ${what}:</p>`,
    `<blockquote>${escapeHtml(quoted)}</blockquote>`,
    params.postUrl ? `<p><a href="${escapeHtml(params.postUrl)}">Read the conversation</a></p>` : `<p>${escapeHtml(NO_LINK_ONE)}</p>`,
    `<p style="color:#666;font-size:12px">You get these because replies to you are on. ` +
      `<a href="${escapeHtml(params.unsubscribeUrl)}">Stop emails about replies</a>.</p>`,
  ].join('\n');

  return { subject: params.toComment ? 'Someone replied to your comment' : 'Someone commented on your post', text, html };
}

/**
 * The replies held back by the hourly limit, as one email.
 *
 * COUNTS AND LINKS ONLY, decided 2026-09-27: no reply's words are stored while it
 * waits, so there are none to quote — which also means a reply deleted or hidden in
 * the meantime is never shown here. Each line is one post: how many replies there were,
 * to a comment of yours or to the post itself, and a link to read them -- or, with no
 * host to link on (postUrl null, as for a reply), the counts and where to read them.
 */
export function renderReplyDigest(params: {
  groups: Array<{ postUrl: string | null; toComment: boolean; count: number }>;
  unsubscribeUrl: string;
}): RenderedEmail {
  const total = params.groups.reduce((sum, g) => sum + g.count, 0);
  const line = (g: { toComment: boolean; count: number }) =>
    g.toComment
      ? `${g.count} ${g.count === 1 ? 'reply' : 'replies'} to your comment`
      : `${g.count} ${g.count === 1 ? 'comment' : 'comments'} on your post`;
  const subject = `${total} more ${total === 1 ? 'reply' : 'replies'} to you`;
  const unlinked = params.groups.some((g) => !g.postUrl);

  const text = [
    `While you were away, people kept talking. ${subject}:`,
    '',
    ...params.groups.flatMap((g) => (g.postUrl ? [`  ${line(g)}:`, `  ${g.postUrl}`, ''] : [`  ${line(g)}`, ''])),
    ...(unlinked ? [NO_LINK_MANY, ''] : []),
    'You get these because replies to you are on. To stop them:',
    params.unsubscribeUrl,
  ].join('\n');

  const html = [
    `<p>While you were away, people kept talking. ${escapeHtml(subject)}:</p>`,
    '<ul>',
    ...params.groups.map((g) => (g.postUrl ? `<li><a href="${escapeHtml(g.postUrl)}">${escapeHtml(line(g))}</a></li>` : `<li>${escapeHtml(line(g))}</li>`)),
    '</ul>',
    ...(unlinked ? [`<p>${escapeHtml(NO_LINK_MANY)}</p>`] : []),
    `<p style="color:#666;font-size:12px">You get these because replies to you are on. ` +
      `<a href="${escapeHtml(params.unsubscribeUrl)}">Stop emails about replies</a>.</p>`,
  ].join('\n');

  return { subject: `${subject[0]!.toUpperCase()}${subject.slice(1)}`, text, html };
}
