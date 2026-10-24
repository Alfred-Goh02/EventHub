/**
 * Every kind of message this service sends, and whether a user may turn it off.
 *
 * A CLOSED SET, because a category is part of the preference key: an open one would
 * accept a typo as a new category, record the user's opt-out under it, and keep
 * sending them the real one.
 *
 * TRANSACTIONAL categories are part of the service the user asked for and cannot be
 * turned off: a password reset is how someone who is locked out gets back in. OPTIONAL
 * is the Field Guide's MARKETING, renamed because a reply to your own comment is not
 * marketing — it is simply something a person may choose not to receive.
 *
 * Decided 2026-09-26: community replies are ON by default, with a one-click
 * unsubscribe in every one.
 *
 * An INVITATION is transactional for a reason of its own: its reader has no account,
 * so there is nobody whose preference could be asked, and an invitation is not a
 * subscription to leave. It carries no unsubscribe link and no List-Unsubscribe
 * header; an unwanted one is answered by the budget on POST /invites in Auth and by
 * ignoring it.
 */
export const CATEGORIES = {
  PASSWORD_RESET: { kind: 'TRANSACTIONAL', defaultEnabled: true, label: 'Password resets' },
  INVITATION: { kind: 'TRANSACTIONAL', defaultEnabled: true, label: 'Invitations to join' },
  COMMUNITY_REPLY: { kind: 'OPTIONAL', defaultEnabled: true, label: 'Replies to your posts and comments' },
} as const;

export type Category = keyof typeof CATEGORIES;

/** One channel today. Keyed in the table all the same, so a second is data, not a migration. */
export const CHANNELS = ['EMAIL'] as const;
export type Channel = (typeof CHANNELS)[number];

export const isCategory = (value: unknown): value is Category =>
  typeof value === 'string' && Object.prototype.hasOwnProperty.call(CATEGORIES, value);

export const isOptional = (category: Category): boolean => CATEGORIES[category].kind === 'OPTIONAL';
