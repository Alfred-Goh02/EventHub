/**
 * Who wrote something, as the page can say it. Community stores user ids, not names, so
 * the reader's own words are "You" and everyone else's are a member's.
 */
export function authorLabel(authorId: string | null, me: string | null): string {
  if (authorId !== null && me !== null && authorId === me) return "You";
  return "A member";
}

/** What a refused reply means to the person who wrote it. Undefined: the server's words. */
export function commentError(status: number, retry: string | null): string | undefined {
  if (status === 429)
    return `You're replying faster than this space allows. ${retry ?? "Wait a moment and try again."}`;
  if (status === 422) return "This thread is as deep as it goes. Reply further up instead.";
  if (status === 404) return "What you replied to has been removed.";
  if (status === 403) return "You can't reply in this space.";
  return undefined;
}
