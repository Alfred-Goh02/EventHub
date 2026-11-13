import { useEffect, useState } from "react";

/** Auth's own floor: POST /register and /reset-password refuse anything shorter. */
export const MIN_PASSWORD_LENGTH = 8;

/** What is wrong with a new password and its confirmation, or null when nothing is. */
export function passwordProblem(password: string, confirm: string): string | null {
  if (password.length < MIN_PASSWORD_LENGTH)
    return `Use at least ${MIN_PASSWORD_LENGTH} characters.`;
  if (password !== confirm) return "The two passwords don't match.";
  return null;
}

/**
 * The token a link from an email carries, kept by the page and taken out of the address bar.
 *
 * A reset or invitation token is a credential until it is spent. Left in the URL it stays
 * in the history (and in whatever syncs it), in a screen share, and in the Referer of
 * whatever the page fetches next. The page reads it once, holds it in memory, and calls
 * `clear` to replace the URL with one that has no token. A reload after that has nothing
 * to read, so the page asks for the link again.
 */
export function useLinkToken(fromUrl: string | undefined, clear: () => void): string | null {
  const [token] = useState(() => fromUrl ?? null);
  useEffect(() => {
    if (fromUrl) clear();
  }, [fromUrl, clear]);
  return token;
}

/** The search a link page accepts: the token alone, and only as a string. */
export const linkSearch = (s: Record<string, unknown>): { token?: string } =>
  typeof s["token"] === "string" && s["token"] ? { token: s["token"] } : {};
