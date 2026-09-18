import { createHash } from 'node:crypto';

/**
 * How the suppression list names an address: trimmed, lower-cased, then SHA-256.
 *
 * NORMALISED ON BOTH SIDES. Auth lower-cases an address when it is entered, and this
 * does it again for the write and the lookup alike, so a difference in case can never
 * slip a suppressed address past the check.
 *
 * A PSEUDONYM, NOT A SECRET. This database holds no addresses, and the hash keeps them
 * out of its dumps, its logs and casual reads. It does not stop someone with read
 * access who already has a candidate address from hashing it and looking. A keyed
 * hash would, at the cost of one more secret to distribute, for a database in which
 * an address unlocks nothing else.
 *
 * No database import here, so a script can use it without the service's configuration.
 */
export const addressHash = (address: string): string =>
  createHash('sha256').update(address.trim().toLowerCase()).digest('hex');
