import { randomBytes } from 'node:crypto';

/**
 * An id that is safe as an ltree label AND sorts by time.
 *
 * TWO CONSTRAINTS, both from the same column. A comment's id becomes a label in its
 * materialised path:
 *
 *   - ltree labels are alphanumerics and underscores. A UUID's hyphens are not
 *     allowed, and the insert fails with a syntax error at the path expression, which
 *     reads like a bug in the SQL rather than in the id.
 *   - siblings are ordered by comparing labels, so an id that is random all the way
 *     through shuffles replies. Hence the timestamp prefix, and hence cuid() rather
 *     than cuid2 in the schema: cuid2 is deliberately unsortable.
 *
 * Base36 milliseconds, padded to a fixed width so the prefixes compare correctly as
 * strings, then random bytes for uniqueness within a millisecond.
 *
 * THE MILLISECONDS ARE THE DATABASE'S, passed in by the caller: the same now() the
 * row's createdAt is stamped from, in the same transaction. They were `Date.now()`,
 * this process's clock, so a thread's order was whatever each replica believed the
 * time was. Measured with one replica an hour fast and one not: a reply written first
 * sorted last, under createdAt values that said the opposite.
 */
export function sortableId(atMs: number): string {
  if (!Number.isSafeInteger(atMs) || atMs < 0) throw new Error(`not a time in milliseconds: ${atMs}`);
  const at = atMs.toString(36).padStart(9, '0');
  return `c${at}${randomBytes(8).toString('hex')}`;
}
