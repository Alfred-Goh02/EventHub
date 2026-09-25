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
 */
export function sortableId(): string {
  const at = Date.now().toString(36).padStart(9, '0');
  return `c${at}${randomBytes(8).toString('hex')}`;
}
