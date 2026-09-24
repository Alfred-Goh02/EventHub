import { Prisma } from './generated/prisma/client.js';

/**
 * ONE FORMULA, IN ONE PLACE.
 *
 * A score and the counters it derives from drifting apart is a bug nobody notices
 * until the feed order looks wrong, so insert, every update and moderation's recount
 * all build it from here. Additive and time-stable: a post's score changes only when
 * someone acts on it, which is what lets an index answer a ranked list. A decay score
 * has to be recomputed for every row before it can be ordered.
 *
 * The counters are passed in because the SET expressions of an UPDATE all see the
 * PRE-update row: the caller adding a reaction passes `"reactionCount" + 1`, or the
 * score lands one behind for ever.
 *
 * EVERY INTERPOLATED FRAGMENT IS PARENTHESISED, and that is not style.
 *
 * Without the brackets these arguments are pasted in as text and Postgres applies its
 * own precedence. The caller adding a comment passes `"commentCount" + 1`, and
 * `2 * ${comments}` became `2 * "commentCount" + 1` — so a comment was worth 1 instead
 * of 2, and the FIRST comment moved the score not at all: greatest(0 + 1, 1) and
 * greatest(0, 1) are both 1.
 *
 * It hid for a whole part because the verifier recomputed the formula from the stored
 * columns and compared, which agreed with itself, and because the reaction path
 * happens to be unaffected — `rc + 1 + 2 * cc` parses the same either way. It was
 * found by asking a different question: did the number I am about to compare actually
 * move?
 *
 * Moved here from routes/posts.ts when moderation needed it too: a second copy in the
 * moderation route is exactly the drift this comment exists to prevent.
 */
export const rankScore = (reactions: Prisma.Sql, comments: Prisma.Sql, createdAt: Prisma.Sql) =>
  Prisma.sql`ln(greatest((${reactions}) + 2 * (${comments}), 1) + 1) + extract(epoch from ${createdAt}) / 45000.0`;
