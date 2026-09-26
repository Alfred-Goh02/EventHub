/**
 * Preloaded into a service a verifier starts (NODE_OPTIONS=--import=<this file>), to
 * run it with a clock an hour FAST.
 *
 * A check that a timestamp came from the database cannot fail on a machine whose
 * clocks agree: a value stamped by the process lands on the same millisecond, and the
 * check passes whichever clock was used. Measured — the contact-card verifier stayed
 * green with the card stamped by `new Date()`. Skewing the process's clock is the
 * fixture that makes the two clocks different, which is the failure being guarded
 * against: an app server whose clock disagrees with the database's.
 *
 * Only the clock moves. `new Date(x)` with an argument, and every other Date method,
 * behave as before.
 */
const SKEW_MS = 3_600_000;
const RealDate = Date;

class SkewedDate extends RealDate {
  constructor(...args) {
    if (args.length === 0) super(RealDate.now() + SKEW_MS);
    else super(...args);
  }
  static now() {
    return RealDate.now() + SKEW_MS;
  }
}

globalThis.Date = SkewedDate;
// On stderr, so a verifier can assert the skew is really in force.
process.stderr.write(`clock skewed by +${SKEW_MS / 1000}s for this process\n`);
