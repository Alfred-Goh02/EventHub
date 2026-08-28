/**
 * A fixed-window counter, in memory.
 *
 * WHY THIS EXISTS HERE AND NOT AT A GATEWAY. The plan was to defer rate limiting
 * to Kong. Kong is not in this repository — not in docker-compose.yml, not in
 * CI, and there is no rate-limiting middleware anywhere in any service. So
 * "handled at the gateway" was an assumption about infrastructure that no file
 * here describes, and shipping on it would have meant shipping none.
 *
 * /forgot-password is the first route where an ANONYMOUS caller both writes a
 * row and (once Chapter 8 wires real mail) causes an outbound message to an
 * address they chose. Unthrottled, that is a mail-bombing amplifier pointed at
 * anyone, plus unbounded rows in a table nothing prunes. Guessing the token is
 * not the threat — 256 bits sees to that — the unbounded issuance is.
 *
 * WHAT THIS IS NOT. It is per-process memory. It resets on restart and does not
 * survive horizontal scaling: two instances behind a load balancer give an
 * attacker twice the budget. It belongs in Redis, or in the gateway that does
 * not exist yet. It is here because a limit that is honest about being weak
 * beats a limit that is only mentioned in a comment.
 *
 * Fixed window rather than sliding: at a boundary a caller can burst twice the
 * limit. For "5 password reset emails an hour" that is 10 in the worst case,
 * which is still a bounded nuisance rather than an amplifier, and it costs one
 * map and no timers.
 */

export interface RateLimiter {
  /** Records a hit. Returns false when the caller is over budget. */
  hit(key: string): boolean;
}

export function createRateLimiter(opts: { limit: number; windowMs: number }): RateLimiter {
  const counters = new Map<string, { count: number; resetAt: number }>();

  return {
    hit(key) {
      const now = Date.now();
      const existing = counters.get(key);

      if (!existing || existing.resetAt <= now) {
        counters.set(key, { count: 1, resetAt: now + opts.windowMs });

        // Sweep expired entries opportunistically rather than on a timer. An
        // unbounded map keyed by attacker-supplied input is itself a memory
        // exhaustion path, and a setInterval would keep the process alive and
        // have to be unref'd and torn down.
        if (counters.size > 10_000) {
          for (const [k, v] of counters) {
            if (v.resetAt <= now) counters.delete(k);
          }
        }

        return true;
      }

      existing.count += 1;
      return existing.count <= opts.limit;
    },
  };
}
