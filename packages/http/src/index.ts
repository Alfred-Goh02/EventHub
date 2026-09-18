import type { NextFunction, Request, RequestHandler, Response } from 'express';

/**
 * Hands a rejected async handler to Express.
 *
 * Express 4 DOES NOT AWAIT A HANDLER, so a promise that rejects becomes an unhandled
 * rejection and takes the process down — it never reaches the error middleware. That is
 * not theoretical here: a depth-cap violation from Postgres, left unmapped during a
 * mutation run in Community, killed the service mid-request instead of answering 500,
 * and the verifier reported "fetch failed" rather than a status code.
 *
 * Two things have to be true for an error to become a 500, and the same run found both
 * of them false:
 *
 *   1. every async handler is wrapped in this;
 *   2. the error middleware is registered AFTER the routes — Express matches middleware
 *      in order, so one mounted before them never sees their errors.
 *
 * Express 5 handles the first natively. This repository is pinned to 4 to match the
 * shared middleware's types.
 *
 * THERE WERE FOUR IDENTICAL COPIES: auth, event, ticketing and community. Each one's
 * note said the next service would be the point to extract it, and the count was wrong
 * every time — the community copy called itself the third. This is that extraction; all
 * four are gone.
 *
 * The services that solve the same problem another way are deliberately left alone, and
 * listed here so the next person does not have to rediscover them: Analytics wraps its
 * routes in its own `handle()`, which adds metrics and calls next(err); Search catches
 * inside each handler; Notification's two async handlers are `/metrics` and `/ready`,
 * which cannot reject because every await there carries its own catch. Live has no
 * async handlers at all.
 */
export const ah =
  (fn: (req: Request, res: Response, next: NextFunction) => Promise<unknown>): RequestHandler =>
  (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
