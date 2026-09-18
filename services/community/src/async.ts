import type { NextFunction, Request, RequestHandler, Response } from 'express';

/**
 * Hands a rejected async handler to Express.
 *
 * Express 4 does not await a handler, so a promise that rejects becomes an UNHANDLED
 * REJECTION and takes the process down — it never reaches the error middleware. This
 * was not theoretical here: a depth-cap violation from Postgres, deliberately left
 * unmapped during a mutation run, killed the service mid-request instead of answering
 * 500, and the verifier reported "fetch failed" rather than a status code.
 *
 * Express 5 handles it natively. This project is pinned to 4 to match the shared
 * middleware's types.
 *
 * THE THIRD COPY, after services/auth/src/async.ts and services/ticketing/src/async.ts,
 * whose note says the third service is the point to extract it into a package. That is
 * the next change; it touches three services and belongs on its own rather than inside
 * the one that happened to need it.
 */
export const ah =
  (fn: (req: Request, res: Response, next: NextFunction) => Promise<unknown>): RequestHandler =>
  (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
