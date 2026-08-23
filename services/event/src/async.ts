import type { NextFunction, Request, RequestHandler, Response } from 'express';

/**
 * Forwards a rejected async handler to Express's error middleware.
 *
 * Express 4 predates async/await. It calls your handler and ignores the
 * returned promise, so an `async` handler that throws produces an unhandled
 * rejection and a request that never responds — the client hangs until it times
 * out, and the error middleware never runs. Express 5 handles this natively;
 * this project is pinned to 4 to match the shared middleware's types.
 *
 * Every async route handler must be wrapped in this. The failure mode of
 * forgetting is a hung request under an error path you probably are not
 * testing, which is a bad way to find out.
 */
export const ah =
  (fn: (req: Request, res: Response, next: NextFunction) => Promise<unknown>): RequestHandler =>
  (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
