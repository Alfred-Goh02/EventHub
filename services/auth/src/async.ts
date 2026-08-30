import type { NextFunction, Request, RequestHandler, Response } from 'express';

/**
 * Forwards a rejected async handler to Express's error middleware.
 *
 * Express 4 predates async/await. It calls your handler and ignores the
 * returned promise, so an `async` handler that throws produces an UNHANDLED
 * REJECTION — which on Node 15 and later terminates the process by default.
 * One transient database error on any unguarded route takes the whole auth
 * service down, and with it every login and every token refresh.
 *
 * This service ran with eight unwrapped async handlers and no error middleware
 * at all. Two of them caught their own errors; the other six did not, so
 * anything Prisma threw on /invites, /login, /me, /refresh, /logout or
 * /forgot-password was fatal.
 *
 * Express 5 handles this natively. This project is pinned to 4 to match the
 * shared middleware's types.
 *
 * NOTE: this is a deliberate second copy of services/ticketing/src/async.ts.
 * Four lines with no parameters to get wrong is a weaker drift risk than the
 * import graph a shared package would add for it. If a third service needs it,
 * that is the point to extract it.
 */
export const ah =
  (fn: (req: Request, res: Response, next: NextFunction) => Promise<unknown>): RequestHandler =>
  (req, res, next) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
