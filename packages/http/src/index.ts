import type { ErrorRequestHandler, NextFunction, Request, RequestHandler, Response } from 'express';

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

/**
 * What errorHandler needs from a logger. @eventhub/logger's pino logger is one; named
 * structurally so this package does not depend on that one.
 */
export interface ErrorLogger {
  error(obj: object, msg: string): void;
  info(obj: object, msg: string): void;
}

/**
 * The answer to each kind of request body that body-parser refuses, by its `type`.
 *
 * FIXED TEXT, NEVER THE ERROR'S MESSAGE. A JSON parse error's message quotes the body it
 * could not parse (`Unexpected token 'a', "abc" is not valid JSON`), so echoing it would
 * reflect whatever the client sent, and logging it would copy the client's body into the
 * log, password fields included.
 */
const CLIENT_ERRORS = new Map<string, string>([
  ['entity.parse.failed', 'malformed JSON body'],
  ['entity.too.large', 'request body too large'],
  ['charset.unsupported', 'unsupported charset'],
  ['encoding.unsupported', 'unsupported content encoding'],
]);

/**
 * The status to answer when the error says the CLIENT was at fault, otherwise null.
 *
 * Both conditions, as http-errors states them: a 4xx status, and `expose: true`, which is
 * how a library marks an error whose status it means the client to see. body-parser
 * raises exactly those for a body it cannot read: 400 for malformed JSON, 413 past the
 * size limit, 415 for an encoding or charset it does not know. `status` first, then
 * `statusCode`, the order Express's own final handler reads them in.
 *
 * ONE EXCEPTION, BY NAME: a URL Express cannot percent-decode. Express 4 raises a route
 * parameter like `/events/%E0%A4%A` as a URIError with status 400 and no `expose`, so it
 * fell through to the 500 below -- measured on Event before routing reached
 * requireAuth, so with no token at all. Only that exact shape is let through: a URIError
 * Express marked 400. Any other unexposed 4xx stays ours.
 */
function clientStatus(err: unknown): number | null {
  if (typeof err !== 'object' || err === null) return null;
  const { status, statusCode, expose } = err as { status?: unknown; statusCode?: unknown; expose?: unknown };
  const code = typeof status === 'number' ? status : statusCode;
  if (expose !== true) return isUndecodableUrl(err) ? 400 : null;
  return typeof code === 'number' && Number.isInteger(code) && code >= 400 && code <= 499 ? code : null;
}

/** Express 4's own error for a route parameter that is not valid percent-encoding. */
const isUndecodableUrl = (err: object): boolean => {
  const { status, statusCode } = err as { status?: unknown; statusCode?: unknown };
  return err instanceof URIError && (status === 400 || statusCode === 400);
};

/**
 * The error middleware every HTTP service mounts last.
 *
 * A REQUEST BODY THE CLIENT GOT WRONG IS THE CLIENT'S FAULT. Each service ended with its
 * own copy of a handler that mapped every error to 500 and logged it as "unhandled
 * error", and express.json() runs before any route, so `--data '{'` and a body past its
 * 100 kB limit answered 500 from all seven services that were measured: Auth's /login,
 * Event's /events, Ticketing's /reservations, Community's /spaces, Analytics, Search
 * and Live. The gateway admits bodies up to 1 MB, so any caller could produce a 500, and
 * an error-rate alert, on demand. Now those answer 400 and 413, with a fixed message,
 * and are logged at info rather than as a server fault.
 *
 * Everything else is unchanged: logged as "unhandled error" with the error, and 500
 * "internal error". An error that does not say it is the client's — no status, a 5xx,
 * or a 4xx without `expose` — is treated as ours, because a client error answered as a
 * 500 is noisy, while a server fault answered as a 400 is hidden.
 *
 * Once the headers have gone there is no status left to choose, so the error goes to
 * Express's default handler, which closes the connection.
 *
 * Express 4 identifies an error handler by its ARITY: four parameters. Drop `next` and
 * this silently becomes ordinary middleware that never runs on error, and every thrown
 * error turns into a hung request. It must also be mounted AFTER the routes, for the
 * reason `ah` gives above.
 */
export function errorHandler(logger: ErrorLogger): ErrorRequestHandler {
  return (err: unknown, req: Request, res: Response, next: NextFunction) => {
    if (res.headersSent) {
      next(err);
      return;
    }
    const status = clientStatus(err);
    if (status !== null) {
      // The URL's message quotes the URL, as a parse error's quotes the body: fixed text.
      const undecodable = isUndecodableUrl(err as object);
      const raw = (err as { type?: unknown }).type;
      const type = undecodable ? 'url.malformed' : typeof raw === 'string' ? raw : null;
      const known = undecodable ? 'malformed URL' : type ? CLIENT_ERRORS.get(type) : undefined;
      logger.info({ status, type, method: req.method }, 'client error');
      res.status(status).json({ error: known ?? 'bad request' });
      return;
    }
    logger.error({ err }, 'unhandled error');
    res.status(500).json({ error: 'internal error' });
  };
}
