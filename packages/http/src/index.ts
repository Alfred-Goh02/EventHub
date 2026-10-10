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
  // Raised by refuseUnstorableText, below.
  ['text.unstorable', 'request text cannot be stored: it contains a NUL character or a lone surrogate'],
  ['text.uncheckable', 'request has too many values to check'],
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

/**
 * The most values one request may carry for refuseUnstorableText to look at: every
 * string, number, boolean, null, array element and object entry in the body and the
 * query string together. Past it the request is refused, as 'text.uncheckable', rather
 * than let through unread -- a walk that gives up and passes the rest is not a check.
 *
 * Ten thousand is far past any real request: the largest list a route takes is Search's
 * fifty keywords, and a reservation's ten lines. At express.json()'s 100 kB default a
 * body can hold about fifty thousand (`[0,0,...]`), so the bound is reachable, and is
 * what keeps the walk's cost a constant rather than whatever a service's body limit
 * allows. The query string is already small: qs reads at most 1000 parameters, 5 deep.
 *
 * Depth needs no bound of its own: the walk keeps its own stack rather than recursing,
 * so a body nested fifty thousand deep costs what fifty thousand values cost, and is
 * refused by this bound, not by a stack overflow answered as a 500.
 *
 * Measured over HTTP, each a body express.json() admits: 49 000 arrays deep, refused in
 * 0.7 ms; 50 000 zeros, refused in 0.04 ms; 9 999 zeros, walked in 0.6 ms; 5 000 keys,
 * walked in 1.0 ms; one 99 kB string, checked in 0.06 ms.
 */
const MAX_VALUES = 10_000;

/** Whether Postgres TEXT can hold `s` exactly as the client sent it. */
const storable = (s: string): boolean => !s.includes('\u0000') && s.isWellFormed();

/** What JSON.parse and qs build. A Buffer or a Date is not text a client sent, and is not walked. */
const isPlainObject = (v: object): boolean => {
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
};

/** The first fault in `roots`, by the error type it is refused as, or null. Keys are checked as well as values. */
function unstorableIn(roots: unknown[]): 'text.unstorable' | 'text.uncheckable' | null {
  const pending = [...roots];
  let budget = MAX_VALUES;
  while (pending.length > 0) {
    const value = pending.pop();
    if (typeof value === 'string') {
      if (!storable(value)) return 'text.unstorable';
    } else if (Array.isArray(value)) {
      // Charged before anything is pushed, so an enormous array costs one subtraction.
      budget -= value.length;
      if (budget < 0) return 'text.uncheckable';
      for (const item of value) pending.push(item);
    } else if (typeof value === 'object' && value !== null && isPlainObject(value)) {
      const keys = Object.keys(value);
      budget -= keys.length;
      if (budget < 0) return 'text.uncheckable';
      for (const key of keys) {
        if (!storable(key)) return 'text.unstorable';
        pending.push((value as Record<string, unknown>)[key]);
      }
    }
  }
  return null;
}

/**
 * The same check for text that never passes through Express: Live's WebSocket messages,
 * whose question bodies reach Postgres as an HTTP body's would. Null when everything in
 * `value`, keys included, can be stored as sent; otherwise the type it is refused as.
 */
export function unstorableText(value: unknown): 'text.unstorable' | 'text.uncheckable' | null {
  return unstorableIn([value]);
}

/**
 * Refuses, with 400, any text in the request body or query string that Postgres cannot
 * store as sent: a string, or an object key, containing U+0000, or one that is not
 * well-formed UTF-16 (a lone surrogate). Every HTTP service mounts it directly after its
 * body parser, so it runs before any route and before authentication, as the parser does.
 *
 * WHAT IT FIXES, measured through the gateway as a tenant admin before it existed:
 *
 *   - A NUL is refused by Postgres, which cannot hold U+0000 in TEXT at all ("invalid byte
 *     sequence for encoding UTF8: 0x00", SQLSTATE 22021). That error is not the client's
 *     by any test errorHandler can make, so it answered 500 "internal error" and was
 *     logged as a server fault: Event's POST /events with a NUL in the title, Event's
 *     GET /events/search?q=a%00b, and Ticketing's POST /reservations with a NUL in the
 *     event id. Community's POST /spaces read the same failure as a duplicate and answered
 *     409 "a space with that slug already exists" for a slug nobody had.
 *   - A lone surrogate is worse, because nothing fails: node-postgres encodes the string
 *     as UTF-8, which turns the surrogate into U+FFFD, and the row stores something the
 *     client never sent, without an error. CLAUDE.md: refuse at the edge what the store
 *     would silently change.
 *
 * Refusing at the edge, once, rather than in each route's schema, because every route
 * that writes text would otherwise need the same refinement, and the one that forgot it
 * would be found the way these were.
 *
 * FIXED TEXT, NEVER THE OFFENDING STRING, for the reason CLIENT_ERRORS gives: answering
 * or logging it would reflect whatever the client sent. The refusal goes to errorHandler
 * as an exposed 400 with a `type`, so it is logged as every other client error is -- at
 * info, as "client error", with the status and 'text.unstorable' -- and never as
 * "unhandled error".
 *
 * A NUL in the URL PATH is not this middleware's: route parameters are not decoded until a
 * route matches, after this has run, and the gateway -- the only way in, since every
 * service publishes on 127.0.0.1 alone -- refuses one with its own 400 (measured:
 * GET /events/a%00b). A query string cannot carry a lone surrogate at all: percent-
 * decoding never yields one, and qs keeps a value it cannot decode as its raw text. So
 * for the query, the check that matters is the NUL, in a value or in a key.
 */
export function refuseUnstorableText(): RequestHandler {
  return (req, _res, next) => {
    const type = unstorableIn([req.body, req.query]);
    if (type === null) {
      next();
      return;
    }
    next(Object.assign(new Error(type), { status: 400, expose: true, type }));
  };
}
