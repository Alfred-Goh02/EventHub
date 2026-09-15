import pino from 'pino';

/**
 * Keys whose values must never reach a log line.
 *
 * Log streams are read far more widely than the database is: aggregators,
 * sidecars, CI output, support tooling, anyone with read access to a dashboard.
 * That is the whole reason bearer secrets in this system are stored only as
 * SHA-256 hashes — a database dump yields nothing usable. Logging the plaintext
 * hands the same secret to a much lower-privilege audience than a dump requires,
 * which quietly undoes that design.
 *
 * Redaction here is a backstop, not the control. The control is not passing
 * secrets to the logger. This exists because "nobody will log the token" is a
 * claim about every future line anyone writes, and a claim like that fails
 * eventually — most often in an error path where somebody logs the whole request
 * object to work out what went wrong.
 *
 * The wildcard forms catch the common shapes: a bare field, one nested under an
 * arbitrary key, and the request/response objects that middleware tends to dump
 * wholesale.
 *
 * EVERY KEY HERE GETS BOTH FORMS, and three did not. `authorization` had the
 * bare path and the two req shapes but no `*.authorization`, while all six of
 * its neighbours had their wildcard — an omission that reads as a slip rather
 * than a decision, and behaves like one:
 *
 *   ctx.authorization = NESTED-authorization
 *
 * That is real output from this logger, not a reading of the list. `secret` and
 * `privateKey` were absent altogether and leaked in both forms.
 *
 * Both of those name variables this codebase actually holds. webhooks.ts reads
 * PAYMENT_WEBHOOK_SECRET into `const secret`, which is exactly the `{ secret }`
 * shape a debug log produces, and jwt.ts holds the RS256 signing key — the one
 * value in the system that can mint a token for any user in any tenant. The
 * control is still not logging them; this is the backstop being consistent
 * about what it backstops.
 */
const REDACT = [
  'token',
  'tokenHash',
  'password',
  'passwordHash',
  'refreshToken',
  'accessToken',
  'authorization',
  'secret',
  'privateKey',
  // A reset link carries its token. Notification holds one in memory for every
  // email it sends, which is exactly the object an error path dumps to the log.
  'link',
  '*.token',
  '*.tokenHash',
  '*.password',
  '*.passwordHash',
  '*.refreshToken',
  '*.accessToken',
  '*.authorization',
  '*.secret',
  '*.privateKey',
  '*.link',
  'req.headers.authorization',
  'req.body.password',
  'headers.authorization',
];

export function createLogger(serviceName: string) {
  return pino({
    name: serviceName,
    level: process.env.LOG_LEVEL ?? 'info',
    formatters: {
      level: (label) => ({ level: label }),
    },
    redact: { paths: REDACT, censor: '[redacted]' },
  });
}

export type Logger = ReturnType<typeof createLogger>;
