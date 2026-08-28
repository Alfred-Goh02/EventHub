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
 */
const REDACT = [
  'token',
  'tokenHash',
  'password',
  'passwordHash',
  'refreshToken',
  'accessToken',
  'authorization',
  '*.token',
  '*.tokenHash',
  '*.password',
  '*.passwordHash',
  '*.refreshToken',
  '*.accessToken',
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
