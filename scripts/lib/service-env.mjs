import { readFileSync } from 'node:fs';
import { join } from 'node:path';

/**
 * The environment to spawn a service with: its own .env.example, plus whatever
 * the caller overrides.
 *
 * THREE TIMES a verifier that spawns Search broke because the service gained a
 * required variable and the verifier's hand-written environment did not —
 * SCHEMA_REGISTRY_URL in part 2, JWT_ISSUER, JWT_AUDIENCE and EVENT_SERVICE_URL in
 * part 3, SEARCH_CURSOR_SECRET in part 4. Each was a list maintained by hand, and
 * each fell behind the service the moment it changed.
 *
 * Starting from .env.example means a new variable reaches every verifier as soon
 * as it is documented, and scripts/verify-env-documented.mjs already refuses a
 * variable the code reads that .env.example does not mention. So the chain is
 * closed: read it, and it must be documented; document it, and every verifier
 * that starts the service has it.
 *
 * Parsed the way scripts/verify-env-example.mjs parses it: KEY=value, blank lines
 * and # comments, no quoting, because none of these files use any.
 */
export function serviceEnv(root, service, overrides = {}) {
  const env = { PATH: process.env['PATH'], SystemRoot: process.env['SystemRoot'] };

  for (const raw of readFileSync(join(root, 'services', service, '.env.example'), 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    env[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }

  // An undefined override means "the caller had no value": keep the example's.
  for (const [key, value] of Object.entries(overrides)) {
    if (value !== undefined) env[key] = value;
  }
  return env;
}
