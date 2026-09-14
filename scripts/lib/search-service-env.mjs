import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { serviceEnv } from './service-env.mjs';

/**
 * The environment to spawn the Search service with: its own .env.example (see
 * service-env.mjs for why that is the starting point), the generated public key,
 * and whatever the caller overrides.
 */
export function searchServiceEnv(root, overrides = {}) {
  const env = serviceEnv(root, 'search');

  // Generated, not configured: the example leaves it empty for exactly this.
  if (env['AUTH_PUBLIC_KEY_B64'] === '') {
    env['AUTH_PUBLIC_KEY_B64'] = readFileSync(join(root, 'services', 'auth', 'keys', 'public.pem')).toString('base64');
  }

  // An undefined override means "the caller had no value": keep the example's.
  for (const [key, value] of Object.entries(overrides)) {
    if (value !== undefined) env[key] = value;
  }
  return env;
}
