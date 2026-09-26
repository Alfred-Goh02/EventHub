import { z } from 'zod';
import { baseEnvSchema, loadEnv } from '@eventhub/config';

/**
 * The HTTP service's configuration: the shared base, plus what only the server needs.
 *
 * Imported by index.ts and nothing else. db.ts keeps loadBaseEnv(), because the
 * contact-cards command imports db.ts, and a module every entrypoint loads must not
 * require what only one of them uses. Setup runs that command with the bare minimum
 * (the compose job once crashed it on a PORT nobody had given it), and it builds no
 * links, so nothing here is its business.
 */
const authEnvSchema = baseEnvSchema.extend({
  /**
   * Where an accept-invite or password-reset link points: the web app a reader opens
   * the link in. The localhost default is right on a laptop and wrong everywhere
   * else.
   */
  PUBLIC_APP_URL: z.string().url().default('http://localhost:3000'),
});

export const env = loadEnv(authEnvSchema);

/**
 * A production process that forgot PUBLIC_APP_URL would build every reset link on
 * http://localhost:3000, the reader's own machine. Nothing fails: the link is made,
 * sealed and emailed, and password reset is simply broken for everyone, silently. A
 * reset token also belongs on nothing but https. So production requires https.
 */
if (env.NODE_ENV === 'production' && new URL(env.PUBLIC_APP_URL).protocol !== 'https:') {
  throw new Error('refusing to start: PUBLIC_APP_URL must be https in production (the default points at localhost)');
}
