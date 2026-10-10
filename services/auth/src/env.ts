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

  /**
   * POST /invites' budgets. Every invitation is an email this platform sends to an
   * address an admin typed, so an admin -- or anyone holding an admin's token -- can
   * point the platform's mail at anybody. The content is fixed, but the complaints
   * land on the platform's sending reputation, which is what password resets travel
   * on. See the route for why these numbers.
   *
   * Validated rather than read loosely: a typo that became NaN would compare false
   * against every count, which is "no limit" with nothing to say so.
   */
  INVITES_PER_TENANT_PER_HOUR: z.coerce.number().int().positive().default(100),
  INVITES_PER_ADDRESS_PER_DAY: z.coerce.number().int().positive().default(3),

  /**
   * How long an invitation, and the sealed email that carries it, stays good. A week:
   * it waits on a human who may not be at their desk. Configurable because the email
   * lives as long as the link -- every sealed request on auth.email-requested can be
   * delivered until it expires -- and a deployment, or a verifier that leaves its
   * requests on the topic, may want them to die sooner.
   */
  INVITE_TTL_HOURS: z.coerce.number().int().positive().default(168),
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
