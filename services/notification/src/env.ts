import { z } from 'zod';
import { appUrlTemplateSchema, loadBaseEnv } from '@eventhub/config';

/**
 * Notification's own configuration, on top of the shared base: PORT, NODE_ENV,
 * LOG_LEVEL and DATABASE_URL (the migration owner) come from there.
 *
 * Auth's public key is read by @eventhub/auth-middleware's publicKeyFromEnv, from
 * AUTH_PUBLIC_KEY_B64 or AUTH_PUBLIC_KEY_FILE, as every other service reads it.
 * Notification's own private key is a file beside the service (keys/private.pem),
 * as Auth's signing key is.
 */
const notificationEnvSchema = z.object({
  /** The role that owns nothing, so RLS policies actually apply to it. */
  APP_DATABASE_URL: z.string().url(),

  KAFKA_BROKERS: z.string().min(1),
  SCHEMA_REGISTRY_URL: z.string().url(),
  /** Every sealed request's iss must be this. Only Auth's requests are acted on. */
  JWT_ISSUER: z.string().min(1),
  /**
   * The audience of the access tokens /preferences accepts, as every other API checks.
   * A sealed email request is for aud eventhub-notification, so it is not accepted
   * here, and an access token is not accepted as a sealed request.
   */
  JWT_AUDIENCE: z.string().min(1),

  /**
   * Signs the token in every unsubscribe link, so the link needs no login and cannot be
   * edited to unsubscribe someone else. At least 32 characters. Changing it breaks
   * every unsubscribe link already sent, which the law requires to keep working.
   */
  UNSUBSCRIBE_SECRET: z.string().min(32),
  /**
   * Where this service answers /unsubscribe, as a reader's mail client reaches it:
   * the origin every unsubscribe link and List-Unsubscribe header points at.
   *
   * ONE HOST FOR EVERY TENANT, on purpose, unlike the template below: the link needs
   * no session, its signed token names the tenant, and the gateway routes it on the api
   * subdomain, which has no tenant in it.
   */
  NOTIFICATION_PUBLIC_URL: z.string().url().default('http://localhost:3007'),
  /**
   * Where the web app shows a post, for the links in a reply email and a digest: the
   * RECIPIENT's tenant's own subdomain, with {tenant} where it goes, read from the slug
   * on their contact card. The same variable Auth builds its links on, checked the same
   * way: see @eventhub/config's app-url.ts.
   */
  APP_URL_TEMPLATE: appUrlTemplateSchema,
  /**
   * Where a new replies group starts on community.feed. latest, unlike password
   * resets: on first deploy, earliest would email everyone about a week of replies
   * that were long since read.
   */
  COMMUNITY_START_FROM: z.enum(['earliest', 'latest']).default('latest'),
  /**
   * Past this age, in milliseconds, a reply is not emailed at all: who is told was
   * decided when the comment was written, and a reply caught up on much later may have
   * been hidden since. An hour: well past the retry tiers (about twenty minutes), well
   * short of a notification nobody wants any more.
   */
  REPLY_MAX_AGE_MS: z.coerce.number().int().positive().default(3_600_000),
  /**
   * Reply emails one person may get in an hour; past it, replies are held and sent as
   * one digest. Decided 2026-09-27: five. See digest/hold.ts.
   */
  REPLY_HOURLY_LIMIT: z.coerce.number().int().positive().default(5),
  /**
   * The digest's own credential: three columns of the held replies, read only, across
   * every tenant — who has anything waiting. Separate from APP_DATABASE_URL for the
   * reason the sweeper's is. It refuses to start if this role can do more.
   */
  DIGEST_DATABASE_URL: z.string().url(),
  /** How often held replies go out as a digest. Decided 2026-09-27: hourly. A verifier shortens it. */
  DIGEST_INTERVAL_MS: z.coerce.number().int().positive().default(3_600_000),
  /** People sent a digest per pass, longest-waiting first; the rest wait for the next pass. */
  DIGEST_BATCH_SIZE: z.coerce.number().int().positive().max(10_000).default(500),
  /**
   * One group per deployment. Verifiers run their own, so they never take its partitions.
   * The retry tiers are read by `<this>-retry`, and replies from community.feed by
   * `<this>-replies`, so a verifier's own group brings its own of each.
   */
  KAFKA_CONSUMER_GROUP: z.string().min(1).default('notification-dispatch'),
  /**
   * The group that keeps the contact-card copy, from auth.user-contact. Its own, so a
   * backlog of cards never waits behind email requests or the reverse. It always
   * starts from the earliest offset: the topic is compacted precisely so a new group
   * can read every user's card, and there is deliberately no setting for that.
   */
  CONTACTS_CONSUMER_GROUP: z.string().min(1).default('notification-contacts'),
  /**
   * Where a group with no committed offset starts. earliest, so a request queued
   * before the service first ran is still delivered if its link has not expired.
   */
  DISPATCH_START_FROM: z.enum(['earliest', 'latest']).default('earliest'),
  /**
   * How long each retry tier waits, in milliseconds: one per tier topic, in order.
   * The defaults are spent inside the thirty minutes a reset link lives. A verifier
   * shortens them to seconds.
   */
  DISPATCH_RETRY_DELAYS_MS: z
    .string()
    .default('30000,300000,900000')
    .transform((list) => list.split(',').map((part) => Number(part.trim())))
    .pipe(z.array(z.number().int().positive()).length(3)),

  /**
   * How long a hard bounce keeps an address suppressed, in days.
   *
   * NOT FOREVER, because the reply that suppresses comes from the relay this service
   * sends through, not necessarily from the recipient's own mail server. A smarthost
   * with a stale recipient table, or a half-finished MX migration, answers 5.1.1 for
   * a mailbox that exists — and a suppression with no end would lock those users out
   * of password resets permanently. Each new hard bounce extends the window, so an
   * address that really is gone costs one refused send per window, not one per reset.
   */
  SUPPRESSION_TTL_DAYS: z.coerce.number().int().positive().default(30),

  /**
   * The sweeper's own credential: SELECT and a three-column UPDATE on the sent-log,
   * across every tenant. Separate from APP_DATABASE_URL because "which deliveries
   * never finished" has no tenant in it, and the process that sends must not gain
   * that reach. It refuses to start if this role can do more than sweep.
   */
  SWEEPER_DATABASE_URL: z.string().url(),
  /** How long a delivery may sit PENDING before it is called abandoned. */
  SWEEP_STUCK_AFTER_MS: z.coerce.number().int().positive().default(3_600_000),
  /** How often to sweep. */
  SWEEP_INTERVAL_MS: z.coerce.number().int().positive().default(60_000),
  /** Rows per pass, which bounds what one statement locks. */
  SWEEP_BATCH_SIZE: z.coerce.number().int().positive().max(10_000).default(500),

  SMTP_HOST: z.string().min(1),
  SMTP_PORT: z.coerce.number().int().positive(),
  EMAIL_FROM: z.string().email(),
});

const base = loadBaseEnv();
const parsed = notificationEnvSchema.safeParse(process.env);

if (!parsed.success) {
  const issues = parsed.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
  throw new Error(`Invalid notification-service environment:\n${issues}`);
}

/**
 * The secret in .env.example is public — it is in the repository. A production process
 * using it would sign unsubscribe links anyone could forge, so it refuses to start. A
 * comment saying "change this in production" is not a control; refusing to boot is.
 */
if (base.NODE_ENV === 'production' && parsed.data.UNSUBSCRIBE_SECRET.startsWith('dev-only-')) {
  throw new Error('refusing to start: UNSUBSCRIBE_SECRET is the development value from .env.example');
}

/**
 * The same for the two public origins. Both are localhost in the example, which is right
 * on a laptop and wrong everywhere else: a production process that kept them would send
 * every reply with an unsubscribe link pointing at the reader's own machine — and replies
 * are on by default, so nobody could turn them off from the email. RFC 8058 also
 * requires one-click unsubscribe over https. So production requires https for both.
 */
if (base.NODE_ENV === 'production') {
  const protocols = {
    NOTIFICATION_PUBLIC_URL: new URL(parsed.data.NOTIFICATION_PUBLIC_URL).protocol,
    APP_URL_TEMPLATE: parsed.data.APP_URL_TEMPLATE.protocol,
  };
  const plain = (Object.keys(protocols) as Array<keyof typeof protocols>).filter((name) => protocols[name] !== 'https:');
  if (plain.length > 0) {
    throw new Error(`refusing to start: ${plain.join(' and ')} must be https in production (the default and the example point at localhost)`);
  }
}

export const env = Object.freeze({ ...base, ...parsed.data });
