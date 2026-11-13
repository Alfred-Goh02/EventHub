import pg from 'pg';
import { createLogger } from '@eventhub/logger';
import { publicKeyFromEnv } from '@eventhub/auth-middleware';
import { env } from '../env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from '../db.js';
import { createSmtpChannel } from '../channels/smtp.js';
import { createCardOpener, loadNotificationPrivateKey } from '../dispatch/open-seal.js';
import { attachPoolErrorHandler } from '../pool.js';
import { assertDigestIsLeastPrivilege, flushOnce } from '../digest/flush.js';

/**
 * One digest pass, by hand, printing what it did.
 *
 *   pnpm --filter @eventhub/notification run digest:once
 *
 * The service sends digests on its own clock, hourly; this is for a person who wants
 * the held replies out now, and for a verifier that must not race a timer.
 *
 * Unlike sweep:once it reads the service's whole configuration: sending a digest is
 * sending email, which takes the app role, the keys and SMTP. It checks both database
 * credentials first, exactly as the service does.
 */
async function main(): Promise<void> {
  const logger = createLogger('notification-digest-once');
  const pool = attachPoolErrorHandler(
    new pg.Pool({ connectionString: env.DIGEST_DATABASE_URL, application_name: 'notification-digest-once', max: 1 }),
    logger,
    'notification-digest-once',
  );
  try {
    await assertRuntimeRoleCannotBypassRls();
    await assertDigestIsLeastPrivilege(pool, logger);
    const [privateKey, authPublicKey] = await Promise.all([loadNotificationPrivateKey(), publicKeyFromEnv()]);
    const r = await flushOnce(
      pool,
      {
        openCard: createCardOpener({ privateKey, authPublicKey, issuer: env.JWT_ISSUER }),
        channel: createSmtpChannel({ host: env.SMTP_HOST, port: env.SMTP_PORT, from: env.EMAIL_FROM }),
        logger,
        unsubscribeSecret: env.UNSUBSCRIBE_SECRET,
        publicUrl: env.NOTIFICATION_PUBLIC_URL,
        appUrl: env.APP_URL_TEMPLATE,
      },
      { batchSize: env.DIGEST_BATCH_SIZE },
    );
    console.log(
      `sent ${r.digestsSent} digest(s) to ${r.waiting} waiting; settled ${r.itemsSettled} held ` +
        `repl${r.itemsSettled === 1 ? 'y' : 'ies'}; dropped ${r.droppedOptedOut} opted out, ${r.droppedNoAddress} without an address ` +
        `and ${r.droppedStale} stale; deferred ${r.deferred}; stepped over ${r.busyElsewhere} another pass had; failed ${r.recipientFailures}; ` +
        // Last, so the line's start reads as it did. Above zero: cards still to republish.
        `${r.withoutLinks} built without links, for a card that names no subdomain`,
    );
    if (r.recipientFailures > 0) process.exitCode = 1;
  } finally {
    await pool.end().catch(() => {});
    await prisma.$disconnect().catch(() => {});
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
