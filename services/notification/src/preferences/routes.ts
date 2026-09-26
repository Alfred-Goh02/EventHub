import { Router, type RequestHandler } from 'express';
import { z } from 'zod';
import { getAuth } from '@eventhub/auth-middleware';
import { ah } from '@eventhub/http';
import type { Logger } from '@eventhub/logger';
import { CATEGORIES, CHANNELS, isOptional, type Category } from './categories.js';
import { listPreferences, setPreference } from './store.js';
import { verifyUnsubscribe } from './unsubscribe-token.js';

/**
 * A user's notification preferences, and the unsubscribe link in every optional email.
 *
 * /preferences needs a signed-in user and only ever reads or writes THEIR choices: the
 * tenant and user come from the verified token, never from the request.
 *
 * /unsubscribe needs no login — it works from the email itself — and carries its own
 * authority in a signed token.
 *
 * GET NEVER UNSUBSCRIBES. Mail scanners and link previews fetch every URL in a
 * message, so an unsubscribe that happened on GET would unsubscribe people who never
 * clicked. GET shows a button; POST does it. RFC 8058's one-click unsubscribe is also a
 * POST, sent by the mail client straight to the same URL, so the email's
 * List-Unsubscribe header and the button in the page share one handler.
 */

const patchBody = z.object({
  category: z.enum(Object.keys(CATEGORIES) as [Category, ...Category[]]),
  channel: z.enum(CHANNELS).default('EMAIL'),
  enabled: z.boolean(),
});

const escapeHtml = (s: string) =>
  s.replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!);

const page = (title: string, body: string) =>
  `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">` +
  `<title>${escapeHtml(title)}</title></head><body style="font-family:system-ui,sans-serif;max-width:32rem;margin:4rem auto;padding:0 1rem;line-height:1.5">` +
  `${body}</body></html>`;

const invalid = page('Unsubscribe', '<h1>This unsubscribe link is not valid</h1><p>It may have been changed or cut short. Use the link from the email again, or change your notification settings after signing in.</p>');

export function preferenceRoutes(opts: { requireAuth: RequestHandler; unsubscribeSecret: string; logger: Logger }): Router {
  const router = Router();

  router.get('/preferences', opts.requireAuth, ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    res.json({ preferences: await listPreferences(tenantId, userId, 'EMAIL') });
  }));

  router.patch('/preferences', opts.requireAuth, ah(async (req, res) => {
    const parsed = patchBody.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
    const { tenantId, userId } = getAuth(req);
    const { category, channel, enabled } = parsed.data;
    // 409 rather than a silent success: the caller asked for something that will not happen.
    if (!isOptional(category)) {
      return res.status(409).json({ error: `${CATEGORIES[category].label} cannot be turned off` });
    }
    const changed = await setPreference(tenantId, userId, category, channel, enabled);
    return res.json({ category, channel, enabled, changed });
  }));

  /** Shows what the link would turn off, and a button. Changes nothing. */
  router.get('/unsubscribe', (req, res) => {
    const token = typeof req.query['token'] === 'string' ? req.query['token'] : '';
    const claims = verifyUnsubscribe(opts.unsubscribeSecret, token);
    if (!claims) return res.status(400).type('html').send(invalid);
    const label = CATEGORIES[claims.category].label.toLowerCase();
    return res.type('html').send(
      page(
        'Unsubscribe',
        `<h1>Stop emails about ${escapeHtml(label)}?</h1>` +
          `<form method="post" action="/unsubscribe?token=${encodeURIComponent(token)}"><button type="submit">Unsubscribe</button></form>` +
          '<p>Emails you need to use your account, such as password resets, are not affected.</p>',
      ),
    );
  });

  /** The one-click unsubscribe (RFC 8058), from the mail client or the button above. */
  router.post('/unsubscribe', ah(async (req, res) => {
    const token = typeof req.query['token'] === 'string' ? req.query['token'] : '';
    const claims = verifyUnsubscribe(opts.unsubscribeSecret, token);
    if (!claims) return res.status(400).type('html').send(invalid);
    const changed = await setPreference(claims.tenantId, claims.userId, claims.category, 'EMAIL', false);
    // Logged without the token: it is a credential for exactly this action.
    opts.logger.info({ tenantId: claims.tenantId, userId: claims.userId, category: claims.category, changed }, 'unsubscribed');
    const label = CATEGORIES[claims.category].label.toLowerCase();
    return res.type('html').send(
      page('Unsubscribed', `<h1>You are unsubscribed</h1><p>You will not get emails about ${escapeHtml(label)} any more. You can turn them back on in your settings.</p>`),
    );
  }));

  return router;
}
