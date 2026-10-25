import { z } from 'zod';

/**
 * Where an emailed link points: the web app, on the subdomain of the tenant the link
 * is for.
 *
 * A TENANT IS ITS SUBDOMAIN. Kong takes the tenant from the Host's first label
 * (infra/kong/kong.yml), and an access token is bound to that subdomain, so a link to
 * anywhere else lands on a page that cannot serve the reader's tenant: a password reset
 * that cannot sign them in, an invitation whose new account is in no tenant the page
 * can reach. Every link used to be built on one tenantless base, PUBLIC_APP_URL, and
 * every one of them landed on http://localhost:3000.
 *
 * So the base is a template, APP_URL_TEMPLATE, with `{tenant}` where the subdomain
 * goes: http://{tenant}.eventhub.localhost:5173 locally, https://{tenant}.eventhub.com
 * in production. A NEW NAME rather than a new meaning for the old one: a deployment
 * still setting PUBLIC_APP_URL=https://app.eventhub.com is refused at startup for a
 * missing variable, which says what changed, rather than either accepting a tenantless
 * base or failing on a value that used to be right.
 *
 * Kept here, beside the env loaders, because Auth and Notification both build these
 * links and must agree on the rule. A module of its own rather than more of index.ts:
 * scripts/verify-env-documented.mjs reads every `NAME:` line of index.ts as a variable
 * of the shared base, which every service would then be asked to document.
 */

/** What stands for the tenant's subdomain in the template. */
export const APP_URL_PLACEHOLDER = '{tenant}';

/**
 * KONG'S RULE FOR A TENANT LABEL, and nothing looser. The gateway refuses any other
 * Host with 400, so a link on any other label is a link nobody can follow:
 *   - one DNS label of lower-case letters, digits and hyphens, starting with a letter
 *     or a digit -- the Lua pattern ^([a-z0-9][a-z0-9%-]*) on the Host;
 *   - at most 63 characters;
 *   - not one of the reserved names below.
 * A hand-written copy, so scripts/verify-app-url-template.mjs reads kong.yml and
 * compares the two.
 */
export const TENANT_SLUG_PATTERN = /^[a-z0-9][a-z0-9-]*$/;
export const TENANT_SLUG_MAX_LENGTH = 63;
export const RESERVED_SUBDOMAINS: readonly string[] = Object.freeze([
  'www',
  'api',
  'admin',
  'app',
  'auth',
  'static',
  'assets',
  'mail',
  'status',
]);

/** Whether a value is a slug Kong would route to a tenant. Anything else is never put into a link. */
export function isTenantSlug(value: unknown): value is string {
  return (
    typeof value === 'string' &&
    value.length <= TENANT_SLUG_MAX_LENGTH &&
    TENANT_SLUG_PATTERN.test(value) &&
    !RESERVED_SUBDOMAINS.includes(value)
  );
}

export interface AppUrlTemplate {
  /** The template as configured. */
  readonly template: string;
  /** http: or https:, for the production guard each service applies. */
  readonly protocol: 'http:' | 'https:';
  /**
   * The web app's origin for one tenant, with no trailing slash, or null.
   *
   * NULL FOR ANY SLUG KONG WOULD NOT ROUTE: a reserved name, a capital, a dot, a
   * slash, an empty string, anything that is not a string. The slug is never put into
   * the template unchecked, so no input -- a header, a database row, a claim -- can
   * move a link to another host, or into the path, or past the domain.
   */
  originFor(slug: unknown): string | null;
}

/** A template that cannot be used, and why. */
export class AppUrlTemplateError extends Error {
  override name = 'AppUrlTemplateError';
}

/** A label that passes the rule, used only to see where the placeholder lands. */
const PROBE = 'tenant-probe';

/**
 * Checks a template, and returns the one function that fills it.
 *
 * REFUSED, each with its own reason:
 *   - no `{tenant}`, or more than one;
 *   - not an http or https URL once it is filled in;
 *   - `{tenant}` anywhere but as the WHOLE FIRST LABEL of the host -- in the path, in
 *     a query, as part of a label (`app-{tenant}.example.com`), or as the whole host;
 *   - credentials, a path, a query or a fragment: it is an origin, and each link adds
 *     its own path and token.
 */
export function parseAppUrlTemplate(raw: string): AppUrlTemplate {
  const template = raw.trim();
  const count = template.split(APP_URL_PLACEHOLDER).length - 1;
  if (count !== 1) {
    throw new AppUrlTemplateError(
      `must contain ${APP_URL_PLACEHOLDER} exactly once, where the tenant's subdomain goes, as in ` +
        `https://${APP_URL_PLACEHOLDER}.eventhub.com (found it ${count} times)`,
    );
  }

  let probe: URL;
  try {
    probe = new URL(template.split(APP_URL_PLACEHOLDER).join(PROBE));
  } catch {
    throw new AppUrlTemplateError(`is not a URL once ${APP_URL_PLACEHOLDER} is filled in`);
  }
  if (probe.protocol !== 'http:' && probe.protocol !== 'https:') {
    throw new AppUrlTemplateError(`must be http or https (it is ${probe.protocol})`);
  }
  const domain = probe.hostname.startsWith(`${PROBE}.`) ? probe.hostname.slice(PROBE.length + 1) : null;
  if (domain === null || domain.length === 0 || domain.startsWith('.')) {
    throw new AppUrlTemplateError(
      `must have ${APP_URL_PLACEHOLDER} as the whole first label of the host, as in https://${APP_URL_PLACEHOLDER}.eventhub.com: ` +
        'the gateway takes the tenant from that label and nothing else',
    );
  }
  if (probe.username !== '' || probe.password !== '') {
    throw new AppUrlTemplateError('must not carry credentials');
  }
  if (probe.pathname !== '/' || probe.search !== '' || probe.hash !== '') {
    throw new AppUrlTemplateError(
      'must be an origin, with no path, query or fragment: each link adds its own (/reset-password, /accept-invite, /posts/<id>)',
    );
  }

  const protocol = probe.protocol;
  return Object.freeze({
    template,
    protocol,
    originFor(slug: unknown): string | null {
      if (!isTenantSlug(slug)) return null;
      // Kong routes an `xn--` label whatever follows it, and so does the rule above, but
      // URL refuses one that is not valid punycode: `xn--a`, `xn--`, `xn---`. That throw
      // escaped as a TypeError, which kafkajs treats as unrecoverable, and Notification's
      // replies consumer crashed on the same message for good, for every tenant behind it.
      // A label no URL can hold gets no link, like any other this cannot use.
      let url: URL;
      try {
        url = new URL(template.split(APP_URL_PLACEHOLDER).join(slug));
      } catch {
        return null;
      }
      // The slug passed the rule, so it can only have filled the first label; this is
      // the check that it did, rather than an assumption that it must have.
      if (url.hostname !== `${slug}.${domain}`) return null;
      return url.origin;
    },
  });
}

/**
 * APP_URL_TEMPLATE as a zod field, for each service's own schema: parsed and checked,
 * or refused at startup with the reason. Required: there is no default, because the
 * one right on a laptop is wrong everywhere else, and every reset and invitation would
 * carry it.
 */
export const appUrlTemplateSchema = z
  .string({
    required_error:
      `required: where emailed links point, with ${APP_URL_PLACEHOLDER} for the tenant's subdomain, ` +
      `e.g. http://${APP_URL_PLACEHOLDER}.eventhub.localhost:5173 (it replaces PUBLIC_APP_URL, which was one host for every tenant)`,
  })
  .transform((raw, ctx): AppUrlTemplate => {
    try {
      return parseAppUrlTemplate(raw);
    } catch (err) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: err instanceof Error ? err.message : String(err) });
      return z.NEVER;
    }
  });
