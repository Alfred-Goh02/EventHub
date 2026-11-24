/**
 * WHERE THE EKS CLUSTER IS REACHED: one setting, and everything that follows from it.
 *
 * A tenant is its subdomain, so the address is a template: APP_URL_TEMPLATE, the same
 * variable Auth and Notification build their emailed links from (packages/config/src/app-url.ts).
 * Choosing a domain is changing DOMAIN_TEMPLATE below. Every other value that names the address
 * is derived from it here, and the installer puts them in the Secrets each pod reads
 * (infra/eks/up.mjs, installSecrets):
 *
 *   NODE_ENV                every service: production exactly when the address is https.
 *                           Auth and Notification refuse to start in production on an http
 *                           template, so the two cannot disagree;
 *   APP_URL_TEMPLATE        Auth and Notification, for their links; Kong, whose render step
 *                           (infra/kong/render-consumer.lua) puts this domain where kong.yml
 *                           has eventhub.com -- its hosts, its CORS origin and its tenant rule;
 *                           and the web app, which uses the real API only on this domain
 *                           (apps/web/src/api/env.ts);
 *   NOTIFICATION_PUBLIC_URL the unsubscribe link's host: api.<domain>, on the gateway's port;
 *   ALLOWED_ORIGIN_SUFFIX   Live's WebSocket origin check: .<domain>.
 *
 * Compose keeps its own values: this is what differs on the cluster, and nothing else does.
 *
 * WITHOUT A DOMAIN, THE LOAD BALANCER'S OWN ADDRESS. The cluster's one way in is a Network
 * Load Balancer (infra/edge), whose addresses are fixed for its life, and sslip.io answers
 * any name holding an address with that address: acme.13-54-1-2.sslip.io is 13.54.1.2. So
 * until DOMAIN_TEMPLATE is set, the installer makes the load balancer first and uses
 * http://{tenant}.<its address, dashed>.sslip.io -- free, a subdomain for every tenant, and
 * plain HTTP, since no certificate can be had for a name nobody here controls. With neither
 * (no load balancer to ask), it is the laptop's address, reached through `kubectl
 * port-forward`. scripts/verify-public-address.mjs derives the values for a real https
 * domain too, renders Kong with them, and runs it.
 */

/** The domain, once there is one: 'https://{tenant}.example.com'. Choosing one is this line. */
export const DOMAIN_TEMPLATE = null;

/** A laptop's address: browsers and curl resolve any *.localhost name to the machine itself. */
export const LAPTOP_TEMPLATE = 'http://{tenant}.eventhub.localhost:5173';

/** The address, before anything is asked of a load balancer: the domain's, or the laptop's. */
export const APP_URL_TEMPLATE = DOMAIN_TEMPLATE ?? LAPTOP_TEMPLATE;

/**
 * The address the cluster is reached at, given the load balancer's IPv4 addresses (none
 * when there is none): the domain's when there is one; else the first of the balancer's
 * addresses, in order, through sslip.io; else the laptop's. The balancer spans zones, so
 * any one of its addresses reaches every target.
 */
export function addressFor(balancerIps = []) {
  if (DOMAIN_TEMPLATE) return DOMAIN_TEMPLATE;
  const ips = [...balancerIps].filter((ip) => /^(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)$/.test(ip));
  if (ips.length !== balancerIps.length) throw new Error(`not IPv4 addresses: ${balancerIps.join(', ')}`);
  if (ips.length === 0) return LAPTOP_TEMPLATE;
  // In numeric order, so the same balancer gives the same address however DNS lists them.
  const key = (ip) => ip.split('.').map((n) => n.padStart(3, '0')).join('.');
  const [first] = ips.sort((a, b) => (key(a) < key(b) ? -1 : key(a) > key(b) ? 1 : 0));
  return `http://{tenant}.${first.replaceAll('.', '-')}.sslip.io`;
}

/** The gateway's port, on every host: the web app calls the API at its own host on 8000. */
export const GATEWAY_PORT = 8000;

/** The domain every laptop resolves to itself, which kong.yml always serves. */
export const DEV_DOMAIN = 'eventhub.localhost';

const LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;

/**
 * A template's parts: { template, scheme ('http' | 'https'), domain, port (string, '' for
 * the scheme's own) }, or a refusal.
 *
 * Stricter than Auth's parse, on purpose: the domain is written into Kong's configuration as
 * a host, a regular expression and a Lua pattern, and into a script the web app's server
 * serves. So it is lower-case letters, digits, hyphens and dots, as typed -- nothing a URL
 * parser would have to fold or encode first -- in two labels or more.
 */
export function publicAddress(template) {
  const raw = String(template ?? '');
  const m = /^(https?):\/\/\{tenant\}\.([^/:?#]+)(?::(\d{1,5}))?\/?$/.exec(raw);
  if (!m) throw new Error(`APP_URL_TEMPLATE must be http(s)://{tenant}.<domain>[:port], an origin with {tenant} as the whole first label (got ${JSON.stringify(raw)})`);
  const [, scheme, domain, port = ''] = m;
  const labels = domain.split('.');
  if (labels.length < 2 || domain.length > 253 || !labels.every((l) => LABEL.test(l))) {
    throw new Error(`APP_URL_TEMPLATE's domain must be two or more lower-case DNS labels (got ${JSON.stringify(domain)})`);
  }
  if (port && (Number(port) < 1 || Number(port) > 65535 || port.startsWith('0'))) throw new Error(`APP_URL_TEMPLATE's port is not a port (got ${port})`);
  return { template: raw, scheme, domain, port };
}

/**
 * The variables the address sets, by Secret: { '<chart>-env': { KEY: value } }, for the
 * charts given, plus kong-env and web-env. Only a chart whose environment from compose
 * already has a key gets it replaced; NODE_ENV is in every service's (compose's x-app).
 * setup-env is compose's alone: setup migrates and seeds, as compose runs it.
 */
export function publicEnvironment(template, charts, envOf) {
  const { scheme, domain } = publicAddress(template);
  const nodeEnv = scheme === 'https' ? 'production' : 'development';
  const derived = {
    NODE_ENV: nodeEnv,
    APP_URL_TEMPLATE: template,
    NOTIFICATION_PUBLIC_URL: `${scheme}://api.${domain}:${GATEWAY_PORT}`,
    ALLOWED_ORIGIN_SUFFIX: `.${domain}`,
  };
  // Who reads what, by the service's own code: a key goes only where it is read.
  const readers = {
    APP_URL_TEMPLATE: ['auth', 'notification'],
    NOTIFICATION_PUBLIC_URL: ['notification'],
    ALLOWED_ORIGIN_SUFFIX: ['live'],
  };
  const out = {};
  for (const chart of charts) {
    const env = envOf(chart);
    const set = {};
    if ('NODE_ENV' in env) set.NODE_ENV = nodeEnv;
    for (const [key, who] of Object.entries(readers)) if (who.includes(chart)) set[key] = derived[key];
    if (Object.keys(set).length) out[`${chart}-env`] = set;
  }
  out['kong-env'] = { APP_URL_TEMPLATE: template };
  out['web-env'] = { APP_URL_TEMPLATE: template };
  return out;
}

/** Where a person opens the app for one tenant: the template, filled. */
export const openAt = (template, tenant = 'acme') => publicAddress(template) && template.replace('{tenant}', tenant).replace(/\/$/, '');
