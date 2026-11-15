/**
 * The EKS cluster's own credentials: every password, secret and keypair the installer puts
 * into a Secret, made once, in and for that cluster, and kept there.
 *
 * WHY NOT COMPOSE'S. Compose's credentials are development values written into this
 * repository: dev_webhook_secret, a role password that is the role's own name, an
 * unsubscribe secret that says change-me. Copied into a cluster with a way in from the
 * internet, anyone who has read the repository could sign a payment webhook or an
 * unsubscribe link. So the installer still builds every Secret from compose -- which
 * Secrets, which keys, every non-secret value -- and then replaces each credential in it
 * with this cluster's own.
 *
 * NOTHING IS LISTED HERE. A credential is found by where it sits, in the Secrets the
 * installer would otherwise have made:
 *   - a variable named *_PASSWORD is the password of the user its *_USER sibling names, in
 *     the store its prefix names (STORES);
 *   - a URL with a password in it is that URL's user's password, in the store its scheme
 *     names;
 *   - a variable whose name ends in SECRET or TOKEN is a secret of its own, one per name;
 *   - an init file's CREATE ROLE ... PASSWORD (Postgres) or CREATE USER ... BY (ClickHouse)
 *     sets that user's password.
 * Each user's password is one value wherever it appears, so the role the init file makes is
 * the role every URL logs in as. A credential found nowhere is never made.
 *
 * KEPT IN THE CLUSTER, in the Secret CREDENTIALS_SECRET. A second run reads it back and
 * makes only what is missing: a store's disk keeps the passwords its first boot set, and
 * tokens signed with a key stay valid. It goes with the cluster; there is no copy anywhere
 * else, and nothing here prints a value.
 */

import { createPublicKey, generateKeyPairSync, randomBytes } from 'node:crypto';

export const CREDENTIALS_SECRET = 'eventhub-credentials';

/** A variable's prefix, and the store whose users it names. */
export const STORES = { POSTGRES: 'postgres', CLICKHOUSE: 'clickhouse' };
/** A URL's scheme, and the store it logs in to. */
const SCHEMES = { 'postgres:': 'postgres', 'postgresql:': 'postgres' };
/** The keypairs: each one's Secret key here. Auth signs tokens; Notification opens what Auth seals to it. */
export const KEYPAIRS = { auth: 'keys.auth', notification: 'keys.notification' };

/** A variable that IS a secret ends in one: PAYMENT_WEBHOOK_SECRET, never ACCESS_TOKEN_TTL. */
const SECRET_NAME = /(^|_)(SECRET|TOKEN)$/;

class CredentialError extends Error {}
const fail = (msg) => {
  throw new CredentialError(msg);
};

/** A user's password slot: postgres.eventhub_auth_app. Secret keys allow [-._a-zA-Z0-9]. */
const userSlot = (store, user) => {
  if (!/^[A-Za-z0-9_]+$/.test(user)) fail(`a ${store} user named ${JSON.stringify(user)} cannot be a credential key`);
  return `${store}.${user}`;
};

/** The SQL statements in an init file that set a password: [{ store, user, slot, start, end }] for the literal. */
function sqlPasswords(sql) {
  const out = [];
  // Postgres: CREATE ROLE name ... PASSWORD 'literal'; within one statement.
  for (const m of sql.matchAll(/CREATE\s+ROLE\s+([A-Za-z0-9_]+)\b([^;]*?)\bPASSWORD\s+'([^']*)'/gi)) {
    const start = m.index + m[0].length - m[3].length - 1;
    out.push({ store: 'postgres', user: m[1], slot: userSlot('postgres', m[1]), start, end: start + m[3].length, dev: m[3] });
  }
  // ClickHouse: CREATE USER [IF NOT EXISTS] name IDENTIFIED WITH ... BY 'literal'.
  for (const m of sql.matchAll(/CREATE\s+USER\s+(?:IF\s+NOT\s+EXISTS\s+)?([A-Za-z0-9_]+)\s+IDENTIFIED\s+WITH\s+\w+\s+BY\s+'([^']*)'/gi)) {
    const start = m.index + m[0].length - m[2].length - 1;
    out.push({ store: 'clickhouse', user: m[1], slot: userSlot('clickhouse', m[1]), start, end: start + m[2].length, dev: m[2] });
  }
  return out;
}

/**
 * The credential a value under `key` is, or null: { slot, dev, kind }. `data` is the
 * environment the value came from, where a *_PASSWORD's *_USER sibling is looked up: a
 * Secret can hold the password alone (infra/stores reads POSTGRES_PASSWORD from one and sets
 * POSTGRES_USER in plain sight), so its sibling is in compose's environment for that service.
 */
function credentialOf(key, value, data) {
  if (/\.sql$/i.test(key)) return null; // init files: sqlPasswords
  const pw = /^([A-Z0-9_]*?)_?PASSWORD$/.exec(key);
  if (pw) {
    const prefix = pw[1];
    const store = Object.entries(STORES).find(([p]) => prefix === p || prefix.startsWith(`${p}_`))?.[1];
    if (!store) fail(`${key} is a password, and its prefix names no store this knows (${Object.keys(STORES).join(', ')})`);
    const userKey = key.replace(/PASSWORD$/, 'USER');
    const user = data[userKey];
    if (!user) fail(`${key} is a password for the user ${userKey} names, and nothing sets ${userKey} beside it`);
    return { slot: userSlot(store, user), dev: value, kind: 'password' };
  }
  if (SECRET_NAME.test(key)) return { slot: `secret.${key}`, dev: value, kind: 'secret' };
  const url = /^([a-z][a-z0-9+.-]*:)\/\/([^:/@]+):([^@/]*)@/i.exec(value);
  if (url) {
    const store = SCHEMES[url[1].toLowerCase()];
    if (!store) fail(`${key} holds a password in a ${url[1]} URL, a scheme that names no store this knows`);
    return { slot: userSlot(store, decodeURIComponent(url[2])), dev: decodeURIComponent(url[3]), kind: 'url' };
  }
  return null;
}

/**
 * Every credential in a set of Secrets ({ name: { key: value } }), as
 * [{ secret, key, slot, dev, kind }]: what the installer would have put there, and which of
 * this cluster's credentials replaces it. `envOf(secret, key)` is the environment that
 * value came from (credentialOf says why); by default, the Secret itself.
 */
export function credentialsIn(secrets, envOf = (secret) => secrets[secret]) {
  const out = [];
  for (const [secret, data] of Object.entries(secrets)) {
    for (const [key, value] of Object.entries(data)) {
      if (/\.sql$/i.test(key)) {
        for (const p of sqlPasswords(value)) out.push({ secret, key, slot: p.slot, dev: p.dev, kind: 'sql' });
        continue;
      }
      const c = credentialOf(key, String(value), envOf(secret, key));
      if (c) out.push({ secret, key, ...c });
    }
  }
  return out;
}

/** A new value for a slot: 32 random bytes as hex, safe in a URL, a SQL literal and an env file. */
const fresh = () => randomBytes(32).toString('hex');

function keypair() {
  return generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
  }).privateKey;
}

/**
 * This cluster's credentials: `existing` (what the cluster already holds) plus a fresh value
 * for every slot `found` needs and `existing` lacks, and a keypair for each of KEYPAIRS it
 * lacks. Returns { creds, made: [slot] }. Existing values are never replaced.
 */
export function ensure(found, existing = {}) {
  const creds = { ...existing };
  const made = [];
  for (const slot of [...new Set(found.map((f) => f.slot))].sort()) {
    if (!creds[slot]) {
      creds[slot] = fresh();
      made.push(slot);
    }
  }
  for (const slot of Object.values(KEYPAIRS)) {
    if (!creds[slot]) {
      creds[slot] = keypair();
      made.push(slot);
    }
  }
  return { creds, made };
}

/** A private key's public half, as the generate-keys scripts write it (SPKI PEM). */
export const publicOf = (privatePem) => createPublicKey(privatePem).export({ type: 'spki', format: 'pem' });

/**
 * The Secrets with every credential replaced by this cluster's. Values that are not
 * credentials are kept exactly. A slot `creds` lacks is a refusal, never a dev value kept.
 */
export function withCredentials(secrets, creds, envOf = (secret) => secrets[secret]) {
  const value = (slot) => creds[slot] || fail(`no credential ${slot}: ensure() it first`);
  const out = {};
  for (const [secret, data] of Object.entries(secrets)) {
    out[secret] = {};
    for (const [key, raw] of Object.entries(data)) {
      const text = String(raw);
      if (/\.sql$/i.test(key)) {
        let sql = text;
        for (const p of sqlPasswords(text).sort((a, b) => b.start - a.start)) sql = sql.slice(0, p.start) + value(p.slot) + sql.slice(p.end);
        out[secret][key] = sql;
        continue;
      }
      const c = credentialOf(key, text, envOf(secret, key));
      if (!c) out[secret][key] = text;
      else if (c.kind === 'url') out[secret][key] = text.replace(/^([a-z][a-z0-9+.-]*:\/\/[^:/@]+:)[^@/]*@/i, `$1${value(c.slot)}@`);
      else out[secret][key] = value(c.slot);
    }
  }
  return out;
}

/** The key Secrets the charts read, from this cluster's keypairs. */
export function keySecrets(creds) {
  const auth = creds[KEYPAIRS.auth] || fail('no Auth keypair: ensure() it first');
  const notification = creds[KEYPAIRS.notification] || fail('no Notification keypair: ensure() it first');
  return {
    // Auth signs with private.pem; every service verifies with public.pem; Auth seals
    // contact cards and reset requests to notification-public.pem.
    'jwt-keys': { 'private.pem': auth, 'public.pem': publicOf(auth), 'notification-public.pem': publicOf(notification) },
    'notification-keys': { 'private.pem': notification, 'public.pem': publicOf(notification) },
  };
}
