import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { hash, verify } from '@node-rs/argon2';
import { z } from 'zod';
import { withTenant } from './db.js';
import { Prisma } from './generated/prisma/client.js';
import { signAccessToken } from './jwt.js';
import { requireAuth, requireRole } from './middleware/auth.js';
import { issueRefreshToken, rotateRefreshToken, revokeFamily } from './tokens.js';
import { issueInvite, lookupInvite } from './invites.js';
import { issueResetToken, lookupResetToken } from './password-reset.js';
import { emitDevToken } from './dev-sink.js';
import { createRateLimiter } from './rate-limit.js';

const env = loadBaseEnv();
const logger = createLogger('auth-service');

/**
 * Where a reset link points. Only ever used to build the dev sink's URL, so the
 * localhost default is honest rather than lazy — there is no front end in this
 * repository yet, and a token is delivered to a developer reading logs.
 */
const RESET_URL_BASE = process.env['PUBLIC_APP_URL'] ?? 'http://localhost:3000';

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'auth' });
});

/**
 * Email, normalised at the SCHEMA boundary rather than at each use site.
 *
 * issueInvite() lowercases before storing, so a user invited as
 * "Bob.Smith@Acme.COM" is stored as "bob.smith@acme.com". /login did NOT
 * lowercase before looking up, and User is @@unique([tenantId, email]) on the
 * stored value -- so that user got 401 when signing in with the exact address
 * they were invited at, and only the lowercased form worked. Demonstrated
 * against the running service.
 *
 * Normalising here rather than in each handler is the point: a route that
 * forgets to lowercase is a lockout that looks exactly like a wrong password,
 * and the next email-shaped input would have forgotten it again.
 *
 * Only case and surrounding whitespace are touched. The local part of an
 * address is case-SENSITIVE per RFC 5321 and lowercasing it is technically
 * lossy, but every mainstream provider treats it case-insensitively, and
 * matching what is already in the column matters more than standards purity
 * that would lock people out.
 */
const emailField = z
  .string()
  // .trim() must come BEFORE .email(), not in the transform after it. Zod runs
  // validation in declaration order, so trimming afterwards would still reject
  // "  alice@acme.com  " with a 400 -- and a pasted address carrying a trailing
  // space is a copy-paste artefact, not a malformed address.
  .trim()
  .email()
  .transform((e) => e.toLowerCase());

const inviteSchema = z.object({
  email: emailField,
  role: z.enum(['ADMIN', 'ORGANIZER', 'ATTENDEE']).default('ATTENDEE'),
});

// Creating an invite requires being an ADMIN of the tenant you are inviting to.
// Both facts come from the verified token: requireAuth establishes who, and
// requireRole establishes whether they may. Neither is client-supplied.
app.post('/invites', requireAuth, requireRole('ADMIN'), async (req, res) => {
  const parsed = inviteSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const { email, role } = parsed.data;
  const { tenantId, userId } = req.auth!;

  const { token, expiresAt } = await issueInvite({ tenantId, email, role, invitedBy: userId });

  // Until the Notification service exists (Chapter 8), the link reaches a
  // developer through the dev sink rather than by email.
  //
  // This used to be an unconditional logger.warn of the raw token, with a
  // comment saying it must never survive into production. A comment is not a
  // control: the sink is off unless AUTH_DEV_TOKEN_SINK is set, logs below the
  // default level, and refuses to boot under NODE_ENV=production. The logger
  // also redacts `token` now, so the old line would have emitted "[redacted]"
  // and quietly stopped working — see dev-sink.ts for why the field it does emit
  // is named the way it is.
  emitDevToken(logger, 'invite', {
    email,
    tenantId,
    url: `${RESET_URL_BASE}/accept-invite?token=${token}`,
  });

  return res.status(201).json({ email, role, expiresAt });
});

const registerSchema = z.object({
  token: z.string().min(1),
  password: z.string().min(8),
});

// No X-Tenant-ID. The tenant comes from the invite, which was created by
// someone already inside that tenant — so the registering user never gets to
// state which tenant they are joining, and there is nothing to lie about.
// The email is pinned by the invite too, for the same reason.
app.post('/register', async (req, res) => {
  const parsed = registerSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const { token, password } = parsed.data;

  const invite = await lookupInvite(token);
  if (!invite.ok) {
    // One generic message for every failure. Distinguishing "expired" from
    // "already used" from "never existed" tells someone probing tokens which
    // guesses were close.
    return res.status(400).json({ error: 'invalid or expired invite' });
  }

  const passwordHash = await hash(password);

  try {
    const user = await withTenant(invite.tenantId, async (tx) => {
      const created = await tx.user.create({
        data: {
          tenantId: invite.tenantId,
          email: invite.email,
          passwordHash,
          role: invite.role,
        },
        select: { id: true, email: true, role: true, createdAt: true },
      });

      /**
       * Claim the invite, and check that THIS caller was the one that did it.
       *
       * `acceptedAt: null` in the WHERE clause is what makes an invite
       * single-use under concurrency. Two simultaneous registrations both pass
       * lookupInvite above -- it only reads -- but only one updateMany matches a
       * row, so only one gets count 1. Checking acceptedAt and then updating as
       * separate steps would let both through, and the second would fail on the
       * unique email instead, with a message about the wrong thing.
       *
       * Same transaction as the user insert, so a crash between the two cannot
       * leave a live invite that has already been redeemed.
       *
       * (This explanation used to live on a claimInvite() helper in invites.ts
       * that nothing ever called -- the pattern was inlined here and the
       * documentation stayed over there. A reader following lookupInvite into
       * that file found the read but not the guarded write.)
       */
      const claimed = await tx.invite.updateMany({
        where: { id: invite.id, acceptedAt: null, revokedAt: null },
        data: { acceptedAt: new Date() },
      });

      if (claimed.count !== 1) {
        throw new Error('invite already claimed');
      }

      return created;
    });

    return res.status(201).json(user);
  } catch (err) {
    if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === 'P2002') {
      return res.status(409).json({ error: 'email already registered' });
    }
    if (err instanceof Error && err.message === 'invite already claimed') {
      return res.status(400).json({ error: 'invalid or expired invite' });
    }
    logger.error({ err }, 'register failed');
    return res.status(500).json({ error: 'internal error' });
  }
});

const loginSchema = z.object({
  email: emailField,
  password: z.string(),
});

app.post('/login', async (req, res) => {
  const tenantId = req.header('X-Tenant-ID');
  if (!tenantId) {
    return res.status(400).json({ error: 'X-Tenant-ID header is required' });
  }

  const parsed = loginSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const { email, password } = parsed.data;

  const user = await withTenant(tenantId, (tx) =>
    tx.user.findUnique({
      where: { tenantId_email: { tenantId, email } },
      select: { id: true, passwordHash: true, role: true },
    }),
  );

  if (!user) {
    return res.status(401).json({ error: 'invalid credentials' });
  }

  const valid = await verify(user.passwordHash, password);
  if (!valid) {
    return res.status(401).json({ error: 'invalid credentials' });
  }

  const accessToken = await signAccessToken(user.id, tenantId, user.role);
  const refreshToken = await issueRefreshToken(user.id, tenantId);
  return res.json({ accessToken, refreshToken });
});

app.get('/me', requireAuth, async (req, res) => {
  const user = await withTenant(req.auth!.tenantId, (tx) =>
    tx.user.findUnique({
      where: { id: req.auth!.userId },
      select: { id: true, email: true, tenantId: true, createdAt: true },
    }),
  );
  return res.json(user);
});

app.post('/refresh', async (req, res) => {
  const parsed = z.object({ refreshToken: z.string() }).safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'refreshToken is required' });
  }

  const result = await rotateRefreshToken(parsed.data.refreshToken);

  if (!result.ok) {
    if (result.reason === 'reused') {
      logger.warn('refresh token reuse detected — family revoked');
    }
    return res.status(401).json({ error: 'invalid refresh token' });
  }

  // Re-read the role rather than carrying it in the refresh token. This is why
  // a demotion takes effect within one access-token lifetime instead of lasting
  // the full 30 days: the role is resolved fresh on every rotation.
  const user = await withTenant(result.tenantId, (tx) =>
    tx.user.findUnique({ where: { id: result.userId }, select: { role: true } }),
  );

  if (!user) {
    return res.status(401).json({ error: 'invalid refresh token' });
  }

  const accessToken = await signAccessToken(result.userId, result.tenantId, user.role);
  return res.json({ accessToken, refreshToken: result.token });
});

app.post('/logout', async (req, res) => {
  const parsed = z.object({ refreshToken: z.string() }).safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: 'refreshToken is required' });
  }

  await revokeFamily(parsed.data.refreshToken);
  return res.status(204).send();
});

/**
 * Rate limits for /forgot-password.
 *
 * Two dimensions, because they stop different things. The per-account limit
 * stops one victim being mail-bombed; the per-IP limit stops one attacker
 * walking a list of addresses. Neither substitutes for the other.
 *
 * See rate-limit.ts for what this deliberately is not.
 */
const resetPerAccount = createRateLimiter({ limit: 5, windowMs: 60 * 60 * 1000 });
const resetPerIp = createRateLimiter({ limit: 20, windowMs: 60 * 60 * 1000 });

const forgotSchema = z.object({ email: emailField });

/**
 * POST /forgot-password
 *
 * ALWAYS 202, whatever happened. Unknown address, unknown tenant, rate-limited,
 * user exists — one status, one body, every time. The moment those differ, the
 * endpoint becomes an account-existence oracle for an unauthenticated caller.
 *
 * X-Tenant-ID is required, and keeping it is a deliberate departure from the
 * reasoning that removed the header from /register. There, the header let a
 * caller conjure membership of a tenant they had no claim to — pure privilege
 * escalation. Here it only selects WHICH tenant's user record to look up, and
 * the resulting link still goes to the address on that row, i.e. to the mailbox
 * the real user controls. Naming somebody else's tenant gets you nothing.
 *
 * It is also load-bearing rather than cosmetic: User is unique on
 * (tenantId, email), and the seed deliberately puts alice@acme.com in two
 * tenants. Without a tenant, "reset the password for this address" has no single
 * answer, and the alternative — fan out to every tenant holding the address —
 * means more unauthenticated mail for the same ambiguity.
 */
app.post('/forgot-password', async (req, res) => {
  const tenantId = req.header('X-Tenant-ID');
  if (!tenantId) {
    return res.status(400).json({ error: 'X-Tenant-ID header is required' });
  }

  const parsed = forgotSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  // The only two 400s are shape errors, decided before any lookup happens, so
  // neither reveals whether an account exists.
  const { email } = parsed.data;
  const accepted = { status: 'accepted' };

  // Over budget is answered with the SAME 202. A limiter that announces itself
  // is an oracle of a different kind: "this address is rate-limited" means
  // somebody has been asking about it, which means it is worth asking about.
  const ip = req.ip ?? 'unknown';
  if (!resetPerIp.hit(ip) || !resetPerAccount.hit(`${tenantId}:${email}`)) {
    logger.warn({ tenantId, ip }, 'password reset rate limited');
    return res.status(202).json(accepted);
  }

  const issued = await withTenant(tenantId, async (tx) => {
    const user = await tx.user.findUnique({
      where: { tenantId_email: { tenantId, email } },
      select: { id: true },
    });

    // No such user in this tenant. Write nothing, and say the same thing.
    if (!user) return null;

    // Revoking siblings and creating the new token share this transaction, so a
    // crash between them cannot leave two live resets — or none.
    return issueResetToken(tx, { userId: user.id, tenantId });
  });

  if (issued) {
    emitDevToken(logger, 'password-reset', {
      email,
      tenantId,
      url: `${RESET_URL_BASE}/reset-password?token=${issued.token}`,
    });
  }

  return res.status(202).json(accepted);
});

const resetSchema = z.object({
  token: z.string().min(1),
  password: z.string().min(8),
});

/**
 * POST /reset-password
 *
 * No X-Tenant-ID, on purpose. The tenant comes from the token row, exactly as
 * /register takes it from the invite — the caller states nothing that could be
 * a lie.
 *
 * ONE generic error for every failure. Distinguishing "expired" from "already
 * used" from "never existed" tells someone probing tokens which guesses were
 * close, and matches what /register already does for invites.
 */
app.post('/reset-password', async (req, res) => {
  const parsed = resetSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const { token, password } = parsed.data;
  const generic = { error: 'invalid or expired reset token' };

  const record = await lookupResetToken(token);
  if (!record.ok) {
    return res.status(400).json(generic);
  }

  /**
   * Hashed BEFORE the transaction opens, matching /register.
   *
   * argon2 is deliberately expensive — tens to hundreds of milliseconds of CPU,
   * on a threadpool with four workers by default. Inside the transaction it
   * would hold a User row lock and a pooled connection for that whole time, so a
   * handful of concurrent resets would drain the pool and start failing
   * unrelated requests with P2024.
   *
   * The cost of hashing first is that a caller who loses the race below did the
   * work for nothing. That is the right trade: the guarded claim still decides
   * who wins, and wasted CPU on a losing request is cheaper than a held lock on
   * a winning one.
   */
  const passwordHash = await hash(password);

  try {
    await withTenant(record.tenantId, async (tx) => {
      /**
       * Claim FIRST, and guarded.
       *
       * `usedAt: null` in the WHERE clause makes the check and the write one
       * statement. lookupResetToken above only READ — its answer was already
       * stale when it returned, because nothing was locked. Two concurrent
       * redemptions of one link would both pass it, both write passwordHash, and
       * last-writer-wins: the victim believes they set their password, and the
       * account holds the attacker's.
       *
       * Under READ COMMITTED the loser blocks on this row, re-evaluates its
       * predicate against the committed version, finds usedAt set, and matches
       * nothing. Same mechanism as the invite claim in /register and the
       * inventory guard in ticketing.
       */
      const claimed = await tx.passwordResetToken.updateMany({
        where: { id: record.id, usedAt: null, revokedAt: null },
        data: { usedAt: new Date() },
      });

      if (claimed.count !== 1) throw new Error('reset already claimed');

      /**
       * updateMany plus a count check, not update({ where: { id } }).
       *
       * "User" is the one table here under FORCE ROW LEVEL SECURITY. If this
       * ever runs without app.tenant_id set, the policy predicate is
       * `"tenantId" = NULL`, which is never true, and the row is invisible.
       * update() would throw P2025; updateMany() would report count 0 and carry
       * on — the endpoint would return success, the token would be spent, every
       * session would be revoked, and THE PASSWORD WOULD BE UNCHANGED. The user
       * ends up signed out everywhere and locked out of an account whose reset
       * link is now used.
       *
       * The count check turns that silent success into a loud failure.
       */
      const updated = await tx.user.updateMany({
        where: { id: record.userId, tenantId: record.tenantId },
        data: { passwordHash },
      });

      if (updated.count !== 1) throw new Error('user update matched no rows');

      // Kill this user's other live reset tokens. Without it, a link harvested
      // before the victim noticed anything survives their own recovery.
      await tx.passwordResetToken.updateMany({
        where: { userId: record.userId, usedAt: null, revokedAt: null },
        data: { revokedAt: new Date() },
      });

      /**
       * And every session.
       *
       * This flow cannot tell a convenience reset from a compromise reset — it
       * is unauthenticated, so there is no session to spare even if it wanted to
       * — so it treats both as compromise. The cost is that resetting logs you
       * out of your other devices.
       *
       * This only holds because rotateRefreshToken now claims with a guarded
       * UPDATE. Before that fix a rotation already in flight would commit its
       * successor AFTER this sweep ran, handing an attacker a fresh 30-day token
       * on the account that had just been secured.
       */
      await tx.refreshToken.updateMany({
        where: { userId: record.userId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
    });

    return res.json({ status: 'reset' });
  } catch (err) {
    const message = err instanceof Error ? err.message : '';
    if (message === 'reset already claimed' || message === 'user update matched no rows') {
      return res.status(400).json(generic);
    }
    logger.error({ err }, 'password reset failed');
    return res.status(500).json({ error: 'internal error' });
  }
});

app.listen(env.PORT, () => {
  logger.info(`auth-service listening on port ${env.PORT}`);
});