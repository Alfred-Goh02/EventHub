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

const env = loadBaseEnv();
const logger = createLogger('auth-service');

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

  // Until the Notification service exists (Chapter 8), the link is logged
  // rather than emailed. This must never survive into production — a real
  // deployment mails it, and logging a live credential is how it leaks.
  logger.warn({ email, tenantId, token }, 'INVITE (dev only — would be emailed)');

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

      // Same transaction as the user insert: if marking the invite used fails,
      // the user is not created either. Otherwise a crash between the two would
      // leave a live invite that has already been redeemed.
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

app.listen(env.PORT, () => {
  logger.info(`auth-service listening on port ${env.PORT}`);
});