import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { hash, verify } from '@node-rs/argon2';
import { z } from 'zod';
import { withTenant } from './db.js';
import { Prisma } from './generated/prisma/client.js';
import { signAccessToken } from './jwt.js';
import { requireAuth } from './middleware/auth.js';
import { issueRefreshToken, rotateRefreshToken, revokeFamily } from './tokens.js';

const env = loadBaseEnv();
const logger = createLogger('auth-service');

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'auth' });
});

const registerSchema = z.object({
  email: z.string().email(),
  password: z.string().min(8),
});

app.post('/register', async (req, res) => {
  const tenantId = req.header('X-Tenant-ID');
  if (!tenantId) {
    return res.status(400).json({ error: 'X-Tenant-ID header is required' });
  }

  const parsed = registerSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const { email, password } = parsed.data;
  const passwordHash = await hash(password);

  try {
    const user = await withTenant(tenantId, (tx) =>
      tx.user.create({
        data: { tenantId, email, passwordHash },
        select: { id: true, email: true, createdAt: true },
      }),
    );
    return res.status(201).json(user);
  } catch (err) {
  if (err instanceof Prisma.PrismaClientKnownRequestError) {
    if (err.code === 'P2002') {
      return res.status(409).json({ error: 'email already registered' });
    }
    if (err.code === 'P2003') {
      return res.status(400).json({ error: 'unknown tenant' });
    }
  }
  logger.error({ err }, 'register failed');
  return res.status(500).json({ error: 'internal error' });
}
});

const loginSchema = z.object({
  email: z.string().email(),
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
      select: { id: true, passwordHash: true },
    }),
  );

  if (!user) {
    return res.status(401).json({ error: 'invalid credentials' });
  }

  const valid = await verify(user.passwordHash, password);
  if (!valid) {
    return res.status(401).json({ error: 'invalid credentials' });
  }

  const accessToken = await signAccessToken(user.id, tenantId);
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

  const accessToken = await signAccessToken(result.userId, result.tenantId);
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