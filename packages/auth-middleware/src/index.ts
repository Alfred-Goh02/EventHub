import { readFile } from 'node:fs/promises';
import type { NextFunction, Request, RequestHandler, Response } from 'express';
import { isRole, type Role } from '@eventhub/types';
import { importSPKI, jwtVerify, type CryptoKey } from 'jose';

/**
 * Access-token verification, shared by every service that consumes Auth's JWTs.
 *
 * Extracted only after Event had genuinely exercised a local copy. The two
 * copies turned out to differ in exactly ONE way — how the key is obtained.
 * Auth already holds an imported key because it must sign; Event decodes a
 * base64 public key from its environment. Everything else was identical.
 *
 * So the key is a PARAMETER. That is the whole shape of this module, and it is
 * why `createRequireAuth` is a factory rather than a ready-made handler:
 *
 *   - No module-level key loading, so merely importing this package cannot
 *     throw. A top-level `await importSPKI(process.env...)` would fail in every
 *     unit test and one-off script that has not populated the environment, and
 *     would inject a top-level await into seven module graphs.
 *   - Auth passes the key it already imported from disk; nothing is duplicated
 *     and no service needs a second copy of a key it already has.
 */

const ALG = 'RS256';

/**
 * MUST live in this file — the file `exports["."].types` points at.
 *
 * TypeScript only applies a global augmentation that a consumer actually loads.
 * Put this in a hand-written `.d.ts` and tsc will not emit it to dist/ at all;
 * put it in a separate module nothing imports and it is emitted but never
 * loaded. Both fail silently: the package builds green and the CONSUMER fails
 * with "Property 'auth' does not exist on type 'Request'", which is the worst
 * place to debug it.
 */
declare global {
  namespace Express {
    interface Request {
      auth?: AuthContext;
    }
  }
}

export interface AuthContext {
  userId: string;
  tenantId: string;
  role: Role;
}

/**
 * isRole comes from @eventhub/types now, and does not live here.
 *
 * This file used to keep its own literal list alongside the imported Role
 * union. Nothing tied the two together, so adding a role to the union and
 * forgetting the list would have compiled cleanly and then rejected every
 * request carrying the new role -- a 401, in every service at once, from a
 * middleware that looks correct.
 *
 * In @eventhub/types the array IS the definition and the union is derived from
 * it, so the two cannot disagree.
 */

/**
 * Decodes a base64 SPKI PEM into a verifying key.
 *
 * Base64 rather than a raw PEM in the environment because a PEM contains
 * newlines, and newlines break `.env` parsing, Helm values and Terraform
 * variables in three different ways.
 */
export async function publicKeyFromBase64(b64: string): Promise<CryptoKey> {
  return importSPKI(Buffer.from(b64, 'base64').toString('utf8'), ALG);
}

/**
 * Auth's public key from the environment, as either a base64 value or a path.
 *
 * TWO WAYS BECAUSE THERE ARE TWO SITUATIONS, not because one is a fallback.
 *
 *   AUTH_PUBLIC_KEY_B64   the key itself. How a deployment injects it, and what
 *                         CI and a laptop use: one value, no shared filesystem.
 *   AUTH_PUBLIC_KEY_FILE  a path to the PEM. What docker compose uses, because
 *                         the keypair does not exist until a setup step has
 *                         generated it, and compose cannot put a value produced
 *                         by one container into another container's environment.
 *                         A shared volume is the only thing both can see.
 *
 * Exactly one, and it REFUSES when both are set rather than silently preferring
 * either. Two sources of the verifying key that disagree is a service that
 * accepts tokens the operator thinks it rejects, and the failure would surface
 * as a 401 nobody can explain.
 *
 * Throws rather than returning undefined. A service that cannot verify a token
 * must not start: with no key it would either reject every request or, far
 * worse, be edited later into treating an absent key as "skip verification".
 */
export async function publicKeyFromEnv(env = process.env): Promise<CryptoKey> {
  const b64 = env['AUTH_PUBLIC_KEY_B64'];
  const file = env['AUTH_PUBLIC_KEY_FILE'];

  if (b64 && file) {
    throw new Error(
      'AUTH_PUBLIC_KEY_B64 and AUTH_PUBLIC_KEY_FILE are both set. Set exactly one: two ' +
        'sources for the verifying key can disagree, and the symptom is a 401 with no ' +
        'explanation.',
    );
  }
  if (b64) return publicKeyFromBase64(b64);
  if (file) {
    const pem = await readFile(file, 'utf8');
    return importSPKI(pem, ALG);
  }
  throw new Error(
    'AUTH_PUBLIC_KEY_B64 or AUTH_PUBLIC_KEY_FILE is required. Without Auth\'s public key this ' +
      'service cannot tell a genuine token from a forged one.',
  );
}

export interface RequireAuthOptions {
  /** Auth's PUBLIC key. Verifies tokens; cannot mint them. */
  publicKey: CryptoKey;
  /** Rejected unless the token's `iss` matches exactly. */
  issuer: string;
  /** Rejected unless the token's `aud` matches exactly. */
  audience: string;
}

export function createRequireAuth(opts: RequireAuthOptions): RequestHandler {
  return async (req: Request, res: Response, next: NextFunction) => {
    const header = req.header('Authorization');

    if (!header?.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'missing bearer token' });
    }

    try {
      const { payload } = await jwtVerify(header.slice('Bearer '.length), opts.publicKey, {
        // All three are required, not decorative.
        //
        // `algorithms` is the security-critical one: without it an attacker can
        // present a token signed HS256 using the PUBLIC key as the shared
        // secret — and the public key is, by definition, public.
        //
        // `issuer` and `audience` are ENFORCED here rather than read and
        // checked later, because a claim nobody verifies is decoration.
        algorithms: [ALG],
        issuer: opts.issuer,
        audience: opts.audience,
      });

      // The signature proves the token is ours and unaltered. It says nothing
      // about the payload having the shape this code expects — a token minted
      // before `role` existed would arrive with role undefined and flow onward
      // as though it were a real value.
      if (!payload.sub || typeof payload.tenantId !== 'string' || !isRole(payload.role)) {
        return res.status(401).json({ error: 'malformed token' });
      }

      req.auth = {
        userId: payload.sub,
        tenantId: payload.tenantId,
        role: payload.role,
      };
      next();
    } catch {
      return res.status(401).json({ error: 'invalid or expired token' });
    }
  };
}

/**
 * Authorization, kept deliberately separate from authentication.
 *
 * requireAuth answers "who is this?". requireRole answers "may they do this?".
 * Keeping them apart matters because tenant isolation is the DATABASE's job via
 * RLS, and permission is the application's. Fold them together and the day
 * someone adds an admin override by deleting a role check, tenant isolation
 * still holds — so nothing looks broken while every user can edit everything.
 */
export function requireRole(...allowed: Role[]): RequestHandler {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!req.auth) return res.status(401).json({ error: 'not authenticated' });
    if (!allowed.includes(req.auth.role)) {
      return res.status(403).json({ error: 'insufficient role' });
    }
    next();
  };
}

/**
 * Reads req.auth or throws.
 *
 * Saves every handler writing `req.auth!`, which is a non-null assertion the
 * type system cannot check — it is only true because of how the route was
 * wired, and a route that forgets requireAuth compiles perfectly.
 */
export function getAuth(req: Request): AuthContext {
  if (!req.auth) {
    throw new Error('getAuth called on a route that is not behind requireAuth');
  }
  return req.auth;
}
