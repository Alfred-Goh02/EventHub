import { readFileSync } from 'node:fs';
import { importPKCS8, importSPKI, SignJWT, jwtVerify } from 'jose';
import type { Role } from '@eventhub/types';

const ALG = 'RS256';
const ACCESS_TTL = '15m';

// Who mints tokens, and who they are for. Both are VERIFIED on the way back in,
// not merely stamped on the way out — a claim nobody checks is decoration.
//   iss  lets a verifier reject a token minted by some other system.
//   aud  stops a token issued for this API being replayed at a different one
//        that happens to trust the same key.
const ISSUER = process.env['JWT_ISSUER'];
const AUDIENCE = process.env['JWT_AUDIENCE'];

if (!ISSUER || !AUDIENCE) {
  throw new Error('JWT_ISSUER and JWT_AUDIENCE are required');
}

// Exported AFTER the guard, so control-flow narrowing gives these a plain
// `string` type. Declaring them earlier would need a `!` assertion on values
// that have not been checked yet — which would still work at runtime, but the
// assertion would be a claim the code had not yet earned.
export const jwtIssuer: string = ISSUER;
export const jwtAudience: string = AUDIENCE;

// Resolved relative to THIS FILE, not the working directory. `keys/private.pem`
// only worked because dev happens to run from services/auth; it would throw
// ENOENT from the repo root, or in a container with a different WORKDIR.
// ../keys resolves correctly from both src/ and the compiled dist/.
const keyPath = (name: string) => new URL(`../keys/${name}`, import.meta.url);

const privateKey = await importPKCS8(readFileSync(keyPath('private.pem'), 'utf8'), ALG);
export const publicKey = await importSPKI(readFileSync(keyPath('public.pem'), 'utf8'), ALG);

export async function signAccessToken(
  userId: string,
  tenantId: string,
  role: Role,
): Promise<string> {
  return new SignJWT({ tenantId, role })
    .setProtectedHeader({ alg: ALG })
    .setSubject(userId)
    .setIssuer(ISSUER!)
    .setAudience(AUDIENCE!)
    .setIssuedAt()
    .setExpirationTime(ACCESS_TTL)
    .sign(privateKey);
}

export async function verifyAccessToken(token: string) {
  // Passing issuer and audience here makes jose REJECT tokens that do not match,
  // rather than just returning them for us to check and forget to.
  // `algorithms` is the important one: without it an attacker can present a
  // token signed HS256 using the PUBLIC key as the shared secret.
  const { payload } = await jwtVerify(token, publicKey, {
    algorithms: [ALG],
    issuer: ISSUER!,
    audience: AUDIENCE!,
  });
  return payload;
}
