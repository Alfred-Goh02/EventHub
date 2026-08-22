import { readFileSync } from 'node:fs';
import { importPKCS8, importSPKI, SignJWT, jwtVerify } from 'jose';

const ALG = 'RS256';
const ACCESS_TTL = '15m';

const privateKey = await importPKCS8(readFileSync('keys/private.pem', 'utf8'), ALG);
const publicKey = await importSPKI(readFileSync('keys/public.pem', 'utf8'), ALG);

export async function signAccessToken(userId: string, tenantId: string): Promise<string> {
  return new SignJWT({ tenantId })
    .setProtectedHeader({ alg: ALG })
    .setSubject(userId)
    .setIssuedAt()
    .setExpirationTime(ACCESS_TTL)
    .sign(privateKey);
}

export async function verifyAccessToken(token: string) {
  const { payload } = await jwtVerify(token, publicKey, { algorithms: [ALG] });
  return payload;
}