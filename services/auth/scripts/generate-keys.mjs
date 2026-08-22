// Generates the RS256 keypair used to sign and verify JWTs.
// Local development only — production keys come from a secrets manager.

import { generateKeyPairSync } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';

const { publicKey, privateKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'spki', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
});

mkdirSync('keys', { recursive: true });
writeFileSync('keys/private.pem', privateKey);
writeFileSync('keys/public.pem', publicKey);

console.log('wrote keys/private.pem (keep secret) and keys/public.pem (shareable)');
