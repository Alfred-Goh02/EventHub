// Generates the RS256 keypair used to sign and verify JWTs.
// Local development only — production keys come from a secrets manager.

import { generateKeyPairSync } from 'node:crypto';
import { mkdirSync, writeFileSync, chmodSync } from 'node:fs';

const { publicKey, privateKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'spki', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
});

mkdirSync('keys', { recursive: true });
// 0600 on the private half: readable by the user that owns it and nobody else.
// The default is 0644, which is the right mode for the public key and the wrong
// one for a signing key — and it is the mode every other process in the same
// container, or every other account on a shared machine, gets to use.
//
// The mode argument only applies when the file is CREATED, so a key written
// before this line existed keeps 0644. chmod is explicit for that reason.
writeFileSync('keys/private.pem', privateKey, { mode: 0o600 });
chmodSync('keys/private.pem', 0o600);
writeFileSync('keys/public.pem', publicKey);

console.log('wrote keys/private.pem (keep secret) and keys/public.pem (shareable)');
