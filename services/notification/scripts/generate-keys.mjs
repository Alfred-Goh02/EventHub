// Generates Notification's RSA keypair: the one Auth seals email requests to.
// Local development only. Production keys come from a secrets manager.
//
// Two things, both safe to repeat:
//   1. The keypair, ONLY IF ABSENT. Regenerating would make every request already
//      sealed to the old key impossible to open, and a consumer could only
//      dead-letter them.
//   2. A copy of the PUBLIC half in Auth's keys directory, on EVERY run, which is
//      where Auth reads it. Only the public half leaves this directory: Auth seals
//      with it and never has to open what it sealed.
//
// Paths resolve from this file, not the working directory, so it writes to the same
// place from the repository root, from services/notification, and in the setup
// container.

import { generateKeyPairSync } from 'node:crypto';
import { chmodSync, copyFileSync, existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ownDir = fileURLToPath(new URL('../keys/', import.meta.url));
const authDir = fileURLToPath(new URL('../../auth/keys/', import.meta.url));
const privatePath = join(ownDir, 'private.pem');
const publicPath = join(ownDir, 'public.pem');
const copyPath = join(authDir, 'notification-public.pem');

mkdirSync(ownDir, { recursive: true });

if (existsSync(privatePath) && existsSync(publicPath)) {
  console.log(`kept the existing keypair in ${ownDir}`);
} else {
  const { publicKey, privateKey } = generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
  });
  // 0600 on the private half, and chmod as well: the mode argument applies only when
  // a file is created.
  writeFileSync(privatePath, privateKey, { mode: 0o600 });
  chmodSync(privatePath, 0o600);
  writeFileSync(publicPath, publicKey);
  console.log(`wrote ${privatePath} (keep secret) and ${publicPath}`);
}

mkdirSync(authDir, { recursive: true });
copyFileSync(publicPath, copyPath);
console.log(`copied the public half to ${copyPath}, where Auth seals to it`);
