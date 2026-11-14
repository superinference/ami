import * as fs from 'fs';

/**
 * Mutual-TLS (client-certificate) support.
 *
 * Enterprise API gateways and TLS-inspecting proxies can require the client to
 * present its own certificate (mutual TLS). AMI reads the certificate, private
 * key, and optional key passphrase from three env vars and presents them on
 * outbound HTTPS connections (direct and through a proxy). Mirrors Claude Code's
 * CLAUDE_CODE_CLIENT_CERT / _KEY / _KEY_PASSPHRASE knobs.
 */

export interface MTLSMaterial {
  /** PEM contents of the client certificate. */
  cert?: string;
  /** PEM contents of the client private key. */
  key?: string;
  /** Passphrase protecting the private key, if any. */
  passphrase?: string;
}

export const MTLS_ENV = {
  cert: 'AMI_CLIENT_CERT',
  key: 'AMI_CLIENT_KEY',
  passphrase: 'AMI_CLIENT_KEY_PASSPHRASE',
} as const;

/**
 * Load the mTLS material from the environment, or `undefined` when none of the
 * three knobs is set (so the dispatcher path stays byte-for-byte unchanged for
 * the common no-mTLS case). The cert/key env vars are FILE PATHS, read as PEM;
 * the passphrase is used verbatim. A missing/unreadable file throws — a
 * misconfigured client certificate is a hard error, not a silent fallback.
 */
export function getMTLSConfig(env: NodeJS.ProcessEnv = process.env): MTLSMaterial | undefined {
  const certPath = env[MTLS_ENV.cert];
  const keyPath = env[MTLS_ENV.key];
  const passphrase = env[MTLS_ENV.passphrase];
  if (!certPath && !keyPath && !passphrase) return undefined;
  const material: MTLSMaterial = {};
  if (certPath) material.cert = fs.readFileSync(certPath, 'utf8');
  if (keyPath) material.key = fs.readFileSync(keyPath, 'utf8');
  if (passphrase) material.passphrase = passphrase;
  return material;
}

/**
 * Convert loaded mTLS material into the TLS connect-options object accepted by
 * undici's `Agent`/`ProxyAgent` (`connect` / `requestTls`), or `undefined` when
 * there is no actual certificate/key to present (a bare passphrase is useless).
 */
export function getTLSConnectOptions(
  material: MTLSMaterial | undefined,
): { cert?: string; key?: string; passphrase?: string } | undefined {
  if (!material || (!material.cert && !material.key)) return undefined;
  const opts: { cert?: string; key?: string; passphrase?: string } = {};
  if (material.cert) opts.cert = material.cert;
  if (material.key) opts.key = material.key;
  if (material.passphrase) opts.passphrase = material.passphrase;
  return opts;
}
