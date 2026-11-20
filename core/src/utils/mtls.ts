import * as fs from 'fs';
import * as tls from 'node:tls';

/**
 * Mutual-TLS (client-certificate) support.
 *
 * Enterprise API gateways and TLS-inspecting proxies can require the client to
 * present its own certificate (mutual TLS), and often sign the upstream with a
 * private CA that is not in the system trust store. AMI reads the certificate,
 * private key, optional key passphrase, and optional CA bundle from env vars and
 * presents / trusts them on outbound HTTPS connections (direct and through a
 * proxy). Mirrors the reference's CLAUDE_CODE_CLIENT_CERT / _KEY / _KEY_PASSPHRASE
 * knobs, as an additive superset (CA bundle, inline PEM, secure TLS floor).
 */

export interface MTLSMaterial {
  /** PEM contents of the client certificate. */
  cert?: string;
  /** PEM contents of the client private key. */
  key?: string;
  /** Passphrase protecting the private key, if any. */
  passphrase?: string;
  /**
   * PEM contents of a custom CA bundle to trust for the upstream / proxy
   * (parity: private-CA enterprise proxies). May concatenate multiple certs.
   */
  ca?: string;
}

/** Accepted TLS protocol floors for {@link getSecureTLSConnectOptions}. */
export type TLSMinVersion = 'TLSv1' | 'TLSv1.1' | 'TLSv1.2' | 'TLSv1.3';

export const MTLS_ENV = {
  cert: 'AMI_CLIENT_CERT',
  key: 'AMI_CLIENT_KEY',
  passphrase: 'AMI_CLIENT_KEY_PASSPHRASE',
  /** NEW (parity): custom CA bundle (file path or inline PEM) to trust. */
  ca: 'AMI_CLIENT_CA',
  /** NEW (parity): minimum negotiated TLS version (default TLSv1.2). */
  minVersion: 'AMI_CLIENT_TLS_MIN_VERSION',
} as const;

/** Ambient Node CA env var (read natively by Node at startup; merged here too). */
export const NODE_EXTRA_CA_CERTS_ENV = 'NODE_EXTRA_CA_CERTS';

/** Node runtime option flags that opt a process into the OS/system trust store. */
export const SYSTEM_CA_NODE_OPTIONS = ['--use-system-ca', '--use-openssl-ca'] as const;

/**
 * True when `flag` is present in the Node runtime options — either as a process
 * `execArgv` entry or inside the `NODE_OPTIONS` env string (bare `--flag` or
 * `--flag=value` forms). Mirrors the reference's `hasNodeOption` so the
 * `--use-system-ca` / `--use-openssl-ca` toggles are honored regardless of how
 * Node was launched. Pure + injectable for tests.
 */
export function hasNodeOption(
  flag: string,
  execArgv: readonly string[] = process.execArgv,
  nodeOptions: string | undefined = process.env.NODE_OPTIONS,
): boolean {
  const matches = (tok: string): boolean => tok === flag || tok.startsWith(flag + '=');
  if (execArgv.some(matches)) return true;
  if (nodeOptions) return nodeOptions.split(/\s+/).filter(Boolean).some(matches);
  return false;
}

/** Injectable TLS accessors so the trust-store assembler is unit-testable offline. */
export interface CATrustDeps {
  rootCertificates?: readonly string[];
  getSystemCACertificates?: () => readonly string[];
  execArgv?: readonly string[];
}

/**
 * Assemble the CA trust bundle to pin on a CUSTOM undici dispatcher (proxy /
 * mTLS agent). Setting `ca` on an agent REPLACES Node's default store, so when
 * any custom trust material is in play this returns the bundled Mozilla roots as
 * the base and ADDS to them, never replacing — mirroring the reference's
 * getCACertificates (caCerts.ts / mtls.ts / proxy.ts). Composed of, in order:
 *   1. bundled roots (`tls.rootCertificates`) — the default store, preserved;
 *   2. the OS/system trust store (`tls.getCACertificates('system')`) when
 *      `--use-system-ca` / `--use-openssl-ca` is set (best-effort; skipped when
 *      the runtime lacks the API);
 *   3. `NODE_EXTRA_CA_CERTS` file contents (best-effort; a missing file is
 *      skipped, matching Node's own non-fatal handling);
 *   4. the explicit AMI client CA bundle (already-loaded PEM), when provided.
 * Returns `undefined` when NONE of (system-ca option, NODE_EXTRA_CA_CERTS,
 * `materialCa`) is present, so the default no-custom-CA path stays byte-for-byte
 * unchanged (Node still honors NODE_EXTRA_CA_CERTS natively for non-dispatcher
 * connections). Deduplicates while preserving order.
 */
export function assembleCATrustStore(
  env: NodeJS.ProcessEnv = process.env,
  materialCa?: string,
  deps: CATrustDeps = {},
): string[] | undefined {
  const execArgv = deps.execArgv ?? process.execArgv;
  const wantSystem = SYSTEM_CA_NODE_OPTIONS.some(f => hasNodeOption(f, execArgv, env.NODE_OPTIONS));
  const extraPath = env[NODE_EXTRA_CA_CERTS_ENV];
  if (!wantSystem && !extraPath && !materialCa) return undefined;

  const roots = deps.rootCertificates ?? tls.rootCertificates;
  const out: string[] = [...roots];

  if (wantSystem) {
    try {
      const sys = deps.getSystemCACertificates
        ? deps.getSystemCACertificates()
        : (typeof tls.getCACertificates === 'function' ? tls.getCACertificates('system') : []);
      out.push(...sys);
    } catch {
      // Best-effort: fall back to the bundled roots already collected.
    }
  }
  if (extraPath) {
    try {
      out.push(loadPem(extraPath, NODE_EXTRA_CA_CERTS_ENV));
    } catch {
      // Node treats an unreadable NODE_EXTRA_CA_CERTS as a non-fatal warning;
      // match that — the bundled roots still provide a working trust store.
    }
  }
  if (materialCa) out.push(materialCa);

  return [...new Set(out)];
}

/**
 * Log-safe one-line summary of the assembled trust store (cert COUNT + which
 * sources contributed). Private key material never flows through here. Returns
 * `undefined` when no custom trust is assembled, so a caller can skip logging.
 */
export function describeCATrustStore(
  env: NodeJS.ProcessEnv = process.env,
  materialCa?: string,
  deps: CATrustDeps = {},
): string | undefined {
  const bundle = assembleCATrustStore(env, materialCa, deps);
  if (!bundle) return undefined;
  const execArgv = deps.execArgv ?? process.execArgv;
  const sources: string[] = ['bundled'];
  if (SYSTEM_CA_NODE_OPTIONS.some(f => hasNodeOption(f, execArgv, env.NODE_OPTIONS))) sources.push('system');
  if (env[NODE_EXTRA_CA_CERTS_ENV]) sources.push('extra-file');
  if (materialCa) sources.push('client-ca');
  return `${bundle.length} CA cert(s) [${sources.join('+')}]`;
}

/** Secure-by-default TLS protocol floor. */
export const DEFAULT_MIN_TLS_VERSION: TLSMinVersion = 'TLSv1.2';

const VALID_MIN_TLS_VERSIONS: ReadonlySet<string> = new Set<TLSMinVersion>([
  'TLSv1',
  'TLSv1.1',
  'TLSv1.2',
  'TLSv1.3',
]);

/**
 * Resolve one PEM-bearing env value to PEM text.
 *
 * NEW (parity): the value may be EITHER a file path (the original behavior) OR
 * inline PEM content (detected by an embedded `-----BEGIN` marker) — this lets
 * secrets be injected directly via env in CI / Kubernetes without a temp file.
 * A missing/unreadable file throws an error that NAMES the env var and path, so
 * a misconfigured client certificate is a hard, diagnosable error rather than a
 * bare ENOENT with no context (previously the raw fs error bubbled up).
 */
function loadPem(value: string, envName: string): string {
  if (value.includes('-----BEGIN')) return value; // inline PEM, used verbatim
  try {
    return fs.readFileSync(value, 'utf8');
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    throw new Error(`mTLS: failed to read ${envName} from "${value}": ${reason}`);
  }
}

/**
 * Load the mTLS material from the environment, or `undefined` when none of the
 * knobs is set (so the dispatcher path stays byte-for-byte unchanged for the
 * common no-mTLS case). The cert/key/ca env vars are a FILE PATH or inline PEM
 * (see {@link loadPem}); the passphrase is used verbatim. A missing/unreadable
 * file throws a labeled error — a misconfigured client certificate is a hard
 * error, not a silent fallback.
 */
export function getMTLSConfig(env: NodeJS.ProcessEnv = process.env): MTLSMaterial | undefined {
  const certVal = env[MTLS_ENV.cert];
  const keyVal = env[MTLS_ENV.key];
  const caVal = env[MTLS_ENV.ca];
  const passphrase = env[MTLS_ENV.passphrase];
  if (!certVal && !keyVal && !caVal && !passphrase) return undefined;
  const material: MTLSMaterial = {};
  if (certVal) material.cert = loadPem(certVal, MTLS_ENV.cert);
  if (keyVal) material.key = loadPem(keyVal, MTLS_ENV.key);
  if (caVal) material.ca = loadPem(caVal, MTLS_ENV.ca);
  if (passphrase) material.passphrase = passphrase;
  return material;
}

/**
 * Convert loaded mTLS material into the TLS connect-options object accepted by
 * undici's `Agent`/`ProxyAgent` (`connect` / `requestTls`), or `undefined` when
 * there is nothing to present or trust (a bare passphrase is useless). NEW
 * (parity): a CA-only material (custom trust, no client cert) now yields options
 * so a private-CA upstream can be trusted without presenting a client cert.
 */
export function getTLSConnectOptions(
  material: MTLSMaterial | undefined,
): { cert?: string; key?: string; passphrase?: string; ca?: string } | undefined {
  if (!material || (!material.cert && !material.key && !material.ca)) return undefined;
  const opts: { cert?: string; key?: string; passphrase?: string; ca?: string } = {};
  if (material.cert) opts.cert = material.cert;
  if (material.key) opts.key = material.key;
  if (material.passphrase) opts.passphrase = material.passphrase;
  if (material.ca) opts.ca = material.ca;
  return opts;
}

/**
 * NEW (parity): TLS connect options with a secure-by-default protocol floor.
 *
 * Builds on {@link getTLSConnectOptions} but ALWAYS pins `minVersion` (defaults
 * to {@link DEFAULT_MIN_TLS_VERSION}, overridable via `AMI_CLIENT_TLS_MIN_VERSION`).
 * Unlike {@link getTLSConnectOptions} it never returns `undefined`: even with no
 * client material it returns `{ minVersion }` so callers can enforce the floor on
 * every outbound connection. An explicitly-set but invalid min version throws a
 * labeled error (surfaced misconfiguration, never a silent downgrade).
 */
export function getSecureTLSConnectOptions(
  material: MTLSMaterial | undefined,
  env: NodeJS.ProcessEnv = process.env,
): { cert?: string; key?: string; passphrase?: string; ca?: string; minVersion: TLSMinVersion } {
  const requested = env[MTLS_ENV.minVersion];
  let minVersion: TLSMinVersion = DEFAULT_MIN_TLS_VERSION;
  if (requested) {
    if (!VALID_MIN_TLS_VERSIONS.has(requested)) {
      throw new Error(
        `mTLS: invalid ${MTLS_ENV.minVersion}="${requested}" ` +
          `(expected one of ${[...VALID_MIN_TLS_VERSIONS].join(', ')})`,
      );
    }
    minVersion = requested as TLSMinVersion;
  }
  const base = getTLSConnectOptions(material);
  return base ? { ...base, minVersion } : { minVersion };
}

/**
 * NEW (parity): produce a log-safe view of mTLS material. Private secrets (the
 * key and passphrase) are replaced with `[redacted]`; the certificate and CA
 * (public material, but still worth not dumping) are summarized by byte length.
 * Use this anywhere mTLS state is logged or surfaced in diagnostics so key
 * material never leaks into logs.
 */
export function redactMTLSMaterial(
  material: MTLSMaterial | undefined,
): Record<string, string> | undefined {
  if (!material) return undefined;
  const out: Record<string, string> = {};
  if (material.cert) out.cert = `<cert:${material.cert.length}b>`;
  if (material.ca) out.ca = `<ca:${material.ca.length}b>`;
  if (material.key) out.key = '[redacted]';
  if (material.passphrase) out.passphrase = '[redacted]';
  return out;
}
