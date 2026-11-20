import {
  assembleCATrustStore,
  hasNodeOption,
  SYSTEM_CA_NODE_OPTIONS,
  NODE_EXTRA_CA_CERTS_ENV,
  type CATrustDeps,
} from './utils/mtls';
import { log } from './logger';

/**
 * Memoized CA-certificate resolution + a runtime invalidation hook.
 *
 * The actual trust-store assembly (bundled Mozilla roots + optional system store
 * + `NODE_EXTRA_CA_CERTS` file contents) is the single canonical
 * {@link assembleCATrustStore} in `utils/mtls.ts`; this module adds ONLY a memo
 * in front of the env-only (no client-cert material) resolution plus an explicit
 * {@link clearCACertsCache} entry point. Parity: the reference's
 * `getCACertificates` / `clearCACertsCache` (utils/caCerts.ts) — it memoizes the
 * same env-driven resolution and exposes a cache-clear so a mid-session trust
 * change (e.g. a settings/trust-apply path writing `NODE_EXTRA_CA_CERTS`) can
 * take effect WITHOUT restarting the process.
 *
 * Previously AMI had no CA-resolution cache and no invalidation hook at all: the
 * only CA-affecting runtime state was the per-dispatcher proxy/mTLS agent cache
 * (provider.ts), so a CA change applied mid-session could not take effect without
 * a restart. {@link clearCACertsCache} closes that gap and is fired alongside the
 * dispatcher-cache clear on the settings-env re-apply path (env-loader.ts).
 */

/**
 * Cheap signature of the env inputs that determine the resolved CA bundle:
 * whether a `--use-system-ca` / `--use-openssl-ca` flag is in effect, and the
 * `NODE_EXTRA_CA_CERTS` path. Two calls with the same signature resolve to the
 * same bundle — UNLESS the certificate FILE at that path (or the system store)
 * changed in place, which is exactly what {@link clearCACertsCache} is for.
 */
function caSignature(env: NodeJS.ProcessEnv): string {
  const wantSystem = SYSTEM_CA_NODE_OPTIONS.some(f =>
    hasNodeOption(f, process.execArgv, env.NODE_OPTIONS),
  );
  return `${wantSystem ? '1' : '0'}\u0000${env[NODE_EXTRA_CA_CERTS_ENV] ?? ''}`;
}

const caCache = new Map<string, string[] | undefined>();

/**
 * Resolve the env-driven CA trust bundle (bundled roots + optional system store +
 * `NODE_EXTRA_CA_CERTS`), memoized by the env signature. Returns `undefined` when
 * no custom trust is configured (the default no-override path). Identical in value
 * to `assembleCATrustStore(env, undefined)` — this is a caching facade over it, so
 * repeated dispatcher construction does not re-read the extra-cert file every time.
 *
 * `deps` (injected TLS accessors for offline unit tests) BYPASS the cache so a
 * deps-driven test never pollutes the real-process memo and vice versa.
 */
export function getCACertificates(
  env: NodeJS.ProcessEnv = process.env,
  deps?: CATrustDeps,
): string[] | undefined {
  if (deps) return assembleCATrustStore(env, undefined, deps);
  const key = caSignature(env);
  const cached = caCache.get(key);
  if (cached !== undefined || caCache.has(key)) return cached;
  const value = assembleCATrustStore(env, undefined);
  caCache.set(key, value);
  return value;
}

/**
 * Invalidate the memoized CA trust store. Call this when state that affects CA
 * resolution may have changed at runtime — e.g. `NODE_EXTRA_CA_CERTS` / `NODE_OPTIONS`
 * were (re)written by a settings/trust-apply path, or the pinned cert file's
 * contents changed in place. Fired alongside `clearProxyAgentCache()` on the
 * settings-env re-apply path so the next request re-resolves the trust store and
 * rebuilds its dispatcher against the new CA material.
 */
export function clearCACertsCache(): void {
  const had = caCache.size;
  caCache.clear();
  if (had > 0) {
    log('provider', `cleared memoized CA trust store (${had} entr${had === 1 ? 'y' : 'ies'}) after env reconfiguration`);
  }
}

/**
 * Diagnostic: number of distinct env signatures currently memoized. Lets a
 * reconfiguration path (or a test) observe that the CA memo was dropped. Mirrors
 * the companion `proxyAgentCacheSize()` for the dispatcher cache.
 */
export function caCertsCacheSize(): number {
  return caCache.size;
}
