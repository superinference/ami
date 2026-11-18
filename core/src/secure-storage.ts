// ===========================================================================
// Secure storage — reusable OS-native secret-vault BUILDING BLOCKS (core layer).
//
// Claude Code protects secrets at rest behind a platform-aware vault
// (`utils/secureStorage/*`): a `SecureStorage` interface, a macOS Keychain
// backend, a plaintext-file fallback, a `createFallbackStorage` composition, and
// a `getSecureStorage()` platform selector whose Linux arm is an explicit
// `// TODO: add libsecret support for Linux` (see `secureStorage/index.ts`).
//
// This module is AMI's parity + SUPERSET of that Linux TODO: a pluggable,
// platform-independent **libsecret (`secret-tool`) backend** usable on any Linux
// host where the Secret Service CLI is present, so AMI credentials can live in
// the OS keyring (GNOME Keyring / KWallet via libsecret) instead of a plaintext
// file. It is a pure core building block — it only spawns the `secret-tool` CLI
// and has NO dependency on any upper layer — so it belongs in `core/` alongside
// the other primitives; the *selection/management* that composes it with the
// plaintext fallback stays in `common/` (`secure-storage.ts` /
// `provision/secure-storage.ts`), matching AGENTS.md's layer split ("keychain/
// libsecret management lives in common").
//
// Secret hygiene (parity with the macOS keychain backend's stdin design): the
// SECRET payload is passed to `secret-tool store` via STDIN — never argv — so a
// process monitor never observes it; only the non-secret service/account
// attributes appear on the command line.
//
// Determinism: the `secret-tool` invocation is an injectable {@link SecretToolRunner}
// seam, so the whole tier is unit-testable on any platform without spawning a real
// subprocess or touching a real keyring.
// ===========================================================================

import { spawnSync } from 'child_process';

// ---------------------------------------------------------------------------
// Generic secure-storage contract (the canonical, payload-generic home)
// ---------------------------------------------------------------------------

/**
 * Result of a {@link SecureStorage} write (Claude-parity with the secure-storage
 * `update()` contract): a write NEVER throws — it reports `success` and, on a
 * degraded-but-persisted path, a non-fatal `warning` the caller should surface.
 */
export interface SecureStorageWriteResult {
  success: boolean;
  warning?: string;
}

/**
 * A single secret-storage backend, generic over the persisted payload `T`
 * (a structured bag for the plugin/MCP vault, or a bare string for a raw key).
 */
export interface SecureStorage<T> {
  /** Stable, human-readable backend name (used in composed names + diagnostics). */
  readonly name: string;
  /** Read the stored payload, or `null` when none is present / unreadable. */
  read(): T | null;
  /** Persist the payload; returns a structured result instead of throwing. */
  update(data: T): SecureStorageWriteResult;
  /** Delete the payload; `true` when gone afterwards (absent counts as success). */
  delete(): boolean;
  /**
   * Explicitly drop any in-memory read cache so the NEXT {@link read} re-fetches
   * from the backing store (Claude-parity with `clearLegacyApiKeyPrefetch()`).
   *
   * In-process {@link update}/{@link delete} already keep the cache coherent, but a
   * credential can also be rotated OUT OF BAND — another process re-writes the
   * keyring entry — while this instance still holds a value cached for up to
   * {@link LIBSECRET_CACHE_TTL_MS}. That stale cached value would then SHADOW the
   * fresh secret for the rest of the TTL window (e.g. the old AES key, so a store
   * re-encrypted under the rotated key no longer decrypts). A caller that detects
   * such a rotation (e.g. a decrypt failure) invokes `invalidate()` to force a
   * through-read before giving up. Idempotent and never throws.
   */
  invalidate(): void;
}

// ---------------------------------------------------------------------------
// `secret-tool` runner seam
// ---------------------------------------------------------------------------

/** Result of a `secret-tool` invocation (the injectable seam's shape). */
export interface SecretToolRunResult {
  /** Process exit code (`null` when the process was signalled / failed to spawn). */
  status: number | null;
  /** Captured stdout (the secret payload for a `lookup`). */
  stdout: string;
}

/** Injectable `secret-tool` runner — mocked in tests, spawns the CLI by default. */
export type SecretToolRunner = (args: string[], input?: string) => SecretToolRunResult;

/**
 * Default `secret-tool` runner: a synchronous spawn. A spawn failure (the CLI is
 * not installed → `ENOENT`) surfaces as `status: null`, which every caller below
 * treats as "backend unavailable" and degrades through the fallback composition.
 */
export const defaultSecretToolRunner: SecretToolRunner = (args, input) => {
  const res = spawnSync('secret-tool', args, { input, encoding: 'utf-8' });
  return { status: res.status, stdout: res.stdout ?? '' };
};

/**
 * Whether the libsecret backend should be PREFERRED over the plaintext file on
 * Linux. Opt-in by design: unlike macOS — where the OS keychain is ubiquitous and
 * safe to default on — a Linux keyring may be absent, headless, or locked, and
 * silently routing every subsystem's secrets into it (or into the dev session's
 * keyring during tests) would be surprising. Enabling it is an explicit choice via
 * `SUPERINFERENCE_SECURE_STORAGE_LIBSECRET=1` (cross-framework alias
 * `AMI_SECURE_STORAGE_LIBSECRET=1`); when off, the existing plaintext/file tier is
 * used unchanged. The common-layer selectors still verify {@link isSecretToolAvailable}
 * before using it, and always compose a plaintext/file fallback behind it.
 */
export function isLibsecretPreferred(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env.SUPERINFERENCE_SECURE_STORAGE_LIBSECRET ?? env.AMI_SECURE_STORAGE_LIBSECRET;
  return raw === '1' || raw === 'true';
}

// Process-lifetime availability probe cache: whether `secret-tool` is installed
// does not change during a CLI session, and a `--version` spawn is wasteful to
// repeat on every credential read, so probe at most once.
let secretToolAvailableCache: boolean | undefined;

/** Reset the cached `secret-tool` availability probe (test hygiene). */
export function resetSecretToolAvailableCache(): void {
  secretToolAvailableCache = undefined;
}

/**
 * Probe whether the libsecret `secret-tool` CLI is available (and the Secret
 * Service is reachable) by running `secret-tool --version` once. Cached for the
 * process lifetime. Used by the common-layer selector so it only PREFERS the
 * libsecret backend when the keyring is actually usable — otherwise it degrades
 * straight to the plaintext fallback instead of spawning a doomed subprocess on
 * every read. Never throws.
 */
export function isSecretToolAvailable(run: SecretToolRunner = defaultSecretToolRunner): boolean {
  if (secretToolAvailableCache !== undefined) return secretToolAvailableCache;
  try {
    secretToolAvailableCache = run(['--version']).status === 0;
  } catch {
    secretToolAvailableCache = false;
  }
  return secretToolAvailableCache;
}

// ---------------------------------------------------------------------------
// libsecret (`secret-tool`) backend
// ---------------------------------------------------------------------------

/**
 * TTL (ms) bounding libsecret read staleness. A short TTL would expire mid-storm
 * (many connectors authenticating at once) and trigger repeat `secret-tool`
 * spawns; a few seconds of cross-process staleness is fine (parity with the
 * macOS keychain backend's `KEYCHAIN_CACHE_TTL_MS`).
 */
export const LIBSECRET_CACHE_TTL_MS = 30_000;

/** Options for {@link createLibsecretStorage}. */
export interface LibsecretStorageOptions<T> {
  /** Secret Service `service` attribute (the lookup key / namespace). */
  service: string;
  /** Secret Service `account` attribute (the per-user identity). */
  account: string;
  /** Serialize the payload to the string stored in the keyring. */
  serialize: (data: T) => string;
  /** Parse the stored string back into a payload (may throw — caught internally). */
  deserialize: (raw: string) => T;
  /** Human-readable label shown in keyring UIs. Defaults to the service name. */
  label?: string;
  /** `secret-tool` runner seam (default spawns the real CLI). */
  run?: SecretToolRunner;
  /** Read-cache TTL in ms (default {@link LIBSECRET_CACHE_TTL_MS}). */
  cacheTtlMs?: number;
}

/**
 * Linux libsecret backend driven by the `secret-tool` CLI (Secret Service API;
 * GNOME Keyring / KWallet). Closes Claude Code's `// TODO: add libsecret support
 * for Linux`. Design mirrors AMI's macOS keychain backend:
 *
 * - **read** — `secret-tool lookup service <s> account <a>`; a hit (exit 0 with
 *   output) is deserialized + cached; a miss (non-zero exit, no output) is `null`.
 *   A TTL cache bounds spawn storms, and a transient read failure serves the
 *   prior value (stale-while-error) rather than caching `null`.
 * - **update** — `secret-tool store --label <l> service <s> account <a>` with the
 *   SECRET payload on STDIN (never argv, so a process monitor never sees it).
 * - **delete** — `secret-tool clear service <s> account <a>`.
 *
 * Every operation routes through the injectable {@link LibsecretStorageOptions.run}
 * seam and never throws — a failed subprocess degrades to `null` /
 * `{ success: false }` so a fallback composition can fall through to a weaker tier.
 */
export function createLibsecretStorage<T>(opts: LibsecretStorageOptions<T>): SecureStorage<T> {
  const run = opts.run ?? defaultSecretToolRunner;
  const label = opts.label ?? opts.service;
  const ttl = opts.cacheTtlMs ?? LIBSECRET_CACHE_TTL_MS;
  const attrs = ['service', opts.service, 'account', opts.account];
  let cache: { data: T | null; cachedAt: number } = { data: null, cachedAt: 0 };

  return {
    name: 'libsecret',
    read(): T | null {
      const prev = cache;
      if (Date.now() - prev.cachedAt < ttl) return prev.data;
      try {
        const res = run(['lookup', ...attrs]);
        if (res.status === 0 && res.stdout.trim()) {
          const data = opts.deserialize(res.stdout.trim());
          cache = { data, cachedAt: Date.now() };
          return data;
        }
      } catch {
        // malformed payload / spawn error → fall through to stale-while-error
      }
      // Stale-while-error: keep serving a prior value rather than caching null so
      // a single transient `secret-tool` failure never shadows a present secret.
      if (prev.data !== null) {
        cache = { data: prev.data, cachedAt: Date.now() };
        return prev.data;
      }
      cache = { data: null, cachedAt: Date.now() };
      return null;
    },
    update(data: T): SecureStorageWriteResult {
      try {
        // Secret via stdin (process-monitor safe); only non-secret attrs on argv.
        const res = run(['store', '--label', label, ...attrs], opts.serialize(data));
        if (res.status !== 0) return { success: false };
        cache = { data, cachedAt: Date.now() };
        return { success: true };
      } catch {
        return { success: false };
      }
    },
    delete(): boolean {
      cache = { data: null, cachedAt: 0 };
      try {
        return run(['clear', ...attrs]).status === 0;
      } catch {
        return false;
      }
    },
    invalidate(): void {
      // Reset the TTL cache so the next read() re-spawns `secret-tool lookup`
      // instead of serving a value that an out-of-band rotation may have staled.
      cache = { data: null, cachedAt: 0 };
    },
  };
}
