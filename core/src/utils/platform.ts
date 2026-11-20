/**
 * Canonical, typed host-platform detection — the single building block every
 * platform-conditional branch normalizes through, instead of the ad-hoc
 * `process.platform === 'win32'` / `!== 'linux'` string comparisons that were
 * scattered across the tree (repl, bash-sandbox, desktop-notify, tui-selection,
 * system-prompt, …). Parity with the reference's `utils/platform.ts` /
 * `utils/systemDirectories.ts`:
 *
 *   - {@link getPlatform} returns the five-member {@link Platform} enum
 *     (`macos | windows | wsl | linux | unknown`), crucially distinguishing
 *     `wsl` (a Linux kernel under Windows Subsystem for Linux, whose path /
 *     interop behavior differs from native Linux) from `linux`, and surfacing a
 *     dedicated `unknown` for the long tail (freebsd/sunos/aix/…) rather than
 *     silently defaulting. An unrecognized runtime emits ONE debug log so it is
 *     observable rather than silently mis-handled (CC's
 *     `logForDebugging('Unknown platform detected, using default paths')`).
 *   - {@link getSystemDirectories} resolves the user's Desktop/Documents/Downloads
 *     per platform (XDG on linux/wsl, USERPROFILE on windows, safe defaults on
 *     macos/unknown), the faithful port of CC's `getSystemDirectories`.
 *
 * WSL detection is delegated to the ONE canonical probe ({@link isWSL} in
 * `utils/env`) so the check can never drift between call sites (§0b.6 — no
 * duplicated platform probe). The logger is injectable (default {@link log}) so
 * the debug-on-unknown path is unit-testable without touching disk.
 */

import { statSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';

import { isWSL } from './env';
import { findOnPath } from './which';
import { log } from '../logger';

/**
 * The normalized host-platform classes AMI reasons about. Mirrors the reference's
 * `Platform` type exactly.
 */
export type Platform = 'macos' | 'windows' | 'wsl' | 'linux' | 'unknown';

/**
 * The allow-list of host platforms AMI officially supports — the single building
 * block any platform-gating decision (startup warning, capability check, feature
 * guard) normalizes through instead of ad-hoc `=== 'win32'` comparisons. Parity
 * with the reference's `SUPPORTED_PLATFORMS` config constant (`utils/platform.ts`),
 * which drives its platform-gating/warnings.
 *
 * AMI is a unix-first CLI whose bash/tooling path assumes a POSIX host, so the
 * supported set is `macos`, `linux`, and `wsl` (a Linux kernel under WSL). This is
 * an intentional, documented divergence from the reference's Desktop-integration list
 * (`['macos', 'wsl']`, which gates a macOS/WSL-only Claude Desktop MCP feature AMI
 * has no equivalent of): AMI's allow-list reflects the platforms AMI itself runs on,
 * so native `windows` and the `unknown` long-tail (freebsd/sunos/aix/…) are the
 * excluded, best-effort cases. Frozen so the allow-list can never be mutated at a
 * call site.
 */
export const SUPPORTED_PLATFORMS: readonly Platform[] = Object.freeze([
  'macos',
  'linux',
  'wsl',
]);

/**
 * True when the given platform (default: the detected host via {@link getPlatform})
 * is in the {@link SUPPORTED_PLATFORMS} allow-list. The single predicate every
 * platform-gating decision should call so the "is this host officially supported"
 * question can never drift between sites. Parity with the reference's
 * `SUPPORTED_PLATFORMS.includes(getPlatform())` gating checks.
 */
export function isSupportedPlatform(platform: Platform = getPlatform()): boolean {
  return SUPPORTED_PLATFORMS.includes(platform);
}

/** Minimal shape of the debug logger (matches {@link log}), kept injectable for tests. */
type DebugLogger = (component: string, msg: string, data?: Record<string, unknown>) => void;

/** Options for {@link getPlatform} — every input is injectable for deterministic tests. */
export interface GetPlatformOptions {
  /** Raw `process.platform`-style id to classify; defaults to `process.platform`. */
  platform?: string;
  /** Environment used by the WSL probe; defaults to `process.env`. */
  env?: NodeJS.ProcessEnv;
  /** WSL detector seam; defaults to the canonical {@link isWSL}. */
  isWsl?: (env?: NodeJS.ProcessEnv) => boolean;
  /** Debug logger seam; defaults to {@link log}. */
  log?: DebugLogger;
}

// Memoized classification for the zero-options production path (the raw
// `process.platform` is constant for a process, so re-probing /proc or env every
// call is wasteful — CC memoizes getPlatform for the same reason).
let cachedPlatform: Platform | undefined;

// Distinct unclassifiable ids already logged, so an `unknown` host is reported
// EXACTLY ONCE per distinct value (not on every classify call) — observable
// without flooding the debug log.
const loggedUnknown = new Set<string>();

/**
 * Classify a host into the typed {@link Platform}. With no options this reads
 * `process.platform` + the canonical {@link isWSL} probe and memoizes the result;
 * pass options to classify an explicit id (e.g. a host-platform override) or to
 * inject the WSL/log seams in tests. An unrecognized platform returns `'unknown'`
 * and emits one debug log per distinct id so the runtime is observable.
 */
export function getPlatform(opts?: GetPlatformOptions): Platform {
  if (!opts && cachedPlatform !== undefined) return cachedPlatform;

  const raw = opts?.platform ?? process.platform;
  const wslProbe = opts?.isWsl ?? isWSL;

  let result: Platform;
  if (raw === 'darwin') {
    result = 'macos';
  } else if (raw === 'win32') {
    result = 'windows';
  } else if (raw === 'linux') {
    result = wslProbe(opts?.env) ? 'wsl' : 'linux';
  } else {
    result = 'unknown';
    const key = String(raw);
    if (!loggedUnknown.has(key)) {
      loggedUnknown.add(key);
      (opts?.log ?? log)('platform', 'Unknown platform detected, using default paths', { platform: key });
    }
  }

  if (!opts) cachedPlatform = result;
  return result;
}

// Memoized musl/npm-source probes for the zero-options production path (libc and
// the resolved npm location are fixed for a process, so re-stat'ing /lib or
// re-scanning PATH every call is wasteful — CC memoizes both for the same reason).
let cachedMusl: boolean | undefined;
let cachedNpmFromWindows: boolean | undefined;

/** Reset the memoized platform + one-shot unknown-log guards (test-only seam). */
export function __resetPlatformCacheForTest(): void {
  cachedPlatform = undefined;
  cachedMusl = undefined;
  cachedNpmFromWindows = undefined;
  loggedUnknown.clear();
}

/** Options for {@link isMuslEnvironment} — every input is injectable for tests. */
export interface MuslOptions {
  /** Raw `process.platform`-style id; defaults to `process.platform`. */
  platform?: string;
  /** `process.arch`-style id; defaults to `process.arch`. */
  arch?: string;
  /** Existence probe for the musl loader path; defaults to a `statSync` check. */
  fileExists?: (p: string) => boolean;
}

/** Default musl-loader existence probe: the file is stat-able (error-safe → false). */
function defaultMuslProbe(p: string): boolean {
  try { statSync(p); return true; } catch { return false; }
}

/**
 * True when the host's C library is musl (Alpine and other musl distros) rather
 * than glibc. Non-Linux always returns `false`. Detection stats the
 * architecture-specific musl loader (`/lib/libc.musl-<arch>.so.1`, with
 * `x64→x86_64` / `arm64→aarch64` normalization), error-safe so an unreadable
 * `/lib` degrades to `false` (glibc). The zero-options path is memoized (libc is
 * fixed for a process); pass options to classify an explicit host or inject the
 * stat seam in tests. Parity with the reference's `isMuslEnvironment`
 * (`utils/envDynamic.ts`) runtime fallback — AMI is unbundled node, so it always
 * uses the runtime stat (CC's compile-time IS_LIBC_MUSL/GLIBC flags don't apply).
 */
export function isMuslEnvironment(opts?: MuslOptions): boolean {
  if (!opts && cachedMusl !== undefined) return cachedMusl;

  const platform = opts?.platform ?? process.platform;
  let result: boolean;
  if (platform !== 'linux') {
    result = false;
  } else {
    const arch = opts?.arch ?? process.arch;
    const muslArch = arch === 'x64' ? 'x86_64' : arch === 'arm64' ? 'aarch64' : arch;
    result = (opts?.fileExists ?? defaultMuslProbe)(`/lib/libc.musl-${muslArch}.so.1`);
  }

  if (!opts) cachedMusl = result;
  return result;
}

/** Options for {@link isNpmFromWindowsPath} — every input is injectable for tests. */
export interface NpmWindowsPathOptions {
  /** Environment for the WSL probe + PATH scan; defaults to `process.env`. */
  env?: NodeJS.ProcessEnv;
  /** WSL detector seam; defaults to the canonical {@link isWSL}. */
  isWsl?: (env?: NodeJS.ProcessEnv) => boolean;
  /** npm resolver seam; defaults to the no-spawn {@link findOnPath}. */
  resolve?: (command: string, env: NodeJS.ProcessEnv) => string | null;
}

/**
 * True when — and only when — running inside WSL AND the resolved `npm`
 * executable lives on the mounted Windows filesystem (`/mnt/c/…`). Running the
 * Windows npm from inside WSL is a common cause of very slow installs and broken
 * native modules (it crosses the 9p filesystem boundary and targets the wrong
 * platform's binaries), so this flags it for a diagnostic. Off-WSL it is always
 * `false` — the question is meaningless on a native host. The zero-options path is
 * memoized (the resolved npm location is fixed for a process); pass options to
 * inject the WSL/resolver seams in tests. Parity with the reference's
 * `isNpmFromWindowsPath` (`utils/env.ts`).
 */
export function isNpmFromWindowsPath(opts?: NpmWindowsPathOptions): boolean {
  if (!opts && cachedNpmFromWindows !== undefined) return cachedNpmFromWindows;

  let result: boolean;
  try {
    const env = opts?.env ?? process.env;
    if (!(opts?.isWsl ?? isWSL)(env)) {
      result = false;
    } else {
      const cmd = (opts?.resolve ?? findOnPath)('npm', env);
      result = cmd !== null && cmd.startsWith('/mnt/c/');
    }
  } catch {
    result = false;
  }

  if (!opts) cachedNpmFromWindows = result;
  return result;
}

/** Cross-platform user directories (parity with CC's `SystemDirectories`). */
export interface SystemDirectories {
  HOME: string;
  DESKTOP: string;
  DOCUMENTS: string;
  DOWNLOADS: string;
}

/** Options for {@link getSystemDirectories} — all injectable for deterministic tests. */
export interface SystemDirectoriesOptions {
  /** Environment to read the XDG / USERPROFILE vars from; defaults to `process.env`. */
  env?: NodeJS.ProcessEnv;
  /** Home directory; defaults to `os.homedir()`. */
  homedir?: string;
  /** Pre-resolved platform; defaults to {@link getPlatform}. */
  platform?: Platform;
  /** Debug logger seam; defaults to {@link log}. */
  log?: DebugLogger;
}

/**
 * Resolve the user's Desktop/Documents/Downloads directories, handling the
 * per-platform conventions (XDG Base Directory on linux/wsl, localized
 * USERPROFILE folders on windows, standard `~/Desktop` etc. on macos). An
 * `unknown` platform falls back to the safe defaults AND emits a debug log so an
 * unrecognized runtime is observable rather than silently mis-handled. Faithful
 * port of the reference's `getSystemDirectories`.
 */
export function getSystemDirectories(opts?: SystemDirectoriesOptions): SystemDirectories {
  const platform = opts?.platform ?? getPlatform();
  const homeDir = opts?.homedir ?? homedir();
  const env = opts?.env ?? process.env;

  const defaults: SystemDirectories = {
    HOME: homeDir,
    DESKTOP: join(homeDir, 'Desktop'),
    DOCUMENTS: join(homeDir, 'Documents'),
    DOWNLOADS: join(homeDir, 'Downloads'),
  };

  switch (platform) {
    case 'windows': {
      // Windows: USERPROFILE handles localized folder names (Bureau, Escritorio…).
      const userProfile = env.USERPROFILE || homeDir;
      return {
        HOME: homeDir,
        DESKTOP: join(userProfile, 'Desktop'),
        DOCUMENTS: join(userProfile, 'Documents'),
        DOWNLOADS: join(userProfile, 'Downloads'),
      };
    }

    case 'linux':
    case 'wsl': {
      // Linux/WSL: honor the XDG Base Directory specification when present.
      return {
        HOME: homeDir,
        DESKTOP: env.XDG_DESKTOP_DIR || defaults.DESKTOP,
        DOCUMENTS: env.XDG_DOCUMENTS_DIR || defaults.DOCUMENTS,
        DOWNLOADS: env.XDG_DOWNLOAD_DIR || defaults.DOWNLOADS,
      };
    }

    case 'macos':
    default: {
      // macOS and unknown platforms use standard paths; an unknown host is logged.
      if (platform === 'unknown') {
        (opts?.log ?? log)('platform', 'Unknown platform detected, using default paths');
      }
      return defaults;
    }
  }
}
