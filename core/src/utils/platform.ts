/**
 * Canonical, typed host-platform detection — the single building block every
 * platform-conditional branch normalizes through, instead of the ad-hoc
 * `process.platform === 'win32'` / `!== 'linux'` string comparisons that were
 * scattered across the tree (repl, bash-sandbox, desktop-notify, tui-selection,
 * system-prompt, …). Parity with Claude Code's `utils/platform.ts` /
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

import { homedir } from 'node:os';
import { join } from 'node:path';

import { isWSL } from './env';
import { log } from '../logger';

/**
 * The normalized host-platform classes AMI reasons about. Mirrors Claude Code's
 * `Platform` type exactly.
 */
export type Platform = 'macos' | 'windows' | 'wsl' | 'linux' | 'unknown';

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

/** Reset the memoized platform + one-shot unknown-log guards (test-only seam). */
export function __resetPlatformCacheForTest(): void {
  cachedPlatform = undefined;
  loggedUnknown.clear();
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
 * port of Claude Code's `getSystemDirectories`.
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
