import { homedir as osHomedir } from 'node:os';
import { join } from 'node:path';

/**
 * The platform classes {@link getSystemDirectories} resolves user folders for.
 * A structural string-literal union identical to the typed platform the rest of
 * the codebase reasons about, so callers can pass their own `getPlatform()`
 * result directly. Kept local (rather than imported from an upper layer) because
 * `core/` must not depend on `common/`.
 */
export type SystemDirectoryPlatform =
  | 'macos'
  | 'windows'
  | 'wsl'
  | 'linux'
  | 'unknown';

/**
 * The user's well-known directories, resolved cross-platform. The index
 * signature keeps it assignable to `Record<string, string>` for callers that
 * iterate the map generically.
 */
export type SystemDirectories = {
  HOME: string;
  DESKTOP: string;
  DOCUMENTS: string;
  DOWNLOADS: string;
  [key: string]: string;
};

type EnvLike = Record<string, string | undefined>;

export type SystemDirectoriesOptions = {
  /** Environment map to read XDG_*_DIR / USERPROFILE from (defaults to process.env). */
  env?: EnvLike;
  /** Home directory override (defaults to os.homedir()). */
  homedir?: string;
  /** Platform override (defaults to {@link defaultSystemDirectoryPlatform}). */
  platform?: SystemDirectoryPlatform;
};

/**
 * Classify the live `process.platform` into a {@link SystemDirectoryPlatform},
 * distinguishing WSL (a Linux kernel under Windows Subsystem for Linux, detected
 * via the `WSL_*` env markers) from native Linux, and collapsing the long tail
 * (freebsd/sunos/aix/…) into `unknown`. Used only as the default when the caller
 * does not inject a platform — the primary consumer passes its own canonical
 * `getPlatform()` so classification stays single-sourced in the real path.
 */
export function defaultSystemDirectoryPlatform(
  platform: NodeJS.Platform = process.platform,
  env: EnvLike = process.env,
): SystemDirectoryPlatform {
  if (platform === 'darwin') return 'macos';
  if (platform === 'win32') return 'windows';
  if (platform === 'linux') {
    return env.WSL_DISTRO_NAME || env.WSL_INTEROP ? 'wsl' : 'linux';
  }
  return 'unknown';
}

/**
 * Resolve the user's well-known directories (HOME, Desktop, Documents, Downloads)
 * cross-platform, mirroring Claude Code's `getSystemDirectories()`:
 *
 * - **Windows**: prefer `USERPROFILE` as the base so LOCALIZED folder names and
 *   relocated profiles resolve correctly, falling back to the home dir.
 * - **Linux / WSL**: honor the XDG user-dirs spec — `XDG_DESKTOP_DIR`,
 *   `XDG_DOCUMENTS_DIR`, `XDG_DOWNLOAD_DIR` — before the `~/Desktop` defaults, so
 *   relocated/localized folders resolve.
 * - **macOS / unknown**: standard `~/Desktop` etc. defaults.
 *
 * All inputs (env, homedir, platform) are injectable for deterministic testing.
 */
export function getSystemDirectories(
  options?: SystemDirectoriesOptions,
): SystemDirectories {
  const env = options?.env ?? process.env;
  const homeDir = options?.homedir ?? osHomedir();
  const platform =
    options?.platform ?? defaultSystemDirectoryPlatform(process.platform, env);

  // Default paths used by most platforms.
  const defaults: SystemDirectories = {
    HOME: homeDir,
    DESKTOP: join(homeDir, 'Desktop'),
    DOCUMENTS: join(homeDir, 'Documents'),
    DOWNLOADS: join(homeDir, 'Downloads'),
  };

  switch (platform) {
    case 'windows': {
      // Prefer USERPROFILE (handles localized folder names / relocated profiles).
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
      // XDG Base Directory user-dirs take precedence over the ~/Desktop defaults.
      return {
        HOME: homeDir,
        DESKTOP: env.XDG_DESKTOP_DIR || defaults.DESKTOP,
        DOCUMENTS: env.XDG_DOCUMENTS_DIR || defaults.DOCUMENTS,
        DOWNLOADS: env.XDG_DOWNLOAD_DIR || defaults.DOWNLOADS,
      };
    }

    case 'macos':
    default:
      return defaults;
  }
}
