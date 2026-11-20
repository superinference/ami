/**
 * Native-installer (system package-manager) detection.
 *
 * Classifies HOW the currently running AMI binary was installed on the host —
 * Homebrew cask, Windows winget, a Linux native package (deb/rpm/apk/pacman), or
 * a polyglot version manager (mise/asdf) — so the self-update advisory (`/update`)
 * can tell the user the CORRECT upgrade command for their install method instead
 * of always printing the generic `curl … install.sh` line.
 *
 * Parity with the reference's `utils/nativeInstaller/packageManagers.ts`
 * (`getPackageManager` cascade + `getOsRelease`/`isDistroFamily` gating + the
 * per-manager detectors). The algorithm:
 *   1. Exec-PATH classifiers (sync, cheap): a Homebrew Caskroom path, a winget
 *      Packages/Links path, or a mise/asdf `installs/` directory signature in the
 *      running executable's path.
 *   2. DB-ownership probes (async, memoized): on Linux, GATED by the distro family
 *      parsed from `/etc/os-release` so we never, e.g., run the `pacman` arcade
 *      game (`/usr/games/pacman`) on Debian, we query the native package DB that
 *      owns the exact executable path (`dpkg -S` / `rpm -qf` / `pacman -Qo` /
 *      `apk info --who-owns`) with a bounded timeout.
 *
 * This is a DIFFERENT axis from `cli/src/update-check.ts`'s `detectInstallMethod`
 * (npm | pnpm | yarn | homebrew | curl — the JS-package install CHANNEL) and from
 * `detectInstallScope` (local | native | global — WHERE the binary lives): this
 * module identifies the SYSTEM package manager that owns the binary. The Homebrew
 * detector here is deliberately Caskroom-specific (not the general Homebrew prefix)
 * so an npm-global package installed under a Homebrew-managed `node` is NOT
 * misreported as a cask install — the whole reason the reference special-cases it.
 *
 * Pure/decision only: it NEVER runs an upgrade. {@link upgradeInstructionFor}
 * returns the command as DATA the caller surfaces under its own permission gate.
 *
 * The WSL classification reuses the shared {@link detectWslForAnalytics} marker
 * check (single source of truth — no duplicate WSL logic), and the memoization
 * reuses the shared {@link memoizeWithTTLAsync} primitive rather than hand-rolling
 * a per-function promise cache.
 */
import { readFile } from 'node:fs/promises';
import { execFileNoThrow, type ExecFileNoThrowResult } from './utils/shell';
import { memoizeWithTTLAsync } from './utils/memoize';
import { detectWslForAnalytics } from './analytics';
import { log } from './logger';

/** The native package manager / install method that owns the running binary. */
export const PACKAGE_MANAGERS = ['homebrew', 'winget', 'pacman', 'deb', 'rpm', 'apk', 'mise', 'asdf', 'unknown'] as const;
export type PackageManager = (typeof PACKAGE_MANAGERS)[number];

/** Coarse host platform used to gate platform-specific detectors. */
export const INSTALL_PLATFORMS = ['macos', 'linux', 'wsl', 'windows'] as const;
export type InstallPlatform = (typeof INSTALL_PLATFORMS)[number];

/** Parsed subset of `/etc/os-release`: the distro id and its family hints. */
export interface OsRelease {
  /** The `ID=` field (e.g. `ubuntu`, `fedora`, `arch`), or `''` when absent. */
  id: string;
  /** The space-split `ID_LIKE=` family list (e.g. `['debian']`), or `[]`. */
  idLike: string[];
}

/**
 * Side-effecting seams gathered on one mockable object so tests can intercept
 * them with `node:test`'s `mock.method` (the frozen-ESM-binding workaround: a
 * plain object property can be replaced, a namespace import cannot).
 */
export const packageManagerHooks = {
  /** Never-throw exec used by the DB-ownership probes. */
  execFile: execFileNoThrow as (
    file: string,
    args: string[],
    options?: { timeout?: number },
  ) => Promise<ExecFileNoThrowResult>,
  /** Read `/etc/os-release` (rejects when the file is absent/unreadable). */
  readOsRelease: (): Promise<string> => readFile('/etc/os-release', 'utf8'),
  /** Path of the running executable (what we inspect to infer provenance). */
  execPath: (): string => process.execPath || process.argv[0] || '',
  /** Coarse platform classification of the running host. */
  platform: (): InstallPlatform => getInstallPlatform(),
};

/** Permanent (process-lifetime) memo: install method never changes mid-run. */
const PERMANENT_TTL = Number.MAX_SAFE_INTEGER;

/**
 * Classify the host as `macos` | `windows` | `wsl` | `linux`. WSL is detected via
 * the shared {@link detectWslForAnalytics} env-marker check so the DB-ownership
 * probes (which gate on `linux`) are skipped under WSL exactly as the reference
 * `getPlatform` does, and Homebrew-on-WSL is still allowed.
 */
export function getInstallPlatform(
  platform: NodeJS.Platform = process.platform,
  env: NodeJS.ProcessEnv = process.env,
): InstallPlatform {
  if (platform === 'darwin') return 'macos';
  if (platform === 'win32') return 'windows';
  return detectWslForAnalytics(env) !== null ? 'wsl' : 'linux';
}

/** Build the parsed {@link OsRelease}, or null when the file is unreadable. */
async function computeOsRelease(): Promise<OsRelease | null> {
  try {
    const content = await packageManagerHooks.readOsRelease();
    // Anchored, quote-stripping, non-greedy single-token for ID; family list for
    // ID_LIKE. Inputs are short controlled config lines — reviewed, no ReDoS.
    const idMatch = content.match(/^ID=["']?(\S+?)["']?\s*$/m);
    const idLikeMatch = content.match(/^ID_LIKE=["']?(.+?)["']?\s*$/m);
    return {
      id: idMatch?.[1] ?? '',
      idLike: idLikeMatch?.[1]?.split(' ') ?? [],
    };
  } catch {
    return null;
  }
}

/**
 * Memoized `/etc/os-release` parse. Returns null on pre-systemd / non-standard
 * systems; callers fall through to the exec in that case (conservative fallback).
 */
export const getOsRelease = memoizeWithTTLAsync(computeOsRelease, PERMANENT_TTL);

/** True when the distro's `id` or any `idLike` family is in `families`. */
export function isDistroFamily(osRelease: OsRelease, families: string[]): boolean {
  return (
    families.includes(osRelease.id) ||
    osRelease.idLike.some((like) => families.includes(like))
  );
}

/**
 * Detect a mise (polyglot version manager) install by matching a `…/mise/installs/…`
 * segment in the executable path (case-insensitive, slash-or-backslash tolerant).
 */
export function detectMise(): boolean {
  const execPath = packageManagerHooks.execPath();
  if (/[/\\]mise[/\\]installs[/\\]/i.test(execPath)) {
    log('package-manager', 'Detected mise installation', { execPath }, 'debug');
    return true;
  }
  return false;
}

/** Detect an asdf install by matching a `…/.asdf/installs/…` segment in the path. */
export function detectAsdf(): boolean {
  const execPath = packageManagerHooks.execPath();
  if (/[/\\]\.?asdf[/\\]installs[/\\]/i.test(execPath)) {
    log('package-manager', 'Detected asdf installation', { execPath }, 'debug');
    return true;
  }
  return false;
}

/**
 * Detect a Homebrew CASK install by matching `/Caskroom/` in the executable path.
 * Caskroom-specific on purpose: an npm-global package placed under a Homebrew
 * `node` (`/opt/homebrew/lib/node_modules/…`) must NOT be misreported as a cask.
 * Only macOS / Linux / WSL can have Homebrew.
 */
export function detectHomebrew(): boolean {
  const platform = packageManagerHooks.platform();
  if (platform !== 'macos' && platform !== 'linux' && platform !== 'wsl') {
    return false;
  }
  const execPath = packageManagerHooks.execPath();
  if (execPath.includes('/Caskroom/')) {
    log('package-manager', 'Detected Homebrew cask installation', { execPath }, 'debug');
    return true;
  }
  return false;
}

/**
 * Detect a winget install (Windows only) by matching a WinGet `Packages` or `Links`
 * path segment in the executable path (forward- or back-slashes).
 */
export function detectWinget(): boolean {
  if (packageManagerHooks.platform() !== 'windows') {
    return false;
  }
  const execPath = packageManagerHooks.execPath();
  const patterns = [
    /Microsoft[/\\]WinGet[/\\]Packages/i,
    /Microsoft[/\\]WinGet[/\\]Links/i,
  ];
  for (const pattern of patterns) {
    if (pattern.test(execPath)) {
      log('package-manager', 'Detected winget installation', { execPath }, 'debug');
      return true;
    }
  }
  return false;
}

/**
 * Shared DB-ownership probe for the Linux native package managers: gate on `linux`
 * and (when `/etc/os-release` is readable) the matching distro `families`, then ask
 * the manager's DB whether it owns the exact executable path, with a 5s timeout.
 * Returns true only on a clean exit with non-empty stdout (the owning package).
 */
async function detectLinuxOwner(
  manager: PackageManager,
  families: string[],
  command: string,
  argsFor: (execPath: string) => string[],
): Promise<boolean> {
  if (packageManagerHooks.platform() !== 'linux') {
    return false;
  }
  const osRelease = await getOsRelease();
  if (osRelease && !isDistroFamily(osRelease, families)) {
    return false;
  }
  const execPath = packageManagerHooks.execPath();
  const result = await packageManagerHooks.execFile(command, argsFor(execPath), {
    timeout: 5000,
  });
  if (result.code === 0 && result.stdout) {
    log('package-manager', `Detected ${manager} installation`, { owner: result.stdout.trim() }, 'debug');
    return true;
  }
  return false;
}

/** Detect a pacman (Arch family) install via `pacman -Qo <execPath>`. */
export const detectPacman = memoizeWithTTLAsync(
  () => detectLinuxOwner('pacman', ['arch'], 'pacman', (p) => ['-Qo', p]),
  PERMANENT_TTL,
);

/** Detect a .deb (Debian family) install via `dpkg -S <execPath>`. */
export const detectDeb = memoizeWithTTLAsync(
  () => detectLinuxOwner('deb', ['debian'], 'dpkg', (p) => ['-S', p]),
  PERMANENT_TTL,
);

/** Detect an RPM (Fedora/RHEL/SUSE family) install via `rpm -qf <execPath>`. */
export const detectRpm = memoizeWithTTLAsync(
  () => detectLinuxOwner('rpm', ['fedora', 'rhel', 'suse'], 'rpm', (p) => ['-qf', p]),
  PERMANENT_TTL,
);

/** Detect an Alpine APK install via `apk info --who-owns <execPath>`. */
export const detectApk = memoizeWithTTLAsync(
  () => detectLinuxOwner('apk', ['alpine'], 'apk', (p) => ['info', '--who-owns', p]),
  PERMANENT_TTL,
);

/**
 * Memoized ordered cascade classifying how the running binary was installed. The
 * sync exec-path classifiers run first (cheap), then the Linux DB-ownership probes
 * (gated + timeout-bounded). Returns `'unknown'` when nothing matches.
 */
export const getPackageManager = memoizeWithTTLAsync(async (): Promise<PackageManager> => {
  if (detectHomebrew()) return 'homebrew';
  if (detectWinget()) return 'winget';
  if (detectMise()) return 'mise';
  if (detectAsdf()) return 'asdf';
  if (await detectPacman()) return 'pacman';
  if (await detectApk()) return 'apk';
  if (await detectDeb()) return 'deb';
  if (await detectRpm()) return 'rpm';
  return 'unknown';
}, PERMANENT_TTL);

/**
 * The upgrade command/instruction that matches a detected {@link PackageManager}.
 * `'unknown'` falls back to the generic install-script line so the advisory is
 * never empty. Pure data — the caller surfaces it; it is never executed here.
 */
export function upgradeInstructionFor(pm: PackageManager): string {
  switch (pm) {
    case 'homebrew':
      return 'brew upgrade --cask superinference';
    case 'winget':
      return 'winget upgrade superinference';
    case 'pacman':
      return 'sudo pacman -Syu superinference';
    case 'deb':
      return 'sudo apt-get update && sudo apt-get install --only-upgrade superinference';
    case 'rpm':
      return 'sudo dnf upgrade superinference';
    case 'apk':
      return 'sudo apk upgrade superinference';
    case 'mise':
      return 'mise upgrade superinference';
    case 'asdf':
      return 'asdf install superinference latest && asdf global superinference latest';
    case 'unknown':
      return 'curl -fsSL https://www.superinference.org/install.sh | bash';
  }
}

/** Clear all memoized package-manager detection caches (test seam / re-probe). */
export function resetPackageManagerCaches(): void {
  getOsRelease.cache.clear();
  detectPacman.cache.clear();
  detectDeb.cache.clear();
  detectRpm.cache.clear();
  detectApk.cache.clear();
  getPackageManager.cache.clear();
}
