/**
 * Path conversion utilities for IDE communication.
 *
 * When AMI runs inside WSL but cooperates with a Windows-side editor/diff viewer,
 * the two live in different path spaces: the editor speaks Windows drive paths
 * (`C:\Users\me\project`) while the agent speaks WSL paths (`/mnt/c/Users/me/project`).
 * A converter translates between them so paths read from an IDE lockfile land in the
 * agent's filesystem and paths sent back to the IDE (diff views, "open file") resolve
 * for Windows.
 *
 * This mirrors the reference's `utils/idePathConversion.ts`
 * (`WindowsToWSLConverter` + `checkWSLDistroMatch`): the canonical conversion runs
 * the real `wslpath -u`/`-w` tool and falls back to a deterministic manual mapping
 * when `wslpath` is unavailable. A WSL UNC path belonging to a *different* distro is
 * returned unchanged, because `wslpath` cannot resolve a foreign distro's path and
 * would corrupt it.
 *
 * NOTE: this is distinct from `utils/windows-paths.ts`, which maps the Git-Bash /
 * MSYS2 / Cygwin POSIX forms (`/c/...`, `/cygdrive/c/...`) that appear when AMI runs
 * natively ON Windows. This module maps the WSL `/mnt/<drive>/...` space that appears
 * when AMI runs INSIDE WSL alongside a Windows editor — a different path space and a
 * different tool (`wslpath`), so the two do not overlap.
 *
 * [FUNCTIONAL CHANGE] New module — Windows<->WSL IDE path-space parity with the reference.
 */

import { execFileSync } from 'child_process';

/**
 * Matches a Windows WSL UNC path and captures the distro name. Both the
 * `\\wsl.localhost\<distro>\...` (Windows 11) and legacy `\\wsl$\<distro>\...`
 * forms are accepted. Capture group 1 is the distro name; group 2 is the
 * remainder of the path. Mirrors the reference's regex verbatim.
 */
export const WSL_UNC_PATH_RE = /^\\\\wsl(?:\.localhost|\$)\\([^\\]+)(.*)$/;

/** Bound (ms) on each `wslpath` invocation so a wedged tool cannot block the
 *  thread. Additive safety guard over the reference (which has no timeout). */
const WSLPATH_TIMEOUT_MS = 5_000;

/**
 * Mockable subprocess seam (ESM-safe). Tests swap `execFileSync` via
 * `mock.method(execHooks, 'execFileSync', ...)` to drive the success / fallback
 * arms deterministically without a real `wslpath` binary. Defaults to the real
 * implementation; production behavior is unchanged.
 */
export const execHooks = {
  execFileSync: execFileSync as typeof execFileSync,
};

/**
 * Convert a path from the IDE's format to the agent's local format, and back.
 *
 * - `toLocalPath` — called when reading workspace folders from an IDE lockfile.
 * - `toIDEPath` — called when sending paths to the IDE (diff views, open-file).
 */
export interface IDEPathConverter {
  /** Convert a path from the IDE format to the agent's local format. */
  toLocalPath(idePath: string): string;
  /** Convert a path from the agent's local format to the IDE format. */
  toIDEPath(localPath: string): string;
}

/**
 * Check whether a Windows WSL UNC path belongs to the given distro.
 *
 * Returns `false` only when the path is a WSL UNC path (`\\wsl.localhost\<d>\...`
 * or `\\wsl$\<d>\...`) whose distro `<d>` differs from `wslDistroName`. Any
 * non-UNC path (a plain drive path, a POSIX path, …) is considered a match and
 * returns `true`, because there is no distro to mismatch. Parity with the reference
 * Code's `checkWSLDistroMatch`.
 */
export function checkWSLDistroMatch(windowsPath: string, wslDistroName: string): boolean {
  const wslUncMatch = windowsPath.match(WSL_UNC_PATH_RE);
  if (wslUncMatch) {
    return wslUncMatch[1] === wslDistroName;
  }
  // Not a WSL UNC path, so there is no distro to mismatch.
  return true;
}

/**
 * Deterministic Windows->WSL mapping used when `wslpath` is unavailable: flip
 * backslashes to forward slashes, then rewrite a leading drive letter
 * (`C:` → `/mnt/c`). Pure + exported for direct coverage. Parity with the
 * catch-branch of the reference's `toLocalPath`.
 */
export function manualWindowsToWSL(windowsPath: string): string {
  return windowsPath
    .replace(/\\/g, '/')
    .replace(/^([A-Z]):/i, (_, letter: string) => `/mnt/${letter.toLowerCase()}`);
}

/**
 * Converter for the "Windows IDE + WSL agent" scenario. Translates between
 * Windows drive/UNC paths and WSL `/mnt/<drive>/...` paths using the real
 * `wslpath` tool, with a manual fallback and cross-distro protection.
 */
export class WindowsToWSLConverter implements IDEPathConverter {
  constructor(private readonly wslDistroName: string | undefined) {}

  toLocalPath(windowsPath: string): string {
    if (!windowsPath) return windowsPath;

    // A path from a *different* WSL distro cannot be resolved by this distro's
    // `wslpath`; return it unchanged rather than corrupting it.
    if (this.wslDistroName && !checkWSLDistroMatch(windowsPath, this.wslDistroName)) {
      return windowsPath;
    }

    try {
      return execHooks
        .execFileSync('wslpath', ['-u', windowsPath], {
          encoding: 'utf8',
          timeout: WSLPATH_TIMEOUT_MS,
          // wslpath writes "wslpath: <errortext>" to stderr — discard it.
          stdio: ['pipe', 'pipe', 'ignore'],
        })
        .toString()
        .trim();
    } catch {
      // `wslpath` missing/failed — fall back to the deterministic manual mapping.
      return manualWindowsToWSL(windowsPath);
    }
  }

  toIDEPath(wslPath: string): string {
    if (!wslPath) return wslPath;

    try {
      return execHooks
        .execFileSync('wslpath', ['-w', wslPath], {
          encoding: 'utf8',
          timeout: WSLPATH_TIMEOUT_MS,
          stdio: ['pipe', 'pipe', 'ignore'],
        })
        .toString()
        .trim();
    } catch {
      // `wslpath` missing/failed — there is no safe manual Windows form, so
      // return the original path unchanged (parity with the reference).
      return wslPath;
    }
  }
}

/**
 * Build a {@link WindowsToWSLConverter} from the environment, reading the active
 * distro name from `WSL_DISTRO_NAME`. `env` is injectable for tests. The distro
 * name is optional: when absent, cross-distro UNC protection is skipped and every
 * path is passed to `wslpath`.
 */
export function createWSLConverterFromEnv(
  env: NodeJS.ProcessEnv = process.env,
): WindowsToWSLConverter {
  return new WindowsToWSLConverter(env.WSL_DISTRO_NAME || undefined);
}
