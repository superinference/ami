import { execFileSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

// Cross-platform executable resolution — a Node-portable analogue of the
// `which`/`where.exe` probe (and Bun.which fast-path) Claude Code uses. AMI's
// container-runtime detection previously ran `command -v <cmd>` through a POSIX
// shell, which silently fails on Windows cmd.exe (the correct probe is
// `where.exe`) and always spawns a subprocess even though PATH can be scanned
// directly. This helper:
//   * special-cases win32 (`where.exe`, PATHEXT) so detection works there;
//   * uses a NO-SPAWN fast path (scan PATH directly) that avoids a subprocess;
//   * returns the fully-resolved executable path (not just the bare command).

const DEFAULT_WHICH_TIMEOUT_MS = 10_000;

/** Wall-clock bound for the subprocess fallback so a wedged shell/`where.exe`
 *  cannot block the thread forever. Overridable via AMI_WHICH_TIMEOUT_MS. */
function whichTimeoutMs(): number {
  const n = parseInt(process.env.AMI_WHICH_TIMEOUT_MS ?? '', 10);
  return n > 0 ? n : DEFAULT_WHICH_TIMEOUT_MS;
}

function isWindows(): boolean {
  return process.platform === 'win32';
}

/** On win32 an executable is identified by its extension (PATHEXT); on POSIX
 *  there is no extension, so a single empty suffix is used. The empty suffix is
 *  always included first so an explicit `docker.exe` still matches as-is. */
function pathExtensions(): string[] {
  if (!isWindows()) return [''];
  const exts = (process.env.PATHEXT ?? '.COM;.EXE;.BAT;.CMD')
    .split(';')
    .map((e) => e.trim())
    .filter(Boolean);
  return ['', ...exts];
}

function isExecutableFile(candidate: string): boolean {
  try {
    const st = fs.statSync(candidate);
    if (!st.isFile()) return false;
    // Windows has no executable bit; the PATHEXT filter already selected an
    // executable extension, so a regular file is enough.
    if (isWindows()) return true;
    fs.accessSync(candidate, fs.constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

/**
 * No-spawn PATH lookup: resolve `command` by scanning the given environment's
 * `PATH` (plus `PATHEXT` on win32) directly, or — when `command` already contains
 * a path separator — by probing it as a path. Returns the first executable match,
 * else null. NEVER executes anything: it only `stat`s/`access`es candidate files,
 * which is the security-correct way to probe for a tool (a `git --version`-style
 * exec would run an attacker-planted binary from an untrusted cwd). `env` is
 * injectable so a caller can probe a specific `PATH` deterministically (e.g. a
 * git-availability gate) without mutating `process.env`.
 */
export function findOnPath(command: string, env: NodeJS.ProcessEnv = process.env): string | null {
  if (!command) return null;
  const exts = pathExtensions();
  const hasSep = command.includes('/') || (isWindows() && command.includes('\\'));
  if (hasSep) {
    for (const ext of exts) {
      const candidate = path.resolve(command + ext);
      if (isExecutableFile(candidate)) return candidate;
    }
    return null;
  }
  const dirs = (env.PATH ?? '').split(path.delimiter).filter(Boolean);
  for (const dir of dirs) {
    for (const ext of exts) {
      const candidate = path.join(dir, command + ext);
      if (isExecutableFile(candidate)) return candidate;
    }
  }
  return null;
}

/** No-spawn fast path for {@link whichSync}: delegates to {@link findOnPath}
 *  against the live `process.env`. */
function fastPathLookup(command: string): string | null {
  return findOnPath(command, process.env);
}

/** Subprocess fallback mirroring the platform-native probe: `where.exe` on
 *  win32, `command -v` on POSIX. The command name is passed as an ARGUMENT
 *  (never interpolated into a shell string) so it cannot inject shell
 *  metacharacters. Bounded by whichTimeoutMs(). */
function spawnLookup(command: string): string | null {
  try {
    if (isWindows()) {
      const out = execFileSync('where.exe', [command], {
        encoding: 'utf-8',
        stdio: ['ignore', 'pipe', 'ignore'],
        timeout: whichTimeoutMs(),
        killSignal: 'SIGKILL',
      });
      // where.exe prints one path per line; take the first.
      return out.toString().trim().split(/\r?\n/)[0] || null;
    }
    const out = execFileSync('sh', ['-c', 'command -v -- "$1"', 'sh', command], {
      encoding: 'utf-8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: whichTimeoutMs(),
      killSignal: 'SIGKILL',
    });
    return out.toString().trim() || null;
  } catch {
    return null;
  }
}

/**
 * Synchronously resolve the full path to an executable, cross-platform.
 * Tries a no-spawn PATH scan first, then falls back to the platform-native
 * probe (`where.exe` / `command -v`).
 *
 * @param command - the command name (or path) to resolve
 * @returns the resolved executable path, or null if not found
 */
export function whichSync(command: string): string | null {
  if (!command) return null;
  return fastPathLookup(command) ?? spawnLookup(command);
}

/**
 * Session cache of binary-presence checks, keyed by the trimmed command name.
 * Stores BOTH positive and negative results so a given command is probed at most
 * once per session. Parity with Claude Code's `binaryCache` (utils/binaryCheck.ts).
 */
const binaryCache = new Map<string, boolean>();

/**
 * True when `command` resolves to an executable — the canonical binary-presence
 * check. Cross-platform via {@link whichSync} (`where.exe`/PATHEXT on Windows,
 * `command -v`/PATH scan on POSIX), session-cached (hits AND misses), and
 * defensive: an empty or whitespace-only command resolves to `false` without any
 * filesystem probe. Parity with Claude Code's `isBinaryInstalled`
 * (utils/binaryCheck.ts). Use this instead of re-spawning `which`/`where` per
 * call site (e.g. LSP server launch).
 */
export function isBinaryInstalled(command: string): boolean {
  const trimmed = (command ?? '').trim();
  if (!trimmed) return false;
  const cached = binaryCache.get(trimmed);
  if (cached !== undefined) return cached;
  const exists = whichSync(trimmed) !== null;
  binaryCache.set(trimmed, exists);
  return exists;
}

/** Clear the {@link isBinaryInstalled} session cache (tests / after a PATH change). */
export function clearBinaryCache(): void {
  binaryCache.clear();
}

/**
 * Return the FIRST of an ordered candidate list that resolves to an installed
 * executable, or `undefined` when none do. The canonical ordered-candidate
 * resolver: a caller lists alternate binary names in preference order (e.g.
 * `['uv', 'pipx', 'pip', 'pip3']`, or a language server's `['pyright-langserver',
 * 'pyright']`) and gets back the first one actually on PATH, so a tool with
 * several interchangeable front-ends degrades gracefully instead of hard-failing
 * on the absence of the single hardcoded one. Mirrors Claude Code's alternate-
 * binary fallback chains (e.g. `detectPythonPackageManager` uvx→pipx→pip→pip3).
 * Empty/whitespace candidates are skipped. Reuses the session-cached
 * {@link isBinaryInstalled} probe by default — it does NOT re-implement a second
 * `which`. `isInstalled` is injectable so a caller with its own binary-presence
 * seam (e.g. the LSP client's `_isBinaryInstalled` test hook) can share this one
 * ordering logic instead of duplicating the loop.
 */
export function firstAvailableBinary(
  candidates: readonly string[],
  isInstalled: (command: string) => boolean = isBinaryInstalled,
): string | undefined {
  for (const candidate of candidates) {
    if (isInstalled(candidate)) return candidate;
  }
  return undefined;
}

/**
 * Resolve an executable *name* to its full PATH path, returning the spawn-ready
 * `{ cmd, args }` pair (parity with Claude Code's `findExecutable`, which
 * replaced spawn-rx's `findActualExecutable`). `cmd` is the resolved absolute
 * path when found, else the original name unchanged (so `spawn` can still apply
 * its own PATH lookup); `args` is passed through untouched. Resolving up front
 * makes subprocess launches deterministic across platforms (notably Windows
 * `PATHEXT`), instead of relying on each spawn site re-scanning PATH.
 */
export function findExecutable(
  exe: string,
  args: string[],
): { cmd: string; args: string[] } {
  return { cmd: whichSync(exe) ?? exe, args };
}
