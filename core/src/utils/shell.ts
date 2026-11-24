import * as child_process from 'child_process';
import { SHELL_DEBUG_ENV } from './shell-debug-env';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import '../tools/ripgrep-config';
import './tmux-socket';
import { DEFAULT_PROGRESS_INTERVAL_MS, DEFAULT_STALL_TIMEOUT_MS, EXIT_DRAIN_QUIET_MS, PROGRESS_TAIL_CHARS, SHELL_SEARCH_PATHS, SPAWN_FAILURE_EXIT_CODE, computeProgressSlices, createOutputBuffer, describeSignal, formatShellPrefixCommand, getShellType, isWindowsBatchFile, looksLikePrompt, quoteWindowsShellArg, resolveDefaultTimeoutMs, resolveTmpBase, rewriteWindowsNullRedirect, shQuote, slowExecThresholdMs } from './shell-compute-progress-slices';
import type { ExecFileNoThrowOptions, ExecFileNoThrowResult, SlowCommandInfo } from './shell-compute-progress-slices';
import { GIT_BASH_PATH_ENV, GIT_DEFAULT_LOCATIONS, POWERSHELL_PATH_ENV, POWERSHELL_PROBE_PATHS, SNAPSHOT_CREATION_TIMEOUT_MS, buildPowerShellArgs, buildSnapshotScript, describeGitBashResolutionError, getEnvironmentOverrides, resolveDefaultShell, shellSnapshotEnabled, snapshotConfigFile, withPowerShellExitCode } from './shell-build-ripgrep-shell-integration';
import type { ShellType } from './shell-build-ripgrep-shell-integration';
import { ANCESTOR_WALK_TIMEOUT_MS, DESCENDANT_WALK_TIMEOUT_MS, PS_TABLE_CMD, destroyStreams } from './shell-exec-command-options';
import type { ExecCommandOptions, ExecCommandResult } from './shell-exec-command-options';
export { DEFAULT_TIMEOUT_ENV, DEFAULT_TIMEOUT_MS, INTERACTIVE_SHELL_TYPES, SLOW_EXEC_THRESHOLD_ENV, SPAWN_FAILURE_EXIT_CODE, computeProgressSlices, formatShellPrefixCommand, getShellType, quoteWindowsShellArg, resolveDefaultTimeoutMs, resolveTmpBase, rewriteWindowsNullRedirect } from './shell-compute-progress-slices';
export type { ExecFileNoThrowOptions, ExecFileNoThrowResult, InteractiveShellType, SlowCommandInfo } from './shell-compute-progress-slices';
export { DEFAULT_SHELL_TYPE, SHELL_SNAPSHOT_ENV, SHELL_TYPES, buildPowerShellArgs, buildRipgrepShellIntegration, describeGitBashResolutionError, getEnvironmentOverrides, resolveDefaultShell, withPowerShellExitCode } from './shell-build-ripgrep-shell-integration';
export type { ShellType } from './shell-build-ripgrep-shell-integration';
export { ANCESTOR_WALK_TIMEOUT_MS, DESCENDANT_WALK_TIMEOUT_MS, PS_TABLE_CMD } from './shell-exec-command-options';
export type { ExecCommandOptions, ExecCommandResult } from './shell-exec-command-options';

/**
 * Mockable seam (ESM-safe). Tests replace these via
 * `mock.method(shellHooks, 'name', ...)` to drive platform-specific branches
 * (Windows taskkill), inject a fake child process (deterministic
 * stdout/stderr/error/close events), simulate process-tree kills without a
 * real subprocess, and force shell-resolution / cwd-recovery branches
 * (`accessSync`/`execFileSync`/`existsSync`). Behavior-preserving: production
 * routes every platform/spawn/kill/probe call through this object, which
 * defaults to the real implementations.
 */
export const shellHooks = {
  platform: ((): NodeJS.Platform => os.platform()) as () => NodeJS.Platform,
  spawn: child_process.spawn as typeof child_process.spawn,
  execSync: child_process.execSync as typeof child_process.execSync,
  execFile: child_process.execFile as typeof child_process.execFile,
  execFileSync: child_process.execFileSync as typeof child_process.execFileSync,
  accessSync: fs.accessSync as typeof fs.accessSync,
  existsSync: fs.existsSync as typeof fs.existsSync,
  /**
   * Synchronous `stat`, used by {@link createShellSnapshot} to verify the capture
   * artifact really exists on disk after the capture shell exits 0 and to record
   * its byte size — parity with the reference's post-creation `stat().size` check
   * (ShellSnapshot.ts), which distinguishes an exit-0-but-file-missing/empty case
   * from a genuine success. Replaceable in tests via `mock.method(shellHooks, 'statSync', …)`.
   */
  statSync: fs.statSync as typeof fs.statSync,
  /**
   * Synchronous `readdir`, used by {@link createShellSnapshot} to log the
   * snapshot directory's contents when the artifact is missing after a
   * successful-exit capture (parity with the reference's `readdir(snapshotsDir)`
   * diagnostic in the `tengu_shell_unknown_error` path). Replaceable in tests.
   */
  readdirSync: fs.readdirSync as typeof fs.readdirSync,
  realpathSync: fs.realpathSync as typeof fs.realpathSync,
  kill: ((pid: number, signal: NodeJS.Signals): void => { process.kill(pid, signal); }),
  /**
   * Slow-command diagnostic sink (see {@link SLOW_EXEC_THRESHOLD_ENV}). Default
   * writes a single `[slow-exec]` line to stderr; it only ever fires when the
   * opt-in threshold env is set AND exceeded, so default runs produce no output.
   * Tests replace it via `mock.method(shellHooks, 'onSlowCommand', …)`.
   */
  onSlowCommand: ((info: SlowCommandInfo): void => {
    process.stderr.write(`[slow-exec] ${Math.round(info.durationMs)}ms (threshold ${info.thresholdMs}ms): ${info.command.slice(0, 200)}\n`);
  }),
  mkdirSync: fs.mkdirSync as typeof fs.mkdirSync,
  /**
   * Async directory creation, used by the per-shell async {@link getEnvironmentOverrides}
   * seam (e.g. preparing the isolated tmux socket dir before a bash command runs).
   * Separate from the synchronous {@link mkdirSync} so the async env-prep path is
   * genuinely async (and independently mockable in tests).
   */
  mkdirAsync: fs.promises.mkdir as typeof fs.promises.mkdir,
  readFileSync: fs.readFileSync as typeof fs.readFileSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
  openSync: fs.openSync as typeof fs.openSync,
  writeSync: fs.writeSync as typeof fs.writeSync,
  closeSync: fs.closeSync as typeof fs.closeSync,
  /**
   * Shell-command-assembly debug sink (see {@link SHELL_DEBUG_ENV}). Default
   * writes a single `[shell-debug]` line to stderr, but ONLY when
   * {@link SHELL_DEBUG_ENV} is set, so default runs produce no output. Surfaces
   * the snapshot-missing login-shell fallback, the `-l` login-flag skip, and the
   * snapshot-source decision — an opt-in introspection surface for the shell
   * command-assembly decisions (parity with the reference's `logForDebugging` in
   * bashProvider.ts). Tests replace it via `mock.method(shellHooks, 'onShellDebug', …)`.
   */
  onShellDebug: ((message: string): void => {
    if ((process.env[SHELL_DEBUG_ENV] || '').trim() !== '') {
      process.stderr.write(`[shell-debug] ${message}\n`);
    }
  }),
  /**
   * Per-shell async environment-override seam (parity with the reference's
   * `ShellProvider.getEnvironmentOverrides(command)`, shellProvider.ts:L28-32).
   * Computes extra env vars for the resolved shell family just before spawn and
   * MAY perform async initialization (the reference cites tmux socket setup for
   * bash). {@link execCommand} merges the result on top of the caller's `env`.
   * Replaceable in tests / by a custom provider via
   * `mock.method(shellHooks, 'getEnvironmentOverrides', …)`. Defaults to
   * {@link getEnvironmentOverrides} (sync `{}` fast path; async only when a shell
   * needs init). Returns a plain record for the common case so the executor's
   * same-tick spawn is preserved, and a Promise only when init is required.
   */
  getEnvironmentOverrides: ((shellType: ShellType, command: string): Record<string, string> | Promise<Record<string, string>> =>
    getEnvironmentOverrides(shellType, command)),
  /**
   * Cap for {@link ExecCommandOptions.persistFullOutput} streaming. Defaults to
   * {@link MAX_PERSISTED_OUTPUT_BYTES}; tests may lower it so the room≤0 / capped
   * branch is reachable without writing tens of megabytes.
   */
  // Default mirrors {@link MAX_PERSISTED_OUTPUT_BYTES} (defined below); reassigned
  // after that const so a single source of truth is kept. Tests may lower it.
  getMaxPersistedOutputBytes: (): number => 64 * 1024 * 1024,
};

/**
 * Single slow-command diagnostic boundary, shared by the async {@link execCommand}
 * path and the synchronous {@link execSyncWatched}/{@link execFileSyncWatched}
 * wrappers so there is ONE place that decides a command ran slow. Reports through
 * {@link shellHooks.onSlowCommand} only when {@link SLOW_EXEC_THRESHOLD_ENV} is set
 * AND the elapsed wall-clock time (since `startedAt`) reaches it; a no-op otherwise.
 */
function reportSlowExecIfSlow(command: string, startedAt: number): void {
  const threshold = slowExecThresholdMs();
  if (threshold === null) return;
  const durationMs = Date.now() - startedAt;
  if (durationMs >= threshold) {
    shellHooks.onSlowCommand({ command, durationMs, thresholdMs: threshold });
  }
}

/**
 * Centralized synchronous-exec watchdog boundary for `child_process.execSync`.
 *
 * A synchronous exec BLOCKS the event loop for its whole duration, so a slow one
 * is strictly worse than a slow async spawn. This wrapper times the call and, when
 * {@link SLOW_EXEC_THRESHOLD_ENV} is set and exceeded, reports it through the SAME
 * {@link shellHooks.onSlowCommand} sink the async path uses — no second diagnostic
 * mechanism. The command's return value and any thrown error pass through
 * unchanged; reporting runs in `finally`, so a slow command that also throws (e.g.
 * a non-zero exit) is still flagged. This is the sync counterpart of the reference's
 * `execSync_DEPRECATED` slowLogging boundary (execSyncWrapper.ts). Prefer the async
 * {@link execCommand} where possible; use this only for genuinely synchronous call
 * sites that cannot be made async.
 */
export const execSyncWatched = ((command: string, options?: child_process.ExecSyncOptions): Buffer | string => {
  const startedAt = Date.now();
  try {
    return child_process.execSync(command, options);
  } finally {
    reportSlowExecIfSlow(command, startedAt);
  }
}) as typeof child_process.execSync;

/**
 * Centralized synchronous-exec watchdog boundary for `child_process.execFileSync`
 * (no-shell argv form). Same slow-command diagnostic contract as
 * {@link execSyncWatched}; the reported command label is the file plus its args so
 * a slow `git clone …` is identifiable in the diagnostic line.
 */
export const execFileSyncWatched = ((file: string, args?: readonly string[], options?: child_process.ExecFileSyncOptions): Buffer | string => {
  const startedAt = Date.now();
  const label = args && args.length ? `${file} ${args.join(' ')}` : file;
  try {
    return child_process.execFileSync(file, args as string[], options);
  } finally {
    reportSlowExecIfSlow(label, startedAt);
  }
}) as typeof child_process.execFileSync;

// ---------------------------------------------------------------------------
// Shared never-throw file+args exec wrapper (execFileNoThrow)
//
// Parity with the reference's `execFileNoThrow` / `execFileNoThrowWithCwd`
// (execFileNoThrow.ts): ONE canonical wrapper over `child_process.execFile`
// (argv form, NO shell by default → injection-safe) that:
//   - ALWAYS resolves, never throws — a non-zero exit, a spawn ENOENT, a
//     timeout, or an abort all come back as a structured result;
//   - gives cross-platform `.bat`/`.cmd` handling + argument escaping when run
//     through a shell (the exact value the reference centralizes via execa):
//     on Windows a `.bat`/`.cmd` target is auto-run through a shell because Node
//     refuses to spawn it directly (CVE-2024-27980 hardening);
//   - exposes a per-call `shell?: boolean | string` toggle so a caller can opt
//     into/out of shell execution (or name a specific shell) for one call
//     (reference `ExecFileWithCwdOptions.shell`);
//   - honors `preserveOutputOnError` (drop output on a genuine non-zero exit for
//     noisy probe commands) exactly like the reference.
// It shares the SAME slow-command diagnostic boundary ({@link reportSlowExecIfSlow})
// as {@link execCommand} / {@link execSyncWatched}, so there is ONE slow-exec sink.
// AMI implements it on Node's own `child_process` (no execa dependency) to keep
// core/ dependency-free; the contract (shape + never-throw + Windows shell
// handling) matches the reference.
// ---------------------------------------------------------------------------

/** Default per-call timeout for {@link execFileNoThrow} (parity: reference 10 min). */
export const DEFAULT_EXEC_FILE_TIMEOUT_MS = 10 * 60 * 1000;

/** Default captured-output ceiling (bytes) for {@link execFileNoThrow}. */
export const DEFAULT_EXEC_FILE_MAX_BUFFER = 10 * 1024 * 1024;

/**
 * Never-throw file+args exec. Runs `file` with `args` via {@link shellHooks.execFile}
 * (no shell by default — argv form, injection-safe) and ALWAYS resolves with an
 * {@link ExecFileNoThrowResult}: a non-zero exit, spawn error (ENOENT/EACCES), a
 * timeout, or an abort are mapped to the result instead of throwing. When a shell
 * is used (explicit `shell`, or an auto-detected Windows `.bat`/`.cmd`), the file
 * and args are assembled into a single escaped command string so arguments can't
 * be reinterpreted as shell metacharacters. Parity with the reference's
 * `execFileNoThrowWithCwd` (execFileNoThrow.ts), implemented on Node's own
 * `child_process` so core/ stays dependency-free.
 */
export function execFileNoThrow(
  file: string,
  args: string[],
  options: ExecFileNoThrowOptions = {},
): Promise<ExecFileNoThrowResult> {
  const {
    cwd,
    env,
    timeout = DEFAULT_EXEC_FILE_TIMEOUT_MS,
    abortSignal,
    maxBuffer = DEFAULT_EXEC_FILE_MAX_BUFFER,
    preserveOutputOnError = true,
    shell,
    input,
  } = options;
  const startedAt = Date.now();
  const label = args.length ? `${file} ${args.join(' ')}` : file;

  return new Promise<ExecFileNoThrowResult>((resolve) => {
    const settle = (res: ExecFileNoThrowResult): void => {
      reportSlowExecIfSlow(label, startedAt);
      resolve(res);
    };
    if (abortSignal?.aborted) {
      settle({ stdout: '', stderr: '', code: 1, error: 'Aborted' });
      return;
    }
    const isWindows = shellHooks.platform() === 'win32';
    // On Windows, Node refuses to spawn a .bat/.cmd via execFile without a shell
    // (CVE-2024-27980). Auto-enable the shell for those — mirroring execa's
    // BAT/CMD handling — unless the caller set `shell` explicitly.
    const effectiveShell = shell ?? (isWindows && isWindowsBatchFile(file) ? true : undefined);

    // Through a shell: assemble a single escaped command string so args are not
    // reinterpreted as metacharacters (the automatic escaping the reference gets
    // from execa). Direct (no-shell) execFile passes the argv array verbatim.
    let execTarget = file;
    let execArgs: string[] = args;
    if (effectiveShell !== undefined && effectiveShell !== false) {
      const quote = isWindows ? quoteWindowsShellArg : shQuote;
      execTarget = [file, ...args].map(quote).join(' ');
      execArgs = [];
    }

    const child = shellHooks.execFile(
      execTarget,
      execArgs,
      {
        cwd,
        timeout,
        maxBuffer,
        encoding: 'utf8',
        windowsHide: true,
        ...(env ? { env } : {}),
        ...(effectiveShell !== undefined ? { shell: effectiveShell } : {}),
        ...(abortSignal ? { signal: abortSignal } : {}),
      },
      (err: child_process.ExecFileException | null, stdout: string | Buffer, stderr: string | Buffer) => {
        const out = typeof stdout === 'string' ? stdout : stdout.toString();
        const errOut = typeof stderr === 'string' ? stderr : stderr.toString();
        if (!err) {
          settle({ stdout: out, stderr: errOut, code: 0 });
          return;
        }
        // Never-throw: map the error. Node sets a numeric `code` for a non-zero
        // exit and a string errno (e.g. 'ENOENT') / signal for a launch/kill.
        const e = err as NodeJS.ErrnoException & { signal?: NodeJS.Signals };
        const code = typeof e.code === 'number' ? e.code : 1;
        if (preserveOutputOnError) {
          const error = e.message || (e.signal ? String(e.signal) : String(code));
          settle({ stdout: out, stderr: errOut, code, error });
        } else {
          settle({ stdout: '', stderr: '', code });
        }
      },
    );
    if (input != null && child.stdin) {
      // A child that exits before reading triggers EPIPE on end; swallow it.
      child.stdin.on('error', () => { /* EPIPE: child closed stdin early */ });
      child.stdin.end(input);
    }
  });
}

/**
 * Hard ceiling (bytes) on the complete command output persisted to disk when a
 * caller opts in with {@link ExecCommandOptions.persistFullOutput}. Mirrors
 * the reference's `MAX_PERSISTED_SIZE` (BashTool.tsx:L732): the full stream is
 * streamed to a file so the model can `file_read` the WHOLE output (including the
 * middle AMI's in-memory head+tail window drops), truncated only at this 64MB
 * boundary so a runaway producer can't exhaust the disk.
 */
export const MAX_PERSISTED_OUTPUT_BYTES = 64 * 1024 * 1024;
shellHooks.getMaxPersistedOutputBytes = (): number => MAX_PERSISTED_OUTPUT_BYTES;

/** True when `shellPath` can be executed (X_OK, or `--version` as a fallback). */
function isExecutable(shellPath: string): boolean {
  try {
    shellHooks.accessSync(shellPath, fs.constants.X_OK);
    return true;
  } catch {
    // Fallback for Nix and other environments where the X_OK check misreports:
    // run the shell with `--version`, which exits quickly, to confirm it works.
    try {
      shellHooks.execFileSync(shellPath, ['--version'], { timeout: 1000, stdio: 'ignore' });
      return true;
    } catch {
      return false;
    }
  }
}

// Memoize the resolved shell per (platform, override, SHELL) tuple so probing
// only happens once per session for a given environment, while still
// re-resolving when the relevant env changes (e.g. in tests).
const shellResolutionCache = new Map<string, string | null>();

function resolveShell(override: string, envShell: string): string | null {
  // 1. Explicit override (AMI_SHELL / CLAUDE_CODE_SHELL) wins when it names a
  //    supported shell type and is actually executable. Emit introspective
  //    feedback on both outcomes (parity with the reference's Shell.ts override
  //    logging) through the opt-in {@link shellHooks.onShellDebug} surface so a
  //    user can see which shell ran — or that their override was rejected and why.
  if (override) {
    const supported = override.includes('bash') || override.includes('zsh');
    if (supported && isExecutable(override)) {
      shellHooks.onShellDebug(`Using shell override: ${override}`);
      return override;
    }
    shellHooks.onShellDebug(`Shell override "${override}" is not a valid bash/zsh path, falling back to detection`);
  }
  // 2. Prefer the user's login shell type; probe standard install locations.
  //    Classify via the canonical {@link getShellType} so the bash-vs-zsh
  //    preference lives in exactly one place.
  const envType = getShellType(envShell);
  const preferBash = envType === 'bash';
  const isEnvSupported = envType === 'bash' || envType === 'zsh';
  const order = preferBash ? ['bash', 'zsh'] : ['zsh', 'bash'];
  const candidates = order.flatMap(sh => SHELL_SEARCH_PATHS.map(p => `${p}/${sh}`));
  // 3. Always try the SHELL env var first when it is a supported, runnable shell.
  if (isEnvSupported && isExecutable(envShell)) candidates.unshift(envShell);
  return candidates.find(c => isExecutable(c)) ?? null;
}

/**
 * Resolve the best available POSIX shell (non-Windows), honoring the
 * `AMI_SHELL` / `CLAUDE_CODE_SHELL` override and the `SHELL` preference, then
 * probing `/bin`, `/usr/bin`, `/usr/local/bin`, `/opt/homebrew/bin` for
 * `bash`/`zsh` with an executability check. Returns `null` when no suitable
 * shell exists (callers surface an actionable error). Memoized per session.
 */
export function findSuitableShell(): string | null {
  const override = process.env.AMI_SHELL || process.env.CLAUDE_CODE_SHELL || '';
  const envShell = process.env.SHELL || '';
  const key = `${shellHooks.platform()}|${override}|${envShell}`;
  const cached = shellResolutionCache.get(key);
  if (cached !== undefined) return cached;
  const resolved = resolveShell(override, envShell);
  shellResolutionCache.set(key, resolved);
  return resolved;
}

/**
 * Env knob that enables verbose shell-command-assembly debug logging through
 * {@link shellHooks.onShellDebug} (snapshot-missing login fallback, `-l` skip,
 * snapshot-source decision). Unset/blank disables it. Parity with the reference's
 * `logForDebugging` tracing in bashProvider.ts.
 */
export { SHELL_DEBUG_ENV } from './shell-debug-env';

/**
 * Capture a shell-environment snapshot for `shellPath` and return the path to
 * the sourceable snapshot file, or `null` if capture failed (missing shell,
 * timeout, permission error — the caller then falls back to a login shell).
 * Runs the user's shell as a login shell (`-l`) so profile/rc files are sourced,
 * then records the resulting aliases/functions/options/PATH. Parity with the reference
 * Code's `createAndSaveSnapshot`, except it is synchronous (so {@link execCommand}
 * can assemble the command string before spawning) and returns `null` instead of
 * throwing on failure.
 */
export function createShellSnapshot(shellPath: string): string | null {
  try {
    const configFile = snapshotConfigFile(shellPath);
    const configFileExists = shellHooks.existsSync(configFile);
    const dir = path.join(resolveTmpBase(), 'ami-shell-snapshots');
    shellHooks.mkdirSync(dir, { recursive: true });
    const shellType = shellPath.includes('zsh') ? 'zsh' : shellPath.includes('bash') ? 'bash' : 'sh';
    const file = path.join(dir, `snapshot-${shellType}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}.sh`);
    const script = buildSnapshotScript(shellPath, configFileExists);
    shellHooks.execFileSync(shellPath, ['-c', '-l', script], {
      timeout: SNAPSHOT_CREATION_TIMEOUT_MS,
      maxBuffer: 1024 * 1024,
      stdio: ['ignore', 'ignore', 'ignore'],
      env: { ...process.env, AMI_SNAPSHOT_FILE: file, GIT_EDITOR: 'true' },
    });
    // Post-creation verification (parity with the reference's `stat().size` check,
    // ShellSnapshot.ts:L521-568): the capture shell can exit 0 yet leave no artifact
    // (e.g. a `>| "$SNAPSHOT_FILE"` that silently failed, or the tmpdir was swept
    // mid-capture). Distinguish that exit-0-but-missing case from a real success,
    // and when it happens log the snapshot directory's contents so the
    // "process succeeded but produced nothing" edge is diagnosable.
    if (!shellHooks.existsSync(file)) {
      shellHooks.onShellDebug(`Snapshot file was not created: ${file}`);
      try {
        const dirContents = shellHooks.readdirSync(dir);
        shellHooks.onShellDebug(`Snapshot directory ${dir} contains ${dirContents.length} file(s)`);
      } catch {
        shellHooks.onShellDebug(`Snapshot directory not accessible: ${dir}`);
      }
      return null;
    }
    // Report the artifact's byte size (bounded by the 1MB maxBuffer capture cap)
    // so a zero-byte / runaway snapshot is observable via the debug surface.
    let snapshotSize: number | undefined;
    try {
      snapshotSize = shellHooks.statSync(file).size;
    } catch {
      // stat raced the file away after existsSync; size stays unknown.
    }
    shellHooks.onShellDebug(
      snapshotSize === undefined
        ? `Shell snapshot created (size unknown): ${file}`
        : `Shell snapshot created successfully (${snapshotSize} bytes)`,
    );
    return file;
  } catch (err) {
    shellHooks.onShellDebug(`Failed to create shell snapshot: ${(err as Error).message}`);
    return null;
  }
}

// Memoize the snapshot per shell path so capture (a login-shell subprocess) runs
// at most once per session per shell — parity with the reference's one-snapshot-per-session.
const shellSnapshotCache = new Map<string, string | null>();

/**
 * Memoized {@link createShellSnapshot}. The snapshot is captured once per shell
 * path per session. Parity with the reference's per-session snapshot promise.
 */
export function getCachedShellSnapshot(shellPath: string): string | null {
  const cached = shellSnapshotCache.get(shellPath);
  if (cached !== undefined) return cached;
  const created = createShellSnapshot(shellPath);
  shellSnapshotCache.set(shellPath, created);
  return created;
}

/** Reset the memoized snapshot(s) (tests / after a config change). */
export function resetShellSnapshotCache(): void {
  shellSnapshotCache.clear();
}

/** Best-effort symlink resolution; returns the input unchanged if it can't be resolved. */
function realpathSafe(p: string): string {
  try { return shellHooks.realpathSync(p); } catch { return p; }
}

/**
 * True when `execPath` resolves inside the current working directory subtree — a
 * planted `pwsh`/`powershell`/`git.exe`/`git.bat` in (or under) the CWD that
 * could hijack execution. Parity with the reference's `where.exe` cwd-shadowing
 * security filter (windowsPaths.ts:L65: `pathDir === cwd ||
 * normalizedPath.startsWith(cwd + sep)`): a candidate sitting directly in the CWD
 * OR under one of its subdirectories is rejected so a malicious binary dropped
 * alongside the project files is never run in place of the real shell/tool.
 */
function isCwdShadowed(execPath: string): boolean {
  // Use the host's native path flavor so Windows-style (drive-letter / UNC) and
  // POSIX paths are each classified correctly: a Windows absolute path like
  // `C:\...` must NOT be mistaken for a cwd-relative path on a non-Windows host.
  const p = shellHooks.platform() === 'win32' ? path.win32 : path.posix;
  const cwd = p.resolve(process.cwd());
  const resolved = p.resolve(execPath);
  // An empty relative path means `resolved === cwd`; a relative path that is
  // neither absolute nor `..`-prefixed means `resolved` is within the cwd tree.
  const rel = p.relative(cwd, resolved);
  return rel === '' || (!rel.startsWith('..') && !p.isAbsolute(rel));
}

/** Locate `cmd` on PATH (sync), returning the first hit or `null`. */
function whichSync(cmd: string): string | null {
  const isWindows = shellHooks.platform() === 'win32';
  try {
    const out = shellHooks.execFileSync(isWindows ? 'where.exe' : 'which', [cmd], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 2000,
    }).toString();
    return out.split(/\r?\n/).map(s => s.trim()).find(Boolean) ?? null;
  } catch {
    return null;
  }
}

/**
 * Resolve a PowerShell executable (uncached). Prefers `pwsh` (PowerShell 7+)
 * over `powershell` (Windows PowerShell 5.1) — parity with the reference's
 * `findPowerShell` (powershellDetection.ts). An explicit `AMI_POWERSHELL_PATH` /
 * `CLAUDE_CODE_POWERSHELL_PATH` override wins. On Linux, when PATH (or its
 * symlink target) points `pwsh` at a `/snap/` launcher, the known apt/rpm
 * install locations are probed instead to avoid snap's subprocess hang. A
 * candidate that resolves into the CWD is rejected ({@link isCwdShadowed}).
 * Returns `null` when no PowerShell is available.
 */
export function findPowerShell(): string | null {
  const override = POWERSHELL_PATH_ENV.map(k => (process.env[k] || '').trim()).find(Boolean);
  if (override) return override;

  const pwsh = whichSync('pwsh');
  if (pwsh && !isCwdShadowed(pwsh)) {
    if (shellHooks.platform() === 'linux') {
      const resolved = realpathSafe(pwsh);
      if (pwsh.startsWith('/snap/') || resolved.startsWith('/snap/')) {
        for (const direct of POWERSHELL_PROBE_PATHS) {
          if (!shellHooks.existsSync(direct)) continue;
          const directResolved = realpathSafe(direct);
          if (!direct.startsWith('/snap/') && !directResolved.startsWith('/snap/')) return direct;
        }
      }
    }
    return pwsh;
  }

  const powershell = whichSync('powershell');
  if (powershell && !isCwdShadowed(powershell)) return powershell;
  return null;
}

let cachedPowerShellPath: string | null | undefined;

/**
 * Memoized {@link findPowerShell}. Detection only runs once per session (it
 * shells out to `which`/`where.exe`). Parity with the reference's
 * `getCachedPowerShellPath`.
 */
export function getCachedPowerShellPath(): string | null {
  if (cachedPowerShellPath === undefined) cachedPowerShellPath = findPowerShell();
  return cachedPowerShellPath;
}

/** Reset the memoized PowerShell path (tests / after a PATH change). Parity: `resetPowerShellCache`. */
export function resetPowerShellCache(): void {
  cachedPowerShellPath = undefined;
}

/**
 * Resolve a TRUSTED executable path on Windows. For `git`, well-known install
 * locations are probed first; otherwise `where.exe` is consulted and every
 * candidate that resolves inside the current working directory tree is rejected
 * ({@link isCwdShadowed}) so a `git.exe`/`git.bat` planted alongside the project
 * can never be run in place of the real tool. Returns the first trusted hit, or
 * `null`. Parity with the reference's `findExecutable` (windowsPaths.ts:L29-80).
 */
export function findExecutable(executable: string): string | null {
  if (executable === 'git') {
    for (const loc of GIT_DEFAULT_LOCATIONS) {
      if (shellHooks.existsSync(loc)) return loc;
    }
  }
  try {
    const out = shellHooks.execFileSync('where.exe', [executable], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 2000,
    }).toString();
    const candidates = out.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
    // SECURITY: skip any candidate inside the cwd tree (a planted git.exe/.bat).
    // Each rejection is traced through the opt-in {@link shellHooks.onShellDebug}
    // introspection surface so a user debugging why their git/shell was not picked
    // up can see the security filter firing — parity with the reference's
    // `logForDebugging('Skipping potentially malicious executable …')`
    // (windowsPaths.ts:L64-68).
    for (const candidate of candidates) {
      if (isCwdShadowed(candidate)) {
        shellHooks.onShellDebug(
          `Skipping potentially malicious executable in current directory: ${candidate}`,
        );
        continue;
      }
      return candidate;
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Resolve git-bash's `bash.exe` on Windows (uncached). An explicit
 * `AMI_GIT_BASH_PATH` / `CLAUDE_CODE_GIT_BASH_PATH` override wins when it names
 * an existing file; otherwise the real `git.exe` is located
 * ({@link findExecutable}) and `<git>/cmd/git.exe` is mapped to its sibling
 * `<git>/bin/bash.exe`. Returns `null` when no trusted bash can be found
 * (callers surface an actionable error). Parity with the reference's
 * `findGitBashPath` (windowsPaths.ts:L98-125), except AMI returns `null` instead
 * of exiting the process.
 */
export function findGitBashPath(): string | null {
  const override = GIT_BASH_PATH_ENV.map(k => (process.env[k] || '').trim()).find(Boolean);
  if (override) {
    if (shellHooks.existsSync(override)) return override;
    // The user explicitly configured a git-bash path that does not exist — trace
    // the bad value they supplied (distinct from the "not found at all" case) so
    // a typo is self-evident. Parity with the reference echoing back
    // `CLAUDE_CODE_GIT_BASH_PATH` (windowsPaths.ts:L104-106).
    shellHooks.onShellDebug(`Configured git-bash path "${override}" does not exist`);
    return null;
  }

  const gitPath = findExecutable('git');
  if (gitPath) {
    // <git>/cmd/git.exe → <git>/bin/bash.exe
    const bashPath = path.win32.join(gitPath, '..', '..', 'bin', 'bash.exe');
    if (shellHooks.existsSync(bashPath)) return bashPath;
  }
  shellHooks.onShellDebug('git-bash not found; install git-for-Windows or set AMI_GIT_BASH_PATH to your bash.exe');
  return null;
}

let cachedGitBashPath: string | null | undefined;

/**
 * Memoized {@link findGitBashPath}. Discovery shells out to `where.exe`, so it
 * runs once per session. Parity with the reference's `memoize(findGitBashPath)`.
 */
export function getCachedGitBashPath(): string | null {
  if (cachedGitBashPath === undefined) cachedGitBashPath = findGitBashPath();
  return cachedGitBashPath;
}

/** Reset the memoized git-bash path (tests / after a PATH or env change). */
export function resetGitBashCache(): void {
  cachedGitBashPath = undefined;
}

/**
 * Executes a shell command using the resolved POSIX shell (bash/zsh, see
 * {@link findSuitableShell}) via `-c`. On Windows the command runs through
 * git-bash's `bash.exe` (see {@link findGitBashPath}), NOT `cmd`, so POSIX bash
 * semantics hold on every platform. Streams output through an optional `onData`
 * callback.
 *
 * Environment knobs (introspectable; apply to every `execCommand` caller):
 * - `AMI_DEFAULT_SHELL` / `CLAUDE_CODE_SHELL_TYPE` — default shell family
 *   (`bash` | `powershell`) when a call does not set {@link ExecCommandOptions.shellType}
 *   (see {@link resolveDefaultShell}); unrecognized values fall back to `bash`.
 * - `AMI_POWERSHELL_PATH` / `CLAUDE_CODE_POWERSHELL_PATH` — explicit PowerShell
 *   executable path, overriding `pwsh`/`powershell` detection (see {@link findPowerShell}).
 * - `AMI_SHELL` / `CLAUDE_CODE_SHELL` — explicit shell path override.
 * - `AMI_GIT_BASH_PATH` / `CLAUDE_CODE_GIT_BASH_PATH` — explicit git-bash
 *   `bash.exe` path on Windows, overriding discovery (see {@link findGitBashPath}).
 * - `AMI_LOGIN_SHELL` — when set, spawn the shell as a login shell (`-l`) so
 *   the user's profile/rc files are sourced.
 * - `AMI_SHELL_PREFIX` / `CLAUDE_CODE_SHELL_PREFIX` — an executable+args that
 *   wraps the command, quoted per component (see {@link formatShellPrefixCommand}).
 * - `AMI_SLOW_EXEC_MS` ({@link SLOW_EXEC_THRESHOLD_ENV}) — when set, report any
 *   command whose wall-clock duration reaches that many ms via
 *   {@link shellHooks.onSlowCommand} (opt-in slow-command diagnostics).
 * - `AMI_TMPDIR` / `CLAUDE_CODE_TMPDIR` — relocate AMI's temp-file base
 *   (see {@link resolveTmpBase}).
 * - `AMI_DEFAULT_TIMEOUT_MS` / `CLAUDE_CODE_SHELL_TIMEOUT` ({@link DEFAULT_TIMEOUT_ENV})
 *   — override the default per-command timeout for calls that pass none
 *   (see {@link resolveDefaultTimeoutMs}); unset/invalid keeps the 120 000 ms baseline.
 *
 * - Respects timeout (default {@link resolveDefaultTimeoutMs}, baseline 120 000 ms) and abort signal.
 * - Kills the entire process tree on timeout or abort.
 * - Recovers if the working directory no longer exists (falls back to a
 *   known-good root; fails with a clear message when none is available).
 * - Rewrites hallucinated Windows `>nul` redirects to `/dev/null` on the POSIX
 *   path (see {@link rewriteWindowsNullRedirect}).
 * - Distinguishes a launch failure (no shell / spawn error →
 *   `exitCode: {@link SPAWN_FAILURE_EXIT_CODE}`) from a timeout/abort
 *   (`exitCode: null`), and surfaces the terminating signal name on a crash.
 * - Keeps the first 670k and last 330k characters of stdout and stderr.
 */
export function execCommand(
  command: string,
  options: ExecCommandOptions,
): Promise<ExecCommandResult> {
  const { cwd, timeout = resolveDefaultTimeoutMs(), abortSignal, onData, env, stallTimeoutMs = DEFAULT_STALL_TIMEOUT_MS, input, stdin, preserveOutputOnError, shellType, shellPath, skipSnapshot, onProgress, progressIntervalMs, preventCwdChanges, persistFullOutput } = options;

  // Opt-in cwd tracking: only when the caller explicitly allows cwd changes to
  // persist (`preventCwdChanges === false`). Default (undefined/true) leaves
  // AMI's per-call cwd contract and all existing callers unchanged.
  const trackCwd = preventCwdChanges === false;

  // Wall-clock start for slow-command diagnostics (see SLOW_EXEC_THRESHOLD_ENV).
  const startedAt = Date.now();
  const reportSlowExec = (): void => reportSlowExecIfSlow(command, startedAt);

  // Pick the shell family up-front (an explicit per-call `shellType` wins, else
  // the operator default) so the per-shell env-override seam can run before spawn.
  const effectiveShellType: ShellType = shellType ?? resolveDefaultShell();

  // The spawn executor, parameterized by the resolved per-shell env overrides
  // (merged on top of the caller's `env`). Kept as an inner factory so the
  // override seam can run either synchronously (common fast path — the executor
  // then runs same-tick and spawns immediately) or asynchronously (only when a
  // shell needs init, e.g. bash tmux-socket isolation) without otherwise changing
  // the executor's behavior.
  const run = (envOverrides: Record<string, string>): Promise<ExecCommandResult> =>
    new Promise<ExecCommandResult>((resolve) => {
    // If already aborted, short-circuit
    if (abortSignal?.aborted) {
      resolve({ stdout: '', stderr: 'Aborted', exitCode: null, signal: null });
      return;
    }

    const isWindows = shellHooks.platform() === 'win32';

    const stdoutBuf = createOutputBuffer();
    const stderrBuf = createOutputBuffer();

    // Temp file the bash shell writes its final `pwd -P` to when cwd tracking is
    // opted in (preventCwdChanges === false). Read back + unlinked in finish().
    let cwdFile: string | null = null;

    // --- Full-output persistence (persistFullOutput) ---
    // When opted in, the complete stdout stream is tee'd to a temp file (capped at
    // MAX_PERSISTED_OUTPUT_BYTES) so the model can file_read the WHOLE output —
    // including the middle the in-memory head+tail window drops. The fd is opened
    // lazily on the first stdout chunk so a silent command creates no file.
    let fullOutputPath: string | null = null;
    let fullOutputFd: number | null = null;
    let fullOutputBytes = 0;
    let fullOutputCapped = false;
    const teeFullOutput = (chunk: string): void => {
      if (!persistFullOutput || fullOutputCapped) return;
      try {
        if (fullOutputFd === null) {
          fullOutputPath = path.join(resolveTmpBase(), `ami-bash-output-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}.log`);
          fullOutputFd = shellHooks.openSync(fullOutputPath, 'w');
        }
        const buf = Buffer.from(chunk, 'utf8');
        const room = shellHooks.getMaxPersistedOutputBytes() - fullOutputBytes;
        if (room <= 0) { fullOutputCapped = true; return; }
        const slice = buf.length > room ? buf.subarray(0, room) : buf;
        shellHooks.writeSync(fullOutputFd, slice);
        fullOutputBytes += slice.length;
        if (slice.length < buf.length) fullOutputCapped = true;
      } catch {
        // Persistence is best-effort — a write/open failure must never fail the
        // command. Stop teeing and drop the (possibly partial) file.
        fullOutputCapped = true;
        if (fullOutputFd !== null) { try { shellHooks.closeSync(fullOutputFd); } catch { /* ignore */ } fullOutputFd = null; }
        if (fullOutputPath !== null) { try { shellHooks.unlinkSync(fullOutputPath); } catch { /* ignore */ } fullOutputPath = null; }
      }
    };

    // --- Structured progress (onProgress) accumulators ---
    // Combined stdout+stderr seen so far (the "command output"), total bytes,
    // the monotone total-line counter, and whether new output has arrived since
    // the last heartbeat tick.
    let progressText = '';
    let progressBytes = 0;
    let progressTotalLines = 0;
    let progressDirty = false;

    // --- Working-directory recovery ---
    // A command (or a prior one) may delete its own CWD. Rather than failing to
    // spawn, recover to a known-good root and tell the model what happened.
    let effectiveCwd = cwd;
    if (!shellHooks.existsSync(effectiveCwd)) {
      let fallback: string | null = null;
      for (const candidate of [process.cwd(), os.homedir()]) {
        if (shellHooks.existsSync(candidate)) { fallback = candidate; break; }
      }
      if (fallback === null) {
        resolve({
          stdout: '',
          stderr: `Working directory "${cwd}" no longer exists and no fallback directory is available.`,
          exitCode: null,
          signal: null,
        });
        return;
      }
      stderrBuf.push(`[Working directory "${cwd}" no longer exists; recovered to "${fallback}"]\n`);
      effectiveCwd = fallback;
    }

    // --- Shell resolution ---
    // The shell family (`effectiveShellType`) was resolved up-front (an explicit
    // per-call `shellType` wins, otherwise the operator default
    // `AMI_DEFAULT_SHELL` → `'bash'`). PowerShell is opt-in and never
    // auto-selected (parity with the reference's resolveDefaultShell).
    const explicitShellPath = (shellPath || '').trim();

    let shell: string;
    let shellArgs: string[];

    if (effectiveShellType === 'powershell') {
      const psPath = explicitShellPath || getCachedPowerShellPath();
      if (psPath === null || psPath === '') {
        resolve({
          stdout: stdoutBuf.value(),
          stderr: stderrBuf.value() + 'No PowerShell found. AMI needs PowerShell for shellType "powershell": install pwsh, or set AMI_POWERSHELL_PATH to its path.',
          exitCode: SPAWN_FAILURE_EXIT_CODE,
          signal: null,
        });
        return;
      }
      shell = psPath;
      // Hardened, non-interactive invocation with exit-code normalization. No
      // `>nul` rewrite here — that repairs a POSIX-shell artifact; PowerShell
      // uses `$null` and would never create a literal `nul` file.
      shellArgs = buildPowerShellArgs(withPowerShellExitCode(command));
    } else {
      // Resolve the bash executable. On Windows that means git-bash's `bash.exe`
      // (trusted discovery with a cwd-shadow filter) — AMI runs user commands
      // through bash, NOT `cmd`, so the bash security guards' POSIX assumptions
      // hold on every platform. On POSIX, probe for bash/zsh.
      let resolved: string | null;
      if (isWindows) {
        resolved = explicitShellPath || getCachedGitBashPath();
        if (resolved === null || resolved === '') {
          resolve({
            stdout: stdoutBuf.value(),
            stderr: stderrBuf.value() + describeGitBashResolutionError(),
            exitCode: SPAWN_FAILURE_EXIT_CODE,
            signal: null,
          });
          return;
        }
      } else {
        resolved = explicitShellPath || findSuitableShell();
        if (resolved === null) {
          resolve({
            stdout: stdoutBuf.value(),
            stderr: stderrBuf.value() + 'No suitable shell found. AMI requires a POSIX shell: install bash or zsh, or set AMI_SHELL to its path.',
            exitCode: SPAWN_FAILURE_EXIT_CODE,
            signal: null,
          });
          return;
        }
      }
      shell = resolved;

      // bash is a POSIX shell on BOTH platforms (git-bash on Windows), so repair
      // hallucinated Windows `>nul` redirects to `/dev/null` everywhere — under
      // bash `2>nul` would otherwise create a literal `nul` file in the cwd.
      const normalizedCommand = rewriteWindowsNullRedirect(command);

      // Optional shell-environment snapshot sourcing (opt-in via AMI_SHELL_SNAPSHOT):
      // capture the user's aliases/functions/options/PATH once per session and
      // source them before the command so model-run commands match the user's
      // interactive shell. Aliases only expand on a second parse pass, so the
      // command is wrapped in `eval` after the source (parity with the reference's
      // bashProvider). When the snapshot is requested but unavailable (creation
      // failed or the file vanished mid-session) we gracefully fall back to a
      // login shell (`-l`) so the profile/rc files are at least sourced.
      const snapshotRequested = shellSnapshotEnabled() && skipSnapshot !== true;
      let snapshotPath: string | null = null;
      if (snapshotRequested) {
        snapshotPath = getCachedShellSnapshot(shell);
        if (snapshotPath && !shellHooks.existsSync(snapshotPath)) {
          shellHooks.onShellDebug(`Snapshot file missing, falling back to login shell: ${snapshotPath}`);
          snapshotPath = null;
        }
      }
      const snapshotFallbackLogin = snapshotRequested && snapshotPath === null;
      const assembledCommand = snapshotPath
        ? `source ${shQuote(snapshotPath)} 2>/dev/null || true && eval ${shQuote(normalizedCommand)}`
        : normalizedCommand;
      if (snapshotPath) shellHooks.onShellDebug('Sourcing shell snapshot; spawning without login (-l skipped)');

      // Optional command wrapping via a configured shell prefix. The reference
      // name (CLAUDE_CODE_SHELL_PREFIX) is accepted as a fallback for parity.
      const prefix = process.env.AMI_SHELL_PREFIX || process.env.CLAUDE_CODE_SHELL_PREFIX;
      const finalCommand = prefix ? formatShellPrefixCommand(prefix, assembledCommand) : assembledCommand;

      // Login-shell flag sources profile/rc files when requested (POSIX + git-bash),
      // or as the snapshot-unavailable fallback above.
      const useLogin = !!process.env.AMI_LOGIN_SHELL || snapshotFallbackLogin;
      if (snapshotFallbackLogin) shellHooks.onShellDebug('Shell snapshot unavailable; spawning login shell (-l) as fallback');
      const loginArgs = useLogin ? ['-l'] : [];

      // Opt-in cwd tracking (preventCwdChanges === false): after the user command
      // runs, write the shell's final working directory to a temp file WITHOUT
      // disturbing the command's own exit code, so a `cd` can be persisted across
      // invocations (parity with the reference's nativeCwdFilePath / `pwd -P`,
      // Shell.ts:L385-414). Read back + unlinked in finish().
      let trackedCommand = finalCommand;
      if (trackCwd) {
        cwdFile = path.join(resolveTmpBase(), `ami-cwd-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`);
        trackedCommand = `${finalCommand}\n__ami_ec=$?\ncommand pwd -P > ${shQuote(cwdFile)} 2>/dev/null\nexit $__ami_ec`;
      }
      shellArgs = [...loginArgs, '-c', trackedCommand];
    }

    // Merge the per-shell env overrides on top of the caller's `env` (or the
    // inherited process env when none was passed). Overrides win so an async
    // per-shell preparation (e.g. tmux `TMUX_TMPDIR` isolation) takes effect;
    // with no overrides the spawn env is unchanged (the common fast path passes
    // `env` verbatim or inherits, exactly as before).
    const hasOverrides = Object.keys(envOverrides).length > 0;
    const baseEnv = env ?? (hasOverrides ? process.env : undefined);
    const finalEnv = baseEnv ? { ...baseEnv, ...envOverrides } : undefined;

    const proc = shellHooks.spawn(shell, shellArgs, {
      // Pipe stdin when the caller supplies input; otherwise honor the explicit
      // `stdin` mode (default 'ignore' so interactive prompts are still
      // detected-and-killed by stall detection).
      stdio: [input != null ? 'pipe' : (stdin ?? 'ignore'), 'pipe', 'pipe'],
      cwd: effectiveCwd,
      detached: !isWindows,
      // Suppress the console window a spawned `cmd /c` would otherwise flash on
      // Windows. Ignored (no-op) on POSIX platforms, so it is safe to set
      // unconditionally.
      windowsHide: true,
      ...(finalEnv ? { env: finalEnv } : {}),
    });

    // Feed the caller-provided payload to the child's stdin, then close it.
    // `input != null` forces stdio[0]='pipe' above, so proc.stdin is present.
    if (input != null) {
      const stdin = proc.stdin!;
      // A child that exits before reading triggers EPIPE on write/end; swallow
      // it so a one-shot stdin feed never rejects the exec promise.
      stdin.on('error', () => { /* EPIPE: child closed stdin early */ });
      stdin.end(input);
    }

    // stdio positions 1 and 2 are always 'pipe', so these streams are present.
    const procStdout = proc.stdout!;
    const procStderr = proc.stderr!;

    let settled = false;
    let exitDrainTimer: ReturnType<typeof setTimeout> | null = null;

    const finish = (exitCode: number | null, signal: NodeJS.Signals | null = null, backgrounded = false): void => {
      if (settled) return;
      settled = true;
      cleanUp();
      reportSlowExec();
      // Optionally discard captured output on a genuine non-zero exit (probe
      // commands). A timeout/abort/launch failure (exitCode === null) always
      // keeps its output so the caller can tell WHY it failed.
      const suppress = preserveOutputOnError === false && exitCode != null && exitCode !== 0;
      // Read back the tracked final cwd, if the command wrote one (opt-in). A
      // soft-interrupt background leaves the command running, so `pwd -P` has not
      // run yet — skip the read in that case.
      let finalCwd: string | undefined;
      if (cwdFile && !backgrounded && shellHooks.existsSync(cwdFile)) {
        try {
          const captured = String(shellHooks.readFileSync(cwdFile, 'utf8')).trim();
          if (captured) finalCwd = captured;
        } catch { /* unreadable — leave cwd unset */ }
      }
      if (cwdFile) {
        try { shellHooks.unlinkSync(cwdFile); } catch { /* may not exist if the command died first */ }
      }
      // Finalize full-output persistence: close the fd, then keep the file ONLY
      // when the in-memory window actually dropped stdout (so the on-disk copy is
      // strictly more complete than the inline result). Otherwise the inline
      // output already has everything — discard the temp file so a small command
      // leaves no retrieval surface. A backgrounded soft-interrupt or a suppressed
      // probe result keeps nothing either.
      let persistedPath: string | undefined;
      let persistedSize: number | undefined;
      if (fullOutputFd !== null) { try { shellHooks.closeSync(fullOutputFd); } catch { /* ignore */ } }
      if (fullOutputPath !== null) {
        if (!suppress && !backgrounded && fullOutputBytes > 0 && stdoutBuf.truncated()) {
          persistedPath = fullOutputPath;
          persistedSize = fullOutputBytes;
        } else {
          try { shellHooks.unlinkSync(fullOutputPath); } catch { /* ignore */ }
        }
      }
      resolve({
        stdout: suppress ? '' : stdoutBuf.value(),
        stderr: suppress ? '' : stderrBuf.value(),
        exitCode,
        signal,
        ...(backgrounded ? { backgrounded: true } : {}),
        ...(finalCwd !== undefined ? { cwd: finalCwd } : {}),
        ...(persistedPath !== undefined ? { fullOutputPath: persistedPath, fullOutputSize: persistedSize } : {}),
      });
    };

    // Conclude, first surfacing a human-readable note when the child was killed
    // by a signal (crash/OOM/self-signal) so the model sees WHAT killed it
    // instead of a bare `exitCode: null`. Shared by the `'close'` and the
    // `'exit'`-fallback completion paths (guarded by `!settled` so a
    // timeout/abort that already resolved — and then SIGKILLs the tree — doesn't
    // tack a misleading signal note onto its result).
    const concludeWithSignalNote = (code: number | null, signal: NodeJS.Signals | null): void => {
      if (!settled && signal) {
        stderrBuf.push(`\n[Process terminated by signal ${signal}${describeSignal(signal)}]`);
      }
      finish(code, signal);
    };

    // --- Timeout handling ---
    const timer = setTimeout(() => {
      destroyStreams(proc);
      killTree(proc);
      finish(null);
    }, timeout);

    // --- Abort signal handling ---
    const onAbort = (): void => {
      // Soft interrupt: the caller submitted a new message while the command was
      // still running (abortSignal.reason === 'interrupt'). Do NOT kill — resolve
      // and leave the process running so the caller can background it and keep
      // its in-flight/partial work. Any other abort reason is a hard abort that
      // kills the whole process tree. Parity: the reference's #abortHandler
      // interrupt branch (ShellCommand.ts:L186-193).
      if (abortSignal?.reason === 'interrupt') {
        finish(null, null, true);
        return;
      }
      destroyStreams(proc);
      killTree(proc);
      finish(null);
    };

    abortSignal?.addEventListener('abort', onAbort, { once: true });

    // --- Stall detection ---
    let lastDataTime = Date.now();
    const stallCheck = stallTimeoutMs > 0 ? setInterval(() => {
      const elapsed = Date.now() - lastDataTime;
      if (elapsed >= stallTimeoutMs) {
        const combined = stdoutBuf.value() + stderrBuf.value();
        if (looksLikePrompt(combined)) {
          stderrBuf.push('\n[Stall detected: process appears to be waiting for interactive input. Killed.]');
          destroyStreams(proc);
          killTree(proc);
          finish(null);
        }
      }
    }, 5000) : null;

    // --- Structured progress heartbeat (onProgress) ---
    // Emits the last-5/last-100 line tail + a monotone total-line count + total
    // bytes on a cadence, firing EVEN on a silent tick (empty slices) so a
    // caller's progress loop wakes on a quiet long-runner. Armed only when a
    // caller supplies onProgress and the cadence is > 0.
    const emitProgress = (): void => {
      if (settled || !onProgress) return;
      if (!progressDirty) {
        // No new output since the last tick — still fire so the loop wakes.
        onProgress('', '', progressTotalLines, progressBytes, false);
        return;
      }
      progressDirty = false;
      const { last5, last100, totalLines: sampleLines } = computeProgressSlices(progressText);
      const bytesRead = Buffer.byteLength(progressText);
      // Exact when the whole output fits in the retained tail; otherwise
      // extrapolate from the tail sample, with a monotone max so a tick with
      // longer lines never walks the counter backwards (parity: TaskOutput.ts
      // L143-149).
      const totalLines = bytesRead >= progressBytes
        ? sampleLines
        : Math.max(progressTotalLines, Math.round((progressBytes / bytesRead) * sampleLines));
      progressTotalLines = totalLines;
      onProgress(last5, last100, totalLines, progressBytes, bytesRead < progressBytes);
    };
    const effProgressInterval = progressIntervalMs ?? DEFAULT_PROGRESS_INTERVAL_MS;
    const progressTimer = (onProgress && effProgressInterval > 0)
      ? setInterval(emitProgress, effProgressInterval)
      : null;

    const cleanUp = (): void => {
      clearTimeout(timer);
      if (stallCheck) clearInterval(stallCheck);
      if (progressTimer) clearInterval(progressTimer);
      if (exitDrainTimer) clearTimeout(exitDrainTimer);
      abortSignal?.removeEventListener('abort', onAbort);
    };

    // Accumulate combined stdout+stderr for the structured progress tail/bytes.
    // Only the last PROGRESS_TAIL_CHARS are retained (bounded memory); the exact
    // total byte count is tracked separately so the line count can be
    // extrapolated from the tail sample (parity: TaskOutput.ts PROGRESS_TAIL_BYTES).
    const accumulateProgress = (chunk: string): void => {
      if (!onProgress) return;
      progressText = (progressText + chunk).slice(-PROGRESS_TAIL_CHARS);
      progressBytes += Buffer.byteLength(chunk);
      progressDirty = true;
    };

    // --- Stream stdout ---
    procStdout.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stdoutBuf.push(chunk);
      teeFullOutput(chunk);
      lastDataTime = Date.now();
      accumulateProgress(chunk);
      onData?.(chunk);
    });

    // --- Stream stderr ---
    procStderr.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stderrBuf.push(chunk);
      lastDataTime = Date.now();
      accumulateProgress(chunk);
      onData?.(chunk);
    });

    // Swallow EBADF errors on pipe streams (see process-manager.ts for details)
    procStdout.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EBADF') { stderrBuf.push(err.message); finish(null); }
    });
    procStderr.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EBADF') { stderrBuf.push(err.message); finish(null); }
    });

    // --- Process exit ---
    // Resolve on the shell's OWN 'exit', not only on 'close'. 'close' fires only
    // once every inherited stdio fd is closed — which a detached grandchild that
    // inherited the stdout/stderr pipes (e.g. `echo done; sleep 300 &`) keeps
    // open for its whole lifetime. A 'close'-only wait would therefore block for
    // the entire timeout even though the command already finished (stall
    // detection won't fire either — a silent grandchild doesn't look like a
    // prompt). On 'exit' we wait for a short QUIET window (no new output) so a
    // normally-draining pipe still delivers its tail and 'close' can win the
    // race; only when 'close' stays blocked do we destroy the pipes and conclude
    // with the shell's real exit code/signal.
    proc.on('exit', (code, signal) => {
      if (settled) return;
      const drain = (): void => {
        exitDrainTimer = setTimeout(() => {
          if (settled) return;
          // Output arrived within the window — the pipe is still flushing, so
          // extend the quiet period rather than truncating a large result.
          if (Date.now() - lastDataTime < EXIT_DRAIN_QUIET_MS) { drain(); return; }
          destroyStreams(proc);
          concludeWithSignalNote(code, signal);
        }, EXIT_DRAIN_QUIET_MS);
      };
      drain();
    });

    // Node's 'close' passes (code, signal): a signal-killed child reports
    // code === null with signal set (e.g. 'SIGSEGV'). When it fires it carries
    // the fully-drained output, so it is preferred over the 'exit' fallback.
    proc.on('close', (code, signal) => {
      concludeWithSignalNote(code, signal);
    });

    proc.on('error', (err: NodeJS.ErrnoException) => {
      // Map the common "can't start the process at all" failure to actionable
      // guidance naming the shell + cwd, instead of the raw errno message.
      if (err.code === 'ENOENT') {
        stderrBuf.push(`Failed to start command: shell "${shell}" or working directory "${effectiveCwd}" could not be found (${err.message}).`);
      } else {
        stderrBuf.push(err.message);
      }
      // A spawn/pre-exec failure is a launch error, not a timeout — report the
      // dedicated exit code so callers don't mistake it for a hang and e.g.
      // re-run it in the background.
      finish(SPAWN_FAILURE_EXIT_CODE);
    });
    });

  // Resolve the per-shell env overrides, then launch. The seam returns a plain
  // record for the common case (launch SYNCHRONOUSLY, same-tick spawn preserved)
  // and a Promise only when a shell needs async init (then launch once it
  // settles). An async-prep failure is best-effort: fall back to no overrides so
  // the command still runs.
  const overridesResult = shellHooks.getEnvironmentOverrides(effectiveShellType, command);
  if (typeof (overridesResult as Promise<Record<string, string>>).then === 'function') {
    return (overridesResult as Promise<Record<string, string>>).then(run, () => run({}));
  }
  return run(overridesResult as Record<string, string>);
}

/**
 * Read the system process table once as a child-pid → parent-pid (ppid) map,
 * bounded by a hard `timeoutMs` on the underlying `ps`/`wmic` invocation. Returns
 * an EMPTY map (never throws) when the table can't be read — a non-zero exit, a
 * timeout, or an unparseable table — so callers degrade to "no ancestors known"
 * rather than crashing. POSIX reads {@link PS_TABLE_CMD} (`pid` then `ppid`);
 * Windows reads `wmic process get ParentProcessId,ProcessId`, whose two numeric
 * columns wmic prints in alphabetical order (ParentProcessId then ProcessId).
 */
export function readParentPidMap(timeoutMs = ANCESTOR_WALK_TIMEOUT_MS): Map<number, number> {
  const parentOf = new Map<number, number>();
  const isWindows = shellHooks.platform() === 'win32';
  let table: string;
  try {
    const cmd = isWindows ? 'wmic process get ParentProcessId,ProcessId' : PS_TABLE_CMD;
    table = shellHooks.execSync(cmd, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: timeoutMs,
    }).toString();
  } catch {
    return parentOf; // empty — no-throw fallback
  }
  for (const line of table.split('\n')) {
    const match = line.trim().match(/^(\d+)\s+(\d+)$/);
    if (!match) continue; // skip headers / blank / malformed rows
    const a = Number(match[1]);
    const b = Number(match[2]);
    // POSIX `ps` prints `pid ppid`; wmic prints `ParentProcessId ProcessId`
    // (alphabetical). Either way, record child → parent.
    if (isWindows) parentOf.set(b, a);
    else parentOf.set(a, b);
  }
  return parentOf;
}

/**
 * Walk the ANCESTOR (parent) chain of `startPid` UPWARD, returning ancestor pids
 * from immediate parent to furthest ancestor — parity with the reference's
 * getAncestorPidsAsync (genericProcessUtils.ts:L36-L86). Three defensive bounds:
 *   - a `maxDepth` cap (default 10) so a corrupt/cyclic ppid link can't loop
 *     forever (a visited-set ALSO breaks a cycle before the cap is reached),
 *   - a hard per-read `timeoutMs` ({@link ANCESTOR_WALK_TIMEOUT_MS}) via
 *     {@link readParentPidMap} so a hung `ps`/`wmic` can't stall the caller, and
 *   - a no-throw fallback to `[]` (unreadable table, `startPid` ≤ 1, or no parent).
 * Uses ONE process-table snapshot and walks it in-process (not N sequential `ps`
 * invocations). Stops at a pid ≤ 1 (0 = process group, 1 = init) like the reference.
 */
export function collectAncestorPids(
  startPid: number,
  maxDepth = 10,
  timeoutMs = ANCESTOR_WALK_TIMEOUT_MS,
): number[] {
  if (!Number.isInteger(startPid) || startPid <= 1 || maxDepth <= 0) return [];
  const parentOf = readParentPidMap(timeoutMs);
  if (parentOf.size === 0) return [];
  const ancestors: number[] = [];
  const seen = new Set<number>([startPid]);
  let cur = startPid;
  for (let depth = 0; depth < maxDepth; depth++) {
    const ppid = parentOf.get(cur);
    if (ppid === undefined || ppid <= 1 || seen.has(ppid)) break;
    ancestors.push(ppid);
    seen.add(ppid);
    cur = ppid;
  }
  return ancestors;
}

/**
 * Enumerate a process and ALL its descendants (the process TREE) by reading the
 * system process table (`ps`) and walking the ppid graph down from `rootPid`.
 *
 * Used by {@link killTree} on POSIX so a descendant that escaped the child's
 * process group — via `setsid()` / becoming its own session leader — is still
 * terminated. A bare process-group kill (`kill -<pid>`) misses those, leaving
 * orphaned processes that keep consuming resources after a timeout/abort/kill.
 * Only the subtree rooted at `rootPid` is returned, never an unrelated pid, so
 * the kill is safe by construction. Falls back to `[rootPid]` if the process
 * table can't be read.
 *
 * The underlying `ps` read is bounded by a hard `timeoutMs`
 * ({@link DESCENDANT_WALK_TIMEOUT_MS}) — matching the bounded ancestor walk
 * ({@link readParentPidMap}) and the reference's 1000ms getChildPids cap — so a
 * hung `ps` can never stall the kill path; on timeout the throw degrades to the
 * `[rootPid]` fallback.
 */
export function collectProcessTree(rootPid: number, timeoutMs = DESCENDANT_WALK_TIMEOUT_MS): number[] {
  let table: string;
  try {
    table = shellHooks.execSync(PS_TABLE_CMD, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: timeoutMs,
    }).toString();
  } catch {
    return [rootPid];
  }
  const childrenOf = new Map<number, number[]>();
  for (const line of table.split('\n')) {
    const match = line.trim().match(/^(\d+)\s+(\d+)$/);
    if (!match) continue;
    const pid = Number(match[1]);
    const ppid = Number(match[2]);
    const siblings = childrenOf.get(ppid);
    if (siblings) siblings.push(pid);
    else childrenOf.set(ppid, [pid]);
  }
  const tree: number[] = [];
  const seen = new Set<number>();
  const stack = [rootPid];
  while (stack.length > 0) {
    const pid = stack.pop()!;
    if (seen.has(pid)) continue;
    seen.add(pid);
    tree.push(pid);
    for (const child of childrenOf.get(pid) ?? []) stack.push(child);
  }
  return tree;
}

/**
 * Kill the process and its descendants.
 *
 * On Windows `taskkill /T` is already tree-based. On POSIX we kill every pid in
 * the process TREE individually (via {@link collectProcessTree} — catching
 * descendants that escaped the group with `setsid()`), THEN signal the process
 * group (negative pid) to mop up anything the `ps` snapshot missed due to a
 * race. If nothing could be signalled, fall back to the ChildProcess handle.
 */
export function killTree(proc: child_process.ChildProcess): void {
  if (proc.pid == null) return;

  if (shellHooks.platform() === 'win32') {
    try {
      shellHooks.execSync(`taskkill /pid ${proc.pid} /T /F`, { stdio: 'ignore' });
    } catch {
      try { proc.kill('SIGKILL'); } catch { /* already dead */ }
    }
    return;
  }

  let signalled = false;
  for (const pid of collectProcessTree(proc.pid)) {
    try { shellHooks.kill(pid, 'SIGKILL'); signalled = true; } catch { /* already dead */ }
  }
  // Process-group sweep (negative pid) for anything the snapshot raced past.
  try { shellHooks.kill(-proc.pid, 'SIGKILL'); signalled = true; } catch { /* group gone */ }
  if (!signalled) {
    try { proc.kill('SIGKILL'); } catch { /* nothing left to do */ }
  }
}
