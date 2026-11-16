import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

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
  execFileSync: child_process.execFileSync as typeof child_process.execFileSync,
  accessSync: fs.accessSync as typeof fs.accessSync,
  existsSync: fs.existsSync as typeof fs.existsSync,
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
};

/** Payload handed to {@link shellHooks.onSlowCommand} when a command runs slow. */
export interface SlowCommandInfo {
  command: string;
  durationMs: number;
  thresholdMs: number;
}

/**
 * Env knob (ms) that turns on slow-command diagnostics. When set to a finite,
 * non-negative number, any {@link execCommand} whose wall-clock duration reaches
 * that many milliseconds is reported through {@link shellHooks.onSlowCommand}.
 * Unset / blank / non-numeric / negative disables it (no diagnostic). Mirrors
 * Claude Code's `slowLogging` exec wrapper (execFileNoThrowPortable.ts:L70),
 * surfacing exec calls that silently run slow — an opt-in introspection surface
 * per AMI's introspection-on-every-change invariant.
 */
export const SLOW_EXEC_THRESHOLD_ENV = 'AMI_SLOW_EXEC_MS';

/**
 * Parse {@link SLOW_EXEC_THRESHOLD_ENV} into an active threshold in ms, or `null`
 * when slow-command diagnostics are disabled (unset / blank / NaN / negative).
 */
function slowExecThresholdMs(): number | null {
  const raw = (process.env[SLOW_EXEC_THRESHOLD_ENV] || '').trim();
  if (raw === '') return null;
  const ms = Number(raw);
  if (!Number.isFinite(ms) || ms < 0) return null;
  return ms;
}

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
 * a non-zero exit) is still flagged. This is the sync counterpart of Claude Code's
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

/**
 * Base directory for AMI's internal temp files, honoring an operator override.
 * Mirrors Claude Code's `CLAUDE_CODE_TMPDIR` knob (Shell.ts:L204-207): an
 * environment with custom temp requirements (read-only `/tmp`, a size-limited
 * tmpfs, a dedicated scratch volume) relocates every AMI-created temp file by
 * setting `AMI_TMPDIR` (`CLAUDE_CODE_TMPDIR` accepted as a fallback for parity).
 * When the override is unset/blank it falls back to the platform temp dir
 * ({@link os.tmpdir}), so default behavior is unchanged.
 */
export function resolveTmpBase(): string {
  const override = (process.env.AMI_TMPDIR || process.env.CLAUDE_CODE_TMPDIR || '').trim();
  return override || os.tmpdir();
}

const DEFAULT_TIMEOUT_MS = 120_000;
/** Bound captured output. Head plus tail so a long pytest log keeps its summary. */
const HEAD_CHARS = 670_000;
const TAIL_CHARS = 330_000;
const DEFAULT_STALL_TIMEOUT_MS = 45_000;

/**
 * After the shell process itself emits `'exit'`, how long to keep waiting for the
 * stdio pipes to emit `'close'` before concluding anyway.
 *
 * `'close'` fires only once EVERY inherited stdio fd is closed, which a detached
 * grandchild that inherited the pipes keeps open for its whole lifetime
 * (`echo done; sleep 300 &`) — so a `'close'`-only wait would block the whole
 * foreground timeout even though the command already finished. The window is a
 * QUIET period: any new output resets it, so a command still flushing a large
 * result is never truncated — only a genuinely idle-but-unclosed pipe triggers
 * the `'exit'`-based fallback.
 */
const EXIT_DRAIN_QUIET_MS = 150;

/**
 * Exit code reported when the shell/command could not be launched at all (no
 * suitable shell found, or the spawn itself failed — ENOENT/EACCES/etc.). 126 is
 * the standard Unix "command found but not executable / could not execute" code.
 * Surfacing it (instead of `null`) lets callers distinguish a launch failure
 * from a timeout or abort, both of which legitimately yield `exitCode: null`.
 */
export const SPAWN_FAILURE_EXIT_CODE = 126;

/** Candidate directories probed for a POSIX shell when resolving one. */
const SHELL_SEARCH_PATHS = ['/bin', '/usr/bin', '/usr/local/bin', '/opt/homebrew/bin'];

function createOutputBuffer(): { push: (chunk: string) => void; value: () => string } {
  let head = '';
  let tail = '';
  let dropped = 0;
  return {
    push(chunk: string) {
      if (head.length < HEAD_CHARS) {
        const room = HEAD_CHARS - head.length;
        head += chunk.slice(0, room);
        chunk = chunk.slice(room);
      }
      if (chunk.length === 0) return;
      tail += chunk;
      if (tail.length > TAIL_CHARS) {
        dropped += tail.length - TAIL_CHARS;
        tail = tail.slice(tail.length - TAIL_CHARS);
      }
    },
    value() {
      if (dropped === 0) return head + tail;
      // Report the magnitude so the caller/model knows the TRUE output size and
      // how much was kept, not just that some middle was dropped. The total is
      // the sum of the retained head + tail and the dropped middle.
      const total = head.length + dropped + tail.length;
      return head + `\n[kept first ${head.length} + last ${tail.length} of ${total} chars; ${dropped} truncated]\n` + tail;
    },
  };
}

const PROMPT_PATTERNS = [
  /\(y\/n\)\s*$/i,
  /\[y\/N\]\s*$/i,
  /\[Y\/n\]\s*$/i,
  /\(yes\/no\)\s*$/i,
  /press enter/i,
  /press any key/i,
  /password[:\s]*$/i,
  /passphrase[:\s]*$/i,
  /Are you sure\?/i,
  /Continue\?/i,
  /Proceed\?/i,
  // Any line ending in "?" (covers directed questions — "Do you want to
  // continue?", "Overwrite?", "Shall I proceed?" — more broadly than Claude's
  // enumerated prompt list, which only matches specific directed-question stems).
  /\?\s*$/,
  /:\s*$/,
  />\s*$/,
  /\$\s*$/,
  /%\s*$/,
  /#\s*$/,
];

function looksLikePrompt(output: string): boolean {
  // split() always returns at least one element, so the last index is defined.
  const lines = output.trimEnd().split('\n');
  const lastLine = lines[lines.length - 1]!.trim();
  if (lastLine.length === 0 || lastLine.length > 200) return false;
  return PROMPT_PATTERNS.some(p => p.test(lastLine));
}

/**
 * POSIX single-quote a string so it survives one round of shell word-splitting
 * unchanged. Embedded single quotes are closed, escaped, and reopened
 * (`'\''`). Used by {@link formatShellPrefixCommand}.
 */
function shQuote(value: string): string {
  return `'${value.replace(/'/g, `'\\''`)}'`;
}

/**
 * Wrap `command` with a user-configured shell prefix (the executable and its
 * args that should run the command), quoting each component so a path with
 * spaces is safe.
 *
 * - `"bash"`            → `'bash' 'command'`
 * - `"/usr/bin/bash -c"`→ `'/usr/bin/bash' -c 'command'`
 * - `"/Program Files/bash.exe -c"` → `'/Program Files/bash.exe' -c 'command'`
 *
 * The executable (everything before the last ` -<flag>`) is quoted as one unit;
 * the flags are passed through verbatim; the command is quoted as one argument.
 * Honored from the `AMI_SHELL_PREFIX` env var by {@link execCommand}.
 */
export function formatShellPrefixCommand(prefix: string, command: string): string {
  const spaceBeforeDash = prefix.lastIndexOf(' -');
  if (spaceBeforeDash > 0) {
    const execPath = prefix.substring(0, spaceBeforeDash);
    const args = prefix.substring(spaceBeforeDash + 1);
    return `${shQuote(execPath)} ${args} ${shQuote(command)}`;
  }
  return `${shQuote(prefix)} ${shQuote(command)}`;
}

/**
 * Rewrites Windows CMD-style `>nul` redirects to the POSIX `/dev/null`.
 *
 * The model occasionally hallucinates Windows CMD syntax (e.g. `ls 2>nul`) even
 * though AMI always runs commands through a POSIX shell (bash/zsh) on
 * non-Windows hosts. When that POSIX shell sees `2>nul`, it does NOT discard the
 * output — it creates a literal file named `nul` in the working directory, a
 * stray artifact that pollutes the tree and breaks `git add .` / `git clone`.
 * Rewriting the redirect target to `/dev/null` makes the hallucinated form
 * behave as the model intended.
 *
 * Matches the redirect-target `nul`/`NUL` in `>nul`, `> NUL`, `2>nul`, `&>nul`,
 * `>>nul` (case-insensitive). Does NOT match `>null`, `>nullable`, `>nul.txt`,
 * or a bare `cat nul.txt` argument.
 *
 * Limitation: this is a lexical rewrite, not a shell parse, so a quoted literal
 * like `echo ">nul"` is also rewritten. That is extremely rare and harmless
 * (printing `/dev/null` instead of `nul`), which is why the simple regex is
 * preferred over a full tokenizer. Only applied on the POSIX spawn path.
 */
const NUL_REDIRECT_REGEX = /(\d?&?>+\s*)[Nn][Uu][Ll](?=\s|$|[|&;)\n])/g;
export function rewriteWindowsNullRedirect(command: string): string {
  return command.replace(NUL_REDIRECT_REGEX, '$1/dev/null');
}

/**
 * Short human-readable description for the common POSIX termination signals so a
 * signal-killed child reports WHAT killed it (not just a bare `exitCode: null`).
 * Returns `''` for signals without a well-known cause so the caller can still
 * surface the raw signal name on its own.
 */
function describeSignal(signal: NodeJS.Signals): string {
  switch (signal) {
    case 'SIGSEGV': return ' (segmentation fault)';
    case 'SIGKILL': return ' (killed)';
    case 'SIGTERM': return ' (terminated)';
    case 'SIGABRT': return ' (aborted)';
    case 'SIGINT': return ' (interrupt)';
    case 'SIGQUIT': return ' (quit)';
    case 'SIGFPE': return ' (arithmetic error)';
    case 'SIGBUS': return ' (bus error)';
    case 'SIGILL': return ' (illegal instruction)';
    default: return '';
  }
}

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
  //    supported shell type and is actually executable.
  if (override) {
    const supported = override.includes('bash') || override.includes('zsh');
    if (supported && isExecutable(override)) return override;
  }
  // 2. Prefer the user's login shell type; probe standard install locations.
  const preferBash = envShell.includes('bash');
  const isEnvSupported = envShell.includes('bash') || envShell.includes('zsh');
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

// ---------------------------------------------------------------------------
// Cross-shell provider abstraction (PowerShell as a first-class shell type)
//
// Mirrors Claude Code's `src/utils/shell/*` provider split (shellProvider.ts
// SHELL_TYPES/ShellType, powershellProvider.ts invocation flags + exit-code
// normalization, powershellDetection.ts pwsh-over-powershell preference + snap
// avoidance + memoized cache, resolveDefaultShell.ts safe default). AMI keeps
// bash/zsh as the POSIX default and treats PowerShell as an OPT-IN shell family
// selected per call ({@link ExecCommandOptions.shellType}) or globally via the
// `AMI_DEFAULT_SHELL` / `CLAUDE_CODE_SHELL_TYPE` env knob — never auto-flipping
// Windows to PowerShell (parity with Claude's resolveDefaultShell, which does
// not auto-flip either).
// ---------------------------------------------------------------------------

/** The shell families AMI can spawn a command through. */
export const SHELL_TYPES = ['bash', 'powershell'] as const;
export type ShellType = (typeof SHELL_TYPES)[number];

/** Platform-independent default shell family (parity: Claude never auto-flips to PowerShell). */
export const DEFAULT_SHELL_TYPE: ShellType = 'bash';

/**
 * Resolve the default shell family for commands that do not name one, from the
 * `AMI_DEFAULT_SHELL` (or `CLAUDE_CODE_SHELL_TYPE` for parity) env knob. An
 * unset / blank / unrecognized value safely falls back to {@link DEFAULT_SHELL_TYPE}
 * (`'bash'`) — mirrors Claude Code's `resolveDefaultShell()` safe fallback.
 */
export function resolveDefaultShell(): ShellType {
  const raw = (process.env.AMI_DEFAULT_SHELL || process.env.CLAUDE_CODE_SHELL_TYPE || '').trim().toLowerCase();
  return (SHELL_TYPES as readonly string[]).includes(raw) ? (raw as ShellType) : DEFAULT_SHELL_TYPE;
}

/**
 * The hardened, non-interactive PowerShell invocation flags + command. Shared so
 * the flag set lives in exactly one place (parity with Claude Code's
 * `buildPowerShellArgs` in powershellProvider.ts):
 * - `-NoProfile` skips the user/system profile scripts (deterministic env, no
 *   slow or side-effecting startup), and
 * - `-NonInteractive` makes PowerShell fail fast instead of prompting, so a
 *   command that would block on a `Read-Host` returns rather than hanging.
 */
export function buildPowerShellArgs(command: string): string[] {
  return ['-NoProfile', '-NonInteractive', '-Command', command];
}

/**
 * Append PowerShell exit-code normalization to a command so the spawned process
 * exits with the correct code (parity with Claude Code's powershellProvider.ts
 * cwd-tracking `exit $_ec`).
 *
 * On Windows PowerShell 5.1 a native exe that merely writes to a PS-redirected
 * stderr (e.g. `git push 2>&1`) sets `$?` to `$false` even when the exe returned
 * exit 0, so a naive `if (!$?)` reports a false-positive failure. `$LASTEXITCODE`
 * holds the real native exit code and is `$null` only when no native exe has run
 * yet — so we prefer it and fall back to `$?` for cmdlet-only pipelines.
 */
export function withPowerShellExitCode(command: string): string {
  return `${command}\n$_ec = if ($null -ne $LASTEXITCODE) { $LASTEXITCODE } elseif ($?) { 0 } else { 1 }\nexit $_ec`;
}

/** Env knob naming an explicit PowerShell executable path (overrides detection). */
const POWERSHELL_PATH_ENV = ['AMI_POWERSHELL_PATH', 'CLAUDE_CODE_POWERSHELL_PATH'] as const;

/**
 * Known non-snap PowerShell install locations probed when PATH resolves `pwsh`
 * to a snap launcher (which can hang in subprocesses while snapd initializes
 * confinement). The underlying apt/rpm binary is reliable.
 */
const POWERSHELL_PROBE_PATHS = ['/opt/microsoft/powershell/7/pwsh', '/usr/bin/pwsh', '/usr/local/bin/pwsh'];

/** Best-effort symlink resolution; returns the input unchanged if it can't be resolved. */
function realpathSafe(p: string): string {
  try { return shellHooks.realpathSync(p); } catch { return p; }
}

/**
 * True when `execPath` resolves to the current working directory — a planted
 * `pwsh`/`powershell` in the CWD that could hijack execution. Parity with Claude
 * Code's `where.exe` cwd-shadowing security filter (windowsPaths.ts): a candidate
 * sitting directly in the CWD is rejected so a malicious binary dropped alongside
 * the project files is never run in place of the real shell.
 */
function isCwdShadowed(execPath: string): boolean {
  const resolved = path.resolve(execPath);
  return path.dirname(resolved) === path.resolve(process.cwd());
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
 * over `powershell` (Windows PowerShell 5.1) — parity with Claude Code's
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
 * shells out to `which`/`where.exe`). Parity with Claude Code's
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

export interface ExecCommandOptions {
  cwd: string;
  timeout?: number;
  abortSignal?: AbortSignal;
  onData?: (chunk: string) => void;
  env?: NodeJS.ProcessEnv;
  stallTimeoutMs?: number;
  /**
   * Optional string fed to the child process's stdin (then closed). When
   * omitted, stdin is `'ignore'`d as before. Lets callers pipe a payload into a
   * command (e.g. `git apply`, `patch`, `gpg`) without shell interpolation.
   */
  input?: string;
  /**
   * stdin mode for the child when no {@link input} payload is supplied
   * (`'ignore'` by default so interactive prompts are detected-and-killed by
   * stall detection). `'inherit'` connects the parent terminal's stdin; `'pipe'`
   * opens a pipe the caller drives. Ignored when `input` is set (that path always
   * pipes). Parity with Claude Code's exec wrapper `stdin` option
   * (execFileNoThrow.ts:L22).
   */
  stdin?: 'ignore' | 'inherit' | 'pipe';
  /**
   * When `false`, a command that exits with a non-zero code resolves with empty
   * `stdout`/`stderr` (only the exit code/signal are kept). Useful for
   * probe/detection commands whose noisy output on failure should be discarded.
   * Defaults to `true` (output always preserved). Parity with Claude Code's
   * `preserveOutputOnError` (execFileNoThrow.ts:L17). A timeout/abort/launch
   * failure (`exitCode === null`) always keeps its output so the caller can see
   * why it failed.
   */
  preserveOutputOnError?: boolean;
  /**
   * Shell family to run the command through (`'bash'` | `'powershell'`). When
   * omitted, {@link resolveDefaultShell} picks it from the `AMI_DEFAULT_SHELL`
   * env knob (default `'bash'`). Parity with Claude Code's `exec({ shellType })`
   * (Shell.ts). PowerShell is spawned with hardened, non-interactive flags and
   * exit-code normalization (see {@link buildPowerShellArgs} /
   * {@link withPowerShellExitCode}).
   */
  shellType?: ShellType;
  /**
   * Explicit shell executable path, overriding auto-resolution for this call.
   * For `'bash'` it replaces {@link findSuitableShell}; for `'powershell'` it
   * replaces {@link getCachedPowerShellPath}. Parity with Claude Code's
   * configurable `shellPath` provider parameter (shellProvider.ts). When blank,
   * the resolver / env override is used.
   */
  shellPath?: string;
}

export interface ExecCommandResult {
  stdout: string;
  stderr: string;
  exitCode: number | null;
  /**
   * The terminating signal (e.g. `'SIGSEGV'`, `'SIGKILL'`) when the process was
   * killed by one, else `null`. Lets callers distinguish a signal-kill / crash
   * from a timeout or clean exit, both of which otherwise surface as
   * `exitCode: null`.
   */
  signal: NodeJS.Signals | null;
}

/**
 * Executes a shell command using the resolved POSIX shell (bash/zsh, see
 * {@link findSuitableShell}) via `-c`, or `cmd /c` on Windows, streaming output
 * through an optional `onData` callback.
 *
 * Environment knobs (introspectable; apply to every `execCommand` caller):
 * - `AMI_DEFAULT_SHELL` / `CLAUDE_CODE_SHELL_TYPE` — default shell family
 *   (`bash` | `powershell`) when a call does not set {@link ExecCommandOptions.shellType}
 *   (see {@link resolveDefaultShell}); unrecognized values fall back to `bash`.
 * - `AMI_POWERSHELL_PATH` / `CLAUDE_CODE_POWERSHELL_PATH` — explicit PowerShell
 *   executable path, overriding `pwsh`/`powershell` detection (see {@link findPowerShell}).
 * - `AMI_SHELL` / `CLAUDE_CODE_SHELL` — explicit shell path override.
 * - `AMI_LOGIN_SHELL` — when set, spawn the shell as a login shell (`-l`) so
 *   the user's profile/rc files are sourced.
 * - `AMI_SHELL_PREFIX` / `CLAUDE_CODE_SHELL_PREFIX` — an executable+args that
 *   wraps the command, quoted per component (see {@link formatShellPrefixCommand}).
 * - `AMI_SLOW_EXEC_MS` ({@link SLOW_EXEC_THRESHOLD_ENV}) — when set, report any
 *   command whose wall-clock duration reaches that many ms via
 *   {@link shellHooks.onSlowCommand} (opt-in slow-command diagnostics).
 * - `AMI_TMPDIR` / `CLAUDE_CODE_TMPDIR` — relocate AMI's temp-file base
 *   (see {@link resolveTmpBase}).
 *
 * - Respects timeout (default 120 000 ms) and abort signal.
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
  const { cwd, timeout = DEFAULT_TIMEOUT_MS, abortSignal, onData, env, stallTimeoutMs = DEFAULT_STALL_TIMEOUT_MS, input, stdin, preserveOutputOnError, shellType, shellPath } = options;

  // Wall-clock start for slow-command diagnostics (see SLOW_EXEC_THRESHOLD_ENV).
  const startedAt = Date.now();
  const reportSlowExec = (): void => reportSlowExecIfSlow(command, startedAt);

  return new Promise<ExecCommandResult>((resolve) => {
    // If already aborted, short-circuit
    if (abortSignal?.aborted) {
      resolve({ stdout: '', stderr: 'Aborted', exitCode: null, signal: null });
      return;
    }

    const isWindows = shellHooks.platform() === 'win32';

    const stdoutBuf = createOutputBuffer();
    const stderrBuf = createOutputBuffer();

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
    // Pick the shell family: an explicit per-call `shellType` wins, otherwise the
    // operator default (`AMI_DEFAULT_SHELL` → `'bash'`). PowerShell is opt-in and
    // never auto-selected (parity with Claude's resolveDefaultShell).
    const effectiveShellType: ShellType = shellType ?? resolveDefaultShell();
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
      if (isWindows) {
        shell = explicitShellPath || 'cmd';
      } else {
        const resolved = explicitShellPath || findSuitableShell();
        if (resolved === null) {
          resolve({
            stdout: stdoutBuf.value(),
            stderr: stderrBuf.value() + 'No suitable shell found. AMI requires a POSIX shell: install bash or zsh, or set AMI_SHELL to its path.',
            exitCode: SPAWN_FAILURE_EXIT_CODE,
            signal: null,
          });
          return;
        }
        shell = resolved;
      }

      // On the POSIX spawn path, repair hallucinated Windows `>nul` redirects so
      // they discard output via `/dev/null` instead of creating a literal `nul`
      // file. On Windows (cmd), `>nul` is the correct native form — leave it.
      const normalizedCommand = isWindows ? command : rewriteWindowsNullRedirect(command);

      // Optional command wrapping via a configured shell prefix. The Claude-Code
      // name (CLAUDE_CODE_SHELL_PREFIX) is accepted as a fallback for parity.
      const prefix = process.env.AMI_SHELL_PREFIX || process.env.CLAUDE_CODE_SHELL_PREFIX;
      const finalCommand = prefix ? formatShellPrefixCommand(prefix, normalizedCommand) : normalizedCommand;

      // Login-shell flag sources profile/rc files when requested.
      const loginArgs = !isWindows && process.env.AMI_LOGIN_SHELL ? ['-l'] : [];
      shellArgs = isWindows ? ['/c', finalCommand] : [...loginArgs, '-c', finalCommand];
    }

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
      ...(env ? { env } : {}),
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

    const finish = (exitCode: number | null, signal: NodeJS.Signals | null = null): void => {
      if (settled) return;
      settled = true;
      cleanUp();
      reportSlowExec();
      // Optionally discard captured output on a genuine non-zero exit (probe
      // commands). A timeout/abort/launch failure (exitCode === null) always
      // keeps its output so the caller can tell WHY it failed.
      const suppress = preserveOutputOnError === false && exitCode != null && exitCode !== 0;
      resolve({
        stdout: suppress ? '' : stdoutBuf.value(),
        stderr: suppress ? '' : stderrBuf.value(),
        exitCode,
        signal,
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

    const cleanUp = (): void => {
      clearTimeout(timer);
      if (stallCheck) clearInterval(stallCheck);
      if (exitDrainTimer) clearTimeout(exitDrainTimer);
      abortSignal?.removeEventListener('abort', onAbort);
    };

    // --- Stream stdout ---
    procStdout.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stdoutBuf.push(chunk);
      lastDataTime = Date.now();
      onData?.(chunk);
    });

    // --- Stream stderr ---
    procStderr.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stderrBuf.push(chunk);
      lastDataTime = Date.now();
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
}

/**
 * Destroy stdout/stderr/stdin streams before killing a process to prevent
 * EBADF errors. When the child process is killed, the OS closes its pipe
 * ends; if the parent-side streams are still open, Node.js may throw EBADF
 * when it later tries to close the now-invalid file descriptors.
 */
function destroyStreams(proc: child_process.ChildProcess): void {
  for (const stream of [proc.stdout, proc.stderr, proc.stdin]) {
    if (stream && !stream.destroyed) {
      try { stream.destroy(); } catch { /* EBADF is expected here */ }
    }
  }
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
 */
export function collectProcessTree(rootPid: number): number[] {
  let table: string;
  try {
    table = shellHooks.execSync('ps -A -o pid=,ppid=', {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
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
