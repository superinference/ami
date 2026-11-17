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
  execFile: child_process.execFile as typeof child_process.execFile,
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
  mkdirSync: fs.mkdirSync as typeof fs.mkdirSync,
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
   * command-assembly decisions (parity with Claude Code's `logForDebugging` in
   * bashProvider.ts). Tests replace it via `mock.method(shellHooks, 'onShellDebug', …)`.
   */
  onShellDebug: ((message: string): void => {
    if ((process.env[SHELL_DEBUG_ENV] || '').trim() !== '') {
      process.stderr.write(`[shell-debug] ${message}\n`);
    }
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

// ---------------------------------------------------------------------------
// Shared never-throw file+args exec wrapper (execFileNoThrow)
//
// Parity with Claude Code's `execFileNoThrow` / `execFileNoThrowWithCwd`
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

/** Options for {@link execFileNoThrow}. Mirrors the reference `ExecFileWithCwdOptions`. */
export interface ExecFileNoThrowOptions {
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  /** Per-call timeout (ms); defaults to {@link DEFAULT_EXEC_FILE_TIMEOUT_MS}. */
  timeout?: number;
  /** Abort signal; an aborted turn kills the in-flight process (result `code: 1`). */
  abortSignal?: AbortSignal;
  /** Captured stdout/stderr ceiling (bytes); defaults to {@link DEFAULT_EXEC_FILE_MAX_BUFFER}. */
  maxBuffer?: number;
  /**
   * When `false`, a non-zero exit resolves with empty `stdout`/`stderr` (only the
   * `code` is kept) — for noisy probe commands. Defaults to `true`. Parity with
   * the reference `preserveOutputOnError`.
   */
  preserveOutputOnError?: boolean;
  /**
   * Run through a shell (`true`), a specific shell (string path), or not at all
   * (`false`/omitted → direct argv execFile). Parity with the reference
   * `shell?: boolean | string`. On Windows a `.bat`/`.cmd` target is run through a
   * shell automatically even when this is omitted (Node cannot spawn it directly).
   */
  shell?: boolean | string;
  /** Payload fed to the child's stdin, then closed. */
  input?: string;
}

/** Structured result of {@link execFileNoThrow}. Mirrors the reference shape. */
export interface ExecFileNoThrowResult {
  stdout: string;
  stderr: string;
  /** Exit code: `0` on success, the process exit code on a non-zero exit, else `1`. */
  code: number;
  /** Human-readable failure reason (message / signal / code). Absent on success. */
  error?: string;
}

/** True when `file` names a Windows batch script Node cannot `execFile` directly. */
function isWindowsBatchFile(file: string): boolean {
  return /\.(bat|cmd)$/i.test(file);
}

/**
 * Quote one argument for a Windows `cmd.exe` command line: wrap in double quotes
 * (doubling backslashes before a quote and escaping embedded quotes) only when it
 * contains whitespace or a cmd metacharacter, so a plain token stays untouched.
 * Used when {@link execFileNoThrow} assembles a shell command string on Windows —
 * the automatic argument escaping the reference gets from execa.
 */
export function quoteWindowsShellArg(arg: string): string {
  if (arg.length === 0) return '""';
  if (!/[\s"&|<>^()%!]/.test(arg)) return arg;
  const escaped = arg.replace(/(\\*)"/g, '$1$1\\"').replace(/(\\*)$/, '$1$1');
  return `"${escaped}"`;
}

/**
 * Never-throw file+args exec. Runs `file` with `args` via {@link shellHooks.execFile}
 * (no shell by default — argv form, injection-safe) and ALWAYS resolves with an
 * {@link ExecFileNoThrowResult}: a non-zero exit, spawn error (ENOENT/EACCES), a
 * timeout, or an abort are mapped to the result instead of throwing. When a shell
 * is used (explicit `shell`, or an auto-detected Windows `.bat`/`.cmd`), the file
 * and args are assembled into a single escaped command string so arguments can't
 * be reinterpreted as shell metacharacters. Parity with Claude Code's
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

/**
 * Baseline default per-command timeout (ms) when a caller passes none AND no env
 * override is set. Overridable per call via {@link ExecCommandOptions.timeout}
 * and globally via {@link DEFAULT_TIMEOUT_ENV} (see {@link resolveDefaultTimeoutMs}).
 */
export const DEFAULT_TIMEOUT_MS = 120_000;

/**
 * Env knobs (ms) that raise/lower the default per-command timeout for commands
 * that don't pass an explicit {@link ExecCommandOptions.timeout}. AMI's baseline
 * is 2 minutes — conservative for long builds/test suites. Setting
 * `AMI_DEFAULT_TIMEOUT_MS` (or `CLAUDE_CODE_SHELL_TIMEOUT` for parity) lets an
 * operator match Claude Code's 30-minute default (Shell.ts:L44) or any other
 * value. An unset / blank / non-numeric / non-positive value keeps the baseline,
 * so default behavior is unchanged. An opt-in introspection surface per AMI's
 * introspection-on-every-change invariant.
 */
export const DEFAULT_TIMEOUT_ENV = ['AMI_DEFAULT_TIMEOUT_MS', 'CLAUDE_CODE_SHELL_TIMEOUT'] as const;

/**
 * Resolve the default per-command timeout (ms), honoring {@link DEFAULT_TIMEOUT_ENV}.
 * Returns {@link DEFAULT_TIMEOUT_MS} when the override is unset / blank / NaN /
 * non-positive. Used as the fallback timeout by {@link execCommand} when a call
 * supplies no explicit `timeout`.
 */
export function resolveDefaultTimeoutMs(): number {
  const raw = DEFAULT_TIMEOUT_ENV.map(k => (process.env[k] || '').trim()).find(Boolean) || '';
  if (raw === '') return DEFAULT_TIMEOUT_MS;
  const ms = Number(raw);
  if (!Number.isFinite(ms) || ms <= 0) return DEFAULT_TIMEOUT_MS;
  return ms;
}

/** Bound captured output. Head plus tail so a long pytest log keeps its summary. */
const HEAD_CHARS = 670_000;
const TAIL_CHARS = 330_000;
const DEFAULT_STALL_TIMEOUT_MS = 45_000;

/**
 * Hard ceiling (bytes) on the complete command output persisted to disk when a
 * caller opts in with {@link ExecCommandOptions.persistFullOutput}. Mirrors
 * Claude Code's `MAX_PERSISTED_SIZE` (BashTool.tsx:L732): the full stream is
 * streamed to a file so the model can `file_read` the WHOLE output (including the
 * middle AMI's in-memory head+tail window drops), truncated only at this 64MB
 * boundary so a runaway producer can't exhaust the disk.
 */
export const MAX_PERSISTED_OUTPUT_BYTES = 64 * 1024 * 1024;

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

function createOutputBuffer(): { push: (chunk: string) => void; value: () => string; truncated: () => boolean } {
  let head = '';
  let tail = '';
  let dropped = 0;
  return {
    truncated() { return dropped > 0; },
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

/**
 * Default cadence (ms) of the {@link ExecCommandOptions.onProgress} heartbeat.
 * Only armed when a caller supplies `onProgress`; a caller may override it with
 * {@link ExecCommandOptions.progressIntervalMs}. Mirrors Claude Code's
 * progress-poll loop that fires even on a silent command so the loop can wake
 * and decide to background a long runner (TaskOutput.ts:L119-125).
 */
const DEFAULT_PROGRESS_INTERVAL_MS = 5_000;

/**
 * How many trailing characters of combined stdout+stderr are retained to shape
 * the {@link ExecCommandOptions.onProgress} tail slices. Bounds progress memory
 * on a huge-output command; the exact total byte count is tracked separately so
 * the line count is extrapolated from this sample when the output exceeds it
 * (parity with Claude Code's `PROGRESS_TAIL_BYTES`, TaskOutput.ts).
 */
const PROGRESS_TAIL_CHARS = 65_536;

/**
 * Compute the last-5-lines and last-100-lines tail slices plus the total line
 * count of `text`, by walking backwards over newline characters. This is the
 * shaping that feeds {@link ExecCommandOptions.onProgress}'s structured args
 * (parity with Claude Code's TaskOutput poller, TaskOutput.ts:L126-149). The
 * line count is EXACT here (AMI buffers the full command output in memory),
 * which is a superset of Claude's tail-sample extrapolation — no estimation
 * error — while the caller still applies a monotone `Math.max` so the counter
 * never moves backwards between ticks.
 *
 * The backward walk counts a final "no more newline" step as the first line, so
 * `"a\nb\nc"` yields `totalLines === 3` (3 lines, 2 newlines), matching Claude's
 * loop exactly. An empty string yields `totalLines === 0` and empty slices.
 */
export function computeProgressSlices(text: string): { last5: string; last100: string; totalLines: number } {
  let pos = text.length;
  let n5 = 0;
  let n100 = 0;
  let totalLines = 0;
  while (pos > 0) {
    pos = text.lastIndexOf('\n', pos - 1);
    totalLines++;
    if (totalLines === 5) n5 = pos <= 0 ? 0 : pos + 1;
    if (totalLines === 100) n100 = pos <= 0 ? 0 : pos + 1;
  }
  return { last5: text.slice(n5), last100: text.slice(n100), totalLines };
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

/** The interactive login-shell families AMI can classify from `$SHELL`. */
export type InteractiveShellType = 'zsh' | 'bash' | 'fish' | 'unknown';

/**
 * Classify the user's interactive login shell from a shell path (defaults to
 * `process.env.SHELL`) into `'zsh' | 'bash' | 'fish' | 'unknown'`. Lets callers
 * tailor setup/affordance guidance (PATH export, alias syntax) to the user's
 * actual shell — fish's `set -x PATH` differs from bash/zsh's `export PATH`.
 * Parity with Claude Code's `getShellType()` (localInstaller.ts:L156-162),
 * including its `zsh → bash → fish` precedence. Also consolidates the
 * shell-family detection {@link resolveShell} needs (single source for the
 * bash-vs-zsh preference), so there is one canonical classifier.
 */
export function getShellType(shellPath: string = process.env.SHELL || ''): InteractiveShellType {
  if (shellPath.includes('zsh')) return 'zsh';
  if (shellPath.includes('bash')) return 'bash';
  if (shellPath.includes('fish')) return 'fish';
  return 'unknown';
}

// Memoize the resolved shell per (platform, override, SHELL) tuple so probing
// only happens once per session for a given environment, while still
// re-resolving when the relevant env changes (e.g. in tests).
const shellResolutionCache = new Map<string, string | null>();

function resolveShell(override: string, envShell: string): string | null {
  // 1. Explicit override (AMI_SHELL / CLAUDE_CODE_SHELL) wins when it names a
  //    supported shell type and is actually executable. Emit introspective
  //    feedback on both outcomes (parity with Claude Code's Shell.ts override
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

// ---------------------------------------------------------------------------
// Shell-environment snapshot (user aliases / functions / options / PATH)
//
// Parity with Claude Code's ShellSnapshot.ts + bashProvider.ts snapshot sourcing.
// A command run through `bash -c '<cmd>'` does NOT see the user's interactive
// aliases/functions (those live in ~/.bashrc / ~/.zshrc, which a non-login,
// non-interactive shell never sources). Claude captures them ONCE per session
// into a snapshot file and `source`s it before each command, so model-run
// commands behave like the user's interactive shell WITHOUT paying a full
// login-shell startup per command. AMI mirrors this as an OPT-IN capability
// (env knob {@link SHELL_SNAPSHOT_ENV}) so default behavior is unchanged:
// - enabled → create+source the snapshot, spawn WITHOUT `-l`;
// - enabled but snapshot creation failed → gracefully fall back to a login
//   shell (`-l`) so the profile/rc files are at least sourced;
// - disabled → current behavior (login only when `AMI_LOGIN_SHELL` is set).
// Aliases are re-applied via `eval` (a second parse pass) because the shell
// parses a whole command line before executing, so an alias sourced on the same
// line would not expand — the exact reason Claude wraps the user command in
// `eval` after sourcing the snapshot.
// ---------------------------------------------------------------------------

/**
 * Env knob (opt-in) that turns on per-command shell-environment snapshots. When
 * truthy (`1`/`true`/`on`/`yes`), {@link execCommand}'s bash path captures the
 * user's shell config (aliases, functions, options, PATH) once per session and
 * sources it before every command so model-run commands see the user's
 * interactive environment. Unset/blank/falsey leaves the current behavior
 * unchanged. Mirrors Claude Code's always-on snapshot (ShellSnapshot.ts); AMI
 * makes it opt-in to keep the default command environment deterministic.
 */
export const SHELL_SNAPSHOT_ENV = 'AMI_SHELL_SNAPSHOT';

/**
 * Env knob that enables verbose shell-command-assembly debug logging through
 * {@link shellHooks.onShellDebug} (snapshot-missing login fallback, `-l` skip,
 * snapshot-source decision). Unset/blank disables it. Parity with Claude Code's
 * `logForDebugging` tracing in bashProvider.ts.
 */
export const SHELL_DEBUG_ENV = 'AMI_SHELL_DEBUG';

/** True when {@link SHELL_SNAPSHOT_ENV} names an enabled value. */
function shellSnapshotEnabled(): boolean {
  const raw = (process.env[SHELL_SNAPSHOT_ENV] || '').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'on' || raw === 'yes';
}

/** How long the one-shot snapshot-capture shell may run before giving up (ms). */
const SNAPSHOT_CREATION_TIMEOUT_MS = 10_000;

/**
 * The user shell config file a snapshot sources, chosen from the shell family:
 * zsh → `~/.zshrc`, bash → `~/.bashrc`, anything else → `~/.profile`. Parity
 * with Claude Code's `getConfigFile` (ShellSnapshot.ts).
 */
function snapshotConfigFile(shellPath: string): string {
  const fileName = shellPath.includes('zsh')
    ? '.zshrc'
    : shellPath.includes('bash')
      ? '.bashrc'
      : '.profile';
  return path.join(os.homedir(), fileName);
}

/**
 * The capture script that, when run by the user's shell as a login shell, writes
 * a sourceable snapshot of their functions, shell options, aliases and PATH to
 * `$SNAPSHOT_FILE`. Faithful to Claude Code's `getSnapshotScript` /
 * `getUserSnapshotContent`: zsh and bash use their respective introspection
 * (`typeset` vs `declare`, `setopt` vs `shopt`), completion functions
 * (single-underscore prefix) are filtered out while `__`-prefixed helpers are
 * kept, each capture is head-capped at 1000 lines, and `expand_aliases` is
 * force-enabled so a sourced alias expands in the subsequent `eval` pass.
 */
function buildSnapshotScript(shellPath: string, configFileExists: boolean): string {
  const isZsh = shellPath.includes('zsh');
  const functions = isZsh
    ? `typeset -f > /dev/null 2>&1
typeset +f | grep -vE '^_[^_]' | while read func; do typeset -f "$func" >> "$SNAPSHOT_FILE"; done`
    : `declare -f > /dev/null 2>&1
declare -F | cut -d' ' -f3 | grep -vE '^_[^_]' | while read func; do declare -f "$func" >> "$SNAPSHOT_FILE"; done`;
  const options = isZsh
    ? `setopt | sed 's/^/setopt /' | head -n 1000 >> "$SNAPSHOT_FILE"
echo "setopt aliases" >> "$SNAPSHOT_FILE"`
    : `shopt -p | head -n 1000 >> "$SNAPSHOT_FILE"
echo "shopt -s expand_aliases" >> "$SNAPSHOT_FILE"`;
  const userContent = configFileExists
    ? `echo "# Functions" >> "$SNAPSHOT_FILE"
${functions}
echo "# Shell Options" >> "$SNAPSHOT_FILE"
${options}
echo "# Aliases" >> "$SNAPSHOT_FILE"
alias | sed 's/^alias //g' | sed 's/^/alias -- /' | head -n 1000 >> "$SNAPSHOT_FILE"`
    : isZsh
      ? '# No user config file to source'
      : 'echo "shopt -s expand_aliases" >> "$SNAPSHOT_FILE"';
  const configFile = snapshotConfigFile(shellPath);
  return `SNAPSHOT_FILE="$AMI_SNAPSHOT_FILE"
${configFileExists ? `source ${shQuote(configFile)} < /dev/null` : '# No user config file to source'}
echo "# Snapshot file" >| "$SNAPSHOT_FILE"
echo "unalias -a 2>/dev/null || true" >> "$SNAPSHOT_FILE"
${userContent}
echo "export PATH=${shQuote(process.env.PATH || '')}" >> "$SNAPSHOT_FILE"
if [ ! -f "$SNAPSHOT_FILE" ]; then echo "snapshot not created" >&2; exit 1; fi`;
}

/**
 * Capture a shell-environment snapshot for `shellPath` and return the path to
 * the sourceable snapshot file, or `null` if capture failed (missing shell,
 * timeout, permission error — the caller then falls back to a login shell).
 * Runs the user's shell as a login shell (`-l`) so profile/rc files are sourced,
 * then records the resulting aliases/functions/options/PATH. Parity with Claude
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
    if (!shellHooks.existsSync(file)) {
      shellHooks.onShellDebug(`Snapshot file was not created: ${file}`);
      return null;
    }
    return file;
  } catch (err) {
    shellHooks.onShellDebug(`Failed to create shell snapshot: ${(err as Error).message}`);
    return null;
  }
}

// Memoize the snapshot per shell path so capture (a login-shell subprocess) runs
// at most once per session per shell — parity with Claude's one-snapshot-per-session.
const shellSnapshotCache = new Map<string, string | null>();

/**
 * Memoized {@link createShellSnapshot}. The snapshot is captured once per shell
 * path per session. Parity with Claude Code's per-session snapshot promise.
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
 * True when `execPath` resolves inside the current working directory subtree — a
 * planted `pwsh`/`powershell`/`git.exe`/`git.bat` in (or under) the CWD that
 * could hijack execution. Parity with Claude Code's `where.exe` cwd-shadowing
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

// ---------------------------------------------------------------------------
// Windows git-bash resolution (trusted bash.exe discovery)
//
// On Windows AMI runs user bash commands through git-bash's `bash.exe` — the
// same POSIX semantics the bash security guards assume — NOT `cmd`. Parity with
// Claude Code's windowsPaths.ts (findExecutable + findGitBashPath): an explicit
// AMI_GIT_BASH_PATH / CLAUDE_CODE_GIT_BASH_PATH override wins, else git's default
// install locations are probed, else `where.exe` resolves `git` with a
// cwd-shadow security filter and `bash.exe` is derived from it. Unlike Claude
// (which `process.exit(1)`s when none is found), AMI returns `null` so
// {@link execCommand} surfaces an actionable launch failure instead of killing
// the whole process — a library-safe divergence.
// ---------------------------------------------------------------------------

/** Env knobs naming an explicit git-bash `bash.exe` path (overrides discovery). */
const GIT_BASH_PATH_ENV = ['AMI_GIT_BASH_PATH', 'CLAUDE_CODE_GIT_BASH_PATH'] as const;

/**
 * Default git-for-Windows install locations probed before `where.exe` (64-bit
 * before 32-bit). The `cmd\git.exe` entry is used — not `mingw64\bin\git.exe`,
 * whose directory is the "raw" tools without the environment setup git-bash
 * expects. Parity with Claude Code's `findExecutable` default locations.
 */
const GIT_DEFAULT_LOCATIONS = [
  'C:\\Program Files\\Git\\cmd\\git.exe',
  'C:\\Program Files (x86)\\Git\\cmd\\git.exe',
];

/**
 * Resolve a TRUSTED executable path on Windows. For `git`, well-known install
 * locations are probed first; otherwise `where.exe` is consulted and every
 * candidate that resolves inside the current working directory tree is rejected
 * ({@link isCwdShadowed}) so a `git.exe`/`git.bat` planted alongside the project
 * can never be run in place of the real tool. Returns the first trusted hit, or
 * `null`. Parity with Claude Code's `findExecutable` (windowsPaths.ts:L29-80).
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
    return candidates.find(c => !isCwdShadowed(c)) ?? null;
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
 * (callers surface an actionable error). Parity with Claude Code's
 * `findGitBashPath` (windowsPaths.ts:L98-125), except AMI returns `null` instead
 * of exiting the process.
 */
export function findGitBashPath(): string | null {
  const override = GIT_BASH_PATH_ENV.map(k => (process.env[k] || '').trim()).find(Boolean);
  if (override) return shellHooks.existsSync(override) ? override : null;

  const gitPath = findExecutable('git');
  if (gitPath) {
    // <git>/cmd/git.exe → <git>/bin/bash.exe
    const bashPath = path.win32.join(gitPath, '..', '..', 'bin', 'bash.exe');
    if (shellHooks.existsSync(bashPath)) return bashPath;
  }
  return null;
}

let cachedGitBashPath: string | null | undefined;

/**
 * Memoized {@link findGitBashPath}. Discovery shells out to `where.exe`, so it
 * runs once per session. Parity with Claude Code's `memoize(findGitBashPath)`.
 */
export function getCachedGitBashPath(): string | null {
  if (cachedGitBashPath === undefined) cachedGitBashPath = findGitBashPath();
  return cachedGitBashPath;
}

/** Reset the memoized git-bash path (tests / after a PATH or env change). */
export function resetGitBashCache(): void {
  cachedGitBashPath = undefined;
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
  /**
   * When `true`, skip shell-environment snapshot sourcing for this call even if
   * {@link SHELL_SNAPSHOT_ENV} is enabled (the command runs with a plain
   * non-login shell). Used for probe/internal commands that must not inherit the
   * user's aliases/functions. Parity with Claude Code's `createBashShellProvider({
   * skipSnapshot })` (bashProvider.ts). No effect when snapshots are disabled.
   */
  skipSnapshot?: boolean;
  /**
   * Structured progress callback, distinct from {@link onData}'s raw-chunk pipe.
   * Fires on a cadence ({@link progressIntervalMs}) with, positionally:
   *   `(lastLines, allLines, totalLines, totalBytes, isIncomplete)`
   * — the last 5 lines, the last 100 lines, the running (monotone) total line
   * count, total bytes seen, and whether the delivered sample omits earlier
   * output (`totalLines > 100`). It fires EVEN when no new output arrived since
   * the last tick (empty `lastLines`/`allLines`) so a caller's progress loop
   * wakes on a silent long-running command and can decide to background it.
   * Parity with Claude Code's `ExecOptions.onProgress` (Shell.ts:L161-169) +
   * the always-tick poller (TaskOutput.ts:L119-149). `onData` still streams raw
   * chunks for callers that only want the bytes.
   */
  onProgress?: (lastLines: string, allLines: string, totalLines: number, totalBytes: number, isIncomplete: boolean) => void;
  /**
   * Heartbeat cadence (ms) for {@link onProgress}. Only used when `onProgress`
   * is set; defaults to {@link DEFAULT_PROGRESS_INTERVAL_MS}. A value `<= 0`
   * disables the heartbeat entirely (no progress ticks).
   */
  progressIntervalMs?: number;
  /**
   * When `false`, the command's final working directory (captured via `pwd -P`
   * on the bash path) is written into {@link ExecCommandResult.cwd} so a caller
   * can persist a `cd` across invocations (parity with Claude Code's
   * `ExecOptions.preventCwdChanges` / `nativeCwdFilePath`, Shell.ts:L170,
   * L385-414). Defaults to NOT tracking (undefined/`true`) so AMI's existing
   * per-call `cwd` contract and all current callers are unchanged — a caller
   * opts in by passing `preventCwdChanges: false`. No-op for the PowerShell
   * shell family.
   */
  preventCwdChanges?: boolean;
  /**
   * When `true`, stream the command's COMPLETE stdout to a temp file (capped at
   * {@link MAX_PERSISTED_OUTPUT_BYTES} = 64MB) so the model can retrieve the whole
   * output — including the middle that the in-memory head+tail window
   * ({@link HEAD_CHARS}/{@link TAIL_CHARS}) drops — via `file_read`. The path +
   * byte size are returned in {@link ExecCommandResult.fullOutputPath} /
   * {@link ExecCommandResult.fullOutputSize}, but ONLY when the captured output
   * actually overflowed the in-memory window (otherwise the inline result is
   * already complete and the temp file is discarded, so small commands pay no
   * retrieval surface). Defaults to off. Parity with Claude Code's
   * persist-complete-output-to-disk model (BashTool.tsx:L725-753).
   */
  persistFullOutput?: boolean;
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
  /**
   * `true` when a SOFT `'interrupt'` abort (the caller submitted a new message
   * mid-command, i.e. `abortSignal.reason === 'interrupt'`) resolved this call
   * WITHOUT killing the process — it is left running so the caller can background
   * it and keep its in-flight/partial work. A hard abort (any other reason)
   * still kills the process tree and leaves this unset. Parity with Claude
   * Code's `#abortHandler` interrupt branch (ShellCommand.ts:L186-193).
   */
  backgrounded?: boolean;
  /**
   * The command's final working directory, captured via `pwd -P` on the bash
   * path, present ONLY when the caller opted in with
   * {@link ExecCommandOptions.preventCwdChanges} `=== false`. Lets a caller
   * persist a `cd` across invocations. Absent when tracking is off, on the
   * PowerShell path, or when the command exited before `pwd -P` ran.
   */
  cwd?: string;
  /**
   * Path to the temp file holding the command's COMPLETE stdout, present ONLY
   * when the caller set {@link ExecCommandOptions.persistFullOutput} AND the
   * captured stdout overflowed the in-memory head+tail window (so the complete
   * output — including the dropped middle — is on disk for `file_read`). Absent
   * when persistence was off, the output fit in memory, or the command was
   * backgrounded / its output was suppressed.
   */
  fullOutputPath?: string;
  /**
   * Byte size of the persisted complete stdout ({@link fullOutputPath}), capped
   * at {@link MAX_PERSISTED_OUTPUT_BYTES}. Present iff {@link fullOutputPath} is.
   */
  fullOutputSize?: number;
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

  return new Promise<ExecCommandResult>((resolve) => {
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
        const room = MAX_PERSISTED_OUTPUT_BYTES - fullOutputBytes;
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
            stderr: stderrBuf.value() + 'No git-bash found. AMI runs commands through bash on Windows: install git-bash (https://git-scm.com/downloads/win), or set AMI_GIT_BASH_PATH to its bash.exe.',
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
      // command is wrapped in `eval` after the source (parity with Claude Code's
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

      // Optional command wrapping via a configured shell prefix. The Claude-Code
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
      // invocations (parity with Claude Code's nativeCwdFilePath / `pwd -P`,
      // Shell.ts:L385-414). Read back + unlinked in finish().
      let trackedCommand = finalCommand;
      if (trackCwd) {
        cwdFile = path.join(resolveTmpBase(), `ami-cwd-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`);
        trackedCommand = `${finalCommand}\n__ami_ec=$?\ncommand pwd -P > ${shQuote(cwdFile)} 2>/dev/null\nexit $__ami_ec`;
      }
      shellArgs = [...loginArgs, '-c', trackedCommand];
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
      // kills the whole process tree. Parity: Claude Code's #abortHandler
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
 * The POSIX `ps` invocation that dumps the whole process table as `pid ppid`
 * rows. Shared by the descendant ({@link collectProcessTree}) and ancestor
 * ({@link collectAncestorPids}) process-tree walks so the table-read command has
 * ONE definition (No-Duplication rule).
 */
export const PS_TABLE_CMD = 'ps -A -o pid=,ppid=';

/**
 * Default hard timeout (ms) for the single process-table read backing the
 * ancestor walk (parity with Claude Code's getAncestorPidsAsync 3000ms cap), so a
 * hung `ps`/`wmic` can never stall the caller.
 */
export const ANCESTOR_WALK_TIMEOUT_MS = 3000;

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
 * from immediate parent to furthest ancestor — parity with Claude Code's
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
 */
export function collectProcessTree(rootPid: number): number[] {
  let table: string;
  try {
    table = shellHooks.execSync(PS_TABLE_CMD, {
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
