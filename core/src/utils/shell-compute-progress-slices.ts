// Split from shell.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as os from 'os';

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
 * the reference's `slowLogging` exec wrapper (execFileNoThrowPortable.ts:L70),
 * surfacing exec calls that silently run slow — an opt-in introspection surface
 * per AMI's introspection-on-every-change invariant.
 */
export const SLOW_EXEC_THRESHOLD_ENV = 'AMI_SLOW_EXEC_MS';

/**
 * Parse {@link SLOW_EXEC_THRESHOLD_ENV} into an active threshold in ms, or `null`
 * when slow-command diagnostics are disabled (unset / blank / NaN / negative).
 */
export function slowExecThresholdMs(): number | null {
  const raw = (process.env[SLOW_EXEC_THRESHOLD_ENV] || '').trim();
  if (raw === '') return null;
  const ms = Number(raw);
  if (!Number.isFinite(ms) || ms < 0) return null;
  return ms;
}

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
export function isWindowsBatchFile(file: string): boolean {
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
 * Base directory for AMI's internal temp files, honoring an operator override.
 * Mirrors the reference's `CLAUDE_CODE_TMPDIR` knob (Shell.ts:L204-207): an
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
 * operator match the reference's 30-minute default (Shell.ts:L44) or any other
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
export const HEAD_CHARS = 670_000;

export const TAIL_CHARS = 330_000;

export const DEFAULT_STALL_TIMEOUT_MS = 45_000;

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
export const EXIT_DRAIN_QUIET_MS = 150;

/**
 * Exit code reported when the shell/command could not be launched at all (no
 * suitable shell found, or the spawn itself failed — ENOENT/EACCES/etc.). 126 is
 * the standard Unix "command found but not executable / could not execute" code.
 * Surfacing it (instead of `null`) lets callers distinguish a launch failure
 * from a timeout or abort, both of which legitimately yield `exitCode: null`.
 */
export const SPAWN_FAILURE_EXIT_CODE = 126;

/** Candidate directories probed for a POSIX shell when resolving one. */
export const SHELL_SEARCH_PATHS = ['/bin', '/usr/bin', '/usr/local/bin', '/opt/homebrew/bin'];

export function createOutputBuffer(): { push: (chunk: string) => void; value: () => string; truncated: () => boolean } {
  let head = '';
  // Keep the retained tail as a list of chunks plus a running length instead of a
  // single growing string. The old code did `tail += chunk; tail = tail.slice(...)`
  // on every chunk once output exceeded TAIL_CHARS, forcing a full ~330k flatten +
  // re-copy per chunk — O(chunks × TAIL_CHARS) for a large streamed log. Appending
  // to an array and dropping whole leading chunks (partial-slicing only the first
  // retained one) makes push amortized O(chunk); value() joins once. Output is
  // byte-identical (same head + last TAIL_CHARS tail + dropped count).
  const tailChunks: string[] = [];
  let tailLen = 0;
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
      tailChunks.push(chunk);
      tailLen += chunk.length;
      if (tailLen > TAIL_CHARS) {
        let over = tailLen - TAIL_CHARS;
        dropped += over;
        tailLen -= over;
        while (over > 0) {
          const first = tailChunks[0]!;
          if (first.length <= over) {
            over -= first.length;
            tailChunks.shift();
          } else {
            tailChunks[0] = first.slice(over);
            over = 0;
          }
        }
      }
    },
    value() {
      const tail = tailChunks.join('');
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
 * {@link ExecCommandOptions.progressIntervalMs}. Mirrors the reference's
 * progress-poll loop that fires even on a silent command so the loop can wake
 * and decide to background a long runner (TaskOutput.ts:L119-125).
 */
export const DEFAULT_PROGRESS_INTERVAL_MS = 5_000;

/**
 * How many trailing characters of combined stdout+stderr are retained to shape
 * the {@link ExecCommandOptions.onProgress} tail slices. Bounds progress memory
 * on a huge-output command; the exact total byte count is tracked separately so
 * the line count is extrapolated from this sample when the output exceeds it
 * (parity with the reference's `PROGRESS_TAIL_BYTES`, TaskOutput.ts).
 */
export const PROGRESS_TAIL_CHARS = 65_536;

/**
 * Compute the last-5-lines and last-100-lines tail slices plus the total line
 * count of `text`, by walking backwards over newline characters. This is the
 * shaping that feeds {@link ExecCommandOptions.onProgress}'s structured args
 * (parity with the reference's TaskOutput poller, TaskOutput.ts:L126-149). The
 * line count is EXACT here (AMI buffers the full command output in memory),
 * which is a superset of the reference's tail-sample extrapolation — no estimation
 * error — while the caller still applies a monotone `Math.max` so the counter
 * never moves backwards between ticks.
 *
 * The backward walk counts a final "no more newline" step as the first line, so
 * `"a\nb\nc"` yields `totalLines === 3` (3 lines, 2 newlines), matching the reference's
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

export const PROMPT_PATTERNS = [
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
  // continue?", "Overwrite?", "Shall I proceed?" — more broadly than the reference's
  // enumerated prompt list, which only matches specific directed-question stems).
  /\?\s*$/,
  /:\s*$/,
  />\s*$/,
  /\$\s*$/,
  /%\s*$/,
  /#\s*$/,
];

export function looksLikePrompt(output: string): boolean {
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
export function shQuote(value: string): string {
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
export const NUL_REDIRECT_REGEX = /(\d?&?>+\s*)[Nn][Uu][Ll](?=\s|$|[|&;)\n])/g;

export function rewriteWindowsNullRedirect(command: string): string {
  return command.replace(NUL_REDIRECT_REGEX, '$1/dev/null');
}

/**
 * Short human-readable description for the common POSIX termination signals so a
 * signal-killed child reports WHAT killed it (not just a bare `exitCode: null`).
 * Returns `''` for signals without a well-known cause so the caller can still
 * surface the raw signal name on its own.
 */
export function describeSignal(signal: NodeJS.Signals): string {
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

/** The interactive login-shell families AMI can classify from `$SHELL`. */
export const INTERACTIVE_SHELL_TYPES = ['zsh', 'bash', 'fish', 'unknown'] as const;

export type InteractiveShellType = (typeof INTERACTIVE_SHELL_TYPES)[number];

/**
 * Classify the user's interactive login shell from a shell path (defaults to
 * `process.env.SHELL`) into `'zsh' | 'bash' | 'fish' | 'unknown'`. Lets callers
 * tailor setup/affordance guidance (PATH export, alias syntax) to the user's
 * actual shell — fish's `set -x PATH` differs from bash/zsh's `export PATH`.
 * Parity with the reference's `getShellType()` (localInstaller.ts:L156-162),
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
