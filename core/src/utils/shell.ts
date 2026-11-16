import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';

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
  kill: ((pid: number, signal: NodeJS.Signals): void => { process.kill(pid, signal); }),
};

const DEFAULT_TIMEOUT_MS = 120_000;
/** Bound captured output. Head plus tail so a long pytest log keeps its summary. */
const HEAD_CHARS = 670_000;
const TAIL_CHARS = 330_000;
const DEFAULT_STALL_TIMEOUT_MS = 45_000;

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
      return head + `\n[${dropped} chars truncated]\n` + tail;
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
 * - `AMI_SHELL` / `CLAUDE_CODE_SHELL` — explicit shell path override.
 * - `AMI_LOGIN_SHELL` — when set, spawn the shell as a login shell (`-l`) so
 *   the user's profile/rc files are sourced.
 * - `AMI_SHELL_PREFIX` — an executable+args that wraps the command, quoted per
 *   component (see {@link formatShellPrefixCommand}).
 *
 * - Respects timeout (default 120 000 ms) and abort signal.
 * - Kills the entire process tree on timeout or abort.
 * - Recovers if the working directory no longer exists (falls back to a
 *   known-good root; fails with a clear message when none is available).
 * - Keeps the first 670k and last 330k characters of stdout and stderr.
 */
export function execCommand(
  command: string,
  options: ExecCommandOptions,
): Promise<ExecCommandResult> {
  const { cwd, timeout = DEFAULT_TIMEOUT_MS, abortSignal, onData, env, stallTimeoutMs = DEFAULT_STALL_TIMEOUT_MS, input } = options;

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
    let shell: string;
    if (isWindows) {
      shell = 'cmd';
    } else {
      const resolved = findSuitableShell();
      if (resolved === null) {
        resolve({
          stdout: stdoutBuf.value(),
          stderr: stderrBuf.value() + 'No suitable shell found. AMI requires a POSIX shell: install bash or zsh, or set AMI_SHELL to its path.',
          exitCode: null,
          signal: null,
        });
        return;
      }
      shell = resolved;
    }

    // Optional command wrapping via a configured shell prefix.
    const prefix = process.env.AMI_SHELL_PREFIX;
    const finalCommand = prefix ? formatShellPrefixCommand(prefix, command) : command;

    // Login-shell flag sources profile/rc files when requested.
    const loginArgs = !isWindows && process.env.AMI_LOGIN_SHELL ? ['-l'] : [];
    const shellArgs = isWindows ? ['/c', finalCommand] : [...loginArgs, '-c', finalCommand];

    const proc = shellHooks.spawn(shell, shellArgs, {
      // Pipe stdin only when the caller supplies input; otherwise ignore it so
      // interactive prompts are still detected-and-killed by stall detection.
      stdio: [input != null ? 'pipe' : 'ignore', 'pipe', 'pipe'],
      cwd: effectiveCwd,
      detached: !isWindows,
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

    const finish = (exitCode: number | null, signal: NodeJS.Signals | null = null): void => {
      if (settled) return;
      settled = true;
      cleanUp();
      resolve({
        stdout: stdoutBuf.value(),
        stderr: stderrBuf.value(),
        exitCode,
        signal,
      });
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
    // Node's 'close' passes (code, signal): a signal-killed child reports
    // code === null with signal set (e.g. 'SIGSEGV'), so we surface the signal
    // to distinguish a crash from a clean exit or a timeout.
    proc.on('close', (code, signal) => {
      finish(code, signal);
    });

    proc.on('error', (err: NodeJS.ErrnoException) => {
      // Map the common "can't start the process at all" failure to actionable
      // guidance naming the shell + cwd, instead of the raw errno message.
      if (err.code === 'ENOENT') {
        stderrBuf.push(`Failed to start command: shell "${shell}" or working directory "${effectiveCwd}" could not be found (${err.message}).`);
      } else {
        stderrBuf.push(err.message);
      }
      finish(null);
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
 * Kill the process and its descendants.
 */
function killTree(proc: child_process.ChildProcess): void {
  if (proc.pid == null) return;

  try {
    if (shellHooks.platform() === 'win32') {
      // On Windows, use taskkill to kill the tree
      shellHooks.execSync(`taskkill /pid ${proc.pid} /T /F`, { stdio: 'ignore' });
    } else {
      // On Unix, kill the process group (negative pid)
      shellHooks.kill(-proc.pid, 'SIGKILL');
    }
  } catch {
    // Process may already be dead; swallow the error
    try {
      proc.kill('SIGKILL');
    } catch {
      // Nothing left to do
    }
  }
}
