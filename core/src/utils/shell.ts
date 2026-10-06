import * as child_process from 'child_process';
import * as os from 'os';

const DEFAULT_TIMEOUT_MS = 120_000;
/** Bound captured output. Head plus tail so a long pytest log keeps its summary. */
const HEAD_CHARS = 670_000;
const TAIL_CHARS = 330_000;
const DEFAULT_STALL_TIMEOUT_MS = 45_000;

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
  const lastLine = output.trimEnd().split('\n').pop()?.trim() || '';
  if (lastLine.length === 0 || lastLine.length > 200) return false;
  return PROMPT_PATTERNS.some(p => p.test(lastLine));
}

export interface ExecCommandOptions {
  cwd: string;
  timeout?: number;
  abortSignal?: AbortSignal;
  onData?: (chunk: string) => void;
  env?: NodeJS.ProcessEnv;
  stallTimeoutMs?: number;
}

export interface ExecCommandResult {
  stdout: string;
  stderr: string;
  exitCode: number | null;
}

/**
 * Executes a shell command using `bash -c` (or `cmd /c` on Windows), streaming
 * output through an optional `onData` callback.
 *
 * - Respects timeout (default 120 000 ms) and abort signal.
 * - Kills the entire process tree on timeout or abort.
 * - Keeps the first 670k and last 330k characters of stdout and stderr.
 */
export function execCommand(
  command: string,
  options: ExecCommandOptions,
): Promise<ExecCommandResult> {
  const { cwd, timeout = DEFAULT_TIMEOUT_MS, abortSignal, onData, env, stallTimeoutMs = DEFAULT_STALL_TIMEOUT_MS } = options;

  return new Promise<ExecCommandResult>((resolve) => {
    // If already aborted, short-circuit
    if (abortSignal?.aborted) {
      resolve({ stdout: '', stderr: 'Aborted', exitCode: null });
      return;
    }

    const isWindows = os.platform() === 'win32';
    const shell = isWindows ? 'cmd' : 'bash';
    const shellArgs = isWindows ? ['/c', command] : ['-c', command];

    const proc = child_process.spawn(shell, shellArgs, {
      cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: !isWindows,
      ...(env ? { env } : {}),
    });

    const stdoutBuf = createOutputBuffer();
    const stderrBuf = createOutputBuffer();
    let settled = false;

    const finish = (exitCode: number | null): void => {
      if (settled) return;
      settled = true;
      cleanUp();
      resolve({
        stdout: stdoutBuf.value(),
        stderr: stderrBuf.value(),
        exitCode,
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
    proc.stdout.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stdoutBuf.push(chunk);
      lastDataTime = Date.now();
      onData?.(chunk);
    });

    // --- Stream stderr ---
    proc.stderr.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stderrBuf.push(chunk);
      lastDataTime = Date.now();
      onData?.(chunk);
    });

    // Swallow EBADF errors on pipe streams (see process-manager.ts for details)
    proc.stdout.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EBADF') { stderrBuf.push(err.message); finish(null); }
    });
    proc.stderr.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EBADF') { stderrBuf.push(err.message); finish(null); }
    });

    // --- Process exit ---
    proc.on('close', (code) => {
      finish(code);
    });

    proc.on('error', (err) => {
      stderrBuf.push(err.message);
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
    if (os.platform() === 'win32') {
      // On Windows, use taskkill to kill the tree
      child_process.execSync(`taskkill /pid ${proc.pid} /T /F`, { stdio: 'ignore' });
    } else {
      // On Unix, kill the process group (negative pid)
      process.kill(-proc.pid, 'SIGKILL');
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

