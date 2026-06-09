import * as child_process from 'child_process';
import * as os from 'os';

const DEFAULT_TIMEOUT_MS = 120_000;
const MAX_OUTPUT_CHARS = 100_000;

export interface ExecCommandOptions {
  cwd: string;
  timeout?: number;
  abortSignal?: AbortSignal;
  onData?: (chunk: string) => void;
  env?: NodeJS.ProcessEnv;
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
 * - Truncates stdout/stderr to 100 000 characters max.
 */
export function execCommand(
  command: string,
  options: ExecCommandOptions,
): Promise<ExecCommandResult> {
  const { cwd, timeout = DEFAULT_TIMEOUT_MS, abortSignal, onData, env } = options;

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

    let stdout = '';
    let stderr = '';
    let settled = false;

    const finish = (exitCode: number | null): void => {
      if (settled) return;
      settled = true;
      cleanUp();
      resolve({
        stdout: truncate(stdout),
        stderr: truncate(stderr),
        exitCode,
      });
    };

    // --- Timeout handling ---
    const timer = setTimeout(() => {
      killTree(proc);
      finish(null);
    }, timeout);

    // --- Abort signal handling ---
    const onAbort = (): void => {
      killTree(proc);
      finish(null);
    };

    abortSignal?.addEventListener('abort', onAbort, { once: true });

    const cleanUp = (): void => {
      clearTimeout(timer);
      abortSignal?.removeEventListener('abort', onAbort);
    };

    // --- Stream stdout ---
    proc.stdout.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stdout += chunk;
      onData?.(chunk);
    });

    // --- Stream stderr ---
    proc.stderr.on('data', (data: Buffer) => {
      const chunk = data.toString();
      stderr += chunk;
      onData?.(chunk);
    });

    // --- Process exit ---
    proc.on('close', (code) => {
      finish(code);
    });

    proc.on('error', (err) => {
      stderr += err.message;
      finish(null);
    });
  });
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

/**
 * Truncate a string to `MAX_OUTPUT_CHARS`, appending a marker if truncated.
 */
function truncate(text: string): string {
  if (text.length <= MAX_OUTPUT_CHARS) return text;
  return text.slice(0, MAX_OUTPUT_CHARS) + '\n[truncated]';
}
