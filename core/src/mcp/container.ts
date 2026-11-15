import { execSync, execFileSync, spawn } from 'child_process';

export interface ContainerConfig {
  image: string;
  name?: string;
  runtime?: 'docker' | 'podman' | 'auto';
  extraArgs?: string[];
}

// Default wall-clock bound for the *synchronous* runtime/daemon probes
// (`command -v`, `image inspect`). Without a timeout an unresponsive container
// daemon (hung socket, wedged VM) blocks the calling thread forever; with it
// execSync/execFileSync throws and the existing try/catch degrades gracefully.
// Overridable via AMI_CONTAINER_PROBE_TIMEOUT_MS.
const DEFAULT_PROBE_TIMEOUT_MS = 10_000;

function probeTimeoutMs(): number {
  const n = parseInt(process.env.AMI_CONTAINER_PROBE_TIMEOUT_MS ?? '', 10);
  return n > 0 ? n : DEFAULT_PROBE_TIMEOUT_MS;
}

export function detectRuntime(): string | null {
  for (const cmd of ['podman', 'docker']) {
    try {
      execSync(`command -v ${cmd}`, {
        stdio: 'pipe',
        timeout: probeTimeoutMs(),
        killSignal: 'SIGKILL',
      });
      return cmd;
    } catch {}
  }
  return null;
}

export function resolveRuntime(preference?: 'docker' | 'podman' | 'auto'): string | null {
  if (preference && preference !== 'auto') {
    try {
      execSync(`command -v ${preference}`, {
        stdio: 'pipe',
        timeout: probeTimeoutMs(),
        killSignal: 'SIGKILL',
      });
      return preference;
    } catch {
      return null;
    }
  }
  return detectRuntime();
}

export function buildContainerArgs(config: ContainerConfig): string[] {
  const args = ['run', '-i', '--rm'];
  if (config.name) args.push('--name', config.name);
  if (config.extraArgs) args.push(...config.extraArgs);
  args.push(config.image);
  return args;
}

export interface ImageInfo {
  available: boolean;
  id?: string;
  size?: string;
  created?: string;
}

export function isImageAvailable(runtime: string, image: string): boolean {
  try {
    // execFileSync (no shell) so an attacker-controlled image name cannot inject
    // shell metacharacters (H66). timeout guards against a wedged daemon.
    execFileSync(runtime, ['image', 'inspect', image], {
      stdio: 'pipe',
      timeout: probeTimeoutMs(),
      killSignal: 'SIGKILL',
    });
    return true;
  } catch {
    return false;
  }
}

// Default wall-clock bound for an image pull. A `docker/podman pull` that stalls
// (dead registry, silent network drop with the socket still open, auth hang)
// never emits 'close' or 'error', so without this the returned Promise would
// hang forever. Pulls can legitimately be slow for large images, so the default
// is generous; overridable via AMI_CONTAINER_PULL_TIMEOUT_MS.
const DEFAULT_PULL_TIMEOUT_MS = 600_000;

function pullTimeoutMs(): number {
  const n = parseInt(process.env.AMI_CONTAINER_PULL_TIMEOUT_MS ?? '', 10);
  return n > 0 ? n : DEFAULT_PULL_TIMEOUT_MS;
}

export interface PullImageOptions {
  /** Max wall-clock time before the pull is aborted. <=0 disables the timeout.
   *  Defaults to AMI_CONTAINER_PULL_TIMEOUT_MS or DEFAULT_PULL_TIMEOUT_MS. */
  timeoutMs?: number;
  /** Cancels an in-flight pull; killing the child and rejecting with AbortError. */
  signal?: AbortSignal;
}

function makeAbortError(signal: AbortSignal): Error {
  const reason = signal.reason;
  if (reason instanceof Error) return reason;
  const err = new Error('container pull aborted');
  err.name = 'AbortError';
  return err;
}

export function pullImage(
  runtime: string,
  image: string,
  onProgress?: (line: string) => void,
  options?: PullImageOptions,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const signal = options?.signal;
    // Short-circuit a signal that is already aborted before we spawn anything.
    if (signal && signal.aborted) {
      reject(makeAbortError(signal));
      return;
    }

    const timeoutMs = options?.timeoutMs ?? pullTimeoutMs();
    const proc = spawn(runtime, ['pull', image], { stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = '';
    let settled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const cleanup = () => {
      if (timer) clearTimeout(timer);
      if (signal) signal.removeEventListener('abort', onAbort);
    };
    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      cleanup();
      fn();
    };
    const onAbort = () => {
      proc.kill('SIGTERM');
      finish(() => reject(makeAbortError(signal!)));
    };

    if (timeoutMs > 0) {
      timer = setTimeout(() => {
        proc.kill('SIGTERM');
        finish(() => reject(new Error(`${runtime} pull timed out after ${timeoutMs}ms`)));
      }, timeoutMs);
    }
    if (signal) signal.addEventListener('abort', onAbort);

    proc.stdout?.on('data', (chunk: Buffer) => {
      const lines = chunk.toString().split('\n').filter(Boolean);
      for (const line of lines) onProgress?.(line);
    });
    proc.stderr?.on('data', (chunk: Buffer) => {
      stderr += chunk.toString();
      const lines = chunk.toString().split('\n').filter(Boolean);
      for (const line of lines) onProgress?.(line);
    });
    proc.on('close', (code) => {
      finish(() => {
        if (code === 0) resolve();
        else reject(new Error(`${runtime} pull failed (exit ${code}): ${stderr.trim()}`));
      });
    });
    proc.on('error', (err) => finish(() => reject(err)));
  });
}

export function getImageInfo(runtime: string, image: string): ImageInfo {
  try {
    // execFileSync (no shell) — the image name is never passed through a shell (H66).
    // timeout guards against a wedged daemon.
    const out = execFileSync(
      runtime,
      ['image', 'inspect', '--format', '{{.Id}}|||{{.Size}}|||{{.Created}}', image],
      { stdio: 'pipe', encoding: 'utf-8', timeout: probeTimeoutMs(), killSignal: 'SIGKILL' },
    );
    const [id, size, created] = out.trim().split('|||');
    return { available: true, id, size, created };
  } catch {
    return { available: false };
  }
}
