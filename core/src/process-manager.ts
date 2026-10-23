import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';
import { FlowManager } from './flow-manager';

const MAX_OUTPUT_BYTES = 5 * 1024 * 1024; // 5 MB per task

/** Time to wait after SIGTERM before escalating to SIGKILL. */
export const GRACEFUL_SHUTDOWN_MS = 1500;

/** Maximum concurrent background processes per session. */
export const MAX_BACKGROUND_PROCESSES = 20;

export function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: any) {
    return err?.code !== 'ESRCH';
  }
}

export async function forceKillAfterTimeout(pid: number, timeoutMs: number = GRACEFUL_SHUTDOWN_MS): Promise<boolean> {
  if (!isProcessAlive(pid)) return false;
  try { process.kill(pid, 'SIGTERM'); } catch { return false; }
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (!isProcessAlive(pid)) return true;
    await new Promise(r => setTimeout(r, 50));
  }
  if (!isProcessAlive(pid)) return true;
  try { process.kill(pid, 'SIGKILL'); } catch { /* already dead */ }
  return true;
}

const INFINITE_LOOP_PATTERNS = [
  /\bwhile\s+true\s*;?\s*do\b/,
  /\bwhile\s*\(\s*true\s*\)/,
  /\bwhile\s+:\s*;?\s*do\b/,
  /\bfor\s*\(\s*;\s*;\s*\)/,
  /\byes\s*\|/,
  /\bcat\s+\/dev\/urandom\b/,
  /\btail\s+-f\s+\/dev\/null\b/,
];

export function validateBackgroundCommand(command: string): { allowed: boolean; reason?: string } {
  if (!command || !command.trim()) return { allowed: false, reason: 'empty command' };
  for (const pattern of INFINITE_LOOP_PATTERNS) {
    if (pattern.test(command)) {
      return { allowed: false, reason: 'command may create an infinite loop' };
    }
  }
  if (/>\s*\/dev\/null\s*2>&1\s*$/.test(command) || /&>\s*\/dev\/null\s*$/.test(command)) {
    return { allowed: false, reason: 'command discards all output to /dev/null' };
  }
  return { allowed: true };
}

export interface BackgroundProcess {
  taskId: string;
  pid: number;
  command: string;
  description: string;
  status: 'running' | 'completed' | 'failed' | 'killed';
  exitCode: number | null;
  outputPath: string;
  startTime: number;
  /** Cumulative token usage for background agents (undefined for bash tasks). */
  tokenCount?: number;
}

interface InternalProcess extends BackgroundProcess {
  proc: child_process.ChildProcess;
  outputFd: number | null;
  bytesWritten: number;
  _intervalHandle?: ReturnType<typeof setInterval>;
}

export class ProcessManager extends EventEmitter {
  private processes = new Map<string, InternalProcess>();
  private tasksDir: string;
  readonly flowManager = new FlowManager();

  constructor(cwd: string) {
    super();
    this.tasksDir = path.join(cwd, '.superinference', 'tasks');
    fs.mkdirSync(this.tasksDir, { recursive: true });
  }

  spawn(command: string, opts: { cwd: string; description?: string; env?: NodeJS.ProcessEnv; timeoutMs?: number }): string {
    const taskId = `bg-${crypto.randomBytes(4).toString('hex')}`;
    const outputPath = path.join(this.tasksDir, `${taskId}.output`);

    const isWindows = os.platform() === 'win32';
    const shell = isWindows ? 'cmd' : 'bash';
    const shellArgs = isWindows ? ['/c', command] : ['-c', command];

    const proc = child_process.spawn(shell, shellArgs, {
      cwd: opts.cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: !isWindows,
      ...(opts.env ? { env: opts.env } : {}),
    });

    const fd = fs.openSync(outputPath, 'w');

    const entry: InternalProcess = {
      taskId,
      pid: proc.pid!,
      command,
      description: opts.description || command.slice(0, 80),
      status: 'running',
      exitCode: null,
      outputPath,
      startTime: Date.now(),
      proc,
      outputFd: fd,
      bytesWritten: 0,
    };

    this.processes.set(taskId, entry);

    const writeChunk = (data: Buffer) => {
      if (entry.bytesWritten >= MAX_OUTPUT_BYTES) return;
      const remaining = MAX_OUTPUT_BYTES - entry.bytesWritten;
      const chunk = data.length <= remaining ? data : data.subarray(0, remaining);
      try {
        fs.writeSync(fd, chunk);
        entry.bytesWritten += chunk.length;
        if (entry.bytesWritten >= MAX_OUTPUT_BYTES) {
          fs.writeSync(fd, Buffer.from('\n[output truncated: exceeded 5MB disk cap]\n'));
        }
      } catch { /* fd may be closed */ }
    };

    proc.stdout?.on('data', writeChunk);
    proc.stderr?.on('data', writeChunk);
    // Swallow EBADF errors on pipe streams. When a process is killed, the OS
    // may close the pipe fds before Node.js finishes reading, emitting an
    // EBADF error event. Without this handler, the error becomes uncaught.
    const swallowEBADF = (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EBADF') { this.emit('error', err); }
    };
    proc.stdout?.on('error', swallowEBADF);
    proc.stderr?.on('error', swallowEBADF);

    // Stall detection for background processes
    let lastOutputSize = 0;
    const stallCheck = setInterval(() => {
      const currentSize = this.getOutputSize(taskId);
      if (currentSize === lastOutputSize) {
        const result = this.getOutput(taskId, 10);
        if (result && this.looksLikePrompt(result.output)) {
          this.kill(taskId);
        }
      }
      lastOutputSize = currentSize;
    }, 45_000);

    let bgTimeout: ReturnType<typeof setTimeout> | undefined;
    if (opts.timeoutMs && opts.timeoutMs > 0) {
      bgTimeout = setTimeout(() => {
        if (entry.status === 'running') {
          try { fs.writeSync(fd, Buffer.from(`\n[background command stopped: exceeded ${Math.round(opts.timeoutMs! / 1000)}s time limit]\n`)); } catch { /* fd may be closed */ }
          this.kill(taskId);
        }
      }, opts.timeoutMs);
    }

    proc.on('close', (code) => {
      clearInterval(stallCheck);
      if (bgTimeout) clearTimeout(bgTimeout);
      if (entry.status !== 'killed') {
        entry.status = code === 0 ? 'completed' : 'failed';
      }
      entry.exitCode = code;
      try { fs.closeSync(fd); } catch { /* already closed */ }
      entry.outputFd = null;
      this.emit('complete', { taskId, exitCode: code, command: entry.command, description: entry.description, outputPath: entry.outputPath });
    });

    proc.on('error', (err) => {
      clearInterval(stallCheck);
      entry.status = 'failed';
      try {
        fs.writeSync(fd, Buffer.from(`\n[process error: ${err.message}]\n`));
        fs.closeSync(fd);
      } catch { /* ignore */ }
      entry.outputFd = null;
      this.emit('complete', { taskId, exitCode: null, command: entry.command, description: entry.description, outputPath: entry.outputPath });
    });

    return taskId;
  }

  kill(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;

    entry.status = 'killed';
    if (entry._intervalHandle) {
      clearInterval(entry._intervalHandle);
      entry._intervalHandle = undefined;
    } else {
      // Destroy stdout/stderr streams BEFORE killing the process to prevent
      // EBADF errors. When the process is killed, the OS closes the child end
      // of the pipes. If the parent-side streams are still reading, Node.js
      // can throw EBADF when it tries to close the now-invalid fds.
      this.destroyStreams(entry.proc);

      try {
        if (os.platform() === 'win32') {
          child_process.execSync(`taskkill /pid ${entry.proc.pid} /T /F`, { stdio: 'ignore' });
        } else {
          process.kill(-entry.proc.pid!, 'SIGKILL');
        }
      } catch {
        try { entry.proc.kill('SIGKILL'); } catch { /* already dead */ }
      }
    }

    if (entry.outputFd !== null) {
      try { fs.closeSync(entry.outputFd); } catch { /* ignore */ }
      entry.outputFd = null;
    }

    return true;
  }

  private destroyStreams(proc: child_process.ChildProcess): void {
    for (const stream of [proc.stdout, proc.stderr, proc.stdin]) {
      if (stream && !stream.destroyed) {
        try { stream.destroy(); } catch { /* EBADF is expected here */ }
      }
    }
  }

  getOutput(taskId: string, tailLines = 50): { status: string; exitCode: number | null; output: string; elapsedMs: number } | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;

    let output = '';
    try {
      const content = fs.readFileSync(entry.outputPath, 'utf-8');
      if (tailLines > 0) {
        const lines = content.split('\n');
        output = lines.slice(-tailLines).join('\n');
      } else {
        output = content;
      }
    } catch { /* file may not exist yet */ }

    return {
      status: entry.status,
      exitCode: entry.exitCode,
      output,
      elapsedMs: Date.now() - entry.startTime,
    };
  }

  /**
   * Incremental tail: read only the bytes appended since `fromOffset` instead of
   * re-reading the whole output file every tick. Returns the new chunk plus the
   * byte offset to pass on the next call. If the file shrank below `fromOffset`
   * (truncated or rotated), `reset` is true and the chunk is the full file read
   * from the start so the caller can rebuild its buffer.
   */
  getOutputTail(taskId: string, fromOffset = 0): { output: string; offset: number; reset: boolean } | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;

    try {
      const size = fs.statSync(entry.outputPath).size;
      let start = fromOffset;
      let reset = false;
      if (fromOffset < 0 || fromOffset > size) {
        // file shrank or rotated since the last read — rebuild from the start
        start = 0;
        reset = true;
      }
      if (start >= size) {
        return { output: '', offset: size, reset };
      }
      const fd = fs.openSync(entry.outputPath, 'r');
      let output = '';
      try {
        const length = size - start;
        const buf = Buffer.allocUnsafe(length);
        const bytesRead = fs.readSync(fd, buf, 0, length, start);
        output = buf.subarray(0, bytesRead).toString('utf-8');
      } finally {
        fs.closeSync(fd);
      }
      return { output, offset: size, reset };
    } catch {
      // file may not exist yet
      return { output: '', offset: 0, reset: false };
    }
  }

  list(): BackgroundProcess[] {
    return Array.from(this.processes.values()).map(({ proc, outputFd, bytesWritten, ...rest }) => rest);
  }

  get(taskId: string): BackgroundProcess | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;
    const { proc, outputFd, bytesWritten, ...rest } = entry;
    return rest;
  }

  getOutputSize(taskId: string): number {
    const entry = this.processes.get(taskId);
    if (!entry) return 0;
    return entry.bytesWritten;
  }

  private looksLikePrompt(text: string): boolean {
    const lastLine = text.trim().split('\n').pop() ?? '';
    return /\(y\/n\)|\[y\/n\]|\(yes\/no\)|Press Enter|Continue\?|Overwrite\?|password:/i.test(lastLine);
  }

  runningCount(): number {
    let count = 0;
    for (const entry of this.processes.values()) {
      if (entry.status === 'running') count++;
    }
    return count;
  }

  runningAgentCount(): number {
    let count = 0;
    for (const entry of this.processes.values()) {
      if (entry.command?.startsWith('[agent]') && entry.status === 'running') count++;
    }
    return count;
  }

  registerAgent(taskId: string, opts: {
    command: string;
    description: string;
    outputPath: string;
    abortFn: () => void;
  }): void {
    this.processes.set(taskId, {
      taskId,
      pid: process.pid,
      command: opts.command,
      description: opts.description,
      status: 'running',
      exitCode: null,
      outputPath: opts.outputPath,
      startTime: Date.now(),
      proc: { pid: process.pid, kill: opts.abortFn } as any,
      outputFd: null,
      bytesWritten: 0,
      tokenCount: 0,
    });
  }

  updateEntry(taskId: string, updates: Partial<Pick<BackgroundProcess, 'status' | 'exitCode' | 'tokenCount'>>): void {
    const entry = this.processes.get(taskId);
    if (!entry) return;
    if (updates.status !== undefined) entry.status = updates.status;
    if (updates.exitCode !== undefined) entry.exitCode = updates.exitCode;
    if (updates.tokenCount !== undefined) entry.tokenCount = updates.tokenCount;
  }

  monitorMcp(serverId: string, checkFn: () => Promise<boolean>, intervalMs: number = 30000): string {
    const taskId = `monitor-${serverId}-${Date.now()}`;
    const interval = setInterval(async () => {
      try {
        const healthy = await checkFn();
        if (!healthy) {
          clearInterval(interval);
          this.emit('complete', { taskId, exitCode: 1, command: `monitor:${serverId}`, description: `MCP monitor: ${serverId}`, outputPath: '' });
        }
      } catch {
        clearInterval(interval);
        this.emit('complete', { taskId, exitCode: 1, command: `monitor:${serverId}`, description: `MCP monitor: ${serverId}`, outputPath: '' });
      }
    }, intervalMs);
    this.processes.set(taskId, {
      taskId,
      pid: 0,
      command: `monitor:${serverId}`,
      description: `MCP monitor: ${serverId}`,
      status: 'running',
      exitCode: null,
      outputPath: '',
      startTime: Date.now(),
      proc: null as any,
      outputFd: null,
      bytesWritten: 0,
      _intervalHandle: interval,
    });
    return taskId;
  }

  cleanup(): void {
    for (const entry of this.processes.values()) {
      if (entry.status === 'running') {
        this.kill(entry.taskId);
      }
    }
    this.removeAllListeners();
  }
}
