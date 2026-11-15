import * as child_process from 'child_process';
import { projectStateDir } from './reference-journal';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';
import { FlowManager } from './flow-manager';

const MAX_OUTPUT_BYTES = 5 * 1024 * 1024; // 5 MB per task

/**
 * Upper bound on how many bytes `getOutput` reads into memory in a single call.
 * Even though our own writer caps each task's file at MAX_OUTPUT_BYTES, a
 * registered agent (registerAgent) can point at an externally-written output
 * file of arbitrary size, so the read path tails only the last MAX_READ_BYTES
 * rather than slurping the whole file (mirrors Claude Code's byte-bounded
 * diskOutput reads). The tail is then line-sliced as before.
 */
const MAX_READ_BYTES = MAX_OUTPUT_BYTES;

/**
 * O_NOFOLLOW where the platform supports it (0 on Windows, where it is absent).
 * Task output files live in a predictable, writable tasks dir, so a sandboxed
 * background command could plant a symlink there; opening with O_NOFOLLOW makes
 * the host open fail instead of following the link and reading/clobbering an
 * arbitrary target (mirrors Claude Code's diskOutput/Shell open flags).
 */
const O_NOFOLLOW_FLAG = fs.constants.O_NOFOLLOW || 0;

/**
 * Open a task output file for writing, hardened against symlink-swap attacks:
 * O_EXCL (fail rather than reuse/overwrite an existing path — task ids are fresh)
 * plus O_NOFOLLOW (never traverse a planted symlink). Used only by spawn().
 */
export function openTaskOutputWrite(outputPath: string): number {
  return fs.openSync(
    outputPath,
    fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | O_NOFOLLOW_FLAG,
  );
}

/** Open a task output file for reading with O_NOFOLLOW symlink hardening. */
export function openTaskOutputRead(outputPath: string): number {
  return fs.openSync(outputPath, fs.constants.O_RDONLY | O_NOFOLLOW_FLAG);
}

const TASK_ID_ALPHABET = '0123456789abcdefghijklmnopqrstuvwxyz';

/**
 * Generate a high-entropy task id: `<prefix>-<8 chars over a 36-symbol alphabet>`
 * (36^8 ≈ 2.8e12 combinations). The id is used directly to build the on-disk
 * output path, so the entropy is sized to resist brute-force symlink attacks on
 * that path (mirrors Claude Code's generateTaskId; replaces the former 4-byte
 * hex id which had ~650x less entropy over a 16-symbol alphabet).
 */
export function generateTaskId(prefix: string): string {
  const bytes = crypto.randomBytes(8);
  let id = '';
  for (let i = 0; i < bytes.length; i++) {
    id += TASK_ID_ALPHABET[bytes[i] % TASK_ID_ALPHABET.length];
  }
  return `${prefix}-${id}`;
}

/** Time to wait after SIGTERM before escalating to SIGKILL. */
export const GRACEFUL_SHUTDOWN_MS = 1500;

/** Maximum concurrent background processes per session. */
export const MAX_BACKGROUND_PROCESSES = 20;

/**
 * DI seam for platform detection. Defaults to the real `os.platform()`; tests
 * override it to drive the Windows-only spawn/kill branches on non-Windows CI.
 */
export const osHooks: { platform: () => NodeJS.Platform } = { platform: () => os.platform() };

export function isProcessAlive(pid: number): boolean {
  // Guard non-task pids: 0 targets the WHOLE current process group (process.kill(0,0)
  // almost always succeeds for the running process) and 1 is init — probing either
  // would falsely report "alive" and defeat stale-lock/orphan recovery. Mirrors
  // Claude Code's isProcessRunning pid<=1 guard.
  if (pid <= 1) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: any) {
    return err?.code !== 'ESRCH';
  }
}

/**
 * DI seam for liveness/signal syscalls so the grace-period escalation branches
 * (SIGTERM-throws, post-loop death, SIGKILL-throws) can be driven deterministically
 * in tests without racing real processes. Defaults to the real implementations.
 */
export const procHooks: {
  isAlive: (pid: number) => boolean;
  kill: (pid: number, signal: NodeJS.Signals | number) => void;
} = {
  isAlive: (pid) => isProcessAlive(pid),
  kill: (pid, signal) => { process.kill(pid, signal); },
};

export async function forceKillAfterTimeout(pid: number, timeoutMs: number = GRACEFUL_SHUTDOWN_MS): Promise<boolean> {
  if (!procHooks.isAlive(pid)) return false;
  try { procHooks.kill(pid, 'SIGTERM'); } catch { return false; }
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (!procHooks.isAlive(pid)) return true;
    await new Promise(r => setTimeout(r, 50));
  }
  if (!procHooks.isAlive(pid)) return true;
  try { procHooks.kill(pid, 'SIGKILL'); } catch { /* already dead */ }
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

/** The lifecycle states of a background task/agent. */
export const TASK_STATUS = ['running', 'completed', 'failed', 'killed'] as const;
export type TaskStatus = (typeof TASK_STATUS)[number];

/** Terminal states — a task in one of these will never run again. */
const TERMINAL_TASK_STATUSES = new Set<TaskStatus>(['completed', 'failed', 'killed']);

/**
 * Whether a task status is terminal (completed/failed/killed). Guards against
 * injecting messages into dead teammates, evicting finished tasks, and orphan
 * cleanup (mirrors Claude Code's isTerminalTaskStatus).
 */
export function isTerminalTaskStatus(status: string | undefined): boolean {
  return !!status && TERMINAL_TASK_STATUSES.has(status as TaskStatus);
}

export interface BackgroundProcess {
  taskId: string;
  pid: number;
  command: string;
  description: string;
  status: TaskStatus;
  exitCode: number | null;
  outputPath: string;
  startTime: number;
  /**
   * Wall-clock time (ms) at which the task reached a terminal state
   * (completed/failed/killed). Once set, elapsed time is frozen at
   * `endTime - startTime` instead of ticking forever against the live clock
   * (mirrors Claude Code's endTime field). Undefined while still running.
   */
  endTime?: number;
  /** Cumulative token usage for background agents (undefined for bash tasks). */
  tokenCount?: number;
}

/** Payload emitted on 'complete' and buffered for durable delivery (B4). */
export interface CompletedTaskEvent {
  taskId: string;
  exitCode: number | null;
  command: string;
  description: string;
  outputPath: string;
}

interface InternalProcess extends BackgroundProcess {
  proc: child_process.ChildProcess;
  outputFd: number | null;
  bytesWritten: number;
  _intervalHandle?: ReturnType<typeof setInterval>;
  /** True for in-process agents added via registerAgent(): there is NO real child
   *  process — `proc` is a stub whose `pid` is the MAIN process pid and whose
   *  `kill` is an abort callback. kill() MUST invoke that callback and must NEVER
   *  run `process.kill(-pid)` on it (that targets the app's own process group). */
  _registered?: boolean;
  /** Set once the stall watchdog has written interactive-prompt guidance to the
   *  output, so the hint is emitted at most once per task. */
  _stallNotified?: boolean;
}

export class ProcessManager extends EventEmitter {
  private processes = new Map<string, InternalProcess>();
  private tasksDir: string;
  readonly flowManager = new FlowManager();
  /**
   * Durable buffer of task completions not yet surfaced to the model (B4). A
   * completion that fires while no listener is attached (API call in flight), or
   * after the last running task drops runningCount() to 0, would otherwise be lost
   * forever. The engine drains this each turn and at loop exit so every completion
   * is reported exactly once.
   */
  private _completedBuffer: CompletedTaskEvent[] = [];

  constructor(cwd: string) {
    super();
    this.tasksDir = projectStateDir(cwd, 'tasks');
    fs.mkdirSync(this.tasksDir, { recursive: true });
  }

  spawn(command: string, opts: { cwd: string; description?: string; env?: NodeJS.ProcessEnv; timeoutMs?: number }): string {
    const taskId = generateTaskId('bg');
    const outputPath = path.join(this.tasksDir, `${taskId}.output`);

    const isWindows = osHooks.platform() === 'win32';
    const shell = isWindows ? 'cmd' : 'bash';
    const shellArgs = isWindows ? ['/c', command] : ['-c', command];

    const proc = child_process.spawn(shell, shellArgs, {
      cwd: opts.cwd,
      stdio: ['ignore', 'pipe', 'pipe'],
      detached: !isWindows,
      ...(opts.env ? { env: opts.env } : {}),
    });

    const fd = openTaskOutputWrite(outputPath);

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

    // Stall detection for background processes. When output stops growing and the
    // tail looks like an interactive prompt, DO NOT kill the task — write actionable
    // remediation into its own output (which the model reads via task_output) and
    // leave it running, so the model can decide what to do. Auto-killing a task that
    // might self-resolve is more destructive and leaves the model no signal or fix
    // (mirrors Claude Code's notify-and-let-the-model-decide stall watchdog).
    let lastOutputSize = 0;
    const stallCheck = setInterval(() => {
      const currentSize = this.getOutputSize(taskId);
      if (currentSize === lastOutputSize) {
        this.notifyStalledOnPrompt(taskId);
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
      entry.endTime = Date.now();
      entry.exitCode = code;
      try { fs.closeSync(fd); } catch { /* already closed */ }
      entry.outputFd = null;
      this.emitComplete({ taskId, exitCode: code, command: entry.command, description: entry.description, outputPath: entry.outputPath });
    });

    proc.on('error', (err) => {
      clearInterval(stallCheck);
      entry.status = 'failed';
      entry.endTime = Date.now();
      try {
        fs.writeSync(fd, Buffer.from(`\n[process error: ${err.message}]\n`));
        fs.closeSync(fd);
      } catch { /* ignore */ }
      entry.outputFd = null;
      this.emitComplete({ taskId, exitCode: null, command: entry.command, description: entry.description, outputPath: entry.outputPath });
    });

    return taskId;
  }

  /** Emit a completion AND buffer it for durable drain (B4). */
  private emitComplete(payload: CompletedTaskEvent): void {
    this._completedBuffer.push(payload);
    this.emit('complete', payload);
  }

  /**
   * Return and clear the completions buffered since the last drain (B4). The
   * engine calls this each turn (and at loop exit) so a completion that fired
   * outside any listener window is still surfaced to the model exactly once.
   */
  drainCompletedTasks(): CompletedTaskEvent[] {
    const drained = this._completedBuffer;
    this._completedBuffer = [];
    return drained;
  }

  kill(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;

    entry.status = 'killed';
    entry.endTime = Date.now();
    if (entry._intervalHandle) {
      clearInterval(entry._intervalHandle);
      entry._intervalHandle = undefined;
    } else if (entry._registered) {
      // In-process agent (registerAgent): no real child. `proc.kill` is an abort
      // callback — invoke it directly. Running process.kill(-proc.pid) here would
      // signal the MAIN app's own process group (proc.pid === process.pid).
      try { entry.proc.kill(); } catch { /* abort already fired */ }
    } else {
      // Destroy stdout/stderr streams BEFORE killing the process to prevent
      // EBADF errors. When the process is killed, the OS closes the child end
      // of the pipes. If the parent-side streams are still reading, Node.js
      // can throw EBADF when it tries to close the now-invalid fds.
      this.destroyStreams(entry.proc);

      try {
        if (osHooks.platform() === 'win32') {
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

  /** The lifecycle status of a tracked task, or undefined if it isn't tracked. */
  getStatus(taskId: string): TaskStatus | undefined {
    return this.processes.get(taskId)?.status;
  }

  getOutput(taskId: string, tailLines = 50): { status: string; exitCode: number | null; output: string; elapsedMs: number; bytesTotal: number; truncated: boolean } | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;

    let output = '';
    let bytesTotal = 0;
    let truncated = false;
    try {
      const size = fs.statSync(entry.outputPath).size;
      bytesTotal = size;
      // Tail only the last MAX_READ_BYTES into memory rather than the whole file
      // (O_NOFOLLOW-hardened read). `truncated` signals the shown output is a
      // window into a larger file so the caller/model knows content was elided.
      const readStart = size > MAX_READ_BYTES ? size - MAX_READ_BYTES : 0;
      if (readStart > 0) truncated = true;
      const length = size - readStart;
      const fd = openTaskOutputRead(entry.outputPath);
      try {
        const buf = Buffer.allocUnsafe(length);
        const bytesRead = fs.readSync(fd, buf, 0, length, readStart);
        let content = buf.subarray(0, bytesRead).toString('utf-8');
        if (tailLines > 0) {
          const lines = content.split('\n');
          if (lines.length > tailLines) truncated = true;
          content = lines.slice(-tailLines).join('\n');
        }
        output = content;
      } finally {
        fs.closeSync(fd);
      }
    } catch { /* file may not exist yet, or was removed */ }

    return {
      status: entry.status,
      exitCode: entry.exitCode,
      output,
      // Freeze elapsed at endTime once terminal (don't keep ticking on each read),
      // and clamp against clock skew so a backwards clock can't yield a negative.
      elapsedMs: Math.max(0, (entry.endTime ?? Date.now()) - entry.startTime),
      bytesTotal,
      truncated,
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
      const fd = openTaskOutputRead(entry.outputPath);
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
    // Running tasks first, then newest-first (startTime descending) within a group,
    // so finished/killed tasks don't stay interleaved with running ones in raw
    // insertion order (mirrors Claude Code's running-first, newest-first ordering).
    return Array.from(this.processes.values())
      .map(({ proc, outputFd, bytesWritten, _intervalHandle, _registered, _stallNotified, ...rest }) => rest)
      .sort((a, b) => {
        const aRunning = a.status === 'running' ? 1 : 0;
        const bRunning = b.status === 'running' ? 1 : 0;
        if (aRunning !== bRunning) return bRunning - aRunning;
        return b.startTime - a.startTime;
      });
  }

  get(taskId: string): BackgroundProcess | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;
    const { proc, outputFd, bytesWritten, _intervalHandle, _registered, _stallNotified, ...rest } = entry;
    return rest;
  }

  getOutputSize(taskId: string): number {
    const entry = this.processes.get(taskId);
    if (!entry) return 0;
    return entry.bytesWritten;
  }

  private looksLikePrompt(text: string): boolean {
    // `String.split` always yields at least one element, so the last index is
    // always defined — no nullish fallback branch needed.
    const lines = text.trim().split('\n');
    const lastLine = lines[lines.length - 1];
    return /\(y\/n\)|\[y\/n\]|\(yes\/no\)|Press Enter|Continue\?|Overwrite\?|password:/i.test(lastLine);
  }

  /**
   * Called by the stall watchdog when a background task stops producing output and
   * its tail looks like an interactive prompt. Instead of killing the task, write
   * actionable remediation into its own output exactly once (the model reads it via
   * task_output) and leave the process running — mirrors Claude Code's notify-and-
   * let-the-model-decide behavior. Returns true if guidance was written this call.
   */
  private notifyStalledOnPrompt(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running' || entry.outputFd === null) return false;
    if (entry._stallNotified) return false;
    const result = this.getOutput(taskId, 10);
    if (!result || !this.looksLikePrompt(result.output)) return false;
    entry._stallNotified = true;
    const msg = `\n[background command "${entry.description}" appears to be waiting for interactive input. ` +
      'Kill this task and re-run with piped input (e.g. `echo y | command`) or a non-interactive flag if one exists.]\n';
    try {
      const buf = Buffer.from(msg);
      fs.writeSync(entry.outputFd, buf);
      entry.bytesWritten += buf.length;
    } catch { /* fd may be closed */ }
    return true;
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
      _registered: true,
    });
  }

  updateEntry(taskId: string, updates: Partial<Pick<BackgroundProcess, 'status' | 'exitCode' | 'tokenCount'>>): void {
    const entry = this.processes.get(taskId);
    if (!entry) return;
    if (updates.status !== undefined) {
      // A terminal status is final: once a task is killed (or otherwise terminal),
      // a later natural-completion update from the agent loop must NOT clobber it to
      // completed/failed — a deliberately stopped agent stays 'killed' so the
      // stop-vs-failure distinction survives (mirrors Claude Code's atomic terminal
      // guard). Record endTime on the running→terminal transition to freeze elapsed.
      if (!isTerminalTaskStatus(entry.status)) {
        entry.status = updates.status;
        if (isTerminalTaskStatus(updates.status)) entry.endTime = Date.now();
      }
    }
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
