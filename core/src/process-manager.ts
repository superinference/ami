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
 * Human-readable form of {@link MAX_OUTPUT_BYTES}, derived from the numeric cap so
 * the truncation message can never drift out of sync with the limit actually
 * enforced in writeChunk (previously the "5MB" string was hard-coded separately).
 */
const MAX_OUTPUT_DISPLAY = `${Math.round(MAX_OUTPUT_BYTES / (1024 * 1024))}MB`;

/**
 * SECURITY (parity with Claude Code's generateTaskId): build a task id with enough
 * entropy to resist a brute-force symlink attack on the derived output-file path.
 * Claude draws randomBytes(8); AMI previously used randomBytes(4) (2^32 ≈ 4.3B —
 * ~1/650th the entropy). We draw 8 random bytes (2^64) rendered as hex behind a
 * short, self-describing type prefix (e.g. `bg`, `monitor-<server>`). Shared so
 * every call site produces ids with the same collision/attack resistance.
 */
export function generateTaskId(prefix: string): string {
  return `${prefix}-${crypto.randomBytes(8).toString('hex')}`;
}

/** Time to wait after SIGTERM before escalating to SIGKILL. */
export const GRACEFUL_SHUTDOWN_MS = 1500;

/** Maximum concurrent background processes per session. */
export const MAX_BACKGROUND_PROCESSES = 20;

/**
 * Bounded cap on the durable completed-task buffer (parity with Claude Code's
 * sdkEventQueue `MAX_QUEUE_SIZE`). The engine drains the buffer every turn, so in
 * normal operation it stays tiny; but a very long silent turn (one in-flight API
 * call with no drain between pushes) while many background producers finish could
 * otherwise grow it without bound. At the cap we evict the OLDEST entry (FIFO
 * drop-oldest) before pushing the newest, bounding memory the same way Claude's
 * queue does. NOTE (intentional divergence): unlike sdkEventQueue we do NOT gate
 * enqueue on non-interactive mode — AMI's buffer is a DURABLE delivery mechanism
 * the engine drains in BOTH interactive and detached modes, so dropping in TUI
 * mode would lose completions (a parity regression). We keep the cap, drop the gate.
 */
export const MAX_COMPLETED_BUFFER = 1000;

/**
 * DI seam for platform detection. Defaults to the real `os.platform()`; tests
 * override it to drive the Windows-only spawn/kill branches on non-Windows CI.
 */
export const osHooks: { platform: () => NodeJS.Platform } = { platform: () => os.platform() };

export function isProcessAlive(pid: number): boolean {
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

/**
 * DI seam for the synchronous task-output write syscall. The retry-on-transient-
 * error path (EMFILE on busy CI, EPERM on Windows pending-delete) routes through
 * here so tests can make the first write throw and assert the single retry
 * without racing a real full disk. Defaults to the real `fs.writeSync`.
 */
export const fsHooks: { writeSync: (fd: number, buf: Buffer) => void } = {
  writeSync: (fd, buf) => { fs.writeSync(fd, buf); },
};

/**
 * DI seam for diagnostics when a background-output write fails. A transient write
 * error used to be swallowed silently (`catch { }`), losing a chunk with no trace.
 * We now log through this hook (default no-op; the CLI can wire a real logger) on
 * both the pre-retry and give-up paths so lost output is never invisible.
 */
export const pmLogHooks: { onWriteError: (err: NodeJS.ErrnoException, willRetry: boolean) => void } = {
  onWriteError: () => { /* default: no-op — frontends may attach a logger */ },
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

/**
 * POSIX convention for a process killed by a signal: exit status 128 + signal
 * number. Lets callers tell a SIGTERM timeout (143) from a SIGKILL (137) from a
 * still-undecided task (null), instead of collapsing every signal death to null.
 * Mirrors Claude Code's #exitHandler synthesising a code from the signal.
 */
export function signalToExitCode(signal: NodeJS.Signals): number {
  const signo = os.constants.signals[signal];
  return 128 + (typeof signo === 'number' ? signo : 0);
}

/**
 * Resolve the exit code recorded for a terminated child. A real numeric code
 * wins; when the child died from a signal (code === null) we synthesise 128+signo
 * so the termination reason survives; otherwise null (no code yet / unknown).
 */
export function resolveExitCode(code: number | null, signal: NodeJS.Signals | null): number | null {
  if (code !== null) return code;
  if (signal) return signalToExitCode(signal);
  return null;
}

/**
 * Classify a child's terminal status from its (code, signal). An EXTERNAL
 * SIGTERM/SIGINT (a deliberate interruption the model should not read as a crash)
 * maps to 'killed'; exit 0 is 'completed'; any other non-zero code is 'failed'.
 * Mirrors Claude Code's sessionRunner signal-aware close handler. Self-initiated
 * kills are already flagged 'killed' by kill() before the exit event fires.
 */
export function classifyExitStatus(code: number | null, signal: NodeJS.Signals | null): TaskStatus {
  if (signal === 'SIGTERM' || signal === 'SIGINT') return 'killed';
  return code === 0 ? 'completed' : 'failed';
}

/**
 * FUNCTIONAL (parity): build the single aggregate "stopped by the user"
 * model-facing notification shown after the user kills background agents,
 * mirroring Claude Code's useCancelRequest.killAllAgentsAndNotify (which
 * enqueues ONE notification rather than a per-agent spam). Returns:
 *   - `null`                 when nothing was stopped (0 descriptions)
 *   - `Background agent "X" was stopped by the user.`              (exactly 1)
 *   - `N background agents were stopped by the user: "a", "b".`    (N > 1)
 * Pure/side-effect-free so it is reusable and unit-testable in isolation.
 */
export function formatStoppedByUserNotification(descriptions: string[]): string | null {
  if (descriptions.length === 0) return null;
  if (descriptions.length === 1) {
    return `Background agent "${descriptions[0]}" was stopped by the user.`;
  }
  const list = descriptions.map((d) => `"${d}"`).join(', ');
  return `${descriptions.length} background agents were stopped by the user: ${list}.`;
}

/**
 * FUNCTIONAL (parity): the status-aware model-facing note for a drained background
 * completion, mirroring Claude Code's enqueueShellNotification which branches the
 * summary on the terminal status rather than always saying "completed":
 *   - completed → `completed (exit code N)`   (exit code only when known)
 *   - failed    → `failed with exit code N`   (exit code only when known)
 *   - killed    → `was stopped`               (no exit code — a deliberate stop)
 * Keeps AMI's task_output guidance (the differentiator) and output-file reference.
 * A legacy/unknown status falls back to the completed wording. Pure + exported so
 * the engine drain loop and its tests share one source of truth.
 */
export function formatBackgroundCompletionNote(event: CompletedTaskEvent): string {
  const outputInfo = event.outputPath ? ` Output file: ${event.outputPath}.` : '';
  const hasExit = event.exitCode !== null && event.exitCode !== undefined;
  let summary: string;
  if (event.status === 'killed') {
    summary = 'was stopped';
  } else if (event.status === 'failed') {
    summary = hasExit ? `failed with exit code ${event.exitCode}` : 'failed';
  } else {
    summary = hasExit ? `completed (exit code: ${event.exitCode})` : 'completed';
  }
  return (
    `[Background task "${event.taskId}" ${summary}. Command: ${event.description}.${outputInfo} ` +
    `Use task_output({ task_id: "${event.taskId}" }) to read the output.]`
  );
}

/**
 * Compute a background task's elapsed wall-clock duration, mirroring Claude
 * Code's CoordinatorAgentStatus/useElapsedTime formula:
 *   Math.max(0, (endTime ?? now) - startTime - totalPausedMs)
 * i.e. FREEZE at `endTime` once the task is terminal (so viewing a finished task
 * long after it ended doesn't show an ever-growing figure), SUBTRACT the time the
 * task's clock was paused (e.g. while blocked on an interactive prompt), and CLAMP
 * negative skew (clock jumps, paused > wall) to 0. `now` is injectable for tests.
 */
export function computeElapsedMs(
  startTime: number,
  endTime: number | undefined,
  totalPausedMs: number,
  now: number = Date.now(),
): number {
  return Math.max(0, (endTime ?? now) - startTime - totalPausedMs);
}

/** How long after the shell exits to keep the output fd open so the parent's own
 *  buffered stdout/stderr can flush, before finalizing the task regardless of any
 *  grandchildren that inherited the fds (see the 'exit'-not-'close' rationale). */
export const OUTPUT_FLUSH_MS = 50;

/**
 * Stall-watchdog tuning knobs (parity with Claude Code's LocalShellTask
 * STALL_CHECK_INTERVAL_MS / STALL_THRESHOLD_MS / STALL_TAIL_BYTES). AMI previously
 * conflated the poll cadence and the "no growth" threshold into a single 45s
 * interval, so a stalled task could take up to 90s to be noticed and the tail read
 * pulled the WHOLE file into memory. Splitting them lets the watchdog POLL output
 * size cheaply every {@link STALL_CHECK_INTERVAL_MS} but only ACT once output has
 * not grown for {@link STALL_THRESHOLD_MS}, reading just the last
 * {@link STALL_TAIL_BYTES} to classify the tail as an interactive prompt.
 */
export const STALL_CHECK_INTERVAL_MS = 5_000;
export const STALL_THRESHOLD_MS = 45_000;
export const STALL_TAIL_BYTES = 1024;

/**
 * SECURITY (parity with Claude Code's diskOutput): flags for creating a task
 * output file. O_EXCL fails if anything already exists at the path (so an
 * attacker-planted file/symlink can't be truncated-through) and O_NOFOLLOW
 * refuses to follow a planted symlink. Windows lacks both numerically; its 'wx'
 * string flag is O_WRONLY|O_CREAT|O_EXCL and the sandbox symlink vector is
 * Unix-only, so the string form is the correct fallback there.
 */
function secureWriteFlags(): number | string {
  if (osHooks.platform() === 'win32') return 'wx';
  return fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW;
}

/** SECURITY: open a task-output file for reading without following symlinks. */
function secureReadFlags(): number | string {
  if (osHooks.platform() === 'win32') return 'r';
  return fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW;
}

/** Read a task-output file with O_NOFOLLOW so a planted symlink can't redirect
 *  the read to an arbitrary host file. Returns '' if the file is absent. */
function readOutputFileNoFollow(outputPath: string): string {
  const fd = fs.openSync(outputPath, secureReadFlags());
  try {
    return fs.readFileSync(fd, 'utf-8');
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * SECURITY + PERF (parity with Claude Code's tailFile): read ONLY the last
 * `maxBytes` of a task-output file via a positioned read, so memory/IO stays
 * bounded regardless of how large the log grew. Opens with O_NOFOLLOW so a
 * planted symlink can't redirect the read. Returns the tail content, the total
 * on-disk size (`bytesTotal`), the bytes actually read (`bytesRead`), and the
 * skipped-prefix size (`bytesOmitted`, 0 when the whole file fit the window).
 * Throws (propagated to getOutput's diagnostic catch) if the file is gone.
 */
function readOutputTailBytes(
  outputPath: string,
  maxBytes: number,
): { content: string; bytesTotal: number; bytesRead: number; bytesOmitted: number } {
  const size = fs.statSync(outputPath).size;
  const start = size > maxBytes ? size - maxBytes : 0;
  const length = size - start;
  const fd = fs.openSync(outputPath, secureReadFlags());
  try {
    if (length <= 0) {
      return { content: '', bytesTotal: size, bytesRead: 0, bytesOmitted: start };
    }
    const buf = Buffer.allocUnsafe(length);
    const bytesRead = fs.readSync(fd, buf, 0, length, start);
    return {
      content: buf.subarray(0, bytesRead).toString('utf-8'),
      bytesTotal: size,
      bytesRead,
      bytesOmitted: start,
    };
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Last-line patterns that suggest a background command is blocked waiting for
 * keyboard input. Used by the stall watchdog to gate the actionable "waiting for
 * interactive input" hint — we stay silent on commands that are merely slow and
 * only notify when the tail looks like an interactive prompt the model can act on.
 * Mirrors Claude Code's PROMPT_PATTERNS (LocalShellTask): the directed-question
 * matcher catches "Do you want to continue? " and friends that carry no explicit
 * (y/n) marker, and "Press (any key|Enter)" covers "Press any key" (not just Enter).
 */
const PROMPT_PATTERNS: RegExp[] = [
  /\(y\/n\)/i,
  /\[y\/n\]/i,
  /\(yes\/no\)/i,
  /\b(?:Do you|Would you|Shall I|Are you sure|Ready to)\b.*\? *$/i,
  /Press (any key|Enter)/i,
  /Continue\?/i,
  /Overwrite\?/i,
  /password:/i,
];

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
   * Wall-clock time (ms) at which the task reached a terminal state. Set once on
   * the first terminal transition so elapsed-time readers freeze the duration at
   * completion instead of growing forever; undefined while still running.
   */
  endTime?: number;
  /**
   * Cumulative wall-clock time (ms) the task's elapsed clock has been paused — e.g.
   * while the task sat blocked on an interactive prompt. Subtracted from the
   * wall-clock by computeElapsedMs so the reported duration reflects active work,
   * not idle waiting. Mirrors Claude Code's task.totalPausedMs. Undefined ⇒ never
   * paused (treated as 0).
   */
  totalPausedMs?: number;
  /** Cumulative token usage for background agents (undefined for bash tasks). */
  tokenCount?: number;
  /**
   * Dedup latch (parity with Claude Code's `task.notified`): true once the model
   * has already been told about this task's termination — either because it
   * explicitly killed it via task_kill (kill() latches bash tasks) or because it
   * consumed the result via task_output (markNotified() latches on retrieval).
   * emitComplete() suppresses a redundant completion for a notified task so the
   * model isn't re-announced a termination it already knows about. Undefined/false
   * ⇒ not yet surfaced. Only set for bash tasks; agent kills deliberately stay
   * un-notified so their abort completion (which carries the partial result) still
   * surfaces, matching Claude's bash-vs-agent split in stopTask.
   */
  notified?: boolean;
  /**
   * Id of the agent (sub-agent task id) that spawned this task, or undefined when
   * it was started from the main thread. Recorded so orphaned background shells can
   * be reaped when their owning agent exits ({@link ProcessManager.killTasksForAgent}),
   * preventing zombie processes that outlive the agent. Mirrors Claude Code's
   * LocalShellTaskState.agentId (killShellTasksForAgent).
   */
  agentId?: string;
}

/**
 * Payload emitted on 'complete' and buffered for durable delivery (B4). Carries
 * the terminal `status` (completed/failed/killed) so the model-facing note can be
 * worded status-aware (completed vs failed vs stopped) rather than always saying
 * "completed" — parity with Claude Code's enqueueShellNotification which branches
 * the summary on status.
 */
export interface CompletedTaskEvent {
  taskId: string;
  exitCode: number | null;
  command: string;
  description: string;
  outputPath: string;
  /** Terminal lifecycle status at completion; defaults to 'completed' when a
   *  legacy emitter omits it (e.g. the MCP monitor's bare 'complete'). */
  status?: TaskStatus;
}

/**
 * Payload for the 'killed' introspection event emitted by {@link ProcessManager.kill}.
 * Lets a frontend/engine trace WHY a background process ended (parity with Claude
 * Code's `LocalShellTask ${taskId} kill requested` debug log) and whether the
 * subsequent completion note was suppressed as noise.
 */
export interface KilledTaskEvent {
  taskId: string;
  description: string;
  /** The task-type discriminator ('agent' | 'bash' | undefined-for-monitor). */
  taskType: 'agent' | 'bash' | undefined;
  /** True when the kill latched `notified` (explicit model/user stop), so the
   *  completion note is suppressed. */
  suppressed: boolean;
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
  /** Wall-clock ms at which the current (open) pause began, or undefined when the
   *  clock is running. resumeTask() folds (now - _pausedAt) into totalPausedMs. */
  _pausedAt?: number;
  /** Latched true once the stall-on-prompt actionable hint has been written, so
   *  the watchdog notifies the model exactly once per task. */
  _stallNotified?: boolean;
  /** Wall-clock ms of the last tick at which the output file was observed to GROW.
   *  The stall watchdog only acts once (now - _lastGrowthAt) exceeds
   *  STALL_THRESHOLD_MS, decoupling the poll cadence from the no-growth threshold. */
  _lastGrowthAt?: number;
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

  /**
   * Per-session cap on concurrently RUNNING background processes (parity with
   * Claude Code's background-shell limit). Defaults to MAX_BACKGROUND_PROCESSES;
   * exposed as a mutable field so callers/tests can tune the ceiling without
   * touching the module constant. spawn() enforces it; see atCapacity().
   */
  maxBackgroundProcesses: number = MAX_BACKGROUND_PROCESSES;

  constructor(cwd: string) {
    super();
    this.tasksDir = projectStateDir(cwd, 'tasks');
    fs.mkdirSync(this.tasksDir, { recursive: true });
  }

  /**
   * FUNCTIONAL (parity): true when the number of currently-running tasks has
   * reached `maxBackgroundProcesses`, i.e. a further spawn() would be rejected.
   * Lets callers pre-check and surface a clean message instead of catching the
   * throw. Mirrors Claude Code refusing to launch beyond its background limit.
   */
  atCapacity(): boolean {
    return this.runningCount() >= this.maxBackgroundProcesses;
  }

  spawn(command: string, opts: { cwd: string; description?: string; env?: NodeJS.ProcessEnv; timeoutMs?: number; agentId?: string }): string {
    // FUNCTIONAL (parity): enforce the concurrent background-process ceiling.
    // Previously MAX_BACKGROUND_PROCESSES was declared but never checked, so a
    // session could spawn unbounded detached shells. Claude Code caps concurrent
    // background shells; refuse (throw) once the running set is full.
    if (this.atCapacity()) {
      throw new Error(
        `too many background processes (limit ${this.maxBackgroundProcesses}); ` +
        `wait for one to finish or kill one before starting another`,
      );
    }
    const taskId = generateTaskId('bg');
    const outputPath = path.join(this.tasksDir, `${taskId}.output`);

    const isWindows = osHooks.platform() === 'win32';
    const shell = isWindows ? 'cmd' : 'bash';
    const shellArgs = isWindows ? ['/c', command] : ['-c', command];

    // Open the output file FIRST with fail-closed secure flags: if a symlink/file
    // was planted at outputPath the open throws (O_EXCL) before any child is
    // spawned, so we never leak a process we can't capture output for.
    const fd = fs.openSync(outputPath, secureWriteFlags());

    let proc: child_process.ChildProcess;
    try {
      proc = child_process.spawn(shell, shellArgs, {
        cwd: opts.cwd,
        stdio: ['ignore', 'pipe', 'pipe'],
        detached: !isWindows,
        ...(opts.env ? { env: opts.env } : {}),
      });
    } catch (err) {
      try { fs.closeSync(fd); } catch { /* ignore */ }
      throw err;
    }

    const entry: InternalProcess = {
      taskId,
      pid: proc.pid!,
      command,
      description: opts.description || command.slice(0, 80),
      status: 'running',
      exitCode: null,
      outputPath,
      startTime: Date.now(),
      ...(opts.agentId ? { agentId: opts.agentId } : {}),
      proc,
      outputFd: fd,
      bytesWritten: 0,
    };

    this.processes.set(taskId, entry);

    const writeChunk = (data: Buffer) => {
      if (entry.bytesWritten >= MAX_OUTPUT_BYTES) return;
      const remaining = MAX_OUTPUT_BYTES - entry.bytesWritten;
      const chunk = data.length <= remaining ? data : data.subarray(0, remaining);
      if (!this.safeWrite(fd, chunk)) return;
      entry.bytesWritten += chunk.length;
      if (entry.bytesWritten >= MAX_OUTPUT_BYTES) {
        this.safeWrite(fd, Buffer.from(`\n[output truncated: exceeded ${MAX_OUTPUT_DISPLAY} disk cap]\n`));
      }
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

    // Stall detection for background processes. Delegated to stallTick() so the
    // logic is unit-testable without waiting out the poll interval. The cadence
    // (STALL_CHECK_INTERVAL_MS) is decoupled from the no-growth threshold
    // (STALL_THRESHOLD_MS, enforced inside stallTick) — see the knob JSDoc.
    let lastOutputSize = 0;
    const stallCheck = setInterval(() => {
      lastOutputSize = this.stallTick(taskId, lastOutputSize);
    }, STALL_CHECK_INTERVAL_MS);
    // Observation-only timer: unref() so a still-registered task's poller can never
    // by itself keep the Node event loop alive and block clean process exit
    // (parity with Claude Code unref-ing its progress-poll interval).
    stallCheck.unref();

    let bgTimeout: ReturnType<typeof setTimeout> | undefined;
    if (opts.timeoutMs && opts.timeoutMs > 0) {
      bgTimeout = setTimeout(() => {
        if (entry.status === 'running') {
          try { fs.writeSync(fd, Buffer.from(`\n[background command stopped: exceeded ${Math.round(opts.timeoutMs! / 1000)}s time limit]\n`)); } catch { /* fd may be closed */ }
          this.kill(taskId);
        }
      }, opts.timeoutMs);
      // Watchdog timer must not pin the loop open on its own either.
      bgTimeout.unref();
    }

    // Finalize exactly once: record the terminal status/code/endTime, close the
    // fd, and surface the completion. Guarded so the 'close'/'exit'/'error' races
    // below can all point here without double-reporting.
    let finalized = false;
    let exitTimer: ReturnType<typeof setTimeout> | undefined;
    const finalize = (code: number | null, signal: NodeJS.Signals | null) => {
      if (finalized) return;
      finalized = true;
      clearInterval(stallCheck);
      if (bgTimeout) clearTimeout(bgTimeout);
      clearTimeout(exitTimer);
      if (entry.status !== 'killed') {
        entry.status = classifyExitStatus(code, signal);
      }
      entry.exitCode = resolveExitCode(code, signal);
      this.settlePause(entry);
      entry.endTime = Date.now();
      try { fs.closeSync(fd); } catch { /* already closed (e.g. after kill) */ }
      entry.outputFd = null;
      this.emitComplete({ taskId, exitCode: entry.exitCode, command: entry.command, description: entry.description, outputPath: entry.outputPath, status: entry.status });
    };

    // 'close' is the normal completion signal: it fires once all stdio pipes have
    // drained, so the output file is complete when we finalize from it.
    proc.on('close', (code, signal) => finalize(code, signal));

    // 'close', however, waits for EVERY inherited stdio fd to close — including
    // grandchildren the command backgrounded (e.g. `sleep 30 &`), which could
    // wedge a finished task in 'running' indefinitely. 'exit' fires when the shell
    // itself exits; we arm a bounded timer off it so a lingering grandchild can't
    // hold the task open: if 'close' hasn't finalized within OUTPUT_FLUSH_MS we
    // finalize anyway. In the common (no-grandchild) case 'close' wins first and
    // the timer is cleared, so there is no added latency and no output is lost.
    proc.on('exit', (code, signal) => {
      exitTimer = setTimeout(() => finalize(code, signal), OUTPUT_FLUSH_MS);
      exitTimer.unref();
    });

    proc.on('error', (err) => {
      if (finalized) return;
      finalized = true;
      clearInterval(stallCheck);
      if (bgTimeout) clearTimeout(bgTimeout);
      clearTimeout(exitTimer);
      entry.status = 'failed';
      this.settlePause(entry);
      entry.endTime = Date.now();
      this.safeWrite(fd, Buffer.from(`\n[process error: ${err.message}]\n`));
      try { fs.closeSync(fd); } catch { /* ignore */ }
      entry.outputFd = null;
      this.emitComplete({ taskId, exitCode: null, command: entry.command, description: entry.description, outputPath: entry.outputPath, status: 'failed' });
    });

    return taskId;
  }

  /** Emit a completion AND buffer it for durable drain (B4). */
  private emitComplete(payload: CompletedTaskEvent): void {
    // Dedup (parity with Claude Code's stopTask `notified` flag): a task the model
    // was already told about — because it explicitly killed it (task_kill latches
    // bash tasks in kill()) or already consumed its result (task_output →
    // markNotified()) — must NOT re-surface a redundant completion. The model
    // learned of the termination from the tool result; a later "[Background task X
    // completed (exit 137)]" note is pure noise. Suppress it before it ever reaches
    // the durable buffer / 'complete' listeners.
    const entry = this.processes.get(payload.taskId);
    if (entry?.notified) return;
    // Bounded buffer (parity with Claude Code's sdkEventQueue): evict the oldest
    // entry at the cap so the durable buffer can never grow without bound across a
    // long silent turn with active producers (see MAX_COMPLETED_BUFFER).
    if (this._completedBuffer.length >= MAX_COMPLETED_BUFFER) this._completedBuffer.shift();
    this._completedBuffer.push(payload);
    this.emit('complete', payload);
  }

  /**
   * Dedup check-and-set (parity with Claude Code's markTaskNotified): latch a task
   * as already surfaced to the model so its completion is not re-announced. Called
   * by task_output when a TERMINAL task's output is successfully retrieved (the
   * model consumed the result; a follow-up "completed — call task_output" note
   * would be a wasteful round-trip). Also drops any completion ALREADY buffered for
   * this task so a drain after the retrieval doesn't re-surface it. Idempotent:
   * returns true only on the first transition, false if already notified/unknown.
   */
  markNotified(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.notified) return false;
    entry.notified = true;
    this._completedBuffer = this._completedBuffer.filter((e) => e.taskId !== taskId);
    return true;
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

  /**
   * Stop a running task. `opts.suppressCompletion` (set by the task_kill tool for
   * an EXPLICIT model/user stop) latches a bash task's `notified` flag so its
   * subsequent close→completion is suppressed as noise (the model already learned
   * it stopped from the tool result) — parity with Claude Code's stopTask. INTERNAL
   * kills (the timeout watchdog, cleanup, killAllAgents) pass no option, so their
   * completion STILL surfaces: a task the model did not ask to stop (e.g. it hit
   * its time limit) must be reported. Agent tasks are never latched (the bash
   * branch is the only latch site) so their abort completion, which carries the
   * partial result, always surfaces — matching Claude's bash-vs-agent split.
   */
  kill(taskId: string, opts: { suppressCompletion?: boolean } = {}): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;

    entry.status = 'killed';
    // Close any open pause window into totalPausedMs before freezing the clock so
    // a task killed while stalled keeps correct pause accounting.
    this.settlePause(entry);
    // Freeze the elapsed-time clock at the kill instant. For a real child this is
    // overwritten (with ~the same value) when its 'exit' event fires; for
    // in-process agents / MCP monitors (no exit event) this is the only place the
    // terminal timestamp is set.
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
      // Bash (real child) EXPLICIT kill: latch `notified` so the forthcoming
      // proc.on('close') → finalize() → emitComplete() does NOT re-surface a
      // redundant "exit code 137" completion as noise — the model already learned
      // the task stopped from the task_kill tool result. Parity with Claude Code's
      // stopTask, which suppresses the exit-137 notification for LocalShellTask
      // kills (but deliberately NOT for agent kills, whose abort completion carries
      // the partial result — those run the _registered branch above and stay
      // un-notified, so their completion still surfaces). Only on an EXPLICIT stop:
      // an internal timeout/cleanup kill leaves it un-latched so the model is told.
      if (opts.suppressCompletion) entry.notified = true;

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

    // INTROSPECTION (parity with Claude Code's `LocalShellTask ${taskId} kill
    // requested` debug log): emit a structured 'killed' event so a frontend/engine
    // can trace WHY a background process ended (and whether its completion note was
    // suppressed). Unlike the completion note this fires for EVERY kill — including
    // suppressed explicit stops — so the lifecycle is always observable.
    const killed: KilledTaskEvent = {
      taskId,
      description: entry.description,
      taskType: entry._registered ? 'agent' : (entry.command?.startsWith('monitor:') ? undefined : 'bash'),
      suppressed: !!entry.notified,
    };
    this.emit('killed', killed);

    return true;
  }

  /**
   * FUNCTIONAL (parity): single-shot stop of EVERY running in-process agent
   * (ESC/kill-all cancellation), returning the killed agents plus ONE aggregate
   * model-facing notification. Mirrors Claude Code's useCancelRequest
   * killAllAgentsAndNotify: it filters to `local_agent` tasks that are still
   * running, kills them all, and enqueues a single "stopped by the user" summary
   * rather than a per-agent notification storm. Only {@link registerAgent}-backed
   * tasks (taskType === 'agent') are targeted — background bash shells and MCP
   * monitors are left running, matching Claude's type filter. The returned
   * `notification` is null when nothing was running so callers can skip the
   * enqueue. kill() already freezes each task's elapsed clock and closes its fd.
   */
  killAllAgents(): { stopped: BackgroundProcess[]; notification: string | null } {
    const stopped: BackgroundProcess[] = [];
    for (const taskId of Array.from(this.processes.keys())) {
      const entry = this.processes.get(taskId);
      if (!entry || entry.status !== 'running') continue;
      if (this.taskType(taskId) !== 'agent') continue;
      if (this.kill(taskId)) {
        const snapshot = this.get(taskId);
        if (snapshot) stopped.push(snapshot);
      }
    }
    const notification = formatStoppedByUserNotification(stopped.map((s) => s.description));
    return { stopped, notification };
  }

  /**
   * FUNCTIONAL (parity): pause a RUNNING task's elapsed-time clock. Idempotent — a
   * second pause() before a resume() is a no-op (returns false). Used by the stall
   * watchdog so a task blocked on an interactive prompt doesn't inflate its
   * active-work duration. Mirrors Claude Code's pause/resume accounting
   * (task.totalPausedMs). Returns true when the clock transitioned to paused.
   */
  pauseTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;
    if (entry._pausedAt !== undefined) return false; // already paused
    entry._pausedAt = Date.now();
    return true;
  }

  /**
   * FUNCTIONAL (parity): resume a paused task's clock, folding the elapsed pause
   * window into totalPausedMs. No-op (returns false) when the task isn't currently
   * paused. Returns true when a pause window was closed.
   */
  resumeTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry._pausedAt === undefined) return false;
    this.settlePause(entry);
    return true;
  }

  /** Close any open pause window into totalPausedMs (no-op if not paused). Shared
   *  by resumeTask() and every terminal transition so no paused time is lost. */
  private settlePause(entry: InternalProcess): void {
    if (entry._pausedAt === undefined) return;
    entry.totalPausedMs = (entry.totalPausedMs ?? 0) + (Date.now() - entry._pausedAt);
    entry._pausedAt = undefined;
  }

  /**
   * One stall-check tick for a running background task (extracted from spawn()'s
   * poll interval so it is unit-testable). Mirrors Claude Code's startStallWatchdog:
   * the watchdog POLLS cheaply every STALL_CHECK_INTERVAL_MS but only ACTS once the
   * output has not grown for STALL_THRESHOLD_MS AND the tail (last STALL_TAIL_BYTES)
   * looks like an interactive prompt — then it writes an ACTIONABLE hint into the
   * task's own output file (surfaced to the model via getOutput — the already-wired
   * delivery surface), PAUSES the elapsed clock, and emits a one-shot 'stalled'
   * event, instead of silently KILLING the task. If output grew since the last tick
   * the task is making progress: reset the no-growth timer and resume the clock if
   * paused. `now` is injectable so the threshold can be driven deterministically in
   * tests. Returns the latest observed output size for the next tick.
   */
  private stallTick(taskId: string, lastSize: number, now: number = Date.now()): number {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return lastSize;
    const currentSize = this.getOutputSize(taskId);
    if (currentSize !== lastSize) {
      // Output grew — the task is making progress, not stalled. Reset the
      // no-growth timer so the threshold is measured from this moment.
      entry._lastGrowthAt = now;
      if (entry._pausedAt !== undefined) this.resumeTask(taskId);
      return currentSize;
    }
    // No growth this tick. Start the no-growth timer on first observation and only
    // act once it has been stalled for at least STALL_THRESHOLD_MS (decoupled from
    // the poll cadence) — a merely slow command gets the full grace window.
    if (entry._lastGrowthAt === undefined) {
      entry._lastGrowthAt = now;
      return currentSize;
    }
    if (now - entry._lastGrowthAt < STALL_THRESHOLD_MS) return currentSize;
    if (!entry._stallNotified) {
      // Byte-bounded tail read (STALL_TAIL_BYTES) so classifying a huge log's tail
      // never pulls the whole file into memory (parity with Claude's tailFile).
      const result = this.getOutput(taskId, 10, undefined, STALL_TAIL_BYTES);
      if (result && this.looksLikePrompt(result.output)) {
        entry._stallNotified = true;
        this.pauseTask(taskId);
        this.notifyStall(entry, result.output);
        // Account for the hint bytes just written so the next tick's size
        // comparison is stable (doesn't read as spurious growth → resume).
        return this.getOutputSize(taskId);
      }
    }
    return currentSize;
  }

  /** Write a one-shot actionable stall hint into the task output and emit the
   *  'stalled' event (mirrors Claude Code's task-notification: tell the model the
   *  command is likely blocked on an interactive prompt and how to recover). */
  private notifyStall(entry: InternalProcess, tail: string): void {
    const hint =
      `\n[background task "${entry.description}" appears to be waiting for interactive input. ` +
      `The command is likely blocked on an interactive prompt. Kill this task and re-run ` +
      `with piped input (e.g. \`echo y | command\`) or a non-interactive flag if one exists.]\n`;
    if (entry.outputFd !== null) {
      this.safeWrite(entry.outputFd, Buffer.from(hint));
    }
    this.emit('stalled', {
      taskId: entry.taskId,
      description: entry.description,
      outputPath: entry.outputPath,
      tail,
    });
  }

  /**
   * Write a chunk to the task-output fd, surviving a transient fs error. A write
   * can fail transiently under load — EMFILE on a busy CI box, EPERM on a Windows
   * pending-delete — and the old `catch {}` dropped the chunk silently, leaving
   * callers an empty output file with no diagnostic. We now distinguish a fatal
   * closed fd (EBADF — nothing to retry) from a transient error: log it, retry
   * the write ONCE, and only then give up (logging again). Mirrors Claude Code's
   * #drain retry-once. Returns true if the bytes were written, false if dropped.
   */
  private safeWrite(fd: number, buf: Buffer): boolean {
    try {
      fsHooks.writeSync(fd, buf);
      return true;
    } catch (err) {
      const e = err as NodeJS.ErrnoException;
      if (e.code === 'EBADF') return false; // fd closed — retry is pointless
      pmLogHooks.onWriteError(e, true);
      try {
        fsHooks.writeSync(fd, buf);
        return true;
      } catch (err2) {
        pmLogHooks.onWriteError(err2 as NodeJS.ErrnoException, false);
        return false;
      }
    }
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

  /**
   * PARITY (additive): the task-type discriminator for a tracked task.
   *
   * Returns `'agent'` for an in-process agent added via {@link registerAgent}
   * (no real child; `kill` is an abort callback) and `'bash'` for a spawned
   * background shell. Returns `undefined` when the id isn't tracked. Mirrors
   * Claude Code surfacing `task_type` so stop/list callers can branch on the
   * kind of task without inspecting internals.
   */
  taskType(taskId: string): 'agent' | 'bash' | undefined {
    const entry = this.processes.get(taskId);
    if (!entry) return undefined;
    return entry._registered ? 'agent' : 'bash';
  }

  /**
   * FUNCTIONAL (parity): reap a single FINISHED task — drop it from the tracked
   * map and delete its on-disk output file. Refuses (returns false) for an
   * unknown task or one that is still running, so a live process is never
   * untracked out from under its close/error handler. Returns true when a
   * terminal task was removed. Mirrors Claude Code reaping completed shells so
   * the session's task table and tasks/ directory don't grow without bound.
   */
  removeTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry) return false;
    if (!isTerminalTaskStatus(entry.status)) return false;
    if (entry.outputFd !== null) {
      try { fs.closeSync(entry.outputFd); } catch { /* already closed */ }
      entry.outputFd = null;
    }
    if (entry.outputPath) {
      try { fs.rmSync(entry.outputPath, { force: true }); } catch { /* best-effort */ }
    }
    this.processes.delete(taskId);
    return true;
  }

  /**
   * FUNCTIONAL (parity): bulk-reap every terminal task via removeTask(), leaving
   * only running tasks tracked. Returns the number of tasks reaped. Useful for
   * periodic orphan cleanup without the caller enumerating the table itself.
   */
  pruneTerminal(): number {
    let removed = 0;
    for (const taskId of Array.from(this.processes.keys())) {
      if (this.removeTask(taskId)) removed++;
    }
    return removed;
  }

  /**
   * FUNCTIONAL (parity): kill every RUNNING task spawned by `agentId` (zombie
   * prevention). Invoked from the sub-agent lifecycle's finally block so background
   * shells a sub-agent launched don't outlive the agent that started them — parity
   * with Claude Code's killShellTasksForAgent (prevents the "10-day fake-logs.sh
   * zombie" class of orphan). Only tasks whose recorded `agentId` matches and that
   * are still running are affected; returns the count killed. Internal kills (no
   * suppressCompletion) so a leftover task the model didn't explicitly stop still
   * surfaces its termination.
   */
  killTasksForAgent(agentId: string): number {
    if (!agentId) return 0;
    let killed = 0;
    for (const taskId of Array.from(this.processes.keys())) {
      const entry = this.processes.get(taskId);
      if (!entry || entry.agentId !== agentId || entry.status !== 'running') continue;
      if (this.kill(taskId)) killed++;
    }
    return killed;
  }

  getOutput(
    taskId: string,
    tailLines = 50,
    filter?: string | RegExp,
    maxReadBytes?: number,
  ): {
    status: string;
    exitCode: number | null;
    output: string;
    elapsedMs: number;
    /** Total on-disk size (bytes) of the output file at read time (undefined on a read error). */
    bytesTotal?: number;
    /** Bytes of EARLIER output skipped by a byte-bounded tail read (0 when the whole file was read). */
    bytesOmitted?: number;
    /** Estimated total line count: exact when the whole file was read, else extrapolated from the
     *  tail sample (Math.round(bytesTotal/bytesRead * sampleLines)) so a progress counter can be shown. */
    totalLinesEstimate?: number;
  } | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;

    // FUNCTIONAL (parity): optional line filter, mirroring Claude Code's
    // BashOutput `filter` regex. When supplied, only output lines matching the
    // pattern are retained; the tail is then applied to the FILTERED set so the
    // caller sees the last N *matching* lines rather than N raw lines that may
    // contain no matches. An invalid regex string is treated as "no match".
    let re: RegExp | undefined;
    if (filter !== undefined) {
      try {
        re = filter instanceof RegExp ? filter : new RegExp(filter);
      } catch {
        re = /(?!)/; // never matches — invalid pattern yields empty output
      }
    }

    let output = '';
    let bytesTotal: number | undefined;
    let bytesOmitted: number | undefined;
    let totalLinesEstimate: number | undefined;
    try {
      // FUNCTIONAL (parity with Claude Code's getTaskOutput/tailFile): when a byte
      // ceiling is supplied, read ONLY the last `maxReadBytes` of the file via a
      // positioned read so memory/IO is bounded no matter how large the log grew
      // (AMI previously read the WHOLE file into memory on every poll). The skipped
      // prefix is reported as `bytesOmitted` and the total line count is
      // extrapolated from the sample so the caller can show "~N lines" and an
      // "earlier output omitted" affordance. Without a ceiling the whole file is
      // read as before (bytesOmitted === 0, exact line count).
      let content: string;
      if (maxReadBytes !== undefined && Number.isFinite(maxReadBytes) && maxReadBytes > 0) {
        const read = readOutputTailBytes(entry.outputPath, Math.floor(maxReadBytes));
        content = read.content;
        bytesTotal = read.bytesTotal;
        bytesOmitted = read.bytesOmitted;
        const sampleLines = content === '' ? 0 : content.split('\n').length;
        totalLinesEstimate = bytesOmitted > 0 && read.bytesRead > 0
          ? Math.round((bytesTotal / read.bytesRead) * sampleLines)
          : sampleLines;
      } else {
        content = readOutputFileNoFollow(entry.outputPath);
        bytesTotal = Buffer.byteLength(content, 'utf-8');
        bytesOmitted = 0;
        totalLinesEstimate = content === '' ? 0 : content.split('\n').length;
      }
      let lines = content.split('\n');
      if (re) {
        lines = lines.filter((line) => re!.test(line));
      }
      if (tailLines > 0) {
        output = lines.slice(-tailLines).join('\n');
      } else {
        output = re ? lines.join('\n') : content;
      }
    } catch (err) {
      // SECURITY/OBSERVABILITY (parity with Claude Code's #readStdoutFromFile):
      // a spawned bash task's output file is created up-front (O_CREAT|O_EXCL), so
      // a read failure here means it was deleted/tampered with mid-run — e.g. a
      // concurrent process in the shared project dir removed it. Surface a
      // diagnostic (with the errno) instead of a silent empty read, which is
      // indistinguishable from "no output yet". A registered agent's file is
      // written externally and may legitimately not exist yet, so ENOENT there
      // stays an empty (non-diagnostic) read.
      const code = (err as NodeJS.ErrnoException)?.code;
      const notYetCreated = (!!entry._registered || !entry.outputPath) && code === 'ENOENT';
      if (entry.outputPath && !notYetCreated) {
        output = `<task output unavailable: ${entry.outputPath} could not be read (${code ?? 'unknown'})>`;
      }
    }

    return {
      status: entry.status,
      exitCode: entry.exitCode,
      output,
      // Freeze at the terminal timestamp, discount paused time, and clamp negative
      // skew — the shared Claude-parity formula (see computeElapsedMs).
      elapsedMs: computeElapsedMs(entry.startTime, entry.endTime, entry.totalPausedMs ?? 0),
      bytesTotal,
      bytesOmitted,
      totalLinesEstimate,
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
      const fd = fs.openSync(entry.outputPath, secureReadFlags());
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
    return Array.from(this.processes.values()).map(({ proc, outputFd, bytesWritten, _pausedAt, _stallNotified, _lastGrowthAt, ...rest }) => rest);
  }

  get(taskId: string): BackgroundProcess | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;
    const { proc, outputFd, bytesWritten, _pausedAt, _stallNotified, _lastGrowthAt, ...rest } = entry;
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
    return PROMPT_PATTERNS.some((p) => p.test(lastLine));
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
    /** Id of the PARENT agent that spawned this sub-agent, so a parent's
     *  killTasksForAgent() can reap it if the parent exits first. Undefined for a
     *  top-level agent spawned from the main thread. */
    agentId?: string;
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
      ...(opts.agentId ? { agentId: opts.agentId } : {}),
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
      entry.status = updates.status;
      // Freeze the elapsed clock when an in-process agent reaches a terminal
      // state via updateEntry (its only terminal transition — there is no 'exit'
      // event). Keep the first terminal timestamp if one was already recorded.
      if (isTerminalTaskStatus(updates.status) && entry.endTime === undefined) {
        entry.endTime = Date.now();
      }
    }
    if (updates.exitCode !== undefined) entry.exitCode = updates.exitCode;
    if (updates.tokenCount !== undefined) entry.tokenCount = updates.tokenCount;
  }

  monitorMcp(serverId: string, checkFn: () => Promise<boolean>, intervalMs: number = 30000): string {
    const taskId = generateTaskId(`monitor-${serverId}`);
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
    // Observation-only health poller: unref() so it never blocks process exit.
    interval.unref();
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
