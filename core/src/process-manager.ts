import * as child_process from 'child_process';
import { isTerminalTaskStatus } from './terminal-task-status';
export { isTerminalTaskStatus } from './terminal-task-status';
import { projectStateDir } from './reference-journal';
import { instanceScopedTempName } from './utils/runtime-namespace';
import * as fs from 'fs';
import * as path from 'path';
import 'os';
import 'crypto';
import { EventEmitter } from 'events';
import { FlowManager } from './flow-manager';
import { killTree } from './utils/shell';
import { isSafeSegment } from './skill-catalog';
import { MAX_BACKGROUND_PROCESSES, MAX_COMPLETED_BUFFER, OUTPUT_FLUSH_MS, SPAWN_FAILURE_EXIT_CODE, STALL_CHECK_INTERVAL_MS, STALL_THRESHOLD_MS, TASK_ATTENTION_STATES, buildCompletionSummary, classifyExitStatus, computeElapsedMs, formatByteCap, formatStoppedByUserNotification, fsHooks, generateTaskId, osHooks, pmLogHooks, pmTraceHooks, procHooks, resolveExitCode, resolveSpawnCwd, spawnHooks } from './process-manager-build-completion-summary';
import type { PmLifecyclePhase, TaskKind, TaskStatus } from './process-manager-build-completion-summary';
import { PROMPT_PATTERNS, STALL_TAIL_BYTES, readOutputFileNoFollow, readOutputTailBytes, secureReadFlags, secureWriteFlags } from './process-manager-background-process';
import type { BackgroundProcess, CompletedTaskEvent, ForegroundSync, TaskMessage } from './process-manager-background-process';
import { ADOPT_POLL_INTERVAL_MS, TASK_REGISTRY_FILE } from './process-manager-killed-task-event';
import type { InternalProcess, KilledTaskEvent, OutputDelta, PersistedTaskRecord, ProgressDelta } from './process-manager-killed-task-event';
export { GRACEFUL_SHUTDOWN_MS, MAX_BACKGROUND_PROCESSES, MAX_COMPLETED_BUFFER, OUTPUT_FLUSH_MS, PM_LIFECYCLE_PHASES, PROGRESS_IDLE_MS, SPAWN_FAILURE_EXIT_CODE, STALL_CHECK_INTERVAL_MS, STALL_THRESHOLD_MS, TASK_ATTENTION_STATES, TASK_KINDS, TASK_STATUS, buildCompletionSummary, classifyExitStatus, computeElapsedMs, cwdHooks, forceKillAfterTimeout, formatByteCap, formatStoppedByUserNotification, fsHooks, generateTaskId, isProcessAlive, osHooks, pmLogHooks, pmTraceHooks, procHooks, resolveExitCode, resolveSpawnCwd, signalToExitCode, spawnHooks, validateBackgroundCommand } from './process-manager-build-completion-summary';
export type { PmLifecyclePhase, TaskAttentionState, TaskKind, TaskStatus } from './process-manager-build-completion-summary';
export { STALL_TAIL_BYTES, formatUsageClause } from './process-manager-background-process';
export type { BackgroundProcess, CompletedTaskEvent, ForegroundSync, TaskMessage } from './process-manager-background-process';
export { ADOPT_POLL_INTERVAL_MS, TASK_REGISTRY_FILE, formatBackgroundCompletionNote, formatBackgroundOutputDeltaNote } from './process-manager-killed-task-event';
export type { KilledTaskEvent, OutputDelta, PersistedTaskRecord, ProgressDelta } from './process-manager-killed-task-event';

const MAX_OUTPUT_BYTES = 5 * 1024 * 1024; // 5 MB per task

/** Terminal states — a task in one of these will never run again. */

/**
 * ACTIVE states — a task in one of these is in-flight and belongs in the
 * background-tasks indicator (parity with the reference's `isBackgroundTask`, which
 * admits BOTH `running` and `pending`). `pending` (accepted-but-not-started) is
 * active because the work is queued, not finished.
 */
const ACTIVE_TASK_STATUSES = new Set<TaskStatus>(['pending', 'running']);

/**
 * Whether a task status is terminal (completed/failed/killed). Guards against
 * injecting messages into dead teammates, evicting finished tasks, and orphan
 * cleanup (mirrors the reference's isTerminalTaskStatus).
 */

/**
 * Whether a task status is ACTIVE (running or pending) — the two-state "in-flight"
 * predicate the reference uses to decide background-indicator membership (its
 * `isBackgroundTask` gate: `status !== 'running' && status !== 'pending'` ⇒ not a
 * background task). A queued/not-yet-started task counts as active so it is visible
 * as an in-flight affordance, not invisible until it starts. Shared one site
 * (No-Duplication rule) rather than inlining `status === 'running' || 'pending'`.
 */
export function isActiveTaskStatus(status: string | undefined): boolean {
  return !!status && ACTIVE_TASK_STATUSES.has(status as TaskStatus);
}

/**
 * Whether a tracked task belongs in the background-tasks indicator (parity with
 * the reference's `isBackgroundTask`): it must be ACTIVE (running|pending) AND not
 * explicitly foregrounded. A task whose `isBackgrounded` flag was set to `false`
 * via {@link ProcessManager.foregroundTask} is a FOREGROUND task pulled into the
 * main view and is excluded from the indicator until re-backgrounded; `undefined`
 * (the default — AMI tasks are background-by-construction) counts as a background
 * task. Mirrors the reference's `isBackgrounded === false` exclusion.
 */
export function isBackgroundTask(task: Pick<BackgroundProcess, 'status' | 'isBackgrounded'>): boolean {
  return isActiveTaskStatus(task.status) && task.isBackgrounded !== false;
}

/**
 * Grace period (ms) a TERMINAL task is retained after it finishes before it is
 * eligible for automatic eviction, mirroring the reference's panel-grace /
 * `evictAfter` retention window (teammateViewHelpers PANEL_GRACE_MS). A finished
 * task stays in the task table and on disk for this long so the model can still
 * read its output via task_output and see it in task_list; past the deadline it is
 * reaped opportunistically ({@link ProcessManager.pruneExpired}) so a long session
 * does not accumulate stale completed rows / orphaned output files without bound.
 */
export const TERMINAL_RETENTION_MS = 5 * 60_000; // 5 minutes

/** Cap on the retained per-task stderr tail ({@link InternalProcess._stderrTail}). */
export const STDERR_TAIL_CAP = 64 * 1024; // 64 KB

/**
 * Max bytes of NEW output surfaced per incremental delta by
 * {@link ProcessManager.reportOutputDelta}. A delta larger than this is tail-trimmed
 * to the last {@link OUTPUT_DELTA_MAX_BYTES} bytes (with the dropped prefix counted in
 * {@link OutputDelta.bytesOmitted}) so a burst of background output can never flood a
 * single model attachment. Parity with the reference bounding the per-report tail.
 */
export const OUTPUT_DELTA_MAX_BYTES = 4 * 1024; // 4 KB

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
   * The single task currently FOREGROUNDED into the main view, or undefined when
   * none is (parity with the reference's AppState.foregroundedTaskId). Set by
   * {@link foregroundTask}; cleared by {@link backgroundTask} when it re-backgrounds
   * the foregrounded task. At most one task is foregrounded at a time, mirroring
   * the reference's single-slot foreground.
   */
  private _foregroundedTaskId: string | undefined;

  /**
   * Abort callbacks for RUNNING FOREGROUND in-process subagents, keyed by taskId
   * (parity with the reference's InProcessTeammateDetailDialog 'x' → onKill, which can
   * abort ANY running teammate — foreground OR background). A foreground subagent
   * BLOCKS the parent turn and is deliberately NOT a tracked background row (it is
   * shown live in the roster tree, not the background task list), so {@link kill} —
   * which only targets `processes` entries — can never reach it. Without this registry
   * the only way to stop one was Ctrl+C aborting the WHOLE turn (every sibling
   * subagent with it). The task tool populates this while a foreground subagent runs
   * ({@link registerForegroundAbort}) and clears it on teardown or on promotion to a
   * tracked background task ({@link unregisterForegroundAbort}); a frontend aborts ONE
   * individually via {@link abortForeground}.
   */
  private _foregroundAborts = new Map<string, () => void>();

  /**
   * Per-session cap on concurrently RUNNING background processes (parity with
   * the reference's background-shell limit). Defaults to MAX_BACKGROUND_PROCESSES;
   * exposed as a mutable field so callers/tests can tune the ceiling without
   * touching the module constant. spawn() enforces it; see atCapacity().
   */
  maxBackgroundProcesses: number = MAX_BACKGROUND_PROCESSES;

  /**
   * Per-task output-size cap in bytes (parity with the reference's
   * MAX_TASK_OUTPUT_BYTES / ShellCommand size watchdog). Defaults to
   * {@link MAX_OUTPUT_BYTES}; exposed as a mutable field so a caller/test can tune
   * the ceiling without touching the module constant. writeChunk enforces it: once
   * a task's written bytes reach this cap the producer is KILLED (not silently
   * truncated) — a stuck append loop would otherwise burn CPU/IO forever.
   */
  maxOutputBytes: number = MAX_OUTPUT_BYTES;

  /**
   * The manager's known-good working directory (the session cwd at construction).
   * Used as the recovery fallback when a background spawn's requested cwd has been
   * deleted mid-session (see {@link resolveSpawnCwd}), mirroring the reference's
   * getOriginalCwd() recovery target.
   */
  private readonly rootCwd: string;

  /**
   * The session id this manager's task-output directory is SCOPED to, resolved
   * ONCE at construction and never re-read (parity with the reference's diskOutput
   * memoizing getSessionId() at first call, utils/task/diskOutput.ts:L33-55). Two
   * things depend on pinning it here:
   *   1. Concurrent sessions in the SAME project cwd get DISTINCT task dirs
   *      (`tasks/<sessionId>`), so one session's startup/cleanup can never clobber
   *      another live session's in-flight output files (they previously shared one
   *      project-level `tasks/` dir keyed only by random task ids).
   *   2. A mid-session session-id change (e.g. /clear's regenerateSessionId) does
   *      NOT strand already-running background tasks, because the dir is captured
   *      from the id at construction, not re-derived on each write (the documented
   *      inc-4586 incident). Undefined ⇒ the legacy project-level `tasks/` dir.
   */
  private readonly sessionId: string | undefined;

  constructor(cwd: string, sessionId?: string) {
    super();
    this.rootCwd = cwd;
    this.sessionId = sessionId;
    // Scope the output dir to the session when an id is given so concurrent
    // sessions in the same cwd are isolated; fall back to the project-level dir
    // when none is supplied. isSafeSessionSegment guards against a crafted id
    // (path separators / traversal) escaping the tasks root.
    this.tasksDir = sessionId && isSafeSegment(sessionId)
      ? projectStateDir(cwd, path.join('tasks', sessionId))
      : projectStateDir(cwd, 'tasks');
    fs.mkdirSync(this.tasksDir, { recursive: true });
  }

  /**
   * FUNCTIONAL (parity): true when the number of currently-running tasks has
   * reached `maxBackgroundProcesses`, i.e. a further spawn() would be rejected.
   * Lets callers pre-check and surface a clean message instead of catching the
   * throw. Mirrors the reference refusing to launch beyond its background limit.
   */
  atCapacity(): boolean {
    return this.runningCount() >= this.maxBackgroundProcesses;
  }

  spawn(command: string, opts: { cwd: string; description?: string; env?: NodeJS.ProcessEnv; timeoutMs?: number; agentId?: string; toolUseId?: string; kind?: Extract<TaskKind, 'bash' | 'monitor'>; signal?: AbortSignal; maxOutputBytes?: number; fileMode?: boolean }): string {
    // FUNCTIONAL (parity): enforce the concurrent background-process ceiling.
    // Previously MAX_BACKGROUND_PROCESSES was declared but never checked, so a
    // session could spawn unbounded detached shells. the reference caps concurrent
    // background shells; refuse (throw) once the running set is full.
    if (this.atCapacity()) {
      throw new Error(
        `too many background processes (limit ${this.maxBackgroundProcesses}); ` +
        `wait for one to finish or kill one before starting another`,
      );
    }
    const taskId = generateTaskId('bg');
    const description = opts.description || command.slice(0, 80);

    // ABORT-BEFORE-SPAWN short-circuit (parity with the reference's Shell.ts
    // `if (abortSignal.aborted) return createAbortedCommand()`, L241-243): a caller
    // that cancels the spawn request before we fork should pay NO fork/openSync
    // cost. Record a terminal 'killed' task (no child, no output file) so the id is
    // still trackable/reportable, then return without spawning.
    if (opts.signal?.aborted) {
      return this.createTerminalStub(taskId, { command, description, status: 'killed', exitCode: null, kind: opts.kind, agentId: opts.agentId, toolUseId: opts.toolUseId });
    }

    // CWD existence validation + recovery (parity with the reference's Shell.ts
    // realpath(cwd) recovery, L220-238). If the requested cwd was deleted mid-session
    // (a prior command removed its own temp dir), fall back to the manager's
    // known-good root; if THAT is also gone, short-circuit to 'failed' with a clear
    // message instead of letting child_process.spawn surface a bare async ENOENT.
    const resolvedCwd = resolveSpawnCwd(opts.cwd, this.rootCwd);
    if (resolvedCwd.error) {
      const outputPath = path.join(this.tasksDir, `${taskId}.output`);
      try {
        const fd0 = fs.openSync(outputPath, secureWriteFlags());
        try { fs.writeSync(fd0, Buffer.from(`[${resolvedCwd.error}]\n`)); } finally { fs.closeSync(fd0); }
      } catch { /* best-effort diagnostic */ }
      return this.createTerminalStub(taskId, { command, description, status: 'failed', exitCode: null, kind: opts.kind, agentId: opts.agentId, toolUseId: opts.toolUseId, outputPath, error: resolvedCwd.error });
    }
    const outputPath = path.join(this.tasksDir, `${taskId}.output`);

    const isWindows = osHooks.platform() === 'win32';
    const shell = isWindows ? 'cmd' : 'bash';
    const shellArgs = isWindows ? ['/c', command] : ['-c', command];

    // Open the output file FIRST with fail-closed secure flags: if a symlink/file
    // was planted at outputPath the open throws (O_EXCL) before any child is
    // spawned, so we never leak a process we can't capture output for.
    const fd = fs.openSync(outputPath, secureWriteFlags());

    // SYNCHRONOUS spawn-failure handling (parity with the reference's Shell.ts
    // try/catch around spawn, L315-441). A fork can fail synchronously — EMFILE/
    // EAGAIN under load, or bad spawn options — making child_process.spawn THROW
    // before any child exists. Previously that exception propagated out of spawn()
    // to the caller (bash.ts does not wrap this call), with NO tracked task and the
    // just-opened output fd leaked. We now: close the fd (no leak), write a
    // diagnostic into the output file, and record a terminal 'failed' task with the
    // standard exec-error exit code (126) so the caller ALWAYS gets a trackable
    // taskId and a model-facing completion — mirroring the reference's createFailedCommand.
    // FILE MODE (parity with the reference's TaskOutput stdoutToFile, L21-36): when the
    // caller opts in, wire the child's stdout AND stderr STRAIGHT to the already-open
    // output fd (`stdio: ['ignore', fd, fd]`) so the kernel streams every byte to the
    // file and NEITHER stream enters JS — no 'data' listener, no writeChunk, no pipe
    // backpressure on a very large/fast firehose. Progress/size and the runaway cap are
    // then derived by polling the file (see getOutputSize / enforceFileModeOutputCap).
    // Default (opt-out) stays the PIPE path so the separable stderr tail is preserved.
    const fileMode = opts.fileMode === true;
    let proc: child_process.ChildProcess;
    try {
      proc = spawnHooks.spawn(shell, shellArgs, {
        cwd: resolvedCwd.cwd,
        stdio: fileMode ? ['ignore', fd, fd] : ['ignore', 'pipe', 'pipe'],
        detached: !isWindows,
        ...(opts.env ? { env: opts.env } : {}),
      });
    } catch (err) {
      const msg = `failed to start command: ${(err as Error).message}`;
      try { fs.writeSync(fd, Buffer.from(`[${msg}]\n`)); } catch { /* best-effort diagnostic */ }
      try { fs.closeSync(fd); } catch { /* ignore */ }
      return this.createTerminalStub(taskId, { command, description, status: 'failed', exitCode: SPAWN_FAILURE_EXIT_CODE, kind: opts.kind, agentId: opts.agentId, toolUseId: opts.toolUseId, outputPath, error: msg });
    }

    const entry: InternalProcess = {
      taskId,
      pid: proc.pid!,
      command,
      description,
      status: 'running',
      // A caller may spawn a long-lived STREAMING/monitor shell (`tail -f`, a dev
      // server, a log follower) as kind 'monitor' so the stall watchdog is
      // disabled and its completion is worded as a monitor stream ending rather
      // than a command succeeding/failing. Parity: the reference's BashTaskKind
      // ('bash' | 'monitor') threaded through the spawn input (LocalShellTask).
      kind: opts.kind ?? 'bash',
      exitCode: null,
      outputPath,
      startTime: Date.now(),
      ...(opts.agentId ? { agentId: opts.agentId } : {}),
      ...(opts.toolUseId ? { toolUseId: opts.toolUseId } : {}),
      proc,
      outputFd: fd,
      bytesWritten: 0,
      // Per-spawn output cap override (parity with the reference's ShellCommand
      // maxOutputBytes constructor parameter). When the caller supplies a positive
      // maxOutputBytes it pins THIS task's disk-buffer ceiling; absent, writeChunk
      // falls back to the manager-level this.maxOutputBytes (which stays runtime-tunable).
      ...(typeof opts.maxOutputBytes === 'number' && opts.maxOutputBytes > 0 ? { _maxOutputBytes: opts.maxOutputBytes } : {}),
      // Record the stdio routing so getOutputSize / the cap poll / getStderr branch
      // on it (file mode ⇒ output never entered JS; size is read from disk).
      ...(fileMode ? { _fileMode: true } : {}),
    };

    this.processes.set(taskId, entry);
    this.traceLifecycle('spawn', taskId, { kind: entry.kind, pid: entry.pid, ...(fileMode ? { fileMode: true } : {}) });

    const writeChunk = (data: Buffer) => {
      const cap = entry._maxOutputBytes ?? this.maxOutputBytes;
      if (entry.bytesWritten >= cap) return;
      const remaining = cap - entry.bytesWritten;
      const chunk = data.length <= remaining ? data : data.subarray(0, remaining);
      if (!this.safeWrite(fd, chunk)) return;
      entry.bytesWritten += chunk.length;
      if (entry.bytesWritten >= cap) {
        // Runaway-output watchdog (parity with the reference's ShellCommand size
        // watchdog, which SIGKILLs a task whose output file exceeds the cap).
        // Previously AMI silently truncated and let the producer keep running — a
        // stuck `while true; do echo x; done` would then burn CPU/IO forever while
        // we dropped every byte. FUNCTIONAL CHANGE: write a diagnostic note, then
        // KILL the task with a reason so the model learns it was stopped (and why)
        // rather than seeing a frozen, truncated log from a still-live process.
        const label = formatByteCap(cap);
        this.safeWrite(fd, Buffer.from(`\n[output exceeded ${label} cap — task killed]\n`));
        this.kill(taskId, { reason: `output exceeded ${label} cap` });
      }
    };

    // PIPE MODE ONLY: in file mode the child's stdout/stderr are the raw output fd
    // (proc.stdout/proc.stderr are null) — there is no JS-side stream to listen on,
    // which is the whole point (the firehose bypasses V8). The per-chunk writeChunk
    // path, the separable stderr tail, and the pipe EBADF handler are therefore pipe-
    // mode-exclusive; file mode derives size by polling and enforces the cap there.
    if (!fileMode) {
      proc.stdout?.on('data', writeChunk);
      // STDERR is captured TWICE: into the merged output file (via writeChunk — the
      // existing behavior task_output reads) AND into a bounded, separable in-memory
      // tail so a consumer can surface stderr DISTINCTLY from stdout (parity with
      // the reference's command-output renderer keeping stderr its own block). See
      // {@link ProcessManager.getStderr} + the task_output stderr section.
      proc.stderr?.on('data', (data: Buffer) => {
        const prev = entry._stderrTail ?? '';
        const next = prev + data.toString('utf-8');
        entry._stderrTail = next.length > STDERR_TAIL_CAP ? next.slice(next.length - STDERR_TAIL_CAP) : next;
        writeChunk(data);
      });
      // Swallow EBADF errors on pipe streams. When a process is killed, the OS
      // may close the pipe fds before Node.js finishes reading, emitting an
      // EBADF error event. Without this handler, the error becomes uncaught.
      const swallowEBADF = (err: NodeJS.ErrnoException) => {
        if (err.code !== 'EBADF') { this.emit('error', err); }
      };
      proc.stdout?.on('error', swallowEBADF);
      proc.stderr?.on('error', swallowEBADF);
    }

    // Stall detection for background processes. Delegated to stallTick() so the
    // logic is unit-testable without waiting out the poll interval. The cadence
    // (STALL_CHECK_INTERVAL_MS) is decoupled from the no-growth threshold
    // (STALL_THRESHOLD_MS, enforced inside stallTick) — see the knob JSDoc.
    let lastOutputSize = 0;
    const stallCheck = setInterval(() => {
      // FILE MODE: writeChunk never runs (output bypasses JS), so the runaway byte-cap
      // is enforced HERE by polling the on-disk size — the task is killed once the file
      // reaches the cap, mirroring the reference's ShellCommand file-size watchdog. If it
      // was killed this tick, skip the stall check (the entry is no longer running).
      if (fileMode && this.enforceFileModeOutputCap(taskId)) return;
      lastOutputSize = this.stallTick(taskId, lastOutputSize);
    }, STALL_CHECK_INTERVAL_MS);
    // Observation-only timer: unref() so a still-registered task's poller can never
    // by itself keep the Node event loop alive and block clean process exit
    // (parity with the reference unref-ing its progress-poll interval).
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
      this.setEvictDeadline(entry);
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
      this.setEvictDeadline(entry);
      this.safeWrite(fd, Buffer.from(`\n[process error: ${err.message}]\n`));
      try { fs.closeSync(fd); } catch { /* ignore */ }
      entry.outputFd = null;
      this.emitComplete({ taskId, exitCode: null, command: entry.command, description: entry.description, outputPath: entry.outputPath, status: 'failed' });
    });

    // Cooperative cancellation via an AbortSignal (parity with the reference's
    // ShellCommand abort-listener, ShellCommand.ts:186-193/264-267): a REAL abort
    // kills the process tree; an abort whose reason is 'interrupt' is a SOFT
    // interrupt — the caller intends to BACKGROUND the in-flight work so its partial
    // output survives, so we deliberately do NOT kill. Registered once (the task's
    // own 'close'/'error' finalize cleans everything up on normal exit).
    if (opts.signal) {
      opts.signal.addEventListener('abort', () => {
        if ((opts.signal as AbortSignal & { reason?: unknown }).reason === 'interrupt') return;
        this.kill(taskId);
      }, { once: true });
    }

    // Persist this newly-running detached shell to the crash-safe registry so a CLI
    // restart can re-adopt it (parity: the reference's persistRemoteAgentMetadata at
    // spawn). No-op for the persisted set beyond running bash tasks (see persistRegistry).
    this.syncRegistry();

    return taskId;
  }

  /**
   * Record a synthetic TERMINAL task that never ran a real child — the abort-before-
   * spawn and deleted-cwd short-circuits (see {@link spawn}). The entry is trackable
   * (task_list/task_output/get) and its completion is routed through the durable
   * buffer exactly like a real task's, so the model learns the spawn was refused and
   * why. `proc` is a no-op stub; there is no fd to close.
   */
  private createTerminalStub(
    taskId: string,
    info: { command: string; description: string; status: Extract<TaskStatus, 'killed' | 'failed'>; exitCode: number | null; kind?: Extract<TaskKind, 'bash' | 'monitor'>; agentId?: string; toolUseId?: string; outputPath?: string; error?: string },
  ): string {
    const now = Date.now();
    const entry: InternalProcess = {
      taskId,
      pid: 0,
      command: info.command,
      description: info.description,
      status: info.status,
      kind: info.kind ?? 'bash',
      exitCode: info.exitCode,
      outputPath: info.outputPath ?? '',
      startTime: now,
      endTime: now,
      ...(info.agentId ? { agentId: info.agentId } : {}),
      ...(info.toolUseId ? { toolUseId: info.toolUseId } : {}),
      ...(info.error ? { error: info.error } : {}),
      proc: { pid: 0, kill: () => { /* no child */ } } as unknown as child_process.ChildProcess,
      outputFd: null,
      bytesWritten: 0,
    };
    this.setEvictDeadline(entry);
    this.processes.set(taskId, entry);
    this.emitComplete({ taskId, exitCode: info.exitCode, command: info.command, description: info.description, outputPath: entry.outputPath, status: info.status });
    return taskId;
  }

  /** Emit a completion AND buffer it for durable drain (B4). */
  private emitComplete(payload: CompletedTaskEvent): void {
    // Dedup (parity with the reference's stopTask `notified` flag): a task the model
    // was already told about — because it explicitly killed it (task_kill latches
    // bash tasks in kill()) or already consumed its result (task_output →
    // markNotified()) — must NOT re-surface a redundant completion. The model
    // learned of the termination from the tool result; a later "[Background task X
    // completed (exit 137)]" note is pure noise. Suppress it before it ever reaches
    // the durable buffer / 'complete' listeners.
    const entry = this.processes.get(payload.taskId);
    if (entry?.notified) return;
    // Attach the status-aware human-readable summary (single phrasing source —
    // buildCompletionSummary) so every downstream consumer reads a ready-made
    // outcome string instead of re-deriving it from status/exitCode.
    if (payload.summary === undefined) {
      payload.summary = buildCompletionSummary(payload.status, payload.exitCode, entry?.kind);
    }
    // Attach the active run duration from the tracked entry so the completion
    // note's usage clause can report how long the task ran — parity with the reference
    // Code's completion notification `duration_ms`. Frozen, pause-discounted value
    // via the shared computeElapsedMs formula; omitted for an untracked task or one
    // whose endTime isn't set yet (defensive — emitComplete always runs after the
    // terminal transition froze endTime).
    if (entry && payload.durationMs === undefined && entry.endTime !== undefined) {
      payload.durationMs = computeElapsedMs(entry.startTime, entry.endTime, entry.totalPausedMs ?? 0);
    }
    // Attach the agent's final token / tool-use totals so the completion note's usage
    // clause can report them (parity with the reference's LocalAgentTask completion
    // `total_tokens` / `tool_uses`). Prefer the authoritative result totals recorded
    // at the terminal transition, falling back to the last live counter; omitted for
    // a bash shell (no counts) so its clause stays duration-only. See formatUsageClause.
    if (entry && payload.tokenCount === undefined) {
      const tok = entry.resultTokenCount ?? entry.tokenCount;
      if (typeof tok === 'number' && tok > 0) payload.tokenCount = tok;
    }
    if (entry && payload.toolCount === undefined) {
      const tc = entry.resultToolCount ?? entry.toolCount;
      if (typeof tc === 'number' && tc > 0) payload.toolCount = tc;
    }
    // Bounded buffer (parity with the reference's sdkEventQueue): evict the oldest
    // entry at the cap so the durable buffer can never grow without bound across a
    // long silent turn with active producers (see MAX_COMPLETED_BUFFER).
    if (this._completedBuffer.length >= MAX_COMPLETED_BUFFER) this._completedBuffer.shift();
    this._completedBuffer.push(payload);
    this.emit('complete', payload);
    // A task reaching a terminal state changes the live running-bash set the
    // crash-safe registry mirrors — refresh the sidecar so a later restart doesn't
    // try to re-adopt a pid that has already finished (see rescanTasks).
    this.syncRegistry();
  }

  /**
   * PUBLIC completion entry point for an EXTERNALLY-finalized task — an in-process
   * background AGENT whose terminal transition happens in its drain loop (task.ts),
   * not via a child-process 'exit'/'close' event. Routes through the same
   * {@link emitComplete} path as a bash task so the agent's completion is (a) buffered
   * for durable drain (the engine surfaces it exactly once, just like a shell), (b)
   * enriched with the status-aware summary, run duration, and token/tool-use usage
   * clause, and (c) deduped against a prior task_output read (`notified`). Before this
   * seam agents fired a bare `emit('complete')` that bypassed the buffer entirely —
   * with no live listener anywhere, a finished background agent surfaced NO
   * model-facing completion note at all. Mirrors the reference's LocalAgentTask
   * completion notification.
   */
  emitCompletion(payload: CompletedTaskEvent): void {
    this.emitComplete(payload);
  }

  /**
   * Dedup check-and-set (parity with the reference's markTaskNotified): latch a task
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
   * it stopped from the tool result) — parity with the reference's stopTask. INTERNAL
   * kills (the timeout watchdog, cleanup, killAllAgents) pass no option, so their
   * completion STILL surfaces: a task the model did not ask to stop (e.g. it hit
   * its time limit) must be reported. Agent tasks are never latched (the bash
   * branch is the only latch site) so their abort completion, which carries the
   * partial result, always surfaces — matching the reference's bash-vs-agent split.
   */
  kill(taskId: string, opts: { suppressCompletion?: boolean; reason?: string; deferRegistrySync?: boolean } = {}): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;

    entry.status = 'killed';
    // MEMORY RELEASE (parity with the reference's spawnInProcess on-kill truncation,
    // spawnInProcess.ts:L287-296): a user-KILLED agent releases its retained
    // transcript — collapse the messages buffer to just the FINAL message (the last
    // displayed line) instead of pinning the whole conversation in memory. A
    // completed/failed agent keeps its full transcript (the drill view still shows
    // what it did); ONLY a kill releases it. No-op for a bash/monitor task.
    this.releaseKilledTranscript(entry);
    // Close any open pause window into totalPausedMs before freezing the clock so
    // a task killed while stalled keeps correct pause accounting.
    this.settlePause(entry);
    // Freeze the elapsed-time clock at the kill instant. For a real child this is
    // overwritten (with ~the same value) when its 'exit' event fires; for
    // in-process agents / MCP monitors (no exit event) this is the only place the
    // terminal timestamp is set.
    entry.endTime = Date.now();
    this.setEvictDeadline(entry);
    // A RE-ADOPTED task (rescanTasks) carries a liveness poll instead of a child
    // 'exit' listener — stop it so it can't fire after the kill. This is separate
    // from _intervalHandle so an adopted BASH task still falls through to killTree
    // below (not the monitor branch) and its detached pid is actually terminated.
    if (entry._adoptPoll) { clearInterval(entry._adoptPoll); entry._adoptPoll = undefined; }
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
      // the task stopped from the task_kill tool result. Parity with the reference's
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

      // Robust process-TREE kill (parity with the reference's ShellCommand #doKill →
      // treeKill). A bare process-group signal (`process.kill(-pid)`) misses a
      // descendant that escaped the group via setsid()/double-fork/nohup, leaving an
      // orphaned process alive after the stop. killTree() enumerates the full
      // descendant tree (ps/ppid walk) and signals each pid individually, THEN
      // sweeps the group, so a session leader that broke away is still reaped.
      // Shared helper — not re-implemented here (No-Duplication rule).
      killTree(entry.proc);
    }

    if (entry.outputFd !== null) {
      try { fs.closeSync(entry.outputFd); } catch { /* ignore */ }
      entry.outputFd = null;
    }

    // INTROSPECTION (parity with the reference's `LocalShellTask ${taskId} kill
    // requested` debug log): emit a structured 'killed' event so a frontend/engine
    // can trace WHY a background process ended (and whether its completion note was
    // suppressed). Unlike the completion note this fires for EVERY kill — including
    // suppressed explicit stops — so the lifecycle is always observable.
    const killed: KilledTaskEvent = {
      taskId,
      description: entry.description,
      // Derive the agent/bash discriminator from the authoritative `kind` field
      // (a monitor maps to undefined) instead of brittle command-string parsing.
      taskType: this.taskType(taskId),
      suppressed: !!entry.notified,
      ...(opts.reason ? { reason: opts.reason } : {}),
    };
    this.traceLifecycle('kill', taskId, { kind: entry.kind, suppressed: !!entry.notified, ...(opts.reason ? { reason: opts.reason } : {}) });
    this.emit('killed', killed);

    // A killed agent is idle forever — wake any leader waiting on {@link waitForIdle}
    // so an event-driven wait doesn't hang past the task's destruction.
    this.flushIdleWaiters(entry);

    // The task left the running set — refresh the crash-safe registry. For a normal
    // bash task the later 'close' → finalize → emitComplete also syncs; for an
    // ADOPTED task (no 'close' event) this is the only sync, so it must run here.
    // Bulk teardown loops pass `deferRegistrySync` and call syncRegistry() ONCE after
    // the loop so killing N tasks is one registry rewrite, not N (persistRegistry
    // always writes the full running set, so a single trailing sync is identical).
    if (!opts.deferRegistrySync) this.syncRegistry();

    return true;
  }

  /**
   * FUNCTIONAL (parity): single-shot stop of EVERY running in-process agent
   * (ESC/kill-all cancellation), returning the killed agents plus ONE aggregate
   * model-facing notification. Mirrors the reference's useCancelRequest
   * killAllAgentsAndNotify: it filters to `local_agent` tasks that are still
   * running, kills them all, and enqueues a single "stopped by the user" summary
   * rather than a per-agent notification storm. Only {@link registerAgent}-backed
   * tasks (taskType === 'agent') are targeted — background bash shells and MCP
   * monitors are left running, matching the reference's type filter. The returned
   * `notification` is null when nothing was running so callers can skip the
   * enqueue. kill() already freezes each task's elapsed clock and closes its fd.
   */
  killAllAgents(): { stopped: BackgroundProcess[]; notification: string | null } {
    const stopped: BackgroundProcess[] = [];
    for (const taskId of Array.from(this.processes.keys())) {
      const entry = this.processes.get(taskId);
      if (!entry || entry.status !== 'running') continue;
      if (this.taskType(taskId) !== 'agent') continue;
      if (this.kill(taskId, { deferRegistrySync: true })) {
        const snapshot = this.get(taskId);
        if (snapshot) stopped.push(snapshot);
      }
    }
    if (stopped.length > 0) this.syncRegistry();
    const notification = formatStoppedByUserNotification(stopped.map((s) => s.description));
    return { stopped, notification };
  }

  /**
   * FUNCTIONAL (parity with the reference's TeamsDialog 'p' prune-idle handler,
   * TeamsDialog.tsx:L207-217): bulk-stop every tracked AGENT task that is currently
   * {@link isIdle idle} (running, finished its turn, awaiting the next message), in
   * one operation. Mirrors {@link killAllAgents} but filtered to the IDLE subset, so
   * a leader can reclaim concurrency slots held by teammates that have gone quiet
   * WITHOUT tearing down agents still doing work (a busy teammate — running and not
   * idle — is left alone). Background bash shells and MCP monitors are never targeted
   * (type filter, like {@link killAllAgents}). Returns the snapshots of the agents it
   * stopped (empty when none were idle), so a caller can reconcile its own selection
   * state after the removals (parity: the reference recomputing the selection index).
   */
  pruneIdleAgents(): BackgroundProcess[] {
    const stopped: BackgroundProcess[] = [];
    for (const taskId of Array.from(this.processes.keys())) {
      const entry = this.processes.get(taskId);
      if (!entry || entry.status !== 'running' || entry.idle !== true) continue;
      if (this.taskType(taskId) !== 'agent') continue;
      if (this.kill(taskId, { deferRegistrySync: true })) {
        const snapshot = this.get(taskId);
        if (snapshot) stopped.push(snapshot);
      }
    }
    if (stopped.length > 0) this.syncRegistry();
    return stopped;
  }

  /**
   * FUNCTIONAL (parity: the reference's clearConversation task teardown,
   * commands/clear/conversation.ts:L146-166): on a conversation CLEAR, kill every
   * RUNNING task whose id is NOT in {@link preservedIds} and evict its on-disk
   * output, so background shells / MCP monitors / non-preserved agents spawned in
   * the just-cleared conversation stop consuming resources instead of being
   * orphaned across the session-id regeneration (where they would keep executing /
   * completing commands the user believes were discarded). Explicitly preserved
   * background agents — the alive teammates the caller partitioned out — are left
   * running with their output intact. Each kill+evict is wrapped in its OWN
   * try/catch error boundary so one failing teardown never aborts the clear
   * (parity: the reference's per-task try/catch + logError). Returns the snapshots of the
   * tasks it stopped (empty when nothing non-preserved was running).
   */
  killForClear(preservedIds: ReadonlySet<string> = new Set()): BackgroundProcess[] {
    const stopped: BackgroundProcess[] = [];
    for (const taskId of Array.from(this.processes.keys())) {
      if (preservedIds.has(taskId)) continue;
      const entry = this.processes.get(taskId);
      if (!entry || entry.status !== 'running') continue;
      // Snapshot BEFORE removeTask drops the entry so the caller/introspection can
      // still report what was torn down.
      const snapshot = this.get(taskId);
      try {
        // `suppressCompletion` keeps a wiped conversation from re-surfacing an
        // exit-137 completion note for a task the user already discarded; `reason`
        // distinguishes a clear-driven teardown from an ESC/timeout in the trace.
        this.kill(taskId, { suppressCompletion: true, reason: 'conversation cleared', deferRegistrySync: true });
        // Evict the killed task's output file + drop its table row (parity: the reference
        // evicting the task's disk output on clear). kill() has already transitioned
        // it to 'killed', so removeTask (which refuses a running task) accepts it.
        this.removeTask(taskId);
      } catch { /* error boundary: a failing teardown must not abort the clear */ }
      if (snapshot) stopped.push(snapshot);
    }
    if (stopped.length > 0) this.syncRegistry();
    return stopped;
  }

  /**
   * FUNCTIONAL (parity): pause a RUNNING task's elapsed-time clock. Idempotent — a
   * second pause() before a resume() is a no-op (returns false). Used by the stall
   * watchdog so a task blocked on an interactive prompt doesn't inflate its
   * active-work duration. Mirrors the reference's pause/resume accounting
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

  /**
   * Derive the automatic-eviction deadline for a just-finished task from its frozen
   * {@link BackgroundProcess.endTime}: `endTime + TERMINAL_RETENTION_MS`. Called at
   * every terminal transition (finalize / error / kill / terminal updateEntry)
   * right after endTime is set, so {@link pruneExpired} can reap stale terminal rows
   * once their grace window elapses. No-op if endTime is somehow unset. Shared one
   * site (No-Duplication rule) rather than inlining the arithmetic per transition.
   */
  private setEvictDeadline(entry: InternalProcess): void {
    if (entry.endTime !== undefined) entry.evictAfter = entry.endTime + TERMINAL_RETENTION_MS;
  }

  /**
   * Collapse a KILLED task's transcript buffer down to just its FINAL message —
   * the memory-release teardown mirroring the reference's spawnInProcess on-kill
   * truncation (messages → `[messages.at(-1)]`). Preserves the last displayed line
   * so a drill view still shows where the agent stopped, while dropping the rest of
   * the retained conversation. Also clears the foreground sync cursor so a later
   * re-foreground starts fresh. No-op when the task has ≤1 message / no buffer.
   * Shared one site (No-Duplication rule); only invoked from {@link kill}.
   */
  private releaseKilledTranscript(entry: InternalProcess): void {
    if (entry.messages && entry.messages.length > 1) {
      entry.messages = [entry.messages[entry.messages.length - 1]];
    }
    entry._lastSyncedMessagesLength = undefined;
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
   * poll interval so it is unit-testable). Mirrors the reference's startStallWatchdog:
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
    // A monitor task is EXPECTED to idle between bursts (a log follower waiting for
    // the next line, a server between requests), so the no-growth stall watchdog is
    // disabled for it — otherwise a healthy monitor would be mis-flagged as blocked
    // on an interactive prompt and paused. Parity: the reference's startStallWatchdog
    // returns a no-op cleanup (`if (kind === 'monitor') return () => {}`).
    if (entry.kind === 'monitor') return lastSize;
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
      // never pulls the whole file into memory (parity with the reference's tailFile).
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
   *  'stalled' event (mirrors the reference's task-notification: tell the model the
   *  command is likely blocked on an interactive prompt and how to recover). */
  private notifyStall(entry: InternalProcess, tail: string): void {
    // A task blocked on an interactive prompt NEEDS the user's attention — flag it
    // so task_list can surface a "needs input" badge (parity with the reference's
    // waiting-for-input task state). Orthogonal to the lifecycle status (still
    // 'running'); cleared only when the task terminates.
    entry.attentionState = 'needs_input';
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
   * the write ONCE, and only then give up (logging again). Mirrors the reference's
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
   * the reference surfacing `task_type` so stop/list callers can branch on the
   * kind of task without inspecting internals.
   */
  taskType(taskId: string): 'agent' | 'bash' | undefined {
    const kind = this.processes.get(taskId)?.kind;
    if (kind === 'agent') return 'agent';
    if (kind === 'bash') return 'bash';
    // A monitor (or an untracked id) has no agent/bash task_type — previously a
    // monitor was MISCLASSIFIED as 'bash' (it ran the non-registered branch).
    return undefined;
  }

  /**
   * PARITY (additive): the full task-KIND discriminator for a tracked task —
   * `'bash' | 'monitor' | 'agent'`, or undefined when the id isn't tracked. Unlike
   * {@link taskType} (which collapses monitors to undefined to keep its agent/bash
   * contract) this exposes the monitor kind too, so a pill/label renderer can split
   * "1 shell, 1 monitor" exactly like the reference's getPillLabel.
   */
  taskKind(taskId: string): TaskKind | undefined {
    return this.processes.get(taskId)?.kind;
  }

  /**
   * FUNCTIONAL (parity): reap a single FINISHED task — drop it from the tracked
   * map and delete its on-disk output file. Refuses (returns false) for an
   * unknown task or one that is still running, so a live process is never
   * untracked out from under its close/error handler. Returns true when a
   * terminal task was removed. Mirrors the reference reaping completed shells so
   * the session's task table and tasks/ directory don't grow without bound.
   */
  removeTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry) return false;
    if (!isTerminalTaskStatus(entry.status)) return false;
    // Defensive: stop any re-adoption liveness poll before dropping the entry so a
    // reaped adopted task can never leave a dangling timer.
    if (entry._adoptPoll) { clearInterval(entry._adoptPoll); entry._adoptPoll = undefined; }
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
   * outputFileRedundant optimization (parity with the reference's TaskOutput
   * `outputFileRedundant = bytesTotal <= bytesRead`, diskOutput.ts:L45-48 /
   * TaskOutput.ts:L306-311,L348-359): once a TERMINAL task's ENTIRE output has
   * already been read inline into a tool_result (nothing omitted), the on-disk copy
   * is redundant — the model holds every byte — so the duplicate file can be
   * deleted instead of lingering on disk for the common small-output case. The
   * tracked entry STAYS (status/exit code still readable via task_list / get); only
   * its `outputPath` is blanked so a later {@link getOutput} returns '' for the
   * now-intentionally-absent file rather than a "could not be read" diagnostic.
   *
   * Refuses (returns false) when: the task is unknown, still RUNNING (more output
   * may arrive), has no output file, or `bytesOmitted > 0` (the whole file did NOT
   * fit the read, so the on-disk copy still holds the omitted earlier output).
   * Returns true when the redundant file was deleted. Idempotent — a second call
   * after the path is blanked returns false.
   */
  deleteRedundantOutput(taskId: string, bytesOmitted: number): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || !isTerminalTaskStatus(entry.status)) return false;
    if (!entry.outputPath) return false;
    if (bytesOmitted > 0) return false;
    if (entry.outputFd !== null) {
      try { fs.closeSync(entry.outputFd); } catch { /* already closed */ }
      entry.outputFd = null;
    }
    try { fs.rmSync(entry.outputPath, { force: true }); } catch { /* best-effort */ }
    entry.outputPath = '';
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
   * FUNCTIONAL (parity with the reference's `evictAfter` retention): reap every
   * TERMINAL task whose eviction deadline ({@link BackgroundProcess.evictAfter},
   * set to endTime + {@link TERMINAL_RETENTION_MS}) has passed, via removeTask().
   * Unlike {@link pruneTerminal} (which drops ALL terminal tasks immediately) this
   * honours the panel grace window, so a freshly-finished task stays readable via
   * task_output/task_list for TERMINAL_RETENTION_MS before it is auto-evicted.
   * Called opportunistically on reads (task_list) so a long session doesn't
   * accumulate stale completed rows / orphaned output files without a timer.
   * `now` is injectable for deterministic tests. Returns the count reaped.
   */
  pruneExpired(now: number = Date.now()): number {
    let removed = 0;
    for (const taskId of Array.from(this.processes.keys())) {
      const entry = this.processes.get(taskId);
      if (!entry || entry.evictAfter === undefined || entry.evictAfter > now) continue;
      if (this.removeTask(taskId)) removed++;
    }
    return removed;
  }

  /**
   * FUNCTIONAL (parity): kill every RUNNING task spawned by `agentId` (zombie
   * prevention). Invoked from the sub-agent lifecycle's finally block so background
   * shells a sub-agent launched don't outlive the agent that started them — parity
   * with the reference's killShellTasksForAgent (prevents the "10-day fake-logs.sh
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
      // Reason-bearing kill (parity with the reference's killShellTasksForAgent debug
      // line) so the emitted 'killed' event explains WHY the orphan shell vanished.
      if (this.kill(taskId, { reason: `agent ${agentId} exiting`, deferRegistrySync: true })) killed++;
    }
    if (killed > 0) this.syncRegistry();
    return killed;
  }

  getOutput(
    taskId: string,
    tailLines = 50,
    filter?: string | RegExp,
    maxReadBytes?: number,
  ): {
    status: string;
    /** The task KIND discriminator (bash | monitor | agent) so a consumer can label
     *  a background shell vs an internal monitor vs an in-process agent without
     *  parsing the command text (parity: the reference's shell-vs-monitor wording). */
    kind: TaskKind;
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

    // FUNCTIONAL (parity): optional line filter, mirroring the reference's
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
      // FUNCTIONAL (parity with the reference's getTaskOutput/tailFile): when a byte
      // ceiling is supplied, read ONLY the last `maxReadBytes` of the file via a
      // positioned read so memory/IO is bounded no matter how large the log grew
      // (AMI previously read the WHOLE file into memory on every poll). The skipped
      // prefix is reported as `bytesOmitted` and the total line count is
      // extrapolated from the sample so the caller can show "~N lines" and an
      // "earlier output omitted" affordance. Without a ceiling the whole file is
      // read as before (bytesOmitted === 0, exact line count).
      let content: string;
      let sampleBytesRead = 0;
      if (maxReadBytes !== undefined && Number.isFinite(maxReadBytes) && maxReadBytes > 0) {
        const read = readOutputTailBytes(entry.outputPath, Math.floor(maxReadBytes));
        content = read.content;
        bytesTotal = read.bytesTotal;
        bytesOmitted = read.bytesOmitted;
        sampleBytesRead = read.bytesRead;
      } else {
        content = readOutputFileNoFollow(entry.outputPath);
        bytesTotal = Buffer.byteLength(content, 'utf-8');
        bytesOmitted = 0;
      }
      // Split the content ONCE and derive the line count from the same array
      // (the old code ran content.split('\n') twice — once for the count, once for
      // `lines` — over a whole output file up to maxOutputBytes).
      const allLines = content.split('\n');
      const sampleLines = content === '' ? 0 : allLines.length;
      totalLinesEstimate = bytesOmitted > 0 && sampleBytesRead > 0
        ? Math.round((bytesTotal / sampleBytesRead) * sampleLines)
        : sampleLines;
      let lines = allLines;
      if (re) {
        lines = lines.filter((line) => re!.test(line));
      }
      if (tailLines > 0) {
        output = lines.slice(-tailLines).join('\n');
      } else {
        output = re ? lines.join('\n') : content;
      }
    } catch (err) {
      // SECURITY/OBSERVABILITY (parity with the reference's #readStdoutFromFile):
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
      kind: entry.kind,
      exitCode: entry.exitCode,
      output,
      // Freeze at the terminal timestamp, discount paused time, and clamp negative
      // skew — the shared the reference-parity formula (see computeElapsedMs).
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
  getOutputTail(taskId: string, fromOffset = 0, maxBytes?: number): { output: string; offset: number; reset: boolean; bytesOmitted?: number } | null {
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
      // Bound the read to the trailing `maxBytes` of the appended region so a task
      // that burst MBs between turns never reads the whole burst into the heap just
      // to have it trimmed afterwards. The skipped prefix is reported as bytesOmitted
      // so the delta's omitted-count semantics are preserved, and offset still
      // advances to `size` so the cursor keeps up with the file.
      let readStart = start;
      let bytesOmitted = 0;
      if (maxBytes !== undefined && maxBytes > 0 && size - start > maxBytes) {
        readStart = size - maxBytes;
        bytesOmitted = readStart - start;
      }
      const fd = fs.openSync(entry.outputPath, secureReadFlags());
      let output = '';
      try {
        const length = size - readStart;
        const buf = Buffer.allocUnsafe(length);
        const bytesRead = fs.readSync(fd, buf, 0, length, readStart);
        output = buf.subarray(0, bytesRead).toString('utf-8');
      } finally {
        fs.closeSync(fd);
      }
      return maxBytes === undefined
        ? { output, offset: size, reset }
        : { output, offset: size, reset, bytesOmitted };
    } catch {
      // file may not exist yet
      return { output: '', offset: 0, reset: false };
    }
  }

  list(): BackgroundProcess[] {
    return Array.from(this.processes.values()).map(({ proc, outputFd, bytesWritten, _pausedAt, _stallNotified, _lastGrowthAt, _abortSignal, _retryFn, _skipFn, _stderrTail, _maxOutputBytes, _fileMode, _adoptPoll, _onIdleCallbacks, _lastSyncedMessagesLength, _lastReportedOutputOffset, ...rest }) => rest);
  }

  get(taskId: string): BackgroundProcess | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;
    const { proc, outputFd, bytesWritten, _pausedAt, _stallNotified, _lastGrowthAt, _abortSignal, _retryFn, _skipFn, _stderrTail, _maxOutputBytes, _fileMode, _adoptPoll, _onIdleCallbacks, _lastSyncedMessagesLength, _lastReportedOutputOffset, ...rest } = entry;
    return rest;
  }

  /**
   * FUNCTIONAL (parity): the bounded, SEPARABLE stderr tail captured for a task —
   * the bytes the child wrote to STDERR, distinct from the merged stdout+stderr
   * output file (parity with the reference surfacing stderr as its own block). Returns
   * '' when the task produced no stderr or isn't tracked. `tailLines > 0` trims to
   * the last N lines; otherwise the whole retained tail (capped at
   * {@link STDERR_TAIL_CAP}) is returned. Lets task_output label a stderr-only
   * command's diagnostics distinctly instead of losing them in the merged stream.
   */
  getStderr(taskId: string, tailLines = 0): string {
    const entry = this.processes.get(taskId);
    const tail = entry?._stderrTail;
    if (!tail) return '';
    if (tailLines > 0) return tail.split('\n').slice(-tailLines).join('\n');
    return tail;
  }

  /**
   * FUNCTIONAL (parity with the reference's LocalAgentTask summary precedence): the
   * EFFECTIVE progress summary for a tracked agent — the authoritative AI summary
   * ({@link BackgroundProcess.agentSummary}) when set, falling back to the raw
   * tool-derived {@link BackgroundProcess.progressSummary}. A raw progress tick never
   * clobbers the AI summary (they are separate fields), so this getter encodes the
   * "existing LLM summary wins" merge. Returns undefined when neither is set / the
   * task isn't tracked.
   */
  effectiveSummary(taskId: string): string | undefined {
    const entry = this.processes.get(taskId);
    if (!entry) return undefined;
    return entry.agentSummary ?? entry.progressSummary;
  }

  /**
   * FUNCTIONAL (parity with the reference's two-tier teammate abort,
   * useBackgroundTaskNavigation L147-165): SOFT-interrupt a running in-process
   * AGENT's current turn WITHOUT killing it. Invokes the agent's abort callback (the
   * same per-turn AbortController `kill()` uses) to stop the in-flight work, but —
   * unlike {@link kill} — does NOT flip the task to 'killed', close its fd, or remove
   * it: the agent entry stays alive so a persistent/mailbox agent
   * (send-message-addressable) can receive and act on a follow-up message instead of
   * being destroyed and re-spawned. Only valid for a RUNNING `agent` task (a bash
   * shell / monitor has no turn concept — returns false). Emits an 'interrupted'
   * introspection event. Returns true when the interrupt was delivered.
   */
  interruptAgentTurn(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running' || entry.kind !== 'agent') return false;
    try { entry.proc.kill(); } catch { /* abort already fired */ }
    this.traceLifecycle('interrupt', taskId, { kind: entry.kind });
    this.emit('interrupted', { taskId, description: entry.description });
    return true;
  }

  /**
   * Register the abort callback of a RUNNING FOREGROUND subagent so a frontend can
   * cancel THIS one subagent individually (parity: InProcessTeammateDetailDialog's
   * onKill for a running in-process teammate). The callback is this subagent's own
   * AbortController.abort — invoking it stops just this subagent's sub-engine, not the
   * parent turn. Called by the task tool right after it begins running a foreground
   * subagent; paired with {@link unregisterForegroundAbort} on teardown. Re-registering
   * the same id replaces the prior callback (a resumed subagent reuses its taskId).
   */
  registerForegroundAbort(taskId: string, abortFn: () => void): void {
    this._foregroundAborts.set(taskId, abortFn);
  }

  /**
   * Whether a running foreground subagent is individually abortable right now — i.e.
   * {@link abortForeground} would deliver an abort for this id. A frontend (e.g. the
   * TUI inspector) uses this to decide whether a running foreground roster row can be
   * killed in place or only dismissed once terminal.
   */
  hasForegroundAbort(taskId: string): boolean {
    return this._foregroundAborts.has(taskId);
  }

  /**
   * Abort ONE running foreground subagent by id (parity: onKill). Invokes its stored
   * abort callback and removes the registration (one-shot: a second press is a no-op).
   * Removal happens BEFORE the callback fires so a re-entrant abort during teardown
   * cannot double-invoke it. A throwing callback (the AbortController already fired) is
   * swallowed so a stop never crashes the caller. Returns true when an abort was
   * delivered, false for an unknown / already-aborted id. Records a 'kill' lifecycle
   * trace (foreground:true) for the same introspection sink background kills use.
   */
  abortForeground(taskId: string): boolean {
    const abortFn = this._foregroundAborts.get(taskId);
    if (!abortFn) return false;
    this._foregroundAborts.delete(taskId);
    this.traceLifecycle('kill', taskId, { foreground: true });
    try { abortFn(); } catch { /* best-effort — the controller may already be aborted */ }
    return true;
  }

  /**
   * Remove a foreground subagent's abort registration (its run ended, or it was
   * promoted to a tracked background task so {@link kill} now owns cancellation). No-op
   * when the id is not registered, so teardown is safe to call unconditionally.
   */
  unregisterForegroundAbort(taskId: string): void {
    this._foregroundAborts.delete(taskId);
  }

  /**
   * FUNCTIONAL (parity with the reference `retryWorkflowAgent(workflowId, agentId)` /
   * `onRetryAgent`): RETRY a single wedged workflow agent — abandon its stuck
   * attempt and re-run it — without aborting the whole workflow. Delegates to the
   * per-agent recovery callback the workflow tool registered at
   * {@link registerAgent} time ({@link InternalProcess._retryFn}). Only valid for a
   * RUNNING `agent` task that supplied a retry callback (a bash shell / monitor, a
   * terminal task, or an agent with no callback returns false).
   */
  retryAgentTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running' || entry.kind !== 'agent' || typeof entry._retryFn !== 'function') return false;
    try { entry._retryFn(); } catch { /* best-effort recovery */ }
    this.emit('agent-retry-requested', { taskId, description: entry.description });
    return true;
  }

  /**
   * FUNCTIONAL (parity with the reference `skipWorkflowAgent(workflowId, agentId)` /
   * `onSkipAgent`): SKIP a single wedged workflow agent — settle it immediately
   * (as a skip) — without aborting the whole workflow. Delegates to the per-agent
   * recovery callback the workflow tool registered at {@link registerAgent} time
   * ({@link InternalProcess._skipFn}). Only valid for a RUNNING `agent` task that
   * supplied a skip callback (returns false otherwise).
   */
  skipAgentTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running' || entry.kind !== 'agent' || typeof entry._skipFn !== 'function') return false;
    try { entry._skipFn(); } catch { /* best-effort recovery */ }
    this.emit('agent-skip-requested', { taskId, description: entry.description });
    return true;
  }

  /**
   * GATED diagnostic trace for a lifecycle transition (see {@link pmTraceHooks} /
   * {@link PmLifecyclePhase}). Thin wrapper so every call site reads intent, not the
   * hook plumbing, and a thrown sink never breaks a lifecycle transition. Shared one
   * site (No-Duplication rule). Default sink is a no-op, so a normal run traces
   * nothing until a frontend attaches a real `onLifecycle`.
   */
  private traceLifecycle(phase: PmLifecyclePhase, taskId: string, info?: Record<string, unknown>): void {
    try { pmTraceHooks.onLifecycle(phase, taskId, info); } catch { /* a diagnostic sink must never break a transition */ }
  }

  /**
   * FUNCTIONAL (parity with the reference's requestTeammateShutdown / InProcessTeammateTask
   * `shutdownRequested`): request a COOPERATIVE graceful shutdown of a running
   * in-process AGENT. Unlike {@link kill} (a hard abort of the in-flight turn) this
   * only SETS a flag; the agent's drain loop checks it between turns via
   * {@link isShutdownRequested} and stops AFTER the current turn finishes, so a graceful
   * stop never truncates mid-thought. Idempotent: a second request before the agent
   * acts is a no-op (returns false). Only valid for a RUNNING `agent` task — a bash
   * shell / monitor has no turn concept (returns false). Emits an 'idle'-adjacent
   * 'shutdown' trace. Returns true when the flag newly transitioned to set.
   */
  requestShutdown(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running' || entry.kind !== 'agent') return false;
    if (entry.shutdownRequested) return false; // already requested
    entry.shutdownRequested = true;
    this.traceLifecycle('shutdown', taskId, { kind: entry.kind });
    this.emit('shutdown-requested', { taskId, description: entry.description });
    return true;
  }

  /**
   * Whether a cooperative graceful shutdown has been requested for a task (see
   * {@link requestShutdown}). The agent drain loop polls this between turns to decide
   * whether to stop gracefully. False for an unknown task or one with no request.
   */
  isShutdownRequested(taskId: string): boolean {
    return this.processes.get(taskId)?.shutdownRequested === true;
  }

  /**
   * FUNCTIONAL (parity with the reference's InProcessTeammateTask `isIdle` +
   * `onIdleCallbacks`): toggle a running AGENT's IDLE flag. An agent that has finished
   * its current turn and is awaiting the next message marks itself idle (`true`); when
   * it resumes work it marks itself busy (`false`). Transitioning TO idle fires and
   * clears every resolver registered via {@link waitForIdle} so a leader waiting on it
   * is woken event-driven (no polling). Only valid for a RUNNING `agent` task (a bash
   * shell / monitor has no turn concept — returns false). No-op (returns false) when
   * the flag already holds the requested value. Returns true when the flag changed.
   */
  markIdle(taskId: string, idle: boolean): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running' || entry.kind !== 'agent') return false;
    if ((entry.idle ?? false) === idle) return false;
    entry.idle = idle;
    this.traceLifecycle('idle', taskId, { idle });
    if (idle) this.flushIdleWaiters(entry);
    return true;
  }

  /** Whether a tracked agent is currently idle (see {@link markIdle}). False for an
   *  unknown task or one not marked idle. */
  isIdle(taskId: string): boolean {
    return this.processes.get(taskId)?.idle === true;
  }

  /**
   * FUNCTIONAL (parity with the reference's `onIdleCallbacks` leader-wait): resolve
   * when a tracked AGENT is idle, WITHOUT polling. Resolves immediately when the task
   * is unknown, already {@link isTerminalTaskStatus terminal} (a finished agent is
   * idle forever), or currently {@link isIdle idle}; otherwise registers a one-shot
   * resolver fired by the next {@link markIdle}(true) or terminal transition
   * ({@link kill} / terminal {@link updateEntry}). The returned promise never rejects.
   */
  waitForIdle(taskId: string): Promise<void> {
    const entry = this.processes.get(taskId);
    if (!entry || isTerminalTaskStatus(entry.status) || entry.idle === true) return Promise.resolve();
    return new Promise<void>((resolve) => {
      (entry._onIdleCallbacks ??= []).push(resolve);
    });
  }

  /** Fire and clear every pending {@link waitForIdle} resolver for an entry (on an
   *  idle transition or a terminal transition). The callbacks are Promise resolvers
   *  (they never throw), so no guard is needed. Shared one site (No-Duplication rule). */
  private flushIdleWaiters(entry: InternalProcess): void {
    const cbs = entry._onIdleCallbacks;
    if (!cbs || cbs.length === 0) return;
    entry._onIdleCallbacks = [];
    for (const cb of cbs) cb();
  }

  /**
   * FUNCTIONAL (parity with the reference's `hasWorkingInProcessTeammates`,
   * teammate.ts:L205-292): whether any tracked AGENT is still WORKING — running AND
   * not {@link isIdle idle}. This is the subtle 'alive but busy' distinction vs
   * {@link runningAgentCount} / `hasActiveAgents` (which count every running agent,
   * including ones that have gone idle between turns). Used to decide whether a
   * leader / headless run must wait for teammates before shutting down.
   */
  hasWorkingAgents(): boolean {
    for (const entry of this.processes.values()) {
      if (entry.status === 'running' && entry.kind === 'agent' && entry.idle !== true) return true;
    }
    return false;
  }

  /**
   * FUNCTIONAL (parity with the reference's `waitForTeammatesToBecomeIdle`,
   * teammate.ts:L205-292): resolve once EVERY tracked agent that is currently working
   * (running AND not idle) has gone idle — or terminal — WITHOUT polling. Snapshots
   * the working set first; returns resolved immediately when none are working;
   * otherwise awaits each via {@link waitForIdle}, which itself resolves instantly for
   * any that became idle/terminal between the snapshot and registration (handling the
   * race the reference handles by re-reading isIdle inside its updater). An optional
   * `timeoutMs` caps the wait so a wedged teammate can't block shutdown forever;
   * a falsy/<=0 value waits indefinitely. Never rejects.
   */
  waitForAllAgentsIdle(timeoutMs?: number): Promise<void> {
    const working: string[] = [];
    for (const [taskId, entry] of this.processes) {
      if (entry.status === 'running' && entry.kind === 'agent' && entry.idle !== true) working.push(taskId);
    }
    if (working.length === 0) return Promise.resolve();
    const all = Promise.all(working.map((id) => this.waitForIdle(id))).then(() => undefined);
    if (!timeoutMs || timeoutMs <= 0) return all;
    return new Promise<void>((resolve) => {
      let settled = false;
      const done = (): void => { if (!settled) { settled = true; clearTimeout(timer); resolve(); } };
      const timer = setTimeout(done, timeoutMs);
      timer.unref?.();
      void all.then(done);
    });
  }

  /**
   * FUNCTIONAL (parity with the reference's InProcessBackend.isActive): composite
   * liveness for a tracked task — is it genuinely still doing work?
   *   - unknown id / non-running status  → false
   *   - registered in-process agent      → running AND its abort signal exists and
   *                                        is NOT aborted (missing signal defaults
   *                                        to INACTIVE, mirroring the reference's
   *                                        `abortController?.signal.aborted ?? true`)
   *   - bash / monitor                   → running status is liveness (no signal)
   * Catches an agent that was signalled to abort but whose status has not yet
   * flipped to killed/failed — a window where the coarse `status === 'running'`
   * check alone would wrongly report it as live.
   */
  isActive(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;
    if (entry._registered) {
      return !(entry._abortSignal?.aborted ?? true);
    }
    return true;
  }

  getOutputSize(taskId: string): number {
    const entry = this.processes.get(taskId);
    if (!entry) return 0;
    // FILE MODE: the child wrote STRAIGHT to the fd, so bytesWritten (the writeChunk
    // counter) was never advanced — read the real size from disk instead so the stall
    // watchdog's growth detection and the runaway cap still see output progress. A stat
    // failure (file deleted / path blanked by deleteRedundantOutput) falls back to the
    // counter (0), matching the pipe-mode "no output" reading.
    if (entry._fileMode && entry.outputPath) {
      try { return fs.statSync(entry.outputPath).size; } catch { return entry.bytesWritten; }
    }
    return entry.bytesWritten;
  }

  /**
   * FILE MODE runaway-output watchdog (parity with the reference's ShellCommand size
   * watchdog). In file mode the child streams straight to the fd, so the per-chunk
   * byte-cap kill in writeChunk never fires; this polled check — driven by the spawn
   * stall interval — kills a task whose ON-DISK output has reached the cap (the
   * per-spawn {@link InternalProcess._maxOutputBytes} override, else the manager-level
   * {@link maxOutputBytes}), writing the same `[output exceeded <cap> cap — task killed]`
   * diagnostic the pipe path writes. Returns true when it killed the task this tick
   * (so the caller skips the subsequent stall check), false otherwise. No-op for a
   * pipe-mode / non-running / untracked task.
   */
  private enforceFileModeOutputCap(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running' || !entry._fileMode) return false;
    const cap = entry._maxOutputBytes ?? this.maxOutputBytes;
    if (this.getOutputSize(taskId) < cap) return false;
    const label = formatByteCap(cap);
    if (entry.outputFd !== null) {
      this.safeWrite(entry.outputFd, Buffer.from(`\n[output exceeded ${label} cap — task killed]\n`));
    }
    this.kill(taskId, { reason: `output exceeded ${label} cap` });
    return true;
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

  /**
   * FUNCTIONAL (parity): count of ACTIVE tasks — running OR pending — via the
   * shared {@link isActiveTaskStatus} predicate. Unlike {@link runningCount} (which
   * counts only actively-executing tasks) this includes queued/not-yet-started
   * tasks, matching the two-state set the reference's background indicator tracks.
   */
  activeCount(): number {
    let count = 0;
    for (const entry of this.processes.values()) {
      if (isActiveTaskStatus(entry.status)) count++;
    }
    return count;
  }

  /**
   * FUNCTIONAL (parity): the tasks that belong in the background-tasks indicator —
   * ACTIVE (running|pending) AND not explicitly foregrounded — via the shared
   * {@link isBackgroundTask} predicate. Mirrors the reference's `isBackgroundTask`
   * filter so a foregrounded task (pulled into the main view) is excluded while it
   * is foregrounded, and a terminal task is excluded entirely. Returns snapshots
   * (no internal fields), like {@link list}.
   */
  listBackgroundTasks(): BackgroundProcess[] {
    return this.list().filter((t) => isBackgroundTask(t));
  }

  /** FUNCTIONAL (parity): count of tasks in the background-tasks indicator (see
   *  {@link listBackgroundTasks} / {@link isBackgroundTask}). */
  backgroundTaskCount(): number {
    let count = 0;
    for (const entry of this.processes.values()) {
      if (isBackgroundTask(entry)) count++;
    }
    return count;
  }

  /**
   * FUNCTIONAL (parity with the reference's useSessionBackgrounding): FOREGROUND a
   * single tracked task into the main view — set its per-task `isBackgrounded` flag
   * to false (excluding it from the background indicator) and record it as the
   * session's single {@link getForegroundedTaskId foregrounded task}. Any
   * previously-foregrounded task is re-backgrounded (its flag set back to true) so
   * at most one task is foregrounded at a time, mirroring the reference's single-slot
   * foregroundedTaskId. No-op (returns false) for an unknown task. Returns true on
   * success.
   */
  foregroundTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry) return false;
    if (this._foregroundedTaskId !== undefined && this._foregroundedTaskId !== taskId) {
      const prev = this.processes.get(this._foregroundedTaskId);
      if (prev) { prev.isBackgrounded = true; prev._lastSyncedMessagesLength = undefined; }
    }
    entry.isBackgrounded = false;
    // Reset the sync cursor so the first {@link syncForegroundedMessages} after a
    // (re-)foreground re-renders the whole transcript (parity: the reference resets
    // lastSyncedMessagesLengthRef to 0 whenever the foreground slot changes).
    entry._lastSyncedMessagesLength = undefined;
    this._foregroundedTaskId = taskId;
    return true;
  }

  /**
   * FUNCTIONAL (parity with the reference's Ctrl+B re-background): re-BACKGROUND a
   * task — set its per-task `isBackgrounded` flag to true so it re-enters the
   * background-tasks indicator, and clear the session's foregrounded slot when it
   * was the foregrounded task. No-op (returns false) for an unknown task. Returns
   * true on success.
   */
  backgroundTask(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry) return false;
    entry.isBackgrounded = true;
    entry._lastSyncedMessagesLength = undefined;
    if (this._foregroundedTaskId === taskId) this._foregroundedTaskId = undefined;
    return true;
  }

  /** The id of the single task currently foregrounded into the main view, or
   *  undefined when none is (parity with the reference's AppState.foregroundedTaskId). */
  getForegroundedTaskId(): string | undefined {
    return this._foregroundedTaskId;
  }

  /**
   * FUNCTIONAL (parity with the reference's BackgroundTaskState.messages append):
   * append a message to a running background AGENT's live transcript buffer (see
   * {@link BackgroundProcess.messages}). Refuses (returns false) for an unknown
   * task, a non-`agent` task (bash/monitor keep no transcript), or a
   * {@link isTerminalTaskStatus terminal} agent — a dead teammate must not accrue
   * new messages (the isTerminalTaskStatus "inject into dead teammates" guard).
   * Returns true when the message was buffered.
   */
  appendTaskMessage(taskId: string, message: TaskMessage): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.kind !== 'agent' || isTerminalTaskStatus(entry.status)) return false;
    (entry.messages ??= []).push(message);
    return true;
  }

  /** A snapshot COPY of a tracked task's live transcript buffer (see
   *  {@link appendTaskMessage}), or `[]` when the task is unknown / has no messages. */
  getTaskMessages(taskId: string): TaskMessage[] {
    const entry = this.processes.get(taskId);
    return entry?.messages ? entry.messages.slice() : [];
  }

  /** The abort signal of the currently-foregrounded agent — to adopt as the main-view
   *  abort target so Escape aborts the foregrounded task (parity with the reference's
   *  `setAbortController(foregroundedTask.abortController)`). Undefined when nothing is
   *  foregrounded, it isn't an agent, or it carries no signal. */
  foregroundedAbortSignal(): AbortSignal | undefined {
    const id = this._foregroundedTaskId;
    return id !== undefined ? this.processes.get(id)?._abortSignal : undefined;
  }

  /**
   * FUNCTIONAL (parity with the reference's useSessionBackgrounding sync effect,
   * useSessionBackgrounding.ts:L91-L127): compute what the main view must mirror for
   * the single {@link foregroundTask foregrounded} agent. Returns `null` when nothing
   * is foregrounded. Otherwise a {@link ForegroundSync} describing:
   *   - `messages` + `changed`: the agent's transcript, with `changed` true ONLY when
   *     the buffer GREW since the last sync (dedup via the per-task cursor, parity with
   *     `lastSyncedMessagesLengthRef`) so the consumer re-renders only on a real change;
   *   - `isLoading`: true while the agent is `running` (drives the main spinner);
   *   - `abortSignal`: the agent's signal to adopt as the main-view abort target;
   *   - `cleared`: true when the foreground slot was just released — the agent VANISHED,
   *     is no longer an `agent` (type !== 'local_agent' parity), was ABORTED (its signal
   *     already fired — auto re-background), or reached a TERMINAL state (restore to
   *     background). On `cleared` the consumer restores the normal view; the foreground
   *     slot is cleared here (the task is re-backgrounded) so the state stays consistent.
   */
  syncForegroundedMessages(): ForegroundSync | null {
    const taskId = this._foregroundedTaskId;
    if (taskId === undefined) return null;
    const entry = this.processes.get(taskId);
    // Vanished or no longer an agent → clear the foreground slot (parity: the reference's
    // `foregroundedTask.type !== 'local_agent'` branch resets foregroundedTaskId).
    if (!entry || entry.kind !== 'agent') {
      this._foregroundedTaskId = undefined;
      if (entry) { entry.isBackgrounded = true; entry._lastSyncedMessagesLength = undefined; }
      return { taskId, messages: [], changed: false, isLoading: false, cleared: true };
    }
    const messages = entry.messages ?? [];
    const changed = messages.length !== (entry._lastSyncedMessagesLength ?? 0);
    if (changed) entry._lastSyncedMessagesLength = messages.length;

    if (entry.status === 'running') {
      // Escape-abort detection (parity L101-121): a running task whose abort signal
      // already fired is auto re-backgrounded and the slot cleared immediately.
      if (entry._abortSignal?.aborted) {
        this.backgroundTask(taskId);
        return { taskId, messages: messages.slice(), changed, isLoading: false, cleared: true };
      }
      return {
        taskId,
        messages: messages.slice(),
        changed,
        isLoading: true,
        ...(entry._abortSignal ? { abortSignal: entry._abortSignal } : {}),
        cleared: false,
      };
    }
    // Terminal → restore to background and clear the foreground view (parity L123+).
    this.backgroundTask(taskId);
    return { taskId, messages: messages.slice(), changed, isLoading: false, cleared: true };
  }

  /**
   * FUNCTIONAL (parity with the reference's toolUseId correlation): find the tracked
   * task spawned by a given tool_use id, or null when none matches. Lets a consumer
   * map a tool call back to the background task it created (e.g. to attach the
   * completion to the originating tool_result). Returns a snapshot (no internal
   * fields), like {@link get}.
   */
  findByToolUseId(toolUseId: string): BackgroundProcess | null {
    if (!toolUseId) return null;
    for (const entry of this.processes.values()) {
      if (entry.toolUseId === toolUseId) return this.get(entry.taskId);
    }
    return null;
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
    /** Targeted per-agent recovery callbacks for a workflow agent (parity:
     *  `onRetryAgent`/`onSkipAgent`). When supplied, {@link retryAgentTask} /
     *  {@link skipAgentTask} invoke them to retry or skip this ONE agent in a
     *  still-running workflow without killing the whole run. */
    retryFn?: () => void;
    skipFn?: () => void;
    /** Id of the PARENT agent that spawned this sub-agent, so a parent's
     *  killTasksForAgent() can reap it if the parent exits first. Undefined for a
     *  top-level agent spawned from the main thread. */
    agentId?: string;
    /** The agent's AbortController signal, stored for composite liveness checks
     *  ({@link isActive}). Pass the same signal whose abort the {@link abortFn}
     *  triggers so liveness reflects an in-flight cancel. */
    abortSignal?: AbortSignal;
    /** Id of the tool_use call that spawned this agent (parity with the reference's
     *  TaskStateBase.toolUseId) — correlates the agent back to its originating tool
     *  invocation. Looked up by {@link findByToolUseId}. */
    toolUseId?: string;
    /** Register the agent in the `pending` (accepted-but-not-yet-started) state
     *  instead of `running` — e.g. when it is queued behind the concurrency cap.
     *  Transition it to running with {@link markRunning} when it actually starts.
     *  Parity with the reference's createTaskStateBase defaulting to 'pending'. */
    pending?: boolean;
    /** True when this agent is a RESUMED continuation of a prior session, not a
     *  fresh spawn (see {@link BackgroundProcess.resumed}). */
    resumed?: boolean;
    /** The active swarm team's name, when the agent joined a team at spawn (see
     *  {@link BackgroundProcess.teamName}). */
    teamName?: string;
    /** Task kind for this registered entry (default `'agent'`). The background
     *  WORKFLOW run registers itself as kind `'workflow'` — a single tracked entry
     *  for the whole run, distinct from the per-agent `'agent'` entries its
     *  {@link WorkflowRuntime} spawns — so `/flows`, the progress tree, and the
     *  introspect task-type catalog can tell a workflow run apart from a plain
     *  background subagent. */
    kind?: TaskKind;
  }): void {
    // On RESUME the taskId is reused (same agent continued), so an entry may already
    // exist from the prior run. Carry forward its ORIGINAL startTime so the agent's
    // elapsed-time clock measures its whole life rather than resetting on every
    // resume (parity: the reference's resume re-registration carries prior state —
    // framework.ts:L74-117). A fresh spawn has no prior entry → starts the clock now.
    const prior = this.processes.get(taskId);
    const startTime = opts.resumed && prior ? prior.startTime : Date.now();
    this.processes.set(taskId, {
      taskId,
      pid: process.pid,
      command: opts.command,
      description: opts.description,
      status: opts.pending ? 'pending' : 'running',
      kind: opts.kind ?? 'agent',
      exitCode: null,
      outputPath: opts.outputPath,
      startTime,
      ...(opts.agentId ? { agentId: opts.agentId } : {}),
      ...(opts.toolUseId ? { toolUseId: opts.toolUseId } : {}),
      ...(opts.abortSignal ? { _abortSignal: opts.abortSignal } : {}),
      ...(opts.retryFn ? { _retryFn: opts.retryFn } : {}),
      ...(opts.skipFn ? { _skipFn: opts.skipFn } : {}),
      ...(opts.resumed ? { resumed: true } : {}),
      ...(opts.teamName ? { teamName: opts.teamName } : {}),
      proc: { pid: process.pid, kill: opts.abortFn } as any,
      outputFd: null,
      bytesWritten: 0,
      tokenCount: 0,
      _registered: true,
    });
    this.traceLifecycle('register', taskId, { pending: !!opts.pending, ...(opts.agentId ? { agentId: opts.agentId } : {}) });
  }

  /**
   * FUNCTIONAL (parity): transition a PENDING task to `running` when it actually
   * starts (e.g. a slot freed up behind the concurrency cap). No-op (returns false)
   * for an unknown task or one not currently pending. Resets `startTime` to now so
   * the elapsed-time clock measures actual run time, not the time spent queued —
   * mirroring the reference starting the clock when a pending task begins. Returns
   * true when the task transitioned from pending to running.
   */
  markRunning(taskId: string): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'pending') return false;
    entry.status = 'running';
    entry.startTime = Date.now();
    this.traceLifecycle('start', taskId, { kind: entry.kind });
    return true;
  }

  updateEntry(taskId: string, updates: Partial<Pick<BackgroundProcess, 'status' | 'exitCode' | 'tokenCount' | 'toolCount' | 'error' | 'resultTokenCount' | 'resultToolCount' | 'progressSummary' | 'attentionState' | 'agentSummary'>>): void {
    const entry = this.processes.get(taskId);
    if (!entry) return;
    if (updates.status !== undefined) {
      entry.status = updates.status;
      // Freeze the elapsed clock when an in-process agent reaches a terminal
      // state via updateEntry (its only terminal transition — there is no 'exit'
      // event). Keep the first terminal timestamp if one was already recorded.
      if (isTerminalTaskStatus(updates.status) && entry.endTime === undefined) {
        entry.endTime = Date.now();
        this.setEvictDeadline(entry);
        // A finished agent is idle forever — wake any {@link waitForIdle} waiter so a
        // leader awaiting its idle transition (e.g. a graceful shutdown) resolves.
        this.flushIdleWaiters(entry);
      }
    }
    if (updates.exitCode !== undefined) entry.exitCode = updates.exitCode;
    if (updates.tokenCount !== undefined) entry.tokenCount = updates.tokenCount;
    // Live tool-call progress counter for a background agent (see
    // BackgroundProcess.toolCount) — surfaced by task_list and the basis for the
    // incremental deltas computed by reportProgressDelta.
    if (updates.toolCount !== undefined) entry.toolCount = updates.toolCount;
    // Dedicated failure-reason channel for a failed agent (see BackgroundProcess.error).
    if (updates.error !== undefined) entry.error = updates.error;
    // FINAL-result stats, distinct from the live progress counters (see
    // BackgroundProcess.resultTokenCount/resultToolCount) — the authoritative
    // totals recorded at the terminal transition.
    if (updates.resultTokenCount !== undefined) entry.resultTokenCount = updates.resultTokenCount;
    if (updates.resultToolCount !== undefined) entry.resultToolCount = updates.resultToolCount;
    // Live RAW progress summary (see BackgroundProcess.progressSummary) — current
    // tool activity. It is a SEPARATE field from the authoritative AI summary
    // (agentSummary) and never overwrites it, so a raw progress tick can never
    // clobber an LLM-written summary (parity: the reference's `existingSummary ?
    // {...progress, summary: existingSummary} : progress`). effectiveSummary()
    // reads agentSummary ?? progressSummary.
    if (updates.progressSummary !== undefined) entry.progressSummary = updates.progressSummary;
    // Authoritative AI summary (see BackgroundProcess.agentSummary) — takes
    // precedence over the raw progressSummary in effectiveSummary().
    if (updates.agentSummary !== undefined) entry.agentSummary = updates.agentSummary;
    // Attention sub-state (see BackgroundProcess.attentionState) — a running task
    // signalling it needs review / is blocked. Ignore an unrecognized value rather
    // than storing garbage that a renderer can't branch on.
    if (updates.attentionState !== undefined && TASK_ATTENTION_STATES.includes(updates.attentionState)) {
      entry.attentionState = updates.attentionState;
    }
    // Stamp last-activity on any LIVE progress signal (token/tool/summary/attention)
    // so a consumer can tell a still-working agent from one that has gone quiet (see
    // BackgroundProcess.lastActivity). A status/exitCode/error-only update is a
    // lifecycle transition, not progress, so it does not refresh the clock.
    if (
      updates.tokenCount !== undefined ||
      updates.toolCount !== undefined ||
      updates.progressSummary !== undefined ||
      updates.agentSummary !== undefined ||
      updates.attentionState !== undefined
    ) {
      entry.lastActivity = Date.now();
    }
  }

  /**
   * FUNCTIONAL (parity with the reference's `lastReportedToolCount` /
   * `lastReportedTokenCount`): return the CHANGE in a background agent's tool-call
   * and token counts since the previous call, latching the current totals as the
   * new baseline. Lets a consumer surface "+N tools, +M tokens since last check"
   * for a still-running agent (incremental progress notifications) without
   * re-deriving absolute totals or re-reporting unchanged work.
   *
   * The FIRST call for a task establishes the baseline and returns zero deltas
   * (`firstReport: true`) — there is nothing to compare against yet, so a consumer
   * shows no "since last check" clause on the first listing. Deltas are clamped at
   * 0 (a count can only grow; a reset/reorder never yields a negative delta).
   * Returns `undefined` for an unknown task.
   */
  reportProgressDelta(taskId: string): ProgressDelta | undefined {
    const entry = this.processes.get(taskId);
    if (!entry) return undefined;
    const curTool = entry.toolCount ?? 0;
    const curTok = entry.tokenCount ?? 0;
    const firstReport = entry._progressBaselined !== true;
    const prevTool = entry._lastReportedToolCount ?? 0;
    const prevTok = entry._lastReportedTokenCount ?? 0;
    entry._lastReportedToolCount = curTool;
    entry._lastReportedTokenCount = curTok;
    entry._progressBaselined = true;
    return {
      toolCountDelta: firstReport ? 0 : Math.max(0, curTool - prevTool),
      tokenCountDelta: firstReport ? 0 : Math.max(0, curTok - prevTok),
      firstReport,
    };
  }

  /**
   * FUNCTIONAL (parity with the reference's LocalShellTaskState `lastReportedTotalLines`
   * delta cursor): return the OUTPUT appended to a background task's output file since
   * the previous call, advancing the per-task offset cursor so the next call reports
   * only still-newer bytes. Lets a consumer (the engine turn loop) push a long-running
   * background shell's progress to the model INCREMENTALLY — just the "new output since
   * last check" — instead of re-reading/re-reporting the whole buffer or learning
   * nothing until the task completes.
   *
   * Returns `undefined` for an unknown task, a task with no output file, or when there
   * is NO new output since the last report (so a quiet task injects nothing). An
   * over-cap burst is tail-trimmed to the last `maxBytes` bytes with the dropped prefix
   * counted in {@link OutputDelta.bytesOmitted}. The underlying read uses
   * {@link getOutputTail} (O_NOFOLLOW, positioned read), so a huge log never enters the
   * heap wholesale and a rotated/truncated file is reported with `reset: true`.
   */
  reportOutputDelta(taskId: string, maxBytes: number = OUTPUT_DELTA_MAX_BYTES): OutputDelta | undefined {
    const entry = this.processes.get(taskId);
    if (!entry || !entry.outputPath) return undefined;
    const from = entry._lastReportedOutputOffset ?? 0;
    // Bound the read at the source: getOutputTail now reads at most `maxBytes` of the
    // trailing appended region (instead of the whole burst) and reports the skipped
    // prefix via bytesOmitted, so a verbose background task no longer loads MBs into
    // the heap each turn to produce a ~4 KB delta.
    const tail = this.getOutputTail(taskId, from, maxBytes);
    if (!tail) return undefined;
    // Advance the cursor even when there is nothing new, so the next report keeps
    // comparing against the latest observed size (and a rotated file re-baselines).
    entry._lastReportedOutputOffset = tail.offset;
    if (tail.output === '') return undefined;
    let output = tail.output;
    let bytesOmitted = tail.bytesOmitted ?? 0;
    // Defensive residual trim: the bounded read already caps bytes, but decode can
    // leave the re-encoded string a few bytes over the cap at a multi-byte boundary.
    const buf = Buffer.from(output, 'utf-8');
    if (buf.length > maxBytes) {
      const sliced = buf.subarray(buf.length - maxBytes);
      output = sliced.toString('utf-8');
      bytesOmitted += buf.length - Buffer.byteLength(output, 'utf-8');
    }
    return { taskId, description: entry.description, kind: entry.kind, output, reset: tail.reset, bytesOmitted };
  }

  /**
   * Collect an incremental {@link OutputDelta} for EVERY still-running background
   * shell/monitor task that produced new output since the last drain. In-process
   * agents ({@link InternalProcess._registered}) are excluded — they spool no shell
   * output; their progress is surfaced via {@link reportProgressDelta} (tokens/tools).
   * The engine calls this each turn to push running-task progress to the model (parity
   * with the reference's periodic still-running-task output attachments), complementing
   * {@link drainCompletedTasks} which only surfaces TERMINAL completions.
   */
  drainRunningOutputDeltas(maxBytes: number = OUTPUT_DELTA_MAX_BYTES): OutputDelta[] {
    const deltas: OutputDelta[] = [];
    for (const entry of this.processes.values()) {
      if (entry.status !== 'running' || entry._registered || !entry.outputPath) continue;
      const delta = this.reportOutputDelta(entry.taskId, maxBytes);
      if (delta) deltas.push(delta);
    }
    return deltas;
  }

  /** Absolute path of the crash-safe task-registry sidecar (see {@link rescanTasks}). */
  private registryPath(): string {
    return path.join(this.tasksDir, TASK_REGISTRY_FILE);
  }

  /**
   * Persist the identity of every still-RUNNING detached BASH task to the registry
   * sidecar (parity with the reference's persistRemoteAgentMetadata). Only real
   * detached children (kind 'bash', a non-zero pid) are recorded — in-process agents
   * and MCP monitors die WITH the CLI, so re-adopting them is meaningless. Atomic
   * (temp-write + rename) so a crash mid-write can't leave a torn file. Best-effort:
   * a write failure is swallowed (the registry is an optimization, not a hard
   * dependency). Re-invoked on spawn and on every terminal transition via
   * {@link syncRegistry} so the file always reflects the live running set.
   */
  private persistRegistry(): void {
    const records: PersistedTaskRecord[] = [];
    for (const entry of this.processes.values()) {
      if (entry.kind !== 'bash' || entry.status !== 'running' || !entry.pid || !entry.outputPath) continue;
      records.push({ taskId: entry.taskId, pid: entry.pid, command: entry.command, description: entry.description, outputPath: entry.outputPath, startTime: entry.startTime });
    }
    try {
      const tmp = instanceScopedTempName(this.registryPath());
      fs.writeFileSync(tmp, JSON.stringify(records), 'utf-8');
      fs.renameSync(tmp, this.registryPath());
    } catch { /* best-effort — registry is an optimization */ }
  }

  /** Refresh the registry sidecar after a running-set change. Thin wrapper so the
   *  spawn / finalize / kill / error call sites read intent, not file plumbing. */
  private syncRegistry(): void {
    this.persistRegistry();
  }

  /**
   * CRASH-SAFE RESUME / RE-ADOPTION (parity with the reference's
   * restoreRemoteAgentTasksImpl, RemoteAgentTask.tsx:L484-532): on startup/resume,
   * reconcile the persisted {@link persistRegistry registry sidecar} against reality.
   * For each persisted task NOT already tracked in-process:
   *   - pid STILL ALIVE  → RE-ADOPT it: register a running bash entry pointing at its
   *     existing output file and arm a liveness watchdog so its eventual completion is
   *     still surfaced (we have no 'exit' event for a re-attached detached pid).
   *   - pid DEAD          → the task finished/died while the CLI was down: surface a
   *     terminal completion (routed through the durable buffer so the engine reports it
   *     exactly once) with an "exit code unknown" wording — honest about the lost code —
   *     and keep its output file readable via task_output.
   * Rewrites the sidecar to the reconciled running set afterwards. `isAlive` is
   * injectable for deterministic tests. Returns how many tasks were re-adopted vs
   * reconciled-as-finished. A missing/corrupt sidecar yields {readopted:0, reaped:0}.
   */
  rescanTasks(opts: { isAlive?: (pid: number) => boolean } = {}): { readopted: number; reaped: number } {
    const isAlive = opts.isAlive ?? ((pid: number) => procHooks.isAlive(pid));
    let records: PersistedTaskRecord[];
    try {
      records = JSON.parse(fs.readFileSync(this.registryPath(), 'utf-8')) as PersistedTaskRecord[];
      if (!Array.isArray(records)) return { readopted: 0, reaped: 0 };
    } catch {
      return { readopted: 0, reaped: 0 };
    }
    let readopted = 0;
    let reaped = 0;
    for (const rec of records) {
      // PARSE-FAILURE GUARD (parity with the reference's ITermBackend split-output
      // check, ITermBackend.ts:L217-L223): the registry sidecar is UNTRUSTED input
      // on resume (corrupt / hand-edited / written by a crashed-or-old build), so a
      // record's id/handle is validated before it is propagated into tracked state.
      // An empty/whitespace taskId is unkillable and unlistable; a non-positive or
      // non-integer pid would turn adoptRunning's `process.kill(-pid, …)` into a
      // signal to the WRONG process group (pid 0 = our OWN group). Reject (skip) the
      // bad record instead of corrupting state — mirroring the reference's
      // "parsed-empty => don't trust it" guard, per-record so one torn entry does not
      // abort the whole reconcile.
      if (!rec || typeof rec.taskId !== 'string' || rec.taskId.trim() === '') continue;
      if (typeof rec.pid !== 'number' || !Number.isInteger(rec.pid) || rec.pid <= 0) continue;
      if (this.processes.has(rec.taskId)) continue; // already live in this process
      if (isAlive(rec.pid)) {
        this.adoptRunning(rec, isAlive);
        readopted++;
      } else {
        // Finished/died while we were down — we cannot know the real exit code, so
        // record a terminal entry (output file still readable) and surface a
        // completion with an explicit "exit code unknown" summary.
        const now = Date.now();
        const entry: InternalProcess = {
          taskId: rec.taskId, pid: rec.pid, command: rec.command, description: rec.description,
          status: 'completed', kind: 'bash', exitCode: null, outputPath: rec.outputPath,
          startTime: rec.startTime, endTime: now,
          proc: { pid: rec.pid, kill: () => { /* already dead */ } } as unknown as child_process.ChildProcess,
          outputFd: null, bytesWritten: 0,
        };
        this.setEvictDeadline(entry);
        this.processes.set(rec.taskId, entry);
        this.emitComplete({ taskId: rec.taskId, exitCode: null, command: rec.command, description: rec.description, outputPath: rec.outputPath, status: 'completed', summary: 'finished while the session was away (exit code unknown)' });
        reaped++;
      }
    }
    // Rewrite the sidecar to the reconciled running set (re-adopted survivors only).
    this.syncRegistry();
    return { readopted, reaped };
  }

  /**
   * Re-attach a still-running detached shell discovered by {@link rescanTasks}. There
   * is no ChildProcess handle to listen to, so we register a running bash entry whose
   * `proc` is a pid-bearing stub (kill() routes it through killTree like any bash
   * task) and arm an unref'd liveness poll that finalizes the task once the pid dies.
   */
  private adoptRunning(rec: PersistedTaskRecord, isAlive: (pid: number) => boolean): void {
    const pid = rec.pid;
    const entry: InternalProcess = {
      taskId: rec.taskId, pid, command: rec.command, description: rec.description,
      status: 'running', kind: 'bash', exitCode: null, outputPath: rec.outputPath,
      startTime: rec.startTime,
      proc: { pid, kill: () => { try { process.kill(-pid, 'SIGKILL'); } catch { try { process.kill(pid, 'SIGKILL'); } catch { /* gone */ } } } } as unknown as child_process.ChildProcess,
      outputFd: null, bytesWritten: 0,
    };
    const poll = setInterval(() => {
      if (!isAlive(pid)) this.finalizeAdopted(rec.taskId);
    }, ADOPT_POLL_INTERVAL_MS);
    poll.unref();
    entry._adoptPoll = poll;
    this.processes.set(rec.taskId, entry);
  }

  /** Finalize a re-adopted task once its pid has died. We never observed the real
   *  exit code, so record it as completed/unknown and surface a completion exactly
   *  once through the durable buffer. No-op if the task is already terminal. */
  private finalizeAdopted(taskId: string): void {
    const entry = this.processes.get(taskId);
    if (!entry || isTerminalTaskStatus(entry.status)) return;
    if (entry._adoptPoll) { clearInterval(entry._adoptPoll); entry._adoptPoll = undefined; }
    entry.status = 'completed';
    entry.endTime = Date.now();
    this.setEvictDeadline(entry);
    this.emitComplete({ taskId, exitCode: null, command: entry.command, description: entry.description, outputPath: entry.outputPath, status: 'completed', summary: 'completed (exit code unknown — re-adopted task)' });
    this.syncRegistry();
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
      kind: 'monitor',
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

  /**
   * FUNCTIONAL (parity with the reference's killShellTasksForAgent reap log): stop
   * every still-running task at session end, REASON-bearing. Previously cleanup()
   * killed each task SILENTLY — the emitted 'killed' event (consumed by the engine
   * → `background_task_killed` analytics) carried no reason, so a developer tracing
   * the lifecycle could not tell a session-end reap from an explicit user/model
   * stop or an orphan-agent reap. We now pass an explicit `'session exiting'`
   * reason, so the already-wired introspection surface records WHY each background
   * process disappeared (mirroring the reference's "killing orphaned shell task …" line).
   * The reason is scoped to shutdown — a plain task_kill still carries no reason.
   */
  cleanup(): void {
    for (const entry of this.processes.values()) {
      if (entry.status === 'running') {
        this.kill(entry.taskId, { reason: 'session exiting' });
      }
    }
    // Drop any lingering foreground-abort registrations — the parent turn's own
    // abort tears those subagents down at session end; the callbacks must not outlive
    // the manager.
    this._foregroundAborts.clear();
    this.removeAllListeners();
  }
}
