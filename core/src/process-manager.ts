import * as child_process from 'child_process';
import { projectStateDir } from './reference-journal';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';
import { FlowManager } from './flow-manager';
import { killTree } from './utils/shell';
import { isSafeSegment } from './skill-catalog';

const MAX_OUTPUT_BYTES = 5 * 1024 * 1024; // 5 MB per task

/**
 * Human-readable byte-cap label, derived from the numeric value so the
 * truncation/kill message can never drift out of sync with the limit actually
 * enforced in writeChunk. Renders MB for a megabyte-scale cap and raw bytes for a
 * small (e.g. test-tuned) cap, so `maxOutputBytes = 20` reads as `20B` not `0MB`.
 */
export function formatByteCap(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${Math.round(bytes / (1024 * 1024))}MB`;
  if (bytes >= 1024) return `${Math.round(bytes / 1024)}KB`;
  return `${bytes}B`;
}

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

/**
 * The lifecycle transition a background task/agent can emit a diagnostic trace for
 * (parity with Claude Code's InProcessBackend gated per-transition trace — it logs
 * `spawn`/`send`/`terminate`/`kill`/`isActive` under a debug gate). AMI surfaces the
 * same transition set through {@link pmTraceHooks} so a frontend can wire a single
 * lifecycle-debug sink instead of listening on the individual `killed`/`stalled`/
 * `interrupted` events piecemeal.
 *   - `spawn`     → a background shell was forked (kind 'bash'/'monitor').
 *   - `register`  → an in-process agent was registered (registerAgent).
 *   - `start`     → a pending task transitioned to running (markRunning).
 *   - `kill`      → a task was stopped (kill).
 *   - `interrupt` → a running agent's current turn was soft-interrupted.
 *   - `shutdown`  → a cooperative graceful shutdown was requested (requestShutdown).
 *   - `idle`      → an agent toggled its idle flag (markIdle).
 */
export type PmLifecyclePhase = 'spawn' | 'register' | 'start' | 'kill' | 'interrupt' | 'shutdown' | 'idle';

/**
 * GATED diagnostic trace seam for task lifecycle transitions (parity with Claude
 * Code's InProcessBackend per-transition debug trace). Default no-op — like
 * {@link pmLogHooks}, a run produces NO trace output until a frontend/test attaches
 * a real sink, so this is the "gate": wire a non-no-op `onLifecycle` to turn tracing
 * on. Each of {@link ProcessManager}'s lifecycle transitions ({@link PmLifecyclePhase})
 * routes through here with the task id plus a small bag of extra context (kind,
 * status, reason) so a developer can trace WHY/WHEN a background task changed state
 * without instrumenting every call site. Shared one sink (No-Duplication rule).
 */
export const pmTraceHooks: {
  onLifecycle: (phase: PmLifecyclePhase, taskId: string, info?: Record<string, unknown>) => void;
} = {
  onLifecycle: () => { /* default: no-op — frontends/tests may attach a trace sink */ },
};

/**
 * DI seam for the child-process spawn syscall. The synchronous spawn-failure path
 * (an EMFILE/EAGAIN-class fork failure, or bad spawn options, that makes
 * child_process.spawn THROW before any child exists — parity with Claude Code's
 * Shell.ts try/catch around spawn) routes through here so a test can make the fork
 * throw deterministically and assert the fd is closed and a terminal 'failed' task
 * is recorded, without racing a real resource-exhaustion failure. Defaults to the
 * real {@link child_process.spawn}.
 */
export const spawnHooks: {
  spawn: (command: string, args: string[], options: child_process.SpawnOptions) => child_process.ChildProcess;
} = {
  spawn: (command, args, options) => child_process.spawn(command, args, options),
};

/**
 * POSIX exit code a shell reports when a command was found but could NOT be
 * executed (permission/exec error). Parity with Claude Code's createFailedCommand,
 * which returns code 126 when the spawn itself fails. AMI records a synthetic
 * terminal task with this code when {@link spawnHooks.spawn} throws synchronously,
 * so the caller always gets a trackable taskId (and a model-facing completion)
 * instead of an exception propagating out of spawn().
 */
export const SPAWN_FAILURE_EXIT_CODE = 126;

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

/**
 * The lifecycle states of a background task/agent (parity with Claude Code's
 * TaskStatus: pending|running|completed|failed|killed).
 *   - `pending`   → accepted/registered but NOT yet started (e.g. an agent queued
 *                   behind the concurrency cap). Distinct from `running` so a
 *                   queued task is not mislabelled as actively executing.
 *   - `running`   → actively executing.
 *   - terminal    → `completed` | `failed` | `killed` (see {@link isTerminalTaskStatus}).
 */
export const TASK_STATUS = ['pending', 'running', 'completed', 'failed', 'killed'] as const;
export type TaskStatus = (typeof TASK_STATUS)[number];

/**
 * Task-KIND discriminator (parity with Claude Code's BackgroundTaskState `type`
 * union + LocalShellTask `kind`): what sort of background work a tracked task is.
 *   - `bash`    → a spawned background shell (real child process)
 *   - `monitor` → an internal poller (MCP health check) with no user-facing output
 *   - `agent`   → an in-process sub-agent registered via registerAgent (no child)
 * Distinct from {@link TaskStatus} (the lifecycle) and recorded on every entry so
 * callers (task_list pill labels, task_kill metadata, orphan reaping) can branch
 * on the kind of task WITHOUT brittle command-string heuristics (`startsWith
 * '[agent]'` / `'monitor:'`), which miscounted a shell whose command merely began
 * with those words. Mirrors Claude's getPillLabel splitting "1 shell, 1 monitor".
 */
export const TASK_KINDS = ['bash', 'monitor', 'agent'] as const;
export type TaskKind = (typeof TASK_KINDS)[number];

/**
 * Per-task ATTENTION state (parity with Claude Code's pillLabel ultraplanPhase /
 * RemoteAgentTask progress stages): an optional sub-state ORTHOGONAL to the
 * lifecycle {@link TaskStatus}, describing whether a still-running task needs the
 * user's attention.
 *   - `working`     → actively progressing, no attention needed (the default).
 *   - `plan_ready`  → produced a plan/result awaiting review (Claude's filled ◆
 *                     "ultraplan ready").
 *   - `needs_input` → blocked waiting for the user to answer (Claude's open ◇
 *                     "ultraplan needs your input" / a stalled interactive prompt).
 * Surfaced by task_list so a long-running agent can signal "come look at me" without
 * leaving the `running` lifecycle state.
 */
export const TASK_ATTENTION_STATES = ['working', 'plan_ready', 'needs_input'] as const;
export type TaskAttentionState = (typeof TASK_ATTENTION_STATES)[number];

/**
 * DI seam + pure helper for pre-spawn working-directory validation (parity with
 * Claude Code's Shell.ts realpath(cwd) recovery, L220-238). A command can delete
 * its own cwd (temp-dir cleanup) before a follow-up background spawn; passing the
 * now-missing path straight to child_process.spawn surfaces only an async ENOENT
 * 'error' event. This resolves the spawn cwd UP FRONT: the requested dir if it
 * exists, else the manager's known-good `fallback` root, else an `error` so the
 * caller short-circuits the task to 'failed' with a clear message WITHOUT forking.
 * `realpathSync` is injectable so tests can drive the deleted-cwd branch
 * deterministically without racing a real rmdir.
 */
export const cwdHooks: { realpathSync: (p: string) => string } = {
  realpathSync: (p) => fs.realpathSync(p),
};

export function resolveSpawnCwd(requested: string, fallback: string): { cwd: string; recovered: boolean; error?: string } {
  try {
    cwdHooks.realpathSync(requested);
    return { cwd: requested, recovered: false };
  } catch {
    try {
      cwdHooks.realpathSync(fallback);
      return { cwd: fallback, recovered: true };
    } catch {
      return {
        cwd: requested,
        recovered: false,
        error: `working directory "${requested}" no longer exists — start from an existing directory`,
      };
    }
  }
}

/** Terminal states — a task in one of these will never run again. */
const TERMINAL_TASK_STATUSES = new Set<TaskStatus>(['completed', 'failed', 'killed']);

/**
 * ACTIVE states — a task in one of these is in-flight and belongs in the
 * background-tasks indicator (parity with Claude Code's `isBackgroundTask`, which
 * admits BOTH `running` and `pending`). `pending` (accepted-but-not-started) is
 * active because the work is queued, not finished.
 */
const ACTIVE_TASK_STATUSES = new Set<TaskStatus>(['pending', 'running']);

/**
 * Whether a task status is terminal (completed/failed/killed). Guards against
 * injecting messages into dead teammates, evicting finished tasks, and orphan
 * cleanup (mirrors Claude Code's isTerminalTaskStatus).
 */
export function isTerminalTaskStatus(status: string | undefined): boolean {
  return !!status && TERMINAL_TASK_STATUSES.has(status as TaskStatus);
}

/**
 * Whether a task status is ACTIVE (running or pending) — the two-state "in-flight"
 * predicate Claude Code uses to decide background-indicator membership (its
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
 * Claude Code's `isBackgroundTask`): it must be ACTIVE (running|pending) AND not
 * explicitly foregrounded. A task whose `isBackgrounded` flag was set to `false`
 * via {@link ProcessManager.foregroundTask} is a FOREGROUND task pulled into the
 * main view and is excluded from the indicator until re-backgrounded; `undefined`
 * (the default — AMI tasks are background-by-construction) counts as a background
 * task. Mirrors Claude's `isBackgrounded === false` exclusion.
 */
export function isBackgroundTask(task: Pick<BackgroundProcess, 'status' | 'isBackgrounded'>): boolean {
  return isActiveTaskStatus(task.status) && task.isBackgrounded !== false;
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
 * FUNCTIONAL (parity): the SHORT human-readable outcome phrase for a terminal
 * task, branched on status + exit code, mirroring Claude Code's
 * enqueueShellNotification wording:
 *   - completed → `completed (exit code: N)`  (exit code only when known)
 *   - failed    → `failed with exit code N`   (exit code only when known)
 *   - killed    → `was stopped`               (no exit code — a deliberate stop)
 * A legacy/unknown status falls back to the completed wording. Pure + exported so
 * every consumer (the completion note below, the CompletedTaskEvent `summary`
 * field set in emitComplete, analytics, tests) shares ONE phrasing source rather
 * than re-deriving it (No-Duplication rule).
 */
export function buildCompletionSummary(
  status: TaskStatus | undefined,
  exitCode: number | null | undefined,
  kind?: TaskKind,
): string {
  const hasExit = exitCode !== null && exitCode !== undefined;
  // A monitor task is a long-lived STREAM, not a command that "succeeds" or
  // "fails" — so its termination is worded as the stream ending/stopping rather
  // than a pass/fail exit. Parity: Claude Code's monitor-kind notification wording
  // ('Monitor "..." stream ended/stopped').
  if (kind === 'monitor') {
    if (status === 'killed') return 'monitor stream stopped';
    if (status === 'failed') return hasExit ? `monitor stream ended with exit code ${exitCode}` : 'monitor stream ended';
    return 'monitor stream ended';
  }
  if (status === 'killed') return 'was stopped';
  if (status === 'failed') return hasExit ? `failed with exit code ${exitCode}` : 'failed';
  return hasExit ? `completed (exit code: ${exitCode})` : 'completed';
}

/**
 * FUNCTIONAL (parity): the `usage` clause appended to a completion note — a
 * ready-made `Usage: ran 3.2s, 1500 tokens, 4 tool uses.` fragment, mirroring
 * Claude Code's LocalAgentTask completion notification which surfaces a usage
 * section (`duration_ms`, `total_tokens`, `tool_uses`) so the model can see how
 * long a finished background task ran and how much work it did. Each metric is
 * rendered only when it is a known, meaningful value: duration when
 * {@link CompletedTaskEvent.durationMs} is a non-negative number, tokens/tool-uses
 * when their counts are present and > 0 (a bash shell reports neither, so its clause
 * stays duration-only). Returns '' when nothing is known so a legacy event's note is
 * unchanged. Pure + exported so the wording has ONE source (No-Duplication rule) and
 * is unit-testable in isolation.
 */
export function formatUsageClause(event: Pick<CompletedTaskEvent, 'durationMs' | 'tokenCount' | 'toolCount'>): string {
  const parts: string[] = [];
  if (typeof event.durationMs === 'number' && event.durationMs >= 0) {
    parts.push(`ran ${(event.durationMs / 1000).toFixed(1)}s`);
  }
  if (typeof event.tokenCount === 'number' && event.tokenCount > 0) {
    parts.push(`${event.tokenCount} ${event.tokenCount === 1 ? 'token' : 'tokens'}`);
  }
  if (typeof event.toolCount === 'number' && event.toolCount > 0) {
    parts.push(`${event.toolCount} ${event.toolCount === 1 ? 'tool use' : 'tool uses'}`);
  }
  if (parts.length === 0) return '';
  return ` Usage: ${parts.join(', ')}.`;
}

/**
 * FUNCTIONAL (parity): the status-aware model-facing note for a drained background
 * completion, mirroring Claude Code's enqueueShellNotification. Prefers the
 * pre-computed {@link CompletedTaskEvent.summary} (set in emitComplete) so the
 * wording is the single source of truth, falling back to {@link buildCompletionSummary}
 * for a legacy emitter that omitted it. Appends a {@link formatUsageClause} usage
 * section (duration/tokens) when the emitter attached them. Keeps AMI's task_output
 * guidance (the differentiator) and output-file reference.
 */
export function formatBackgroundCompletionNote(event: CompletedTaskEvent): string {
  const outputInfo = event.outputPath ? ` Output file: ${event.outputPath}.` : '';
  const summary = event.summary ?? buildCompletionSummary(event.status, event.exitCode);
  const usage = formatUsageClause(event);
  return (
    `[Background task "${event.taskId}" ${summary}.${usage} Command: ${event.description}.${outputInfo} ` +
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
 * Grace period (ms) a TERMINAL task is retained after it finishes before it is
 * eligible for automatic eviction, mirroring Claude Code's panel-grace /
 * `evictAfter` retention window (teammateViewHelpers PANEL_GRACE_MS). A finished
 * task stays in the task table and on disk for this long so the model can still
 * read its output via task_output and see it in task_list; past the deadline it is
 * reaped opportunistically ({@link ProcessManager.pruneExpired}) so a long session
 * does not accumulate stale completed rows / orphaned output files without bound.
 */
export const TERMINAL_RETENTION_MS = 5 * 60_000; // 5 minutes

/**
 * Idle threshold (ms) after which a RUNNING background agent that has produced no
 * live progress signal (token/tool/summary update — see
 * {@link BackgroundProcess.lastActivity}) is surfaced as "idle" rather than
 * actively working, mirroring Claude Code's CoordinatorAgentStatus activity
 * indicator. Below this window the agent is treated as actively making progress.
 */
export const PROGRESS_IDLE_MS = 30_000;

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

/**
 * One captured message in a background AGENT's live transcript (parity with Claude
 * Code's BackgroundTaskState.messages). A foregrounded agent's buffer is synced
 * into the main view ({@link ProcessManager.syncForegroundedMessages}) so the user
 * sees its conversation live; on a user KILL the buffer is COLLAPSED to just the
 * final message ({@link ProcessManager.kill}) to release the retained transcript
 * (parity with Claude Code's spawnInProcess on-kill truncation) while preserving
 * the last displayed line. Bash/monitor tasks keep no transcript.
 */
export interface TaskMessage {
  /** Who produced the message. */
  role: 'user' | 'assistant' | 'system' | 'tool';
  /** The message text, already rendered to a string for display/sync. */
  content: string;
  /** Optional wall-clock timestamp (ms epoch) the message was captured. */
  ts?: number;
}

/**
 * Snapshot returned by {@link ProcessManager.syncForegroundedMessages} — the state
 * the main view needs to mirror the single foregrounded agent (parity with Claude
 * Code's useSessionBackgrounding sync effect, useSessionBackgrounding.ts:L91-L127).
 */
export interface ForegroundSync {
  /** The foregrounded task id this snapshot describes. */
  taskId: string;
  /** The agent's current transcript to render in the main view (a copy). */
  messages: TaskMessage[];
  /** True ONLY when the buffer GREW since the last sync — the consumer re-renders
   *  the transcript only on a change (parity with lastSyncedMessagesLengthRef). */
  changed: boolean;
  /** Whether the main view should show its loading spinner (task still running). */
  isLoading: boolean;
  /** The agent's abort signal, to adopt as the main-view abort target so Escape
   *  aborts the foregrounded task (parity). Undefined when the agent has none. */
  abortSignal?: AbortSignal;
  /** True when the foreground slot was just CLEARED (the task finished, was aborted,
   *  is no longer an agent, or vanished) — the consumer restores the normal view. */
  cleared: boolean;
}

export interface BackgroundProcess {
  taskId: string;
  pid: number;
  command: string;
  description: string;
  status: TaskStatus;
  /**
   * The task-KIND discriminator (bash | monitor | agent) — see {@link TaskKind}.
   * Set at creation (spawn ⇒ 'bash', monitorMcp ⇒ 'monitor', registerAgent ⇒
   * 'agent') so every consumer can reliably tell a background shell from an
   * internal monitor from an in-process agent without parsing the command text.
   */
  kind: TaskKind;
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
  /**
   * Wall-clock deadline (ms epoch) after which a TERMINAL task becomes eligible for
   * automatic eviction — set to `endTime + TERMINAL_RETENTION_MS` on the first
   * terminal transition (see {@link setTerminalTimestamps}). {@link ProcessManager.pruneExpired}
   * reaps terminal entries whose deadline has passed so finished rows/output files
   * do not accumulate forever. Mirrors Claude Code's `evictAfter` retention field.
   * Undefined while the task is still running.
   */
  evictAfter?: number;
  /** Cumulative token usage for background agents (undefined for bash tasks). */
  tokenCount?: number;
  /**
   * Cumulative count of tool calls a background AGENT has made so far (undefined
   * for bash/monitor tasks, which make no tool calls). Reported incrementally via
   * {@link ProcessManager.updateEntry} from the agent's event stream and surfaced
   * by `task_list` alongside {@link tokenCount}. Mirrors Claude Code's live
   * `task.progress.toolCount` — the in-flight tool-use counter that, together with
   * {@link tokenCount}, lets a reader gauge how much work a running agent has done.
   * It is also the basis for incremental progress deltas (see
   * {@link ProcessManager.reportProgressDelta}).
   */
  toolCount?: number;
  /**
   * Dedicated error message for a FAILED task/agent (parity with Claude Code's
   * AsyncAgentDetailDialog error field). Set via {@link ProcessManager.updateEntry}
   * when a background agent ends in a 'failed' state, so a consumer (task_list,
   * agent drill-down) can surface WHY it failed in its own channel instead of the
   * reader having to scrape the output tail. Undefined for a healthy/killed task.
   */
  error?: string;
  /**
   * FINAL-result token total for a finished background AGENT, distinct from the
   * LIVE {@link tokenCount} progress counter (parity with Claude Code's
   * AsyncAgentDetailDialog separating the completed run's `result` stats from the
   * in-flight `progress` stats). The live counter tracks the last `usage_update`
   * seen while running; this is the authoritative total recorded at the terminal
   * transition, which can differ from the last live sample. A consumer reads this
   * for a terminal agent, FALLING BACK to {@link tokenCount} when a legacy producer
   * never set it. Undefined while still running / for bash tasks.
   */
  resultTokenCount?: number;
  /**
   * FINAL-result tool-call total for a finished background AGENT, the terminal
   * counterpart of the live {@link toolCount} (see {@link resultTokenCount}).
   * Recorded at the terminal transition; read for a terminal agent with a fallback
   * to the live {@link toolCount}. Undefined while running / for bash tasks.
   */
  resultToolCount?: number;
  /**
   * Live AI progress summary for a running background AGENT — a short human phrase
   * of what the agent is doing RIGHT NOW (e.g. `bash: npm test`), updated on each
   * tool invocation (parity with Claude Code's CoordinatorAgentStatus
   * `progress.summary`, which takes PRECEDENCE over the static task description).
   * A consumer surfaces this in place of/alongside the description so the reader
   * sees current activity, not just the one-line task label set at spawn. Undefined
   * before the agent's first tool call / for bash tasks.
   */
  progressSummary?: string;
  /**
   * Wall-clock time (ms epoch) of the last observed progress on a background AGENT —
   * stamped by {@link ProcessManager.updateEntry} whenever a live progress signal
   * (tokenCount / toolCount / progressSummary) arrives (parity with Claude Code's
   * CoordinatorAgentStatus `progress.lastActivity`, the basis for its
   * activity-direction indicator). A consumer compares `now - lastActivity` against
   * {@link PROGRESS_IDLE_MS} to tell a still-working agent from one that has gone
   * quiet. Undefined until the first progress signal.
   */
  lastActivity?: number;
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
  /**
   * Id of the tool_use call that spawned this task, or undefined when it was not
   * started from a tool invocation (parity with Claude Code's TaskStateBase
   * `toolUseId` / LocalShellSpawnInput.toolUseId). Recorded so a consumer can
   * correlate a background task back to the specific tool call that created it —
   * e.g. to attach the completion to the originating tool_result, or to reap a
   * task whose tool call was abandoned. Set via spawn()/registerAgent(); looked up
   * by {@link ProcessManager.findByToolUseId}.
   */
  toolUseId?: string;
  /**
   * Per-task foreground/background flag (parity with Claude Code's per-task
   * `isBackgrounded`). AMI tasks are background-by-construction, so this is
   * `undefined` (⇒ background) until a consumer FOREGROUNDS one into the main view
   * via {@link ProcessManager.foregroundTask} (sets `false`) or re-backgrounds it
   * via {@link ProcessManager.backgroundTask} (sets `true`). The
   * {@link isBackgroundTask} predicate excludes a task with `isBackgrounded ===
   * false` from the background-tasks indicator while it is foregrounded, mirroring
   * Claude's task-granular backgrounding (foregroundedTaskId + isBackgrounded).
   */
  isBackgrounded?: boolean;
  /**
   * True when this background AGENT was started by RESUMING a prior agent session
   * rather than a fresh spawn (parity with Claude Code's resumeAgent introspection
   * marker, resumeAgent.ts:L222-225 — a resumed invocation is tagged so it can be
   * distinguished from a first run). Set at {@link registerAgent} time from the
   * caller's resume target; undefined/false for a fresh spawn. Surfaced by
   * task_output ("Invocation: resumed") so a reader can tell a continuation from a
   * first run. Only meaningful for `agent` tasks.
   */
  resumed?: boolean;
  /**
   * The name of the swarm TEAM this background agent belongs to, when it was spawned
   * into an active team (parity with Claude Code's pillLabel `identity.teamName`,
   * the basis for its distinct-team count "N teams"). Set at {@link registerAgent}
   * time from the active team; undefined for a solo agent / bash / monitor task.
   * Consumed by task_list's distinct-team tally.
   */
  teamName?: string;
  /**
   * Per-task ATTENTION sub-state (see {@link TaskAttentionState}) — orthogonal to
   * the lifecycle {@link status}. Lets a still-`running` task signal that it is
   * awaiting review (`plan_ready`) or blocked on the user (`needs_input`) so
   * task_list can flag it for attention. Parity with Claude Code's ultraplanPhase /
   * RemoteAgentTask waiting-for-input badge. Undefined ⇒ `working` (no attention).
   * The stall watchdog sets `needs_input` automatically when a task blocks on an
   * interactive prompt (see {@link ProcessManager.notifyStall}); an orchestrator can
   * set `plan_ready` via {@link ProcessManager.updateEntry}.
   */
  attentionState?: TaskAttentionState;
  /**
   * Natural-language AI SUMMARY of what a background agent has accomplished so far
   * (parity with Claude Code's LocalAgentTask periodic `updateAgentSummary`, a 1-2
   * sentence LLM-written summary). Distinct from {@link progressSummary} (a raw
   * tool-derived "current activity" tick): the AI summary is authoritative and
   * takes PRECEDENCE — a raw progress tick NEVER clobbers it (Claude's
   * `existingSummary ? {...progress, summary: existingSummary} : progress` merge).
   * Read via {@link ProcessManager.effectiveSummary} (agentSummary ?? progressSummary).
   * Set via {@link ProcessManager.updateEntry}. Undefined until the first AI summary.
   */
  agentSummary?: string;
  /**
   * COOPERATIVE graceful-shutdown flag for a running in-process AGENT (parity with
   * Claude Code's InProcessTeammateTask `shutdownRequested`). Distinct from a hard
   * {@link ProcessManager.kill} (which aborts the in-flight turn immediately): the
   * agent's drain loop checks this flag BETWEEN turns via
   * {@link ProcessManager.isShutdownRequested} and stops AFTER the current turn
   * finishes, so a graceful stop never truncates mid-thought. Set by
   * {@link ProcessManager.requestShutdown}; undefined/false ⇒ keep running. Only
   * meaningful for `agent` tasks.
   */
  shutdownRequested?: boolean;
  /**
   * IDLE lifecycle flag for a running in-process AGENT (parity with Claude Code's
   * InProcessTeammateTask `isIdle`): true when the agent has finished its current
   * turn and is quiescent, waiting for the next message rather than actively
   * working. Distinct from the lifecycle {@link status} (a persistent/mailbox agent
   * stays `running` while idle). Toggled by {@link ProcessManager.markIdle}; a
   * leader can await an idle transition WITHOUT polling via
   * {@link ProcessManager.waitForIdle} (event-driven onIdleCallbacks). Undefined ⇒
   * never-yet-idle. Only meaningful for `agent` tasks.
   */
  idle?: boolean;
  /**
   * Live transcript buffer for a background AGENT (parity with Claude Code's
   * BackgroundTaskState.messages). Appended via {@link ProcessManager.appendTaskMessage}
   * while the agent runs; synced into the main view when the task is foregrounded
   * ({@link ProcessManager.syncForegroundedMessages}); COLLAPSED to just the final
   * message when the task is KILLED ({@link ProcessManager.kill}) to release memory
   * while keeping the last displayed line (a completed/failed agent keeps its full
   * transcript for the drill view). Undefined for a bash/monitor task, or an agent
   * that has produced no messages yet.
   */
  messages?: TaskMessage[];
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
  /**
   * SHORT human-readable outcome phrase (e.g. `completed (exit code: 0)`,
   * `failed with exit code 1`, `was stopped`), set by emitComplete via
   * {@link buildCompletionSummary}. Lets a consumer (completion note, analytics,
   * task-complete surfacing) read a ready-made summary without re-deriving the
   * status/exit-code phrasing. Mirrors Claude Code attaching a summary string to
   * its shell-notification events. Undefined only on a legacy direct emit.
   */
  summary?: string;
  /**
   * Wall-clock active duration (ms) the task ran before reaching this terminal
   * state — frozen pause-discounted value from {@link computeElapsedMs}, set by
   * emitComplete from the tracked entry. Surfaced in the completion note's usage
   * clause (`ran 3.2s`), mirroring Claude Code's LocalAgentTask `duration_ms`.
   * Undefined on a legacy direct emit that bypassed emitComplete.
   */
  durationMs?: number;
  /**
   * Total token usage of a finished background AGENT, surfaced in the completion
   * note's usage clause (`… 1500 tokens …`) — parity with Claude Code's LocalAgentTask
   * completion `total_tokens`. Attached by emitComplete from the tracked entry
   * ({@link BackgroundProcess.resultTokenCount}, falling back to the live
   * {@link BackgroundProcess.tokenCount}). Undefined/0 for a bash shell (no tokens)
   * or a legacy direct emit, which then omits the token part of the clause.
   */
  tokenCount?: number;
  /**
   * Total tool-call count of a finished background AGENT, surfaced in the completion
   * note's usage clause (`… 4 tool uses`) — parity with Claude Code's LocalAgentTask
   * completion `tool_uses`. Attached by emitComplete from the tracked entry
   * ({@link BackgroundProcess.resultToolCount}, falling back to the live
   * {@link BackgroundProcess.toolCount}). Undefined/0 for a bash shell or a legacy
   * emit, which then omits the tool-use part of the clause.
   */
  toolCount?: number;
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
  /**
   * Human-readable cause of the kill, when the caller supplied one (parity with
   * Claude Code's `killShellTasksForAgent: killing orphaned shell task ${taskId}
   * (agent ${agentId} exiting)` debug line). Lets a frontend/analytics trace WHY a
   * background process disappeared — e.g. `agent <id> exiting` for an orphan reap,
   * or `output exceeded disk cap`. Undefined for a plain user/model stop.
   */
  reason?: string;
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
  /**
   * The abort signal of a registered in-process agent's AbortController, stored so
   * {@link ProcessManager.isActive} can cross-check liveness against the signal
   * (not just the coarse status) — catching an agent that was signalled to abort
   * but whose status has not yet flipped to killed/failed. Only set for
   * registerAgent()-backed tasks that supplied a signal; undefined otherwise.
   */
  _abortSignal?: AbortSignal;
  /**
   * FUNCTIONAL (parity): targeted per-agent RECOVERY callbacks for a workflow
   * agent, so a SINGLE wedged agent inside a still-running workflow can be retried
   * or skipped without aborting the whole run (parity with the reference workflow
   * detail actions `onRetryAgent`/`onSkipAgent`). Set by {@link ProcessManager.registerAgent}
   * when the spawner (the workflow tool) supplies them; invoked by
   * {@link ProcessManager.retryAgentTask}/{@link ProcessManager.skipAgentTask}.
   */
  _retryFn?: () => void;
  _skipFn?: () => void;
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
  /** Last {@link BackgroundProcess.toolCount} surfaced by {@link ProcessManager.reportProgressDelta},
   *  so the next report can emit only the CHANGE since then. */
  _lastReportedToolCount?: number;
  /** Last {@link BackgroundProcess.tokenCount} surfaced by {@link ProcessManager.reportProgressDelta}. */
  _lastReportedTokenCount?: number;
  /** Latched true once {@link ProcessManager.reportProgressDelta} has established a
   *  baseline for this task, so the FIRST report returns zero deltas (nothing to
   *  compare against yet) instead of reporting the whole current count as "new". */
  _progressBaselined?: boolean;
  /** Bounded in-memory tail of bytes seen on the child's STDERR stream, so a
   *  consumer can surface stderr DISTINCTLY from the merged output file (parity with
   *  Claude Code keeping stderr separate in the command-output renderer). Capped at
   *  {@link STDERR_TAIL_CAP} bytes — stderr still ALSO flows into the merged output
   *  file unchanged (back-compat), this is the separable copy. See {@link ProcessManager.getStderr}. */
  _stderrTail?: string;
  /** Per-spawn output-byte cap override (see spawn()'s `maxOutputBytes` opt). When
   *  set it takes precedence over the manager-level {@link ProcessManager.maxOutputBytes}
   *  in writeChunk; undefined ⇒ use the manager-level cap. */
  _maxOutputBytes?: number;
  /**
   * FILE MODE (parity with Claude Code's TaskOutput `stdoutToFile`): true when this
   * task's stdout AND stderr were wired STRAIGHT to the output-file fd
   * (`stdio: ['ignore', fd, fd]`) at spawn — so neither stream enters the JS heap/
   * event loop (no per-chunk 'data' listener, no `writeChunk`). On very large/fast
   * output this keeps the firehose out of V8 (lower CPU/GC, no pipe backpressure),
   * matching Claude Code's "neither enters JS" path. The trade-offs mirror Claude's:
   *   - progress/size is derived by POLLING the file ({@link getOutputSize} stats the
   *     file in this mode instead of reading {@link bytesWritten}, which stays 0);
   *   - the runaway byte-cap is enforced by the poll ({@link enforceFileModeOutputCap})
   *     rather than per-chunk in writeChunk;
   *   - stderr is INTERLEAVED in the merged file, so the separable in-memory stderr
   *     tail ({@link _stderrTail}) is NOT captured and {@link getStderr} returns ''
   *     (parity with Claude's `getStderr() returns '' in file mode`).
   * Undefined/false ⇒ the default PIPE mode (stream through JS; keeps the separable
   * stderr tail). Opt-in per spawn via spawn()'s `fileMode` option.
   */
  _fileMode?: boolean;
  /** Liveness-poll handle for a task RE-ADOPTED across a restart (see
   *  {@link ProcessManager.rescanTasks}). We have no ChildProcess 'exit' event for a
   *  re-attached detached pid, so a polled watchdog finalizes it when the pid dies.
   *  Distinct from {@link _intervalHandle} (an MCP monitor's own timer) so kill()
   *  still routes an adopted BASH task through killTree, not the monitor branch. */
  _adoptPoll?: ReturnType<typeof setInterval>;
  /** Pending resolvers registered by {@link ProcessManager.waitForIdle} — fired and
   *  cleared the next time the agent transitions to idle ({@link ProcessManager.markIdle}
   *  true) or reaches a terminal state, so a leader can wait event-driven (no poll)
   *  for a teammate to go idle (parity with Claude Code's `onIdleCallbacks`). */
  _onIdleCallbacks?: Array<() => void>;
  /** Cursor for {@link ProcessManager.syncForegroundedMessages}: the messages-buffer
   *  length at the last sync, so a re-sync re-renders ONLY when the buffer grew
   *  (parity with Claude Code's lastSyncedMessagesLengthRef). Reset when the task is
   *  (re-)foregrounded / backgrounded / killed. */
  _lastSyncedMessagesLength?: number;
}

/** Cap on the retained per-task stderr tail ({@link InternalProcess._stderrTail}). */
export const STDERR_TAIL_CAP = 64 * 1024; // 64 KB

/** Filename of the crash-safe task-registry sidecar written under the (session-
 *  scoped) tasks dir — see {@link ProcessManager.rescanTasks}. */
export const TASK_REGISTRY_FILE = '.task-registry.json';

/** Poll cadence (ms) for a RE-ADOPTED detached task's liveness watchdog — see
 *  {@link ProcessManager.rescanTasks}. unref'd so it never pins the event loop. */
export const ADOPT_POLL_INTERVAL_MS = 2_000;

/**
 * One persisted background-task record in the crash-safe registry sidecar (parity
 * with Claude Code's persistRemoteAgentMetadata, RemoteAgentTask.tsx:L442-454). Only
 * the identity needed to RE-ADOPT a still-running detached shell after a CLI restart
 * is stored — never the (potentially huge) live output, which already lives in the
 * on-disk output file this record points at. Written on spawn + every terminal
 * transition, read by {@link ProcessManager.rescanTasks} on startup/resume.
 */
export interface PersistedTaskRecord {
  taskId: string;
  pid: number;
  command: string;
  description: string;
  outputPath: string;
  startTime: number;
}

/**
 * Incremental progress delta for a background agent, returned by
 * {@link ProcessManager.reportProgressDelta}. Reports the CHANGE in tool-call and
 * token counts since the previous report, so a consumer (e.g. `task_list`) can show
 * "+N tools, +M tokens since last check" without re-deriving absolute totals.
 * Mirrors Claude Code's `lastReportedToolCount` / `lastReportedTokenCount` delta
 * caching used "for computing deltas in notifications".
 */
export interface ProgressDelta {
  /** Tool calls made since the previous report (0 on the first/baseline report, never negative). */
  toolCountDelta: number;
  /** Tokens consumed since the previous report (0 on the first/baseline report, never negative). */
  tokenCountDelta: number;
  /** True on the first report for a task — a baseline was just latched and the deltas are 0. */
  firstReport: boolean;
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
   * The single task currently FOREGROUNDED into the main view, or undefined when
   * none is (parity with Claude Code's AppState.foregroundedTaskId). Set by
   * {@link foregroundTask}; cleared by {@link backgroundTask} when it re-backgrounds
   * the foregrounded task. At most one task is foregrounded at a time, mirroring
   * Claude's single-slot foreground.
   */
  private _foregroundedTaskId: string | undefined;

  /**
   * Abort callbacks for RUNNING FOREGROUND in-process subagents, keyed by taskId
   * (parity with Claude Code's InProcessTeammateDetailDialog 'x' → onKill, which can
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
   * Claude Code's background-shell limit). Defaults to MAX_BACKGROUND_PROCESSES;
   * exposed as a mutable field so callers/tests can tune the ceiling without
   * touching the module constant. spawn() enforces it; see atCapacity().
   */
  maxBackgroundProcesses: number = MAX_BACKGROUND_PROCESSES;

  /**
   * Per-task output-size cap in bytes (parity with Claude Code's
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
   * deleted mid-session (see {@link resolveSpawnCwd}), mirroring Claude Code's
   * getOriginalCwd() recovery target.
   */
  private readonly rootCwd: string;

  /**
   * The session id this manager's task-output directory is SCOPED to, resolved
   * ONCE at construction and never re-read (parity with Claude Code's diskOutput
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
   * throw. Mirrors Claude Code refusing to launch beyond its background limit.
   */
  atCapacity(): boolean {
    return this.runningCount() >= this.maxBackgroundProcesses;
  }

  spawn(command: string, opts: { cwd: string; description?: string; env?: NodeJS.ProcessEnv; timeoutMs?: number; agentId?: string; toolUseId?: string; kind?: Extract<TaskKind, 'bash' | 'monitor'>; signal?: AbortSignal; maxOutputBytes?: number; fileMode?: boolean }): string {
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
    const description = opts.description || command.slice(0, 80);

    // ABORT-BEFORE-SPAWN short-circuit (parity with Claude Code's Shell.ts
    // `if (abortSignal.aborted) return createAbortedCommand()`, L241-243): a caller
    // that cancels the spawn request before we fork should pay NO fork/openSync
    // cost. Record a terminal 'killed' task (no child, no output file) so the id is
    // still trackable/reportable, then return without spawning.
    if (opts.signal?.aborted) {
      return this.createTerminalStub(taskId, { command, description, status: 'killed', exitCode: null, kind: opts.kind, agentId: opts.agentId, toolUseId: opts.toolUseId });
    }

    // CWD existence validation + recovery (parity with Claude Code's Shell.ts
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

    // SYNCHRONOUS spawn-failure handling (parity with Claude Code's Shell.ts
    // try/catch around spawn, L315-441). A fork can fail synchronously — EMFILE/
    // EAGAIN under load, or bad spawn options — making child_process.spawn THROW
    // before any child exists. Previously that exception propagated out of spawn()
    // to the caller (bash.ts does not wrap this call), with NO tracked task and the
    // just-opened output fd leaked. We now: close the fd (no leak), write a
    // diagnostic into the output file, and record a terminal 'failed' task with the
    // standard exec-error exit code (126) so the caller ALWAYS gets a trackable
    // taskId and a model-facing completion — mirroring Claude's createFailedCommand.
    // FILE MODE (parity with Claude Code's TaskOutput stdoutToFile, L21-36): when the
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
      // than a command succeeding/failing. Parity: Claude Code's BashTaskKind
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
      // Per-spawn output cap override (parity with Claude Code's ShellCommand
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
        // Runaway-output watchdog (parity with Claude Code's ShellCommand size
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
      // Claude Code's command-output renderer keeping stderr its own block). See
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
      // reaches the cap, mirroring Claude Code's ShellCommand file-size watchdog. If it
      // was killed this tick, skip the stall check (the entry is no longer running).
      if (fileMode && this.enforceFileModeOutputCap(taskId)) return;
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

    // Cooperative cancellation via an AbortSignal (parity with Claude Code's
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
    // restart can re-adopt it (parity: Claude Code's persistRemoteAgentMetadata at
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
    // Dedup (parity with Claude Code's stopTask `notified` flag): a task the model
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
    // note's usage clause can report how long the task ran — parity with Claude
    // Code's completion notification `duration_ms`. Frozen, pause-discounted value
    // via the shared computeElapsedMs formula; omitted for an untracked task or one
    // whose endTime isn't set yet (defensive — emitComplete always runs after the
    // terminal transition froze endTime).
    if (entry && payload.durationMs === undefined && entry.endTime !== undefined) {
      payload.durationMs = computeElapsedMs(entry.startTime, entry.endTime, entry.totalPausedMs ?? 0);
    }
    // Attach the agent's final token / tool-use totals so the completion note's usage
    // clause can report them (parity with Claude Code's LocalAgentTask completion
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
    // Bounded buffer (parity with Claude Code's sdkEventQueue): evict the oldest
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
   * model-facing completion note at all. Mirrors Claude Code's LocalAgentTask
   * completion notification.
   */
  emitCompletion(payload: CompletedTaskEvent): void {
    this.emitComplete(payload);
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
  kill(taskId: string, opts: { suppressCompletion?: boolean; reason?: string } = {}): boolean {
    const entry = this.processes.get(taskId);
    if (!entry || entry.status !== 'running') return false;

    entry.status = 'killed';
    // MEMORY RELEASE (parity with Claude Code's spawnInProcess on-kill truncation,
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

      // Robust process-TREE kill (parity with Claude Code's ShellCommand #doKill →
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

    // INTROSPECTION (parity with Claude Code's `LocalShellTask ${taskId} kill
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
    this.syncRegistry();

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
   * FUNCTIONAL (parity with Claude Code's TeamsDialog 'p' prune-idle handler,
   * TeamsDialog.tsx:L207-217): bulk-stop every tracked AGENT task that is currently
   * {@link isIdle idle} (running, finished its turn, awaiting the next message), in
   * one operation. Mirrors {@link killAllAgents} but filtered to the IDLE subset, so
   * a leader can reclaim concurrency slots held by teammates that have gone quiet
   * WITHOUT tearing down agents still doing work (a busy teammate — running and not
   * idle — is left alone). Background bash shells and MCP monitors are never targeted
   * (type filter, like {@link killAllAgents}). Returns the snapshots of the agents it
   * stopped (empty when none were idle), so a caller can reconcile its own selection
   * state after the removals (parity: Claude recomputing the selection index).
   */
  pruneIdleAgents(): BackgroundProcess[] {
    const stopped: BackgroundProcess[] = [];
    for (const taskId of Array.from(this.processes.keys())) {
      const entry = this.processes.get(taskId);
      if (!entry || entry.status !== 'running' || entry.idle !== true) continue;
      if (this.taskType(taskId) !== 'agent') continue;
      if (this.kill(taskId)) {
        const snapshot = this.get(taskId);
        if (snapshot) stopped.push(snapshot);
      }
    }
    return stopped;
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
   * the memory-release teardown mirroring Claude Code's spawnInProcess on-kill
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
    // A monitor task is EXPECTED to idle between bursts (a log follower waiting for
    // the next line, a server between requests), so the no-growth stall watchdog is
    // disabled for it — otherwise a healthy monitor would be mis-flagged as blocked
    // on an interactive prompt and paused. Parity: Claude Code's startStallWatchdog
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
    // A task blocked on an interactive prompt NEEDS the user's attention — flag it
    // so task_list can surface a "needs input" badge (parity with Claude Code's
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
   * "1 shell, 1 monitor" exactly like Claude Code's getPillLabel.
   */
  taskKind(taskId: string): TaskKind | undefined {
    return this.processes.get(taskId)?.kind;
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
   * outputFileRedundant optimization (parity with Claude Code's TaskOutput
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
   * FUNCTIONAL (parity with Claude Code's `evictAfter` retention): reap every
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
      // Reason-bearing kill (parity with Claude Code's killShellTasksForAgent debug
      // line) so the emitted 'killed' event explains WHY the orphan shell vanished.
      if (this.kill(taskId, { reason: `agent ${agentId} exiting` })) killed++;
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
    return Array.from(this.processes.values()).map(({ proc, outputFd, bytesWritten, _pausedAt, _stallNotified, _lastGrowthAt, _abortSignal, _retryFn, _skipFn, _stderrTail, _maxOutputBytes, _fileMode, _adoptPoll, _onIdleCallbacks, _lastSyncedMessagesLength, ...rest }) => rest);
  }

  get(taskId: string): BackgroundProcess | null {
    const entry = this.processes.get(taskId);
    if (!entry) return null;
    const { proc, outputFd, bytesWritten, _pausedAt, _stallNotified, _lastGrowthAt, _abortSignal, _retryFn, _skipFn, _stderrTail, _maxOutputBytes, _fileMode, _adoptPoll, _onIdleCallbacks, _lastSyncedMessagesLength, ...rest } = entry;
    return rest;
  }

  /**
   * FUNCTIONAL (parity): the bounded, SEPARABLE stderr tail captured for a task —
   * the bytes the child wrote to STDERR, distinct from the merged stdout+stderr
   * output file (parity with Claude Code surfacing stderr as its own block). Returns
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
   * FUNCTIONAL (parity with Claude Code's LocalAgentTask summary precedence): the
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
   * FUNCTIONAL (parity with Claude Code's two-tier teammate abort,
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
   * FUNCTIONAL (parity with Claude Code's requestTeammateShutdown / InProcessTeammateTask
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
   * FUNCTIONAL (parity with Claude Code's InProcessTeammateTask `isIdle` +
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
   * FUNCTIONAL (parity with Claude Code's `onIdleCallbacks` leader-wait): resolve
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
   * FUNCTIONAL (parity with Claude Code's `hasWorkingInProcessTeammates`,
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
   * FUNCTIONAL (parity with Claude Code's `waitForTeammatesToBecomeIdle`,
   * teammate.ts:L205-292): resolve once EVERY tracked agent that is currently working
   * (running AND not idle) has gone idle — or terminal — WITHOUT polling. Snapshots
   * the working set first; returns resolved immediately when none are working;
   * otherwise awaits each via {@link waitForIdle}, which itself resolves instantly for
   * any that became idle/terminal between the snapshot and registration (handling the
   * race Claude handles by re-reading isIdle inside its updater). An optional
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
   * FUNCTIONAL (parity with Claude Code's InProcessBackend.isActive): composite
   * liveness for a tracked task — is it genuinely still doing work?
   *   - unknown id / non-running status  → false
   *   - registered in-process agent      → running AND its abort signal exists and
   *                                        is NOT aborted (missing signal defaults
   *                                        to INACTIVE, mirroring Claude's
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
   * FILE MODE runaway-output watchdog (parity with Claude Code's ShellCommand size
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
   * tasks, matching the two-state set Claude Code's background indicator tracks.
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
   * {@link isBackgroundTask} predicate. Mirrors Claude Code's `isBackgroundTask`
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
   * FUNCTIONAL (parity with Claude Code's useSessionBackgrounding): FOREGROUND a
   * single tracked task into the main view — set its per-task `isBackgrounded` flag
   * to false (excluding it from the background indicator) and record it as the
   * session's single {@link getForegroundedTaskId foregrounded task}. Any
   * previously-foregrounded task is re-backgrounded (its flag set back to true) so
   * at most one task is foregrounded at a time, mirroring Claude's single-slot
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
    // (re-)foreground re-renders the whole transcript (parity: Claude resets
    // lastSyncedMessagesLengthRef to 0 whenever the foreground slot changes).
    entry._lastSyncedMessagesLength = undefined;
    this._foregroundedTaskId = taskId;
    return true;
  }

  /**
   * FUNCTIONAL (parity with Claude Code's Ctrl+B re-background): re-BACKGROUND a
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
   *  undefined when none is (parity with Claude Code's AppState.foregroundedTaskId). */
  getForegroundedTaskId(): string | undefined {
    return this._foregroundedTaskId;
  }

  /**
   * FUNCTIONAL (parity with Claude Code's BackgroundTaskState.messages append):
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
   *  abort target so Escape aborts the foregrounded task (parity with Claude Code's
   *  `setAbortController(foregroundedTask.abortController)`). Undefined when nothing is
   *  foregrounded, it isn't an agent, or it carries no signal. */
  foregroundedAbortSignal(): AbortSignal | undefined {
    const id = this._foregroundedTaskId;
    return id !== undefined ? this.processes.get(id)?._abortSignal : undefined;
  }

  /**
   * FUNCTIONAL (parity with Claude Code's useSessionBackgrounding sync effect,
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
    // Vanished or no longer an agent → clear the foreground slot (parity: Claude's
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
   * FUNCTIONAL (parity with Claude Code's toolUseId correlation): find the tracked
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
    /** Id of the tool_use call that spawned this agent (parity with Claude Code's
     *  TaskStateBase.toolUseId) — correlates the agent back to its originating tool
     *  invocation. Looked up by {@link findByToolUseId}. */
    toolUseId?: string;
    /** Register the agent in the `pending` (accepted-but-not-yet-started) state
     *  instead of `running` — e.g. when it is queued behind the concurrency cap.
     *  Transition it to running with {@link markRunning} when it actually starts.
     *  Parity with Claude Code's createTaskStateBase defaulting to 'pending'. */
    pending?: boolean;
    /** True when this agent is a RESUMED continuation of a prior session, not a
     *  fresh spawn (see {@link BackgroundProcess.resumed}). */
    resumed?: boolean;
    /** The active swarm team's name, when the agent joined a team at spawn (see
     *  {@link BackgroundProcess.teamName}). */
    teamName?: string;
  }): void {
    // On RESUME the taskId is reused (same agent continued), so an entry may already
    // exist from the prior run. Carry forward its ORIGINAL startTime so the agent's
    // elapsed-time clock measures its whole life rather than resetting on every
    // resume (parity: Claude Code's resume re-registration carries prior state —
    // framework.ts:L74-117). A fresh spawn has no prior entry → starts the clock now.
    const prior = this.processes.get(taskId);
    const startTime = opts.resumed && prior ? prior.startTime : Date.now();
    this.processes.set(taskId, {
      taskId,
      pid: process.pid,
      command: opts.command,
      description: opts.description,
      status: opts.pending ? 'pending' : 'running',
      kind: 'agent',
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
   * mirroring Claude Code starting the clock when a pending task begins. Returns
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
    // clobber an LLM-written summary (parity: Claude Code's `existingSummary ?
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
   * FUNCTIONAL (parity with Claude Code's `lastReportedToolCount` /
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

  /** Absolute path of the crash-safe task-registry sidecar (see {@link rescanTasks}). */
  private registryPath(): string {
    return path.join(this.tasksDir, TASK_REGISTRY_FILE);
  }

  /**
   * Persist the identity of every still-RUNNING detached BASH task to the registry
   * sidecar (parity with Claude Code's persistRemoteAgentMetadata). Only real
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
      const tmp = `${this.registryPath()}.${process.pid}.tmp`;
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
   * CRASH-SAFE RESUME / RE-ADOPTION (parity with Claude Code's
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
      if (!rec || typeof rec.taskId !== 'string' || typeof rec.pid !== 'number') continue;
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
   * FUNCTIONAL (parity with Claude Code's killShellTasksForAgent reap log): stop
   * every still-running task at session end, REASON-bearing. Previously cleanup()
   * killed each task SILENTLY — the emitted 'killed' event (consumed by the engine
   * → `background_task_killed` analytics) carried no reason, so a developer tracing
   * the lifecycle could not tell a session-end reap from an explicit user/model
   * stop or an orphan-agent reap. We now pass an explicit `'session exiting'`
   * reason, so the already-wired introspection surface records WHY each background
   * process disappeared (mirroring Claude's "killing orphaned shell task …" line).
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
