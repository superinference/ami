// Split from process-manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as crypto from 'crypto';

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
 * SECURITY (parity with the reference's generateTaskId): build a task id with enough
 * entropy to resist a brute-force symlink attack on the derived output-file path.
 * the reference draws randomBytes(8); AMI previously used randomBytes(4) (2^32 ≈ 4.3B —
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
 * Bounded cap on the durable completed-task buffer (parity with the reference's
 * sdkEventQueue `MAX_QUEUE_SIZE`). The engine drains the buffer every turn, so in
 * normal operation it stays tiny; but a very long silent turn (one in-flight API
 * call with no drain between pushes) while many background producers finish could
 * otherwise grow it without bound. At the cap we evict the OLDEST entry (FIFO
 * drop-oldest) before pushing the newest, bounding memory the same way the reference's
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
 * (parity with the reference's InProcessBackend gated per-transition trace — it logs
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
export const PM_LIFECYCLE_PHASES = ['spawn', 'register', 'start', 'kill', 'interrupt', 'shutdown', 'idle'] as const;

export type PmLifecyclePhase = (typeof PM_LIFECYCLE_PHASES)[number];

/**
 * GATED diagnostic trace seam for task lifecycle transitions (parity with the reference
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
 * child_process.spawn THROW before any child exists — parity with the reference's
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
 * executed (permission/exec error). Parity with the reference's createFailedCommand,
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

export const INFINITE_LOOP_PATTERNS = [
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
 * The lifecycle states of a background task/agent (parity with the reference's
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
 * Task-KIND discriminator (parity with the reference's BackgroundTaskState `type`
 * union + LocalShellTask `kind`): what sort of background work a tracked task is.
 *   - `bash`    → a spawned background shell (real child process)
 *   - `monitor` → an internal poller (MCP health check) with no user-facing output
 *   - `agent`   → an in-process sub-agent registered via registerAgent (no child)
 * Distinct from {@link TaskStatus} (the lifecycle) and recorded on every entry so
 * callers (task_list pill labels, task_kill metadata, orphan reaping) can branch
 * on the kind of task WITHOUT brittle command-string heuristics (`startsWith
 * '[agent]'` / `'monitor:'`), which miscounted a shell whose command merely began
 * with those words. Mirrors the reference's getPillLabel splitting "1 shell, 1 monitor".
 */
export const TASK_KINDS = ['bash', 'monitor', 'agent', 'workflow'] as const;

export type TaskKind = (typeof TASK_KINDS)[number];

/**
 * Per-task ATTENTION state (parity with the reference's pillLabel ultraplanPhase /
 * RemoteAgentTask progress stages): an optional sub-state ORTHOGONAL to the
 * lifecycle {@link TaskStatus}, describing whether a still-running task needs the
 * user's attention.
 *   - `working`     → actively progressing, no attention needed (the default).
 *   - `plan_ready`  → produced a plan/result awaiting review (the reference's filled ◆
 *                     "ultraplan ready").
 *   - `needs_input` → blocked waiting for the user to answer (the reference's open ◇
 *                     "ultraplan needs your input" / a stalled interactive prompt).
 * Surfaced by task_list so a long-running agent can signal "come look at me" without
 * leaving the `running` lifecycle state.
 */
export const TASK_ATTENTION_STATES = ['working', 'plan_ready', 'needs_input'] as const;

export type TaskAttentionState = (typeof TASK_ATTENTION_STATES)[number];

/**
 * DI seam + pure helper for pre-spawn working-directory validation (parity with
 * the reference's Shell.ts realpath(cwd) recovery, L220-238). A command can delete
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

/**
 * POSIX convention for a process killed by a signal: exit status 128 + signal
 * number. Lets callers tell a SIGTERM timeout (143) from a SIGKILL (137) from a
 * still-undecided task (null), instead of collapsing every signal death to null.
 * Mirrors the reference's #exitHandler synthesising a code from the signal.
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
 * Mirrors the reference's sessionRunner signal-aware close handler. Self-initiated
 * kills are already flagged 'killed' by kill() before the exit event fires.
 */
export function classifyExitStatus(code: number | null, signal: NodeJS.Signals | null): TaskStatus {
  if (signal === 'SIGTERM' || signal === 'SIGINT') return 'killed';
  return code === 0 ? 'completed' : 'failed';
}

/**
 * FUNCTIONAL (parity): build the single aggregate "stopped by the user"
 * model-facing notification shown after the user kills background agents,
 * mirroring the reference's useCancelRequest.killAllAgentsAndNotify (which
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
 * task, branched on status + exit code, mirroring the reference's
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
  // than a pass/fail exit. Parity: the reference's monitor-kind notification wording
  // ('Monitor "..." stream ended/stopped').
  if (kind === 'monitor') {
    if (status === 'killed') return 'monitor stream stopped';
    if (status === 'failed') return hasExit ? `monitor stream ended with exit code ${exitCode}` : 'monitor stream ended';
    return 'monitor stream ended';
  }
  // A workflow run is a multi-agent ORCHESTRATION, not a single command — word its
  // termination as the run finishing/failing/stopping (parity with the monitor-kind
  // special-casing above) so the completion note reads naturally for a /flows run.
  if (kind === 'workflow') {
    if (status === 'killed') return 'workflow run stopped';
    if (status === 'failed') return hasExit ? `workflow run failed (exit code ${exitCode})` : 'workflow run failed';
    return 'workflow run completed';
  }
  if (status === 'killed') return 'was stopped';
  if (status === 'failed') return hasExit ? `failed with exit code ${exitCode}` : 'failed';
  return hasExit ? `completed (exit code: ${exitCode})` : 'completed';
}

/**
 * Compute a background task's elapsed wall-clock duration, mirroring the reference
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
 * Idle threshold (ms) after which a RUNNING background agent that has produced no
 * live progress signal (token/tool/summary update — see
 * {@link BackgroundProcess.lastActivity}) is surfaced as "idle" rather than
 * actively working, mirroring the reference's CoordinatorAgentStatus activity
 * indicator. Below this window the agent is treated as actively making progress.
 */
export const PROGRESS_IDLE_MS = 30_000;

/**
 * Stall-watchdog tuning knobs (parity with the reference's LocalShellTask
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
