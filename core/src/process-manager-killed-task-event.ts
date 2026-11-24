// Split from process-manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as child_process from 'child_process';
import { buildCompletionSummary, formatByteCap } from './process-manager-build-completion-summary';
import type { TaskKind } from './process-manager-build-completion-summary';
import { formatUsageClause } from './process-manager-background-process';
import type { BackgroundProcess, CompletedTaskEvent } from './process-manager-background-process';

/**
 * FUNCTIONAL (parity): the status-aware model-facing note for a drained background
 * completion, mirroring the reference's enqueueShellNotification. Prefers the
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
 * Payload for the 'killed' introspection event emitted by {@link ProcessManager.kill}.
 * Lets a frontend/engine trace WHY a background process ended (parity with the reference
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
   * the reference's `killShellTasksForAgent: killing orphaned shell task ${taskId}
   * (agent ${agentId} exiting)` debug line). Lets a frontend/analytics trace WHY a
   * background process disappeared — e.g. `agent <id> exiting` for an orphan reap,
   * or `output exceeded disk cap`. Undefined for a plain user/model stop.
   */
  reason?: string;
}

export interface InternalProcess extends BackgroundProcess {
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
  /**
   * Byte offset into the task's output file that was last surfaced to the model by
   * {@link ProcessManager.reportOutputDelta} (parity with the reference's
   * LocalShellTaskState `lastReportedTotalLines` delta cursor). The next report reads
   * only the bytes appended past this offset via {@link ProcessManager.getOutputTail},
   * so a long-running background shell's progress is pushed to the model INCREMENTALLY
   * — only the NEW output since the last report — rather than re-reading/re-reporting
   * the whole buffer. Undefined ⇒ nothing reported yet (report from offset 0). */
  _lastReportedOutputOffset?: number;
  /** Bounded in-memory tail of bytes seen on the child's STDERR stream, so a
   *  consumer can surface stderr DISTINCTLY from the merged output file (parity with
   *  the reference keeping stderr separate in the command-output renderer). Capped at
   *  {@link STDERR_TAIL_CAP} bytes — stderr still ALSO flows into the merged output
   *  file unchanged (back-compat), this is the separable copy. See {@link ProcessManager.getStderr}. */
  _stderrTail?: string;
  /** Per-spawn output-byte cap override (see spawn()'s `maxOutputBytes` opt). When
   *  set it takes precedence over the manager-level {@link ProcessManager.maxOutputBytes}
   *  in writeChunk; undefined ⇒ use the manager-level cap. */
  _maxOutputBytes?: number;
  /**
   * FILE MODE (parity with the reference's TaskOutput `stdoutToFile`): true when this
   * task's stdout AND stderr were wired STRAIGHT to the output-file fd
   * (`stdio: ['ignore', fd, fd]`) at spawn — so neither stream enters the JS heap/
   * event loop (no per-chunk 'data' listener, no `writeChunk`). On very large/fast
   * output this keeps the firehose out of V8 (lower CPU/GC, no pipe backpressure),
   * matching the reference's "neither enters JS" path. The trade-offs mirror the reference's:
   *   - progress/size is derived by POLLING the file ({@link getOutputSize} stats the
   *     file in this mode instead of reading {@link bytesWritten}, which stays 0);
   *   - the runaway byte-cap is enforced by the poll ({@link enforceFileModeOutputCap})
   *     rather than per-chunk in writeChunk;
   *   - stderr is INTERLEAVED in the merged file, so the separable in-memory stderr
   *     tail ({@link _stderrTail}) is NOT captured and {@link getStderr} returns ''
   *     (parity with the reference's `getStderr() returns '' in file mode`).
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
   *  for a teammate to go idle (parity with the reference's `onIdleCallbacks`). */
  _onIdleCallbacks?: Array<() => void>;
  /** Cursor for {@link ProcessManager.syncForegroundedMessages}: the messages-buffer
   *  length at the last sync, so a re-sync re-renders ONLY when the buffer grew
   *  (parity with the reference's lastSyncedMessagesLengthRef). Reset when the task is
   *  (re-)foregrounded / backgrounded / killed. */
  _lastSyncedMessagesLength?: number;
}

/** Filename of the crash-safe task-registry sidecar written under the (session-
 *  scoped) tasks dir — see {@link ProcessManager.rescanTasks}. */
export const TASK_REGISTRY_FILE = '.task-registry.json';

/** Poll cadence (ms) for a RE-ADOPTED detached task's liveness watchdog — see
 *  {@link ProcessManager.rescanTasks}. unref'd so it never pins the event loop. */
export const ADOPT_POLL_INTERVAL_MS = 2_000;

/**
 * One persisted background-task record in the crash-safe registry sidecar (parity
 * with the reference's persistRemoteAgentMetadata, RemoteAgentTask.tsx:L442-454). Only
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
 * Mirrors the reference's `lastReportedToolCount` / `lastReportedTokenCount` delta
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

/**
 * One incremental OUTPUT delta for a still-running background shell/monitor task,
 * returned by {@link ProcessManager.reportOutputDelta} and
 * {@link ProcessManager.drainRunningOutputDeltas}. Carries only the output appended
 * since the last report (the "new output since last check" the model needs to follow a
 * long-running task's progress), mirroring the reference's `lastReportedTotalLines`
 * line-delta surfaced as a periodic progress attachment.
 */
export interface OutputDelta {
  /** The background task this delta belongs to. */
  taskId: string;
  /** The task's human-readable description (for the attachment label). */
  description: string;
  /** The task KIND (bash | monitor) — agents never produce an output-file delta. */
  kind: TaskKind;
  /** The NEW output appended since the last report (tail-trimmed to {@link OUTPUT_DELTA_MAX_BYTES}). */
  output: string;
  /** True when the output file shrank/rotated since the last report and `output` is a
   *  fresh read from the start rather than a strict append (see {@link ProcessManager.getOutputTail}). */
  reset: boolean;
  /** Bytes dropped from the FRONT of an over-cap delta (0 when the whole delta fit). */
  bytesOmitted: number;
}

/**
 * FUNCTIONAL (parity): the model-facing attachment note for an incremental background
 * output delta, mirroring the reference's periodic "new output from a still-running
 * task" progress attachment. Pure + exported so the wording has ONE source
 * (No-Duplication rule) and is unit-testable in isolation.
 */
export function formatBackgroundOutputDeltaNote(delta: OutputDelta): string {
  const resetNote = delta.reset ? ' [output was truncated/rotated; showing from the start]' : '';
  const omitted = delta.bytesOmitted > 0 ? ` [earlier ${formatByteCap(delta.bytesOmitted)} of this delta omitted]` : '';
  return (
    `[Background task "${delta.taskId}" (${delta.description}) new output${resetNote}${omitted}:\n` +
    `${delta.output.replace(/\n+$/, '')}]`
  );
}
