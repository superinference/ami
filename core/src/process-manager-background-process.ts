// Split from process-manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import { osHooks } from './process-manager-build-completion-summary';
import type { TaskAttentionState, TaskKind, TaskStatus } from './process-manager-build-completion-summary';

/**
 * FUNCTIONAL (parity): the `usage` clause appended to a completion note — a
 * ready-made `Usage: ran 3.2s, 1500 tokens, 4 tool uses.` fragment, mirroring
 * the reference's LocalAgentTask completion notification which surfaces a usage
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

export const STALL_TAIL_BYTES = 1024;

/**
 * SECURITY (parity with the reference's diskOutput): flags for creating a task
 * output file. O_EXCL fails if anything already exists at the path (so an
 * attacker-planted file/symlink can't be truncated-through) and O_NOFOLLOW
 * refuses to follow a planted symlink. Windows lacks both numerically; its 'wx'
 * string flag is O_WRONLY|O_CREAT|O_EXCL and the sandbox symlink vector is
 * Unix-only, so the string form is the correct fallback there.
 */
export function secureWriteFlags(): number | string {
  if (osHooks.platform() === 'win32') return 'wx';
  return fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW;
}

/** SECURITY: open a task-output file for reading without following symlinks. */
export function secureReadFlags(): number | string {
  if (osHooks.platform() === 'win32') return 'r';
  return fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW;
}

/** Read a task-output file with O_NOFOLLOW so a planted symlink can't redirect
 *  the read to an arbitrary host file. Returns '' if the file is absent. */
export function readOutputFileNoFollow(outputPath: string): string {
  const fd = fs.openSync(outputPath, secureReadFlags());
  try {
    return fs.readFileSync(fd, 'utf-8');
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * SECURITY + PERF (parity with the reference's tailFile): read ONLY the last
 * `maxBytes` of a task-output file via a positioned read, so memory/IO stays
 * bounded regardless of how large the log grew. Opens with O_NOFOLLOW so a
 * planted symlink can't redirect the read. Returns the tail content, the total
 * on-disk size (`bytesTotal`), the bytes actually read (`bytesRead`), and the
 * skipped-prefix size (`bytesOmitted`, 0 when the whole file fit the window).
 * Throws (propagated to getOutput's diagnostic catch) if the file is gone.
 */
export function readOutputTailBytes(
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
 * Mirrors the reference's PROMPT_PATTERNS (LocalShellTask): the directed-question
 * matcher catches "Do you want to continue? " and friends that carry no explicit
 * (y/n) marker, and "Press (any key|Enter)" covers "Press any key" (not just Enter).
 */
export const PROMPT_PATTERNS: RegExp[] = [
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
 * One captured message in a background AGENT's live transcript (parity with the reference
 * Code's BackgroundTaskState.messages). A foregrounded agent's buffer is synced
 * into the main view ({@link ProcessManager.syncForegroundedMessages}) so the user
 * sees its conversation live; on a user KILL the buffer is COLLAPSED to just the
 * final message ({@link ProcessManager.kill}) to release the retained transcript
 * (parity with the reference's spawnInProcess on-kill truncation) while preserving
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
 * the main view needs to mirror the single foregrounded agent (parity with the reference
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
   * not idle waiting. Mirrors the reference's task.totalPausedMs. Undefined ⇒ never
   * paused (treated as 0).
   */
  totalPausedMs?: number;
  /**
   * Wall-clock deadline (ms epoch) after which a TERMINAL task becomes eligible for
   * automatic eviction — set to `endTime + TERMINAL_RETENTION_MS` on the first
   * terminal transition (see {@link setTerminalTimestamps}). {@link ProcessManager.pruneExpired}
   * reaps terminal entries whose deadline has passed so finished rows/output files
   * do not accumulate forever. Mirrors the reference's `evictAfter` retention field.
   * Undefined while the task is still running.
   */
  evictAfter?: number;
  /** Cumulative token usage for background agents (undefined for bash tasks). */
  tokenCount?: number;
  /**
   * Cumulative count of tool calls a background AGENT has made so far (undefined
   * for bash/monitor tasks, which make no tool calls). Reported incrementally via
   * {@link ProcessManager.updateEntry} from the agent's event stream and surfaced
   * by `task_list` alongside {@link tokenCount}. Mirrors the reference's live
   * `task.progress.toolCount` — the in-flight tool-use counter that, together with
   * {@link tokenCount}, lets a reader gauge how much work a running agent has done.
   * It is also the basis for incremental progress deltas (see
   * {@link ProcessManager.reportProgressDelta}).
   */
  toolCount?: number;
  /**
   * Dedicated error message for a FAILED task/agent (parity with the reference's
   * AsyncAgentDetailDialog error field). Set via {@link ProcessManager.updateEntry}
   * when a background agent ends in a 'failed' state, so a consumer (task_list,
   * agent drill-down) can surface WHY it failed in its own channel instead of the
   * reader having to scrape the output tail. Undefined for a healthy/killed task.
   */
  error?: string;
  /**
   * FINAL-result token total for a finished background AGENT, distinct from the
   * LIVE {@link tokenCount} progress counter (parity with the reference's
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
   * tool invocation (parity with the reference's CoordinatorAgentStatus
   * `progress.summary`, which takes PRECEDENCE over the static task description).
   * A consumer surfaces this in place of/alongside the description so the reader
   * sees current activity, not just the one-line task label set at spawn. Undefined
   * before the agent's first tool call / for bash tasks.
   */
  progressSummary?: string;
  /**
   * Wall-clock time (ms epoch) of the last observed progress on a background AGENT —
   * stamped by {@link ProcessManager.updateEntry} whenever a live progress signal
   * (tokenCount / toolCount / progressSummary) arrives (parity with the reference's
   * CoordinatorAgentStatus `progress.lastActivity`, the basis for its
   * activity-direction indicator). A consumer compares `now - lastActivity` against
   * {@link PROGRESS_IDLE_MS} to tell a still-working agent from one that has gone
   * quiet. Undefined until the first progress signal.
   */
  lastActivity?: number;
  /**
   * Dedup latch (parity with the reference's `task.notified`): true once the model
   * has already been told about this task's termination — either because it
   * explicitly killed it via task_kill (kill() latches bash tasks) or because it
   * consumed the result via task_output (markNotified() latches on retrieval).
   * emitComplete() suppresses a redundant completion for a notified task so the
   * model isn't re-announced a termination it already knows about. Undefined/false
   * ⇒ not yet surfaced. Only set for bash tasks; agent kills deliberately stay
   * un-notified so their abort completion (which carries the partial result) still
   * surfaces, matching the reference's bash-vs-agent split in stopTask.
   */
  notified?: boolean;
  /**
   * Id of the agent (sub-agent task id) that spawned this task, or undefined when
   * it was started from the main thread. Recorded so orphaned background shells can
   * be reaped when their owning agent exits ({@link ProcessManager.killTasksForAgent}),
   * preventing zombie processes that outlive the agent. Mirrors the reference's
   * LocalShellTaskState.agentId (killShellTasksForAgent).
   */
  agentId?: string;
  /**
   * Id of the tool_use call that spawned this task, or undefined when it was not
   * started from a tool invocation (parity with the reference's TaskStateBase
   * `toolUseId` / LocalShellSpawnInput.toolUseId). Recorded so a consumer can
   * correlate a background task back to the specific tool call that created it —
   * e.g. to attach the completion to the originating tool_result, or to reap a
   * task whose tool call was abandoned. Set via spawn()/registerAgent(); looked up
   * by {@link ProcessManager.findByToolUseId}.
   */
  toolUseId?: string;
  /**
   * Per-task foreground/background flag (parity with the reference's per-task
   * `isBackgrounded`). AMI tasks are background-by-construction, so this is
   * `undefined` (⇒ background) until a consumer FOREGROUNDS one into the main view
   * via {@link ProcessManager.foregroundTask} (sets `false`) or re-backgrounds it
   * via {@link ProcessManager.backgroundTask} (sets `true`). The
   * {@link isBackgroundTask} predicate excludes a task with `isBackgrounded ===
   * false` from the background-tasks indicator while it is foregrounded, mirroring
   * the reference's task-granular backgrounding (foregroundedTaskId + isBackgrounded).
   */
  isBackgrounded?: boolean;
  /**
   * True when this background AGENT was started by RESUMING a prior agent session
   * rather than a fresh spawn (parity with the reference's resumeAgent introspection
   * marker, resumeAgent.ts:L222-225 — a resumed invocation is tagged so it can be
   * distinguished from a first run). Set at {@link registerAgent} time from the
   * caller's resume target; undefined/false for a fresh spawn. Surfaced by
   * task_output ("Invocation: resumed") so a reader can tell a continuation from a
   * first run. Only meaningful for `agent` tasks.
   */
  resumed?: boolean;
  /**
   * The name of the swarm TEAM this background agent belongs to, when it was spawned
   * into an active team (parity with the reference's pillLabel `identity.teamName`,
   * the basis for its distinct-team count "N teams"). Set at {@link registerAgent}
   * time from the active team; undefined for a solo agent / bash / monitor task.
   * Consumed by task_list's distinct-team tally.
   */
  teamName?: string;
  /**
   * Per-task ATTENTION sub-state (see {@link TaskAttentionState}) — orthogonal to
   * the lifecycle {@link status}. Lets a still-`running` task signal that it is
   * awaiting review (`plan_ready`) or blocked on the user (`needs_input`) so
   * task_list can flag it for attention. Parity with the reference's ultraplanPhase /
   * RemoteAgentTask waiting-for-input badge. Undefined ⇒ `working` (no attention).
   * The stall watchdog sets `needs_input` automatically when a task blocks on an
   * interactive prompt (see {@link ProcessManager.notifyStall}); an orchestrator can
   * set `plan_ready` via {@link ProcessManager.updateEntry}.
   */
  attentionState?: TaskAttentionState;
  /**
   * Natural-language AI SUMMARY of what a background agent has accomplished so far
   * (parity with the reference's LocalAgentTask periodic `updateAgentSummary`, a 1-2
   * sentence LLM-written summary). Distinct from {@link progressSummary} (a raw
   * tool-derived "current activity" tick): the AI summary is authoritative and
   * takes PRECEDENCE — a raw progress tick NEVER clobbers it (the reference's
   * `existingSummary ? {...progress, summary: existingSummary} : progress` merge).
   * Read via {@link ProcessManager.effectiveSummary} (agentSummary ?? progressSummary).
   * Set via {@link ProcessManager.updateEntry}. Undefined until the first AI summary.
   */
  agentSummary?: string;
  /**
   * COOPERATIVE graceful-shutdown flag for a running in-process AGENT (parity with
   * the reference's InProcessTeammateTask `shutdownRequested`). Distinct from a hard
   * {@link ProcessManager.kill} (which aborts the in-flight turn immediately): the
   * agent's drain loop checks this flag BETWEEN turns via
   * {@link ProcessManager.isShutdownRequested} and stops AFTER the current turn
   * finishes, so a graceful stop never truncates mid-thought. Set by
   * {@link ProcessManager.requestShutdown}; undefined/false ⇒ keep running. Only
   * meaningful for `agent` tasks.
   */
  shutdownRequested?: boolean;
  /**
   * IDLE lifecycle flag for a running in-process AGENT (parity with the reference's
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
   * Live transcript buffer for a background AGENT (parity with the reference's
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
 * "completed" — parity with the reference's enqueueShellNotification which branches
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
   * status/exit-code phrasing. Mirrors the reference attaching a summary string to
   * its shell-notification events. Undefined only on a legacy direct emit.
   */
  summary?: string;
  /**
   * Wall-clock active duration (ms) the task ran before reaching this terminal
   * state — frozen pause-discounted value from {@link computeElapsedMs}, set by
   * emitComplete from the tracked entry. Surfaced in the completion note's usage
   * clause (`ran 3.2s`), mirroring the reference's LocalAgentTask `duration_ms`.
   * Undefined on a legacy direct emit that bypassed emitComplete.
   */
  durationMs?: number;
  /**
   * Total token usage of a finished background AGENT, surfaced in the completion
   * note's usage clause (`… 1500 tokens …`) — parity with the reference's LocalAgentTask
   * completion `total_tokens`. Attached by emitComplete from the tracked entry
   * ({@link BackgroundProcess.resultTokenCount}, falling back to the live
   * {@link BackgroundProcess.tokenCount}). Undefined/0 for a bash shell (no tokens)
   * or a legacy direct emit, which then omits the token part of the clause.
   */
  tokenCount?: number;
  /**
   * Total tool-call count of a finished background AGENT, surfaced in the completion
   * note's usage clause (`… 4 tool uses`) — parity with the reference's LocalAgentTask
   * completion `tool_uses`. Attached by emitComplete from the tracked entry
   * ({@link BackgroundProcess.resultToolCount}, falling back to the live
   * {@link BackgroundProcess.toolCount}). Undefined/0 for a bash shell or a legacy
   * emit, which then omits the tool-use part of the clause.
   */
  toolCount?: number;
}
