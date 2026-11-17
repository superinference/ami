import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { isTerminalTaskStatus, PROGRESS_IDLE_MS } from '../process-manager';
import type { BackgroundProcess, TaskStatus } from '../process-manager';
import { truncateActivity } from './text-truncate';

/**
 * PARITY (Claude Code BackgroundTask.tsx `activityLimit`, `truncate(cmd, limit,
 * true)`): width budget for the free-text activity column (a shell task's
 * description/command or an agent's live summary). Bounds a single pathological
 * long/multi-line value to one tidy line so it can't dominate a row or inflate
 * the tool result. Wider than Claude's 40-col TUI budget because this is the
 * model-facing listing, but still bounded.
 */
const MAX_ACTIVITY_WIDTH = 120;

/**
 * Prefix stamped on a background subagent's `command` by task.ts / workflow.ts
 * (`[agent] <label>`). Used to tell an autonomous subagent apart from a plain
 * background shell task so the two can be grouped, typed, and addressed by name.
 */
const AGENT_COMMAND_PREFIX = '[agent] ';

/** Whether a tracked entry is a background subagent (vs a shell task / monitor). */
export function isAgentTask(command: string | undefined): boolean {
  return !!command && command.startsWith(AGENT_COMMAND_PREFIX);
}

/** Whether a tracked entry is an internal MCP health monitor (not a user task). */
export function isMonitorTask(command: string | undefined): boolean {
  return !!command && command.startsWith('monitor:');
}

/**
 * The human-facing name a subagent can be addressed by (the label the task was
 * spawned with). Falls back to the description, then the raw command.
 */
export function agentDisplayName(task: Pick<BackgroundProcess, 'command' | 'description'>): string {
  if (isAgentTask(task.command)) {
    const label = task.command.slice(AGENT_COMMAND_PREFIX.length).trim();
    if (label) return label;
  }
  return task.description || task.command || '';
}

/** Compact token-count rendering for background agents (e.g. `1.5k tok`). */
export function formatTokenCount(n: number): string {
  return n < 1000 ? `${n} tok` : `${(n / 1000).toFixed(1)}k tok`;
}

/** Elapsed-time rendering: `850ms` / `42s` / `3.5m`. */
export function formatElapsed(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(0)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
}

const STATUS_ICON: Record<TaskStatus, string> = {
  pending: '…',
  running: '▶',
  completed: '✓',
  failed: '✗',
  killed: '✗',
};

/**
 * Render a task's status. For terminal states the exit code is only appended
 * when it is actually known — `completed (exit null)` was meaningless noise, so
 * a null exit renders as a bare `completed`.
 */
export function formatTaskStatus(task: Pick<BackgroundProcess, 'status' | 'exitCode'>): string {
  const icon = STATUS_ICON[task.status] ?? '•';
  if (task.status === 'running') return `${icon} running`;
  // A queued/not-yet-started task (parity: Claude's getStatusText 'is pending') —
  // rendered distinctly from running so the model can tell "accepted, waiting to
  // start" from "actively executing". No exit code (it hasn't run).
  if (task.status === 'pending') return `${icon} pending`;
  if (task.status === 'killed') return `${icon} killed`;
  // completed / failed
  if (task.exitCode === null || task.exitCode === undefined) return `${icon} ${task.status}`;
  return `${icon} ${task.status} (exit ${task.exitCode})`;
}

/** Count tasks by lifecycle status, for the at-a-glance summary line. */
export function summarizeTasks(tasks: Pick<BackgroundProcess, 'status'>[]): Record<TaskStatus, number> {
  const counts: Record<TaskStatus, number> = { pending: 0, running: 0, completed: 0, failed: 0, killed: 0 };
  for (const t of tasks) {
    if (t.status in counts) counts[t.status]++;
  }
  return counts;
}

/**
 * PARITY (Claude Code's getPillLabel / LocalShellTask `kind`): count tasks by
 * their authoritative {@link BackgroundProcess.kind} discriminator — the per-type
 * label split ("1 shell, 1 monitor") that the footer pill shows. Branches on the
 * `kind` FIELD, not the brittle command-string heuristics ({@link isAgentTask} /
 * {@link isMonitorTask}) a misnamed command could fool. An entry whose kind is
 * absent (an older mock stub that never set it) is skipped so the by-type line is
 * simply omitted rather than miscounted.
 */
export function summarizeTaskKinds(
  tasks: Pick<BackgroundProcess, 'kind'>[],
): { bash: number; monitor: number; agent: number } {
  const counts = { bash: 0, monitor: 0, agent: 0 };
  for (const t of tasks) {
    if (t.kind === 'bash' || t.kind === 'monitor' || t.kind === 'agent') counts[t.kind]++;
  }
  return counts;
}

/**
 * PARITY (Claude Code's getPillLabel): render a per-type count summary using
 * Claude's footer nouns and pluralisation — `1 shell` / `2 shells`, `1 agent` /
 * `2 agents`, `1 monitor` / `2 monitors`. Zero-count kinds are dropped; an
 * all-zero input renders the empty string so the caller can suppress the line.
 */
export function formatKindSummary(counts: { bash: number; monitor: number; agent: number }): string {
  const plural = (n: number, noun: string): string => (n === 1 ? `1 ${noun}` : `${n} ${noun}s`);
  const parts: string[] = [];
  if (counts.bash > 0) parts.push(plural(counts.bash, 'shell'));
  if (counts.agent > 0) parts.push(plural(counts.agent, 'agent'));
  if (counts.monitor > 0) parts.push(plural(counts.monitor, 'monitor'));
  return parts.join(', ');
}

/**
 * PARITY (Claude Code's pillLabel `identity.teamName` distinct-team count, rendered
 * as "N teams"): the number of DISTINCT non-empty team names among the given tasks.
 * A background agent spawned into a swarm team carries its
 * {@link BackgroundProcess.teamName}; solo agents / bash / monitor tasks carry none
 * and are ignored. Pure + exported so the by-type line has ONE counting source.
 */
export function countDistinctTeams(tasks: Pick<BackgroundProcess, 'teamName'>[]): number {
  const teams = new Set<string>();
  for (const t of tasks) {
    if (t.teamName) teams.add(t.teamName);
  }
  return teams.size;
}

/** A terminal task will never run again (completed/failed/killed). A `pending`
 *  task is NON-terminal (queued, will run), so it sorts with running tasks at the
 *  top — shared predicate (No-Duplication rule). */
function isTerminal(status: TaskStatus): boolean {
  return isTerminalTaskStatus(status);
}

export const taskListTool: ToolDefinition = {
  name: 'task_list',
  // PARITY (Claude Code TaskListTool/prompt.ts:28-48): a tool description that
  // teaches WHEN to call it plus closes the list→inspect/act loop by naming the
  // follow-up tools the returned taskId feeds into. AMI's listing unlocks
  // task_output (read/wait) and task_kill (stop); the model should learn that
  // workflow from the wording alone rather than discovering it by trial.
  description:
    'List all background tasks and subagents with their status, PID, elapsed time, ' +
    'token usage (agents), and command. Supports optional filtering by status or type.\n\n' +
    'When to use: to check on long-running bash commands, subagents, or tasks started ' +
    'with run_in_background — see what is still running, inspect progress, or find a ' +
    'finished task to read or clean up.\n\n' +
    'Each row starts with a taskId. Pass that taskId to task_output <taskId> to read a ' +
    "task's output (or wait for it to finish), and to task_kill <taskId> to stop a " +
    'running task.',
  inputSchema: {
    type: 'object',
    properties: {
      // Additive, backward-compatible filters. Omitting both lists everything
      // (minus internal monitors), preserving the original no-arg behavior.
      status: {
        type: 'string',
        enum: ['pending', 'running', 'completed', 'failed', 'killed'],
        description: 'Only list tasks currently in this status.',
      },
      type: {
        type: 'string',
        enum: ['agent', 'task'],
        description: "Only list background subagents ('agent') or plain shell tasks ('task').",
      },
    },
  },
  isReadOnly: true,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available.', isError: true };
    }

    // Opportunistic retention GC (parity: Claude Code's `evictAfter` panel grace):
    // reap terminal tasks whose grace window has elapsed before we list, so a long
    // session doesn't accumulate stale completed rows / orphaned output files
    // without a background timer. Optional-chained so a minimal manager stub (older
    // tests) that lacks pruneExpired simply skips it. A freshly-finished task is
    // still within TERMINAL_RETENTION_MS and therefore remains listed.
    context.processManager.pruneExpired?.();

    const statusFilter = typeof input.status === 'string' ? input.status : undefined;
    const typeFilter = typeof input.type === 'string' ? input.type : undefined;

    const all = context.processManager.list();

    // Internal MCP health monitors are framework plumbing, not user-visible
    // background work — exclude them from the listing (ADDITIVE: they used to
    // appear as noise `monitor-… MCP monitor: …` lines).
    const visible = all.filter(t => !isMonitorTask(t.command));

    if (visible.length === 0) {
      return { output: 'No background tasks.', isError: false };
    }

    // Apply optional filters.
    let tasks = visible;
    if (statusFilter) tasks = tasks.filter(t => t.status === statusFilter);
    if (typeFilter === 'agent') tasks = tasks.filter(t => isAgentTask(t.command));
    else if (typeFilter === 'task') tasks = tasks.filter(t => !isAgentTask(t.command));

    if (tasks.length === 0) {
      const crit = [statusFilter && `status=${statusFilter}`, typeFilter && `type=${typeFilter}`]
        .filter(Boolean)
        .join(', ');
      return { output: `No background tasks match filter (${crit}).`, isError: false };
    }

    // Detect duplicate agent names so ambiguous references can be flagged — an
    // agent addressed by name must be unique, otherwise the id is required.
    const agentNameCounts = new Map<string, number>();
    for (const t of tasks) {
      if (isAgentTask(t.command)) {
        const name = agentDisplayName(t);
        agentNameCounts.set(name, (agentNameCounts.get(name) ?? 0) + 1);
      }
    }

    // Running tasks first, then most-recently-started — the live work the model
    // most likely cares about floats to the top regardless of insertion order.
    const sorted = [...tasks].sort((a, b) => {
      const at = isTerminal(a.status) ? 1 : 0;
      const bt = isTerminal(b.status) ? 1 : 0;
      if (at !== bt) return at - bt;
      return b.startTime - a.startTime;
    });

    const lines = sorted.map(t => {
      // Freeze elapsed at completion for terminal tasks so finished rows show a
      // stable duration instead of an ever-growing one.
      const time = formatElapsed((t.endTime ?? Date.now()) - t.startTime);
      const status = formatTaskStatus(t);
      const parts = [t.taskId, `PID ${t.pid}`, status, time];

      if (isAgentTask(t.command)) {
        const name = agentDisplayName(t);
        let ref = `agent "${name}"`;
        if ((agentNameCounts.get(name) ?? 0) > 1) {
          ref += ` [ambiguous: address by id ${t.taskId}]`;
        }
        // Composite liveness (parity: InProcessBackend.isActive): a running agent
        // whose abort signal has fired but whose status has not yet flipped is
        // "aborting" — surface that transient state so a reader isn't misled into
        // thinking it is still doing work. Optional-chained so a liveness-unaware
        // process manager (older mocks) simply omits the marker (treated as live).
        const live = context.processManager!.isActive?.(t.taskId) ?? true;
        if (t.status === 'running' && !live) {
          ref += ' [aborting]';
        }
        parts.push(ref);
        // Final-result vs live-progress stats (parity: Claude Code's
        // AsyncAgentDetailDialog, which shows a FINISHED agent's `result` totals
        // separately from the in-flight `progress` totals). For a TERMINAL agent
        // prefer the authoritative result totals recorded at completion, falling
        // back to the last live counter when a producer never set them; a RUNNING
        // agent always shows the live counter.
        const term = isTerminal(t.status);
        const dispTokens = term ? (t.resultTokenCount ?? t.tokenCount) : t.tokenCount;
        const dispTools = term ? (t.resultToolCount ?? t.toolCount) : t.toolCount;
        if (typeof dispTokens === 'number' && dispTokens > 0) {
          parts.push(formatTokenCount(dispTokens));
        }
        // Live tool-call progress counter (parity: Claude Code's
        // task.progress.toolCount) — how many tool calls this background agent has
        // made so far. Gated on >0 so a just-spawned agent shows no noise tag.
        if (typeof dispTools === 'number' && dispTools > 0) {
          parts.push(`${dispTools} ${dispTools === 1 ? 'tool' : 'tools'}`);
        }
        // Live AI progress summary (parity: Claude Code's CoordinatorAgentStatus
        // `progress.summary`, which takes PRECEDENCE over the static description):
        // for a still-RUNNING agent, surface what it is doing right now, plus an
        // activity-direction indicator — "idle Ns" once no progress signal has
        // arrived for PROGRESS_IDLE_MS, so a reader can tell a working agent from a
        // quiet one. Both are omitted for a terminal agent / when unset.
        if (t.status === 'running') {
          // Prefer the authoritative AI summary over the raw tool-tick progress
          // summary (parity: Claude Code's LocalAgentTask summary precedence — a raw
          // progress tick never clobbers the LLM summary). effectiveSummary() encodes
          // agentSummary ?? progressSummary; optional-chained for a minimal stub.
          const summary = context.processManager!.effectiveSummary?.(t.taskId) ?? t.progressSummary;
          if (summary) parts.push(`— ${truncateActivity(summary, MAX_ACTIVITY_WIDTH, true)}`);
          if (typeof t.lastActivity === 'number') {
            const idleMs = Date.now() - t.lastActivity;
            if (idleMs >= PROGRESS_IDLE_MS) {
              parts.push(`[idle ${Math.round(idleMs / 1000)}s]`);
            }
          }
        }
        // Incremental progress since the previous task_list (parity: Claude's
        // lastReportedToolCount/lastReportedTokenCount delta caching "for computing
        // deltas in notifications"). For a still-RUNNING agent, surface what changed
        // since the last listing so the model sees forward progress at a glance. The
        // first listing establishes the baseline (firstReport ⇒ no clause).
        // Optional-chained so a minimal manager stub without reportProgressDelta
        // simply omits the clause.
        if (t.status === 'running') {
          const delta = context.processManager!.reportProgressDelta?.(t.taskId);
          if (delta && !delta.firstReport && (delta.toolCountDelta > 0 || delta.tokenCountDelta > 0)) {
            const bits: string[] = [];
            if (delta.toolCountDelta > 0) {
              bits.push(`+${delta.toolCountDelta} ${delta.toolCountDelta === 1 ? 'tool' : 'tools'}`);
            }
            if (delta.tokenCountDelta > 0) bits.push(`+${formatTokenCount(delta.tokenCountDelta)}`);
            parts.push(`(${bits.join(', ')} since last check)`);
          }
        }
      } else {
        // Width-bounded activity column (parity: Claude's BackgroundTask
        // `truncate(command/description, activityLimit, true)`): collapse a
        // multi-line description to one line and cap its width so one long shell
        // task can't blow out the row / inflate the tool result.
        parts.push(truncateActivity(t.description, MAX_ACTIVITY_WIDTH, true));
      }

      // Attention badge (parity: Claude Code's ultraplanPhase / waiting-for-input
      // pill): a still-running task signalling it needs the user's attention — the
      // stall watchdog sets `needs_input` when a task blocks on an interactive
      // prompt; an orchestrator can set `plan_ready` when a plan awaits review.
      // Orthogonal to the lifecycle status, so it renders alongside `running`.
      if (t.status === 'running') {
        if (t.attentionState === 'needs_input') parts.push('[needs input]');
        else if (t.attentionState === 'plan_ready') parts.push('[plan ready]');
      }

      // Lifecycle badges for an in-process agent (parity: Claude Code's
      // InProcessTeammateTask isIdle / shutdownRequested): a graceful shutdown in
      // flight is shown so the reader knows the agent will stop after its current
      // turn, and an explicit idle flag (set when the agent finished its turn and is
      // awaiting the next message) distinguishes a quiescent agent from a busy one.
      // Orthogonal to the lifecycle status, so they render alongside `running`.
      if (t.status === 'running') {
        if (t.shutdownRequested) parts.push('[shutting down]');
        else if (t.idle) parts.push('[idle]');
      }

      // Dedicated failure-reason channel (parity: AsyncAgentDetailDialog error
      // field): show WHY a failed task failed without the reader scraping output.
      if (t.status === 'failed' && t.error) {
        parts.push(`— error: ${t.error}`);
      }

      return parts.join('  ');
    });

    // At-a-glance status breakdown.
    const counts = summarizeTasks(tasks);
    const summary = (['pending', 'running', 'completed', 'failed', 'killed'] as TaskStatus[])
      .filter(s => counts[s] > 0)
      .map(s => `${counts[s]} ${s}`)
      .join(', ');

    // PARITY (Claude getPillLabel): per-type breakdown from the authoritative
    // `kind` field (shells/agents are drawn from the listed tasks). Internal MCP
    // monitors are intentionally kept OUT of the per-task listing (an AMI
    // differentiator — they are framework plumbing), but their running count is
    // surfaced here from the unfiltered set so the model can still see "N monitor"
    // without the noise lines, mirroring Claude's footer monitor count.
    const kindCounts = summarizeTaskKinds(tasks);
    kindCounts.monitor = all.filter(t => t.kind === 'monitor' && t.status === 'running').length;
    const kindSummary = formatKindSummary(kindCounts);
    // Distinct-team tally (parity: Claude pillLabel "N teams") appended to the
    // by-type line when any agent carries a team identity.
    const teamCount = countDistinctTeams(tasks);
    const teamSuffix = teamCount > 0
      ? `${kindSummary ? ', ' : ''}${teamCount} ${teamCount === 1 ? 'team' : 'teams'}`
      : '';
    const typeLineContent = `${kindSummary}${teamSuffix}`;
    const typeLine = typeLineContent ? `By type: ${typeLineContent}\n` : '';

    const header = tasks.length === visible.length
      ? `Background tasks (${tasks.length}):`
      : `Background tasks (${tasks.length} of ${visible.length}):`;

    // Bounded tool result (parity: Claude Code `maxResultSizeChars` on the task
    // tools): cap the listing so a very large task set can't flood the model's
    // context. Rather than slicing mid-row (which leaves a half-rendered line and
    // hides how much was dropped), keep WHOLE task rows up to the char budget and
    // append a `… N more tasks (output truncated)` footer reporting exactly how
    // many rows were elided — a stable, countable boundary the model can reason
    // about.
    const MAX_OUTPUT_CHARS = 30_000;
    const kept: string[] = [];
    let used = 0;
    for (const line of lines) {
      // +1 for the newline that joins each row; always keep at least one row so a
      // single oversized row is still shown (its own activity column is already
      // width-bounded) rather than yielding an empty body.
      const cost = line.length + 1;
      if (kept.length > 0 && used + cost > MAX_OUTPUT_CHARS) break;
      kept.push(line);
      used += cost;
    }
    const dropped = lines.length - kept.length;
    let body = kept.join('\n');
    if (dropped > 0) {
      body += `\n… ${dropped} more task${dropped === 1 ? '' : 's'} (output truncated)`;
    }
    return { output: `${header}\n${summary}\n${typeLine}${body}`, isError: false };
  },
};
