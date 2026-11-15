import { ToolDefinition, ToolResult, ToolContext } from '../types';
import type { BackgroundProcess, TaskStatus } from '../process-manager';

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
  if (task.status === 'killed') return `${icon} killed`;
  // completed / failed
  if (task.exitCode === null || task.exitCode === undefined) return `${icon} ${task.status}`;
  return `${icon} ${task.status} (exit ${task.exitCode})`;
}

/** Count tasks by lifecycle status, for the at-a-glance summary line. */
export function summarizeTasks(tasks: Pick<BackgroundProcess, 'status'>[]): Record<TaskStatus, number> {
  const counts: Record<TaskStatus, number> = { running: 0, completed: 0, failed: 0, killed: 0 };
  for (const t of tasks) {
    if (t.status in counts) counts[t.status]++;
  }
  return counts;
}

/** A terminal task will never run again. */
function isTerminal(status: TaskStatus): boolean {
  return status !== 'running';
}

export const taskListTool: ToolDefinition = {
  name: 'task_list',
  description:
    'List all background tasks and subagents with their status, PID, elapsed time, ' +
    'token usage (agents), and command. Supports optional filtering by status or type.',
  inputSchema: {
    type: 'object',
    properties: {
      // Additive, backward-compatible filters. Omitting both lists everything
      // (minus internal monitors), preserving the original no-arg behavior.
      status: {
        type: 'string',
        enum: ['running', 'completed', 'failed', 'killed'],
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
      const time = formatElapsed(Date.now() - t.startTime);
      const status = formatTaskStatus(t);
      const parts = [t.taskId, `PID ${t.pid}`, status, time];

      if (isAgentTask(t.command)) {
        const name = agentDisplayName(t);
        let ref = `agent "${name}"`;
        if ((agentNameCounts.get(name) ?? 0) > 1) {
          ref += ` [ambiguous: address by id ${t.taskId}]`;
        }
        parts.push(ref);
        if (typeof t.tokenCount === 'number' && t.tokenCount > 0) {
          parts.push(formatTokenCount(t.tokenCount));
        }
      } else {
        parts.push(t.description);
      }

      return parts.join('  ');
    });

    // At-a-glance status breakdown.
    const counts = summarizeTasks(tasks);
    const summary = (['running', 'completed', 'failed', 'killed'] as TaskStatus[])
      .filter(s => counts[s] > 0)
      .map(s => `${counts[s]} ${s}`)
      .join(', ');

    const header = tasks.length === visible.length
      ? `Background tasks (${tasks.length}):`
      : `Background tasks (${tasks.length} of ${visible.length}):`;

    // Bounded tool output: cap the listing so a very large task set can't flood context.
    const MAX_OUTPUT_CHARS = 30_000;
    let body = lines.join('\n');
    if (body.length > MAX_OUTPUT_CHARS) {
      body = body.slice(0, MAX_OUTPUT_CHARS) + `\n... [task list truncated at ${MAX_OUTPUT_CHARS} chars]`;
    }
    return { output: `${header}\n${summary}\n${body}`, isError: false };
  },
};
