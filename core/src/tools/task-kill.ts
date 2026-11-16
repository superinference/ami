import { ToolDefinition, ToolResult, ToolContext } from '../types';
import type { ProcessManager } from '../process-manager';
import { previewCommand } from './command-preview';

const ALLOWED_KEYS = new Set(['task_id', 'cleanup']);

/**
 * PARITY (additive): machine-readable discriminator for a stop/kill outcome,
 * carried on `ToolResult.metadata.reason`. Mirrors Claude Code's StopTaskError
 * code taxonomy (`not_found` / `not_running` / `kill_failed`) plus AMI's extra
 * states, so a non-LLM caller can branch on the result instead of parsing the
 * human-readable message string.
 */
export type TaskKillReason =
  | 'invalid_input'
  | 'manager_unavailable'
  | 'not_found'
  | 'already_terminal'
  | 'killed'
  | 'kill_failed';

/** Structured result surface (ToolResult.metadata) for task_kill. */
export interface TaskKillMetadata {
  reason: TaskKillReason;
  taskId?: string;
  /** 'agent' | 'bash' when known — parity with Claude's `task_type`. */
  taskType?: 'agent' | 'bash';
  /** FULL (untruncated) command/description of the stopped task. */
  command?: string;
  status?: string;
  pid?: number;
  exitCode?: number | null;
}

/**
 * FUNCTIONAL (parity): optionally reap a stopped task after killing it, mirroring
 * Claude Code cleaning up a terminated background shell. When `cleanup` is true we
 * ask the ProcessManager to remove the task from its table and delete its on-disk
 * output file, returning a human-readable suffix describing the outcome.
 *
 * Defensive by design: `removeTask` only exists on the real ProcessManager, so
 * when a caller/test passes a minimal manager stub we silently no-op. A terminal
 * task reaps successfully; a still-running one is refused by removeTask (returns
 * false) and we say so rather than claiming a cleanup that didn't happen.
 */
function maybeCleanup(pm: ProcessManager, taskId: string, cleanup: boolean): string {
  if (!cleanup) return '';
  if (typeof pm.removeTask !== 'function') return '';
  const removed = pm.removeTask(taskId);
  return removed
    ? ' Task reaped (removed from the task list; output file deleted).'
    : ' (cleanup skipped: task is not in a terminal state).';
}

export const taskKillTool: ToolDefinition = {
  name: 'task_kill',
  // PARITY (additive): dedicated tool-search hint + back-compat alias so a model or
  // an old transcript/SDK caller using Claude's deprecated `KillShell` name still
  // resolves to this tool.
  searchHint: 'kill a running background task',
  aliases: ['KillShell'],
  description:
    'Stop a running background task by its ID. Kills the process and its children. ' +
    'Idempotent and concurrency-safe: stopping an already-finished task (or one that ' +
    'exits during the stop) is reported as success, not an error. Pass cleanup:true to ' +
    'also reap the task (remove it from the task list and delete its output file). ' +
    'Returns the stopped task\'s type (agent/bash) and command/description plus a ' +
    'machine-readable reason code on both success and failure ' +
    '([code: invalid_input] / [code: not_found] / [code: kill_failed]).',
  inputSchema: {
    type: 'object',
    properties: {
      task_id: {
        type: 'string',
        description: 'The task ID to stop.',
      },
      cleanup: {
        type: 'boolean',
        description:
          'When true, reap the task after stopping it: remove it from the task list and ' +
          'delete its output file. Defaults to false (the stopped task stays listed as "killed").',
      },
    },
    required: ['task_id'],
  },
  isReadOnly: false,
  // PARITY (additive): stopping a task never mutates shared state the way a file
  // edit does, so it may run alongside other concurrency-safe tools. Mirrors
  // Claude Code's `TaskStop.isConcurrencySafe() => true`.
  isConcurrencySafe: true,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const meta = (m: TaskKillMetadata): Record<string, unknown> => ({ ...m });

    // Strict schema: reject unexpected/injected parameters.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown input key(s): ${unknown.join(', ')}. Allowed: task_id, cleanup. [code: invalid_input]`, isError: true, metadata: meta({ reason: 'invalid_input' }) };
    }

    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available. [code: manager_unavailable]', isError: true, metadata: meta({ reason: 'manager_unavailable' }) };
    }

    // FUNCTIONAL (parity): validate argument types up front instead of blindly
    // casting. An empty/whitespace or non-string task_id can never match a real
    // task; surfacing a clean [code: invalid_input] is clearer than a spurious
    // not_found and mirrors Claude Code rejecting a malformed kill request.
    const rawId = input.task_id;
    if (typeof rawId !== 'string' || rawId.trim() === '') {
      return { output: 'Error: "task_id" must be a non-empty string. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input' }) };
    }
    const taskId = rawId;

    if (input.cleanup !== undefined && typeof input.cleanup !== 'boolean') {
      return { output: 'Error: "cleanup" must be a boolean. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input', taskId }) };
    }
    const cleanup = input.cleanup === true;

    const pm = context.processManager;
    const task = pm.get(taskId);

    if (!task) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks. [code: not_found]`, isError: true, metadata: meta({ reason: 'not_found', taskId }) };
    }

    // PARITY (additive): the task-type discriminator (agent/bash) surfaced in the
    // structured result, when the manager exposes it (real ProcessManager; mock
    // stubs may omit it). Mirrors Claude's `task_type`.
    const taskType = typeof pm.taskType === 'function' ? pm.taskType(taskId) : undefined;

    // Echo the command/description so the model can confirm what it stopped. The
    // human-readable label is truncated to a bounded preview (2 lines / 160 chars)
    // so a huge command never floods the output; the FULL command is preserved in
    // metadata.command for programmatic consumers.
    const fullLabel = task.command || task.description || '';
    const preview = previewCommand(fullLabel);
    const labelTag = preview.text ? ` (${preview.text}${preview.truncated ? '…' : ''})` : '';

    if (task.status !== 'running') {
      // FUNCTIONAL (parity): idempotent stop of an already-terminal task. Only
      // render "(exit code: N)" when an exit code is actually known — a killed
      // task has a null exit code, and printing "exit code: null" was noise.
      const exitPart = task.exitCode !== null && task.exitCode !== undefined ? ` (exit code: ${task.exitCode})` : '';
      const out = `Task "${taskId}"${labelTag} is already ${task.status}${exitPart}.` + maybeCleanup(pm, taskId, cleanup);
      return { output: out, isError: false, metadata: meta({ reason: 'already_terminal', taskId, taskType, command: fullLabel || undefined, status: task.status, exitCode: task.exitCode }) };
    }

    const killed = pm.kill(taskId);
    if (killed) {
      const out = `Task "${taskId}" (PID ${task.pid})${labelTag} killed successfully.` + maybeCleanup(pm, taskId, cleanup);
      return { output: out, isError: false, metadata: meta({ reason: 'killed', taskId, taskType, command: fullLabel || undefined, pid: task.pid }) };
    }

    // FUNCTIONAL (parity): race-safe idempotency. kill() returns false only when
    // the task is no longer running — but we observed it running above, so it must
    // have exited on its own in the window between get() and kill(). Re-check the
    // live status: if it is now terminal, report that as success (the user's goal,
    // "the task is stopped", is satisfied) instead of a misleading kill_failed.
    // A genuinely-still-running task that refused to die is the only real failure.
    const nowStatus = typeof pm.getStatus === 'function' ? pm.getStatus(taskId) : undefined;
    if (nowStatus && nowStatus !== 'running') {
      const out = `Task "${taskId}"${labelTag} had already ${nowStatus} before it could be killed.` + maybeCleanup(pm, taskId, cleanup);
      return { output: out, isError: false, metadata: meta({ reason: 'already_terminal', taskId, taskType, command: fullLabel || undefined, status: nowStatus }) };
    }

    return { output: `Error: Failed to kill task "${taskId}". [code: kill_failed]`, isError: true, metadata: meta({ reason: 'kill_failed', taskId, taskType, command: fullLabel || undefined }) };
  },
};
