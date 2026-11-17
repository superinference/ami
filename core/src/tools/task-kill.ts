import { ToolDefinition, ToolResult, ToolContext } from '../types';
import type { ProcessManager, TaskKind } from '../process-manager';
import { previewCommand } from './command-preview';

const ALLOWED_KEYS = new Set(['task_id', 'cleanup', 'interrupt', 'graceful', 'retry', 'skip', 'shell_id']);

/**
 * Bounded window (ms) a `graceful` stop waits event-driven for the agent to finish
 * its current turn and go idle/terminal before reporting. Parity with Claude Code's
 * leader-wait on `onIdleCallbacks`: we wake the instant the agent goes idle (no
 * polling), but cap the wait so the tool can never hang if the turn runs long — on
 * timeout we report the shutdown was requested and the agent is still finishing.
 */
export const GRACEFUL_SHUTDOWN_WAIT_MS = 10_000;

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
  | 'kill_failed'
  | 'interrupted'
  | 'interrupt_unsupported'
  | 'shutdown_requested'
  | 'shutdown_unsupported'
  // Per-agent workflow recovery (parity: onRetryAgent / onSkipAgent).
  | 'retried'
  | 'retry_unsupported'
  | 'skipped'
  | 'skip_unsupported';

/** Structured result surface (ToolResult.metadata) for task_kill. */
export interface TaskKillMetadata {
  reason: TaskKillReason;
  taskId?: string;
  /** 'agent' | 'bash' when known — parity with Claude's `task_type`. */
  taskType?: 'agent' | 'bash';
  /**
   * The full task-KIND discriminator ('bash' | 'monitor' | 'agent') when the
   * manager exposes it. Richer than {@link taskType} (which collapses a monitor to
   * undefined): lets a programmatic caller distinguish a stopped internal monitor
   * from a stopped shell. Mirrors Claude Code's per-task-type kill taxonomy.
   */
  kind?: TaskKind;
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
    // PARITY (additive): explicit when-to-use + parameter/result contract guidance,
    // mirroring Claude Code's TaskStop prompt.ts so the model picks this tool at the
    // right moment. States when to reach for it, the single required parameter, and
    // that it returns a success or failure status.
    'Use this tool when you need to terminate a long-running background task. ' +
    'Takes a task_id parameter identifying the task to stop and returns a success or ' +
    'failure status. ' +
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
      // PARITY (additive): deprecated backward-compat alias for `task_id`, mirroring
      // Claude Code's KillShell `shell_id` parameter so an old transcript/SDK call
      // using the former name still resolves (id = task_id ?? shell_id).
      shell_id: {
        type: 'string',
        description: 'Deprecated: use task_id instead. Accepted as an alias for the task ID to stop.',
      },
      cleanup: {
        type: 'boolean',
        description:
          'When true, reap the task after stopping it: remove it from the task list and ' +
          'delete its output file. Defaults to false (the stopped task stays listed as "killed").',
      },
      interrupt: {
        type: 'boolean',
        description:
          'When true, SOFT-interrupt a running background AGENT instead of killing it: ' +
          'stop its current turn (abort the in-flight work) but keep the agent ALIVE so ' +
          "you can steer it with a follow-up send_message, rather than destroying it and " +
          'losing its context. Only valid for a running agent (a plain shell/monitor has ' +
          'no turn to interrupt). Defaults to false (a full kill).',
      },
      graceful: {
        type: 'boolean',
        description:
          'When true, request a COOPERATIVE graceful shutdown of a running background ' +
          'AGENT instead of a hard kill: the agent finishes its CURRENT turn and then ' +
          'stops, so its in-flight work is never truncated mid-thought. The tool waits ' +
          '(event-driven, bounded) for the agent to go idle before reporting. Only valid ' +
          'for a running agent; mutually exclusive with interrupt. Defaults to false.',
      },
      retry: {
        type: 'boolean',
        description:
          'When true, RETRY a single wedged WORKFLOW AGENT instead of killing it: ' +
          'abandon its stuck attempt and re-run just that agent, WITHOUT aborting the ' +
          'whole workflow. Use this to recover one hung agent in a still-running ' +
          'workflow rather than tearing down every agent. Only valid for a running ' +
          'workflow agent; mutually exclusive with interrupt/graceful/skip. Defaults to false.',
      },
      skip: {
        type: 'boolean',
        description:
          'When true, SKIP a single wedged WORKFLOW AGENT instead of killing it: ' +
          'settle just that agent immediately (as skipped) so the workflow continues ' +
          'with the remaining agents, WITHOUT aborting the whole run. Only valid for a ' +
          'running workflow agent; mutually exclusive with interrupt/graceful/retry. Defaults to false.',
      },
    },
    required: ['task_id'],
  },
  // PARITY (additive): structured-result schema describing the machine-readable
  // stop outcome AMI carries on `ToolResult.metadata` ({@link TaskKillMetadata}).
  // Mirrors Claude Code's TaskStopTool `outputSchema` (message / task_id /
  // task_type / command) so a resume/SDK/non-LLM caller can branch on the fields
  // without parsing the human message string. Object-rooted so it passes the
  // provider's object-root-only exposure guard (toolDefinitionsToOpenAI).
  outputSchema: {
    type: 'object',
    properties: {
      reason: {
        type: 'string',
        description:
          'Stable outcome discriminator present on every result: invalid_input | ' +
          'manager_unavailable | not_found | already_terminal | killed | kill_failed | ' +
          'interrupted | interrupt_unsupported | shutdown_requested | shutdown_unsupported | ' +
          'retried | retry_unsupported | skipped | skip_unsupported.',
      },
      taskId: { type: 'string', description: 'The task ID the stop targeted, when known.' },
      taskType: { type: 'string', description: "'agent' or 'bash' when the manager classifies it (parity: Claude's task_type)." },
      kind: { type: 'string', description: "Full task-kind discriminator ('bash' | 'monitor' | 'agent') when the manager exposes it." },
      command: { type: 'string', description: 'The FULL (untruncated) command/description of the stopped task.' },
      status: { type: 'string', description: 'Terminal status of the task (e.g. completed/failed/killed) when it was not running.' },
      pid: { type: 'number', description: 'OS process ID of the killed task, when known.' },
      exitCode: { type: ['number', 'null'], description: 'Exit code of an already-terminal task, when known.' },
    },
    required: ['reason'],
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
      return { output: `Error: unknown input key(s): ${unknown.join(', ')}. Allowed: task_id, cleanup, interrupt, graceful, retry, skip, shell_id. [code: invalid_input]`, isError: true, metadata: meta({ reason: 'invalid_input' }) };
    }

    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available. [code: manager_unavailable]', isError: true, metadata: meta({ reason: 'manager_unavailable' }) };
    }

    // FUNCTIONAL (parity): validate argument types up front instead of blindly
    // casting. An empty/whitespace or non-string task_id can never match a real
    // task; surfacing a clean [code: invalid_input] is clearer than a spurious
    // not_found and mirrors Claude Code rejecting a malformed kill request.
    //
    // PARITY (additive): coalesce `id = task_id ?? shell_id` — `shell_id` is the
    // deprecated Claude Code KillShell alias, accepted so an old transcript/SDK call
    // still resolves. `??` only falls back when task_id is null/undefined, so an
    // explicit (even malformed) task_id still takes precedence, matching Claude.
    if (input.shell_id !== undefined && typeof input.shell_id !== 'string') {
      return { output: 'Error: "shell_id" must be a string. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input' }) };
    }
    const rawId = input.task_id ?? input.shell_id;
    if (typeof rawId !== 'string' || rawId.trim() === '') {
      return { output: 'Error: "task_id" (or the deprecated "shell_id" alias) must be a non-empty string. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input' }) };
    }
    const taskId = rawId;

    if (input.cleanup !== undefined && typeof input.cleanup !== 'boolean') {
      return { output: 'Error: "cleanup" must be a boolean. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input', taskId }) };
    }
    const cleanup = input.cleanup === true;

    if (input.interrupt !== undefined && typeof input.interrupt !== 'boolean') {
      return { output: 'Error: "interrupt" must be a boolean. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input', taskId }) };
    }
    const interrupt = input.interrupt === true;

    if (input.graceful !== undefined && typeof input.graceful !== 'boolean') {
      return { output: 'Error: "graceful" must be a boolean. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input', taskId }) };
    }
    const graceful = input.graceful === true;

    if (input.retry !== undefined && typeof input.retry !== 'boolean') {
      return { output: 'Error: "retry" must be a boolean. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input', taskId }) };
    }
    const retry = input.retry === true;

    if (input.skip !== undefined && typeof input.skip !== 'boolean') {
      return { output: 'Error: "skip" must be a boolean. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input', taskId }) };
    }
    const skip = input.skip === true;

    // The per-agent recovery modes (interrupt / graceful / retry / skip) are
    // mutually exclusive — each is a distinct single action on one agent.
    if ([interrupt, graceful, retry, skip].filter(Boolean).length > 1) {
      return { output: 'Error: "interrupt", "graceful", "retry" and "skip" are mutually exclusive — pick one per-agent action. [code: invalid_input]', isError: true, metadata: meta({ reason: 'invalid_input', taskId }) };
    }

    const pm = context.processManager;
    const task = pm.get(taskId);

    if (!task) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks. [code: not_found]`, isError: true, metadata: meta({ reason: 'not_found', taskId }) };
    }

    // GRACEFUL COOPERATIVE SHUTDOWN (parity: Claude Code's requestTeammateShutdown +
    // leader-wait on onIdleCallbacks). Request the agent stop AFTER its current turn
    // (never truncating mid-thought), then wait — event-driven, bounded — for it to go
    // idle/terminal before reporting. Only a running in-process AGENT has turns; a
    // shell/monitor (or a terminal task) has none, so we refuse with a clear code.
    if (graceful) {
      const kind = typeof pm.taskKind === 'function' ? pm.taskKind(taskId) : undefined;
      if (task.status !== 'running' || kind !== 'agent') {
        return {
          output: `Error: task "${taskId}" cannot be gracefully shut down (only a running agent can — this is ${kind ?? 'unknown'}/${task.status}). Use task_kill without graceful to stop it. [code: shutdown_unsupported]`,
          isError: true,
          metadata: meta({ reason: 'shutdown_unsupported', taskId, kind, status: task.status }),
        };
      }
      const requested = typeof pm.requestShutdown === 'function' && pm.requestShutdown(taskId);
      if (!requested) {
        return { output: `Error: Failed to request graceful shutdown of task "${taskId}". [code: kill_failed]`, isError: true, metadata: meta({ reason: 'kill_failed', taskId, kind }) };
      }
      // Wait event-driven for the agent to finish its current turn and go idle, but
      // cap the wait so the tool can never hang on a long-running turn.
      if (typeof pm.waitForIdle === 'function') {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeout = new Promise<void>((resolve) => { timer = setTimeout(resolve, GRACEFUL_SHUTDOWN_WAIT_MS); timer.unref?.(); });
        await Promise.race([pm.waitForIdle(taskId), timeout]);
        if (timer) clearTimeout(timer);
      }
      const settled = typeof pm.getStatus === 'function' ? pm.getStatus(taskId) : undefined;
      const stoppedNote = settled && settled !== 'running'
        ? `Agent "${taskId}" gracefully shut down (${settled}).`
        : `Graceful shutdown requested for agent "${taskId}"; it will stop after its current turn.`;
      return {
        output: stoppedNote,
        isError: false,
        metadata: meta({ reason: 'shutdown_requested', taskId, taskType: 'agent', kind, command: (task.command || task.description) || undefined, status: settled }),
      };
    }

    // SOFT-INTERRUPT (parity: Claude Code's two-tier teammate abort — stop the
    // current turn but keep the agent alive). Only a running in-process AGENT has a
    // turn to interrupt; a shell/monitor (or a terminal task) has none, so we refuse
    // with a clear code instead of silently falling through to a hard kill.
    if (interrupt) {
      const kind = typeof pm.taskKind === 'function' ? pm.taskKind(taskId) : undefined;
      if (task.status !== 'running' || kind !== 'agent') {
        return {
          output: `Error: task "${taskId}" cannot be interrupted (only a running agent can be soft-interrupted; this is ${kind ?? 'unknown'}/${task.status}). Use task_kill without interrupt to stop it. [code: interrupt_unsupported]`,
          isError: true,
          metadata: meta({ reason: 'interrupt_unsupported', taskId, kind, status: task.status }),
        };
      }
      const didInterrupt = typeof pm.interruptAgentTurn === 'function' && pm.interruptAgentTurn(taskId);
      if (didInterrupt) {
        return {
          output: `Agent "${taskId}" current turn interrupted. The agent is still alive — send it a follow-up message (send_message) to steer it.`,
          isError: false,
          metadata: meta({ reason: 'interrupted', taskId, taskType: 'agent', kind, command: (task.command || task.description) || undefined }),
        };
      }
      return { output: `Error: Failed to interrupt task "${taskId}". [code: kill_failed]`, isError: true, metadata: meta({ reason: 'kill_failed', taskId, kind }) };
    }

    // PER-AGENT RETRY (parity: the reference workflow detail `onRetryAgent` →
    // retryWorkflowAgent(workflowId, agentId)). Abandon this ONE wedged workflow
    // agent's stuck attempt and re-run just it, leaving the rest of the workflow
    // untouched. Only a running workflow agent that registered a retry callback
    // supports it; anything else is refused with a clear code instead of a kill.
    if (retry) {
      const kind = typeof pm.taskKind === 'function' ? pm.taskKind(taskId) : undefined;
      if (task.status !== 'running' || kind !== 'agent' || typeof pm.retryAgentTask !== 'function') {
        return {
          output: `Error: task "${taskId}" cannot be retried (only a running workflow agent that supports per-agent retry can be; this is ${kind ?? 'unknown'}/${task.status}). Use task_kill without retry to stop it. [code: retry_unsupported]`,
          isError: true,
          metadata: meta({ reason: 'retry_unsupported', taskId, kind, status: task.status }),
        };
      }
      if (pm.retryAgentTask(taskId)) {
        return {
          output: `Workflow agent "${taskId}" is being retried — its stuck attempt was abandoned and the agent re-run; the rest of the workflow keeps running.`,
          isError: false,
          metadata: meta({ reason: 'retried', taskId, taskType: 'agent', kind, command: (task.command || task.description) || undefined }),
        };
      }
      // A running agent that refused retry registered no per-agent retry callback
      // (e.g. a non-workflow sub-agent) — report that rather than a kill failure.
      return {
        output: `Error: task "${taskId}" does not support per-agent retry (no recovery callback registered). Use task_kill without retry to stop it. [code: retry_unsupported]`,
        isError: true,
        metadata: meta({ reason: 'retry_unsupported', taskId, kind, status: task.status }),
      };
    }

    // PER-AGENT SKIP (parity: the reference workflow detail `onSkipAgent` →
    // skipWorkflowAgent(workflowId, agentId)). Settle this ONE wedged workflow
    // agent immediately (as skipped) so the workflow continues with the remaining
    // agents, without aborting the whole run.
    if (skip) {
      const kind = typeof pm.taskKind === 'function' ? pm.taskKind(taskId) : undefined;
      if (task.status !== 'running' || kind !== 'agent' || typeof pm.skipAgentTask !== 'function') {
        return {
          output: `Error: task "${taskId}" cannot be skipped (only a running workflow agent that supports per-agent skip can be; this is ${kind ?? 'unknown'}/${task.status}). Use task_kill without skip to stop it. [code: skip_unsupported]`,
          isError: true,
          metadata: meta({ reason: 'skip_unsupported', taskId, kind, status: task.status }),
        };
      }
      if (pm.skipAgentTask(taskId)) {
        return {
          output: `Workflow agent "${taskId}" skipped — it was settled as skipped and the rest of the workflow keeps running.`,
          isError: false,
          metadata: meta({ reason: 'skipped', taskId, taskType: 'agent', kind, command: (task.command || task.description) || undefined }),
        };
      }
      // A running agent that refused skip registered no per-agent skip callback.
      return {
        output: `Error: task "${taskId}" does not support per-agent skip (no recovery callback registered). Use task_kill without skip to stop it. [code: skip_unsupported]`,
        isError: true,
        metadata: meta({ reason: 'skip_unsupported', taskId, kind, status: task.status }),
      };
    }

    // PARITY (additive): the task-type discriminator (agent/bash) surfaced in the
    // structured result, when the manager exposes it (real ProcessManager; mock
    // stubs may omit it). Mirrors Claude's `task_type`.
    const taskType = typeof pm.taskType === 'function' ? pm.taskType(taskId) : undefined;
    // The full task-KIND discriminator (bash/monitor/agent) when available, so a
    // programmatic caller can tell a stopped monitor from a stopped shell.
    const kind = typeof pm.taskKind === 'function' ? pm.taskKind(taskId) : undefined;

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
      return { output: out, isError: false, metadata: meta({ reason: 'already_terminal', taskId, taskType, kind, command: fullLabel || undefined, status: task.status, exitCode: task.exitCode }) };
    }

    // suppressCompletion: this is an EXPLICIT model-initiated stop, so a bash
    // task's subsequent "exit code 137" completion is redundant noise (the model
    // learns the outcome from this very tool result). Latch it notified so the
    // engine's completion-drain doesn't re-announce it — parity with Claude Code's
    // stopTask. Agent tasks are unaffected (their abort completion carries the
    // partial result and still surfaces).
    const killed = pm.kill(taskId, { suppressCompletion: true });
    if (killed) {
      const out = `Task "${taskId}" (PID ${task.pid})${labelTag} killed successfully.` + maybeCleanup(pm, taskId, cleanup);
      return { output: out, isError: false, metadata: meta({ reason: 'killed', taskId, taskType, kind, command: fullLabel || undefined, pid: task.pid }) };
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
      return { output: out, isError: false, metadata: meta({ reason: 'already_terminal', taskId, taskType, kind, command: fullLabel || undefined, status: nowStatus }) };
    }

    return { output: `Error: Failed to kill task "${taskId}". [code: kill_failed]`, isError: true, metadata: meta({ reason: 'kill_failed', taskId, taskType, kind, command: fullLabel || undefined }) };
  },
};
