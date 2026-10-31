import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { coerceBool } from '../utils/coerce';

/** Default cap on how long task_output({wait:true}) blocks for a task to finish. */
const DEFAULT_WAIT_TIMEOUT_MS = 10 * 60 * 1000;
const WAIT_POLL_INTERVAL_MS = 400;

export const taskOutputTool: ToolDefinition = {
  name: 'task_output',
  description:
    'Get status and output from a background task (task/agent/bash). Returns status, exit code (if finished), ' +
    'elapsed time, and the tail of output. Set wait:true to BLOCK until the task finishes and return its final ' +
    'result in a single call — prefer this over repeatedly polling (it is read-only, so several task_output(wait) ' +
    'calls in one turn run concurrently and all return together when their tasks complete).',
  inputSchema: {
    type: 'object',
    properties: {
      task_id: {
        type: 'string',
        description: 'The task ID returned by bash/task with run_in_background.',
      },
      tail: {
        type: 'number',
        description: 'Number of lines from the end of output to return. Defaults to 50.',
      },
      wait: {
        type: 'boolean',
        description: 'Block until the task reaches a terminal state (completed/failed/killed), then return its final output. Avoids a poll loop.',
      },
      timeout_ms: {
        type: 'number',
        description: 'Max time to block when wait:true (default 600000 = 10 min).',
      },
    },
    required: ['task_id'],
  },
  isReadOnly: true,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available.', isError: true };
    }

    const taskId = input.task_id as string;
    const tail = (input.tail as number) ?? 50;
    const wait = coerceBool(input.wait);
    const waitTimeoutMs = typeof input.timeout_ms === 'number' && input.timeout_ms > 0
      ? input.timeout_ms : DEFAULT_WAIT_TIMEOUT_MS;

    let result = context.processManager.getOutput(taskId, tail);
    if (!result) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks.`, isError: true };
    }

    // Blocking wait: poll internally until terminal or timeout/abort, so the model
    // spends ONE tool call instead of a multi-turn poll loop (each parent turn is
    // a full model round-trip). task_output is read-only, so several wait calls in
    // one turn run concurrently and all settle when their tasks finish.
    if (wait && result.status === 'running') {
      const deadline = Date.now() + waitTimeoutMs;
      while (result && result.status === 'running' && Date.now() < deadline) {
        if (context.abortSignal?.aborted) break;
        await new Promise(r => setTimeout(r, WAIT_POLL_INTERVAL_MS));
        result = context.processManager.getOutput(taskId, tail);
      }
      if (!result) {
        return { output: `Error: background task "${taskId}" disappeared while waiting.`, isError: true };
      }
    }

    const elapsed = result.elapsedMs < 1000
      ? `${result.elapsedMs}ms`
      : result.elapsedMs < 60000
        ? `${(result.elapsedMs / 1000).toFixed(1)}s`
        : `${(result.elapsedMs / 60000).toFixed(1)}m`;

    const lines = [
      `Task: ${taskId}`,
      `Status: ${result.status}`,
      `Elapsed: ${elapsed}`,
    ];

    if (result.exitCode !== null) {
      lines.push(`Exit code: ${result.exitCode}`);
    }

    if (result.output.trim()) {
      lines.push('', '--- Output (last ' + tail + ' lines) ---', result.output.trim());
    } else {
      lines.push('', '(no output yet)');
    }

    return { output: lines.join('\n'), isError: false };
  },
};
