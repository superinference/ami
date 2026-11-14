import { ToolDefinition, ToolResult, ToolContext } from '../types';

const ALLOWED_KEYS = new Set(['task_id']);

export const taskKillTool: ToolDefinition = {
  name: 'task_kill',
  description:
    'Stop a running background task by its ID. Kills the process and its children. ' +
    'Concurrency-safe. Returns the stopped task\'s command/description and a machine-readable ' +
    'error code ([code: not_found] / [code: kill_failed]) on failure.',
  inputSchema: {
    type: 'object',
    properties: {
      task_id: {
        type: 'string',
        description: 'The task ID to stop.',
      },
    },
    required: ['task_id'],
  },
  isReadOnly: false,

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // Strict schema: reject unexpected/injected parameters.
    const unknown = Object.keys(input).filter(k => !ALLOWED_KEYS.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown input key(s): ${unknown.join(', ')}. Allowed: task_id.`, isError: true };
    }

    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available.', isError: true };
    }

    const taskId = input.task_id as string;
    const task = context.processManager.get(taskId);

    if (!task) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks. [code: not_found]`, isError: true };
    }

    // Echo the command/description so the model can confirm what it stopped.
    const label = task.command || task.description || '';
    const labelTag = label ? ` (${label})` : '';

    if (task.status !== 'running') {
      return { output: `Task "${taskId}"${labelTag} is already ${task.status} (exit code: ${task.exitCode}).`, isError: false };
    }

    const killed = context.processManager.kill(taskId);
    if (killed) {
      return { output: `Task "${taskId}" (PID ${task.pid})${labelTag} killed successfully.`, isError: false };
    }

    return { output: `Error: Failed to kill task "${taskId}". [code: kill_failed]`, isError: true };
  },
};
