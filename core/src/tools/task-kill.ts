import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const taskKillTool: ToolDefinition = {
  name: 'task_kill',
  description:
    'Stop a running background task by its ID. Kills the process and its children.',
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
    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available.', isError: true };
    }

    const taskId = input.task_id as string;
    const task = context.processManager.get(taskId);

    if (!task) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks.`, isError: true };
    }

    if (task.status !== 'running') {
      return { output: `Task "${taskId}" is already ${task.status} (exit code: ${task.exitCode}).`, isError: false };
    }

    const killed = context.processManager.kill(taskId);
    if (killed) {
      return { output: `Task "${taskId}" (PID ${task.pid}) killed successfully.`, isError: false };
    }

    return { output: `Error: Failed to kill task "${taskId}".`, isError: true };
  },
};
