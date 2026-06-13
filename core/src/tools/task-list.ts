import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const taskListTool: ToolDefinition = {
  name: 'task_list',
  description:
    'List all background tasks with their status, PID, elapsed time, and command.',
  inputSchema: {
    type: 'object',
    properties: {},
  },
  isReadOnly: true,

  async execute(_input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    if (!context.processManager) {
      return { output: 'Error: Background process manager is not available.', isError: true };
    }

    const tasks = context.processManager.list();

    if (tasks.length === 0) {
      return { output: 'No background tasks.', isError: false };
    }

    const lines = tasks.map(t => {
      const elapsed = Date.now() - t.startTime;
      const time = elapsed < 1000
        ? `${elapsed}ms`
        : elapsed < 60000
          ? `${(elapsed / 1000).toFixed(0)}s`
          : `${(elapsed / 60000).toFixed(1)}m`;

      const status = t.status === 'running'
        ? '▶ running'
        : t.status === 'completed'
          ? `✓ completed (exit ${t.exitCode})`
          : t.status === 'killed'
            ? '✗ killed'
            : `✗ failed (exit ${t.exitCode})`;

      return `${t.taskId}  PID ${t.pid}  ${status}  ${time}  ${t.description}`;
    });

    return { output: `Background tasks (${tasks.length}):\n${lines.join('\n')}`, isError: false };
  },
};
