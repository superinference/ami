import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const taskOutputTool: ToolDefinition = {
  name: 'task_output',
  description:
    'Get status and output from a background task. Returns the task status, exit code (if finished), ' +
    'elapsed time, and the tail of stdout/stderr output.',
  inputSchema: {
    type: 'object',
    properties: {
      task_id: {
        type: 'string',
        description: 'The task ID returned by bash with run_in_background.',
      },
      tail: {
        type: 'number',
        description: 'Number of lines from the end of output to return. Defaults to 50.',
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

    const result = context.processManager.getOutput(taskId, tail);
    if (!result) {
      return { output: `Error: No background task found with ID "${taskId}". Use task_list to see all tasks.`, isError: true };
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
