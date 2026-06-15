import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';

export const cronDeleteTool: ToolDefinition = {
  name: 'cron_delete',
  description: 'Cancel a scheduled cron job by its ID.',
  inputSchema: {
    type: 'object',
    properties: {
      id: {
        type: 'string',
        description: 'The job ID returned by cron_create.',
      },
    },
    required: ['id'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    const id = input.id as string;
    if (!id) return { output: 'Error: id is required.', isError: true };

    const sched = getScheduler(context.cwd);
    const deleted = sched.delete(id);

    if (!deleted) {
      return { output: `Error: job "${id}" not found.`, isError: true };
    }

    return { output: `Deleted job ${id}.` };
  },
};
