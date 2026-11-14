import { ToolDefinition, ToolResult } from '../types';
import { getScheduler } from '../cron';

const ID_RE = /^[\w-]+$/;

export const cronDeleteTool: ToolDefinition = {
  name: 'cron_delete',
  description:
    'Cancel a scheduled cron job by its ID (as shown by cron_list). ' +
    'You can only delete jobs you own; a job created by another teammate is rejected.',
  inputSchema: {
    type: 'object',
    properties: {
      id: {
        type: 'string',
        description: 'The job ID returned by cron_create / shown by cron_list.',
      },
    },
    required: ['id'],
  },
  isReadOnly: false,

  async execute(input, context): Promise<ToolResult> {
    // Strict input: reject unknown keys.
    const unknown = Object.keys(input).filter(k => k !== 'id');
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}.`, isError: true };
    }
    const id = input.id as string;
    if (!id || typeof id !== 'string' || !ID_RE.test(id)) {
      return { output: 'Error: id is required and must match ^[\\w-]+$.', isError: true, metadata: { errorCode: 1 } };
    }

    const sched = getScheduler(context.cwd);
    let deleted: boolean;
    try {
      // Cross-agent authorization: deleting another teammate's job is refused.
      deleted = sched.delete(id, context.agentId);
    } catch (err) {
      return {
        output: `Error: ${err instanceof Error ? err.message : String(err)}`,
        isError: true,
        metadata: { errorCode: 2 },
      };
    }

    if (!deleted) {
      return { output: `Error: job "${id}" not found.`, isError: true, metadata: { errorCode: 1 } };
    }

    return { output: `Deleted job ${id}.` };
  },
};
