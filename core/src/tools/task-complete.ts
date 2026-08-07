import { ToolDefinition, ToolResult } from '../types';

export const taskCompleteTool: ToolDefinition = {
  name: 'task_complete',
  description:
    'Signal that the task is complete. Call this when your fix is correct and all tests pass. ' +
    'After calling this tool, produce a final text summary of what you changed. Do not make any more tool calls.',
  inputSchema: {
    type: 'object',
    properties: {
      summary: {
        type: 'string',
        description: 'Brief summary of what was done and what changed.',
      },
    },
    required: ['summary'],
  },
  isReadOnly: true,

  async execute(input: Record<string, unknown>): Promise<ToolResult> {
    const summary = (input.summary as string) || 'Task complete.';
    return { output: `Task complete. Summary: ${summary}\n\nProduce your final response now. Do not make any more tool calls.`, isError: false };
  },
};
