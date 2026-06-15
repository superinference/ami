import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const briefTool: ToolDefinition = {
  name: 'brief',
  description: 'Provide a one-sentence summary of the current conversation or a specific topic. Use for quick status checks.',
  inputSchema: {
    type: 'object',
    properties: {
      topic: { type: 'string', description: 'Optional topic to summarize. If omitted, summarizes the conversation.' },
    },
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, _context: ToolContext): Promise<ToolResult> {
    const topic = (input.topic as string) || 'current conversation';
    return { output: `Summarize "${topic}" in exactly one sentence. Be concise and factual.` };
  },
};
