import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const syntheticOutputTool: ToolDefinition = {
  name: 'structured_output',
  description: 'Return structured JSON output matching a specific schema. Use when you need to produce machine-parseable results.',
  inputSchema: {
    type: 'object',
    properties: {
      schema_name: { type: 'string', description: 'Name/label for the output schema.' },
      data: { type: 'object', description: 'The structured JSON data to return.' },
    },
    required: ['data'],
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>): Promise<ToolResult> {
    const data = input.data;
    try {
      return { output: JSON.stringify(data, null, 2) };
    } catch (err) {
      return { output: `Error serializing structured output: ${err}`, isError: true };
    }
  },
};
