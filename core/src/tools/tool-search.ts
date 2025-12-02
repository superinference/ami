import { ToolDefinition, ToolContext, ToolResult } from '../types';

let _allTools: ToolDefinition[] = [];

export function setSearchableTools(tools: ToolDefinition[]): void {
  _allTools = tools;
}

export const toolSearchTool: ToolDefinition = {
  name: 'tool_search',
  description:
    'Search for available tools by keyword. Use when you need a tool that is not in your current set. Use "select:tool_name" to get the full schema for a specific tool.',
  inputSchema: {
    type: 'object',
    properties: {
      query: {
        type: 'string',
        description: 'Keywords describing the tool you need, or "select:tool_name" to load a specific tool\'s full schema.',
      },
    },
    required: ['query'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    _context: ToolContext,
  ): Promise<ToolResult> {
    const query = (input.query as string || '').trim();

    if (!query) {
      return { output: 'Error: query must not be empty.', isError: true };
    }

    if (query.startsWith('select:')) {
      const toolName = query.slice('select:'.length).trim();
      const tool = _allTools.find(t => t.name === toolName);
      if (!tool) {
        return { output: `Tool "${toolName}" not found. Available: ${_allTools.map(t => t.name).join(', ')}`, isError: true };
      }
      return {
        output: JSON.stringify({
          name: tool.name,
          description: tool.description,
          inputSchema: tool.inputSchema,
          isReadOnly: tool.isReadOnly,
        }, null, 2),
        isError: false,
      };
    }

    const keywords = query.toLowerCase().split(/\s+/);
    const scored = _allTools.map(tool => {
      let score = 0;
      const text = `${tool.name} ${tool.description}`.toLowerCase();
      for (const kw of keywords) {
        if (text.includes(kw)) score++;
        if (tool.name.includes(kw)) score += 2;
      }
      return { tool, score };
    })
    .filter(s => s.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 5);

    if (scored.length === 0) {
      return {
        output: `No tools matched "${query}". Available: ${_allTools.map(t => t.name).join(', ')}`,
        isError: false,
      };
    }

    const results = scored.map(s =>
      `- **${s.tool.name}** — ${s.tool.description.slice(0, 100)}${s.tool.description.length > 100 ? '...' : ''}`
    ).join('\n');

    return {
      output: `Tools matching "${query}":\n\n${results}\n\nUse tool_search with "select:tool_name" to get the full schema.`,
      isError: false,
    };
  },
};
