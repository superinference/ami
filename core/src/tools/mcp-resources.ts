import { ToolDefinition, ToolResult, ToolContext } from '../types';

export const listMcpResourcesTool: ToolDefinition = {
  name: 'ListMcpResources',
  description: 'List available resources from MCP servers.',
  inputSchema: {
    type: 'object',
    properties: {
      server: { type: 'string', description: 'Optional server name filter.' },
    },
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const mcpManager = context._mcpManager;
    if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };
    try {
      const resources = await mcpManager.listResources(input.server as string | undefined);
      if (!resources || resources.length === 0) return { output: 'No MCP resources available.' };
      return { output: resources.map((r: any) => `${r.uri} — ${r.name ?? ''} (${r.mimeType ?? 'unknown'})`).join('\n') };
    } catch (err) {
      return { output: `Error listing MCP resources: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
  },
};

export const readMcpResourceTool: ToolDefinition = {
  name: 'ReadMcpResource',
  description: 'Read a specific resource from an MCP server by URI.',
  inputSchema: {
    type: 'object',
    properties: {
      server: { type: 'string', description: 'MCP server name.' },
      uri: { type: 'string', description: 'Resource URI to read.' },
    },
    required: ['server', 'uri'],
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const mcpManager = context._mcpManager;
    if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };
    try {
      const result = await mcpManager.readResource(input.server as string, input.uri as string);
      return { output: typeof result === 'string' ? result.slice(0, 100_000) : JSON.stringify(result).slice(0, 100_000) };
    } catch (err) {
      return { output: `Error reading MCP resource: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
  },
};
