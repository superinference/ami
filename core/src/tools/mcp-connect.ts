import { ToolDefinition, ToolResult, ToolContext } from '../types';

export function createMcpConnectTool(serverName: string, description: string): ToolDefinition {
  const normalizedServer = serverName.replace(/[^a-zA-Z0-9_]/g, '_');

  return {
    name: `mcp__${normalizedServer}__connect`,
    description: `Connect to MCP server '${serverName}': ${description}. This starts the server (pulling the container image if needed) and discovers available tools.`,
    inputSchema: {
      type: 'object',
      properties: {},
    } as any,
    isReadOnly: true,
    async execute(_input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = context._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };

      try {
        await mcpManager.ensureConnected(serverName);
        const tools = mcpManager.getToolsForServer(serverName);
        const toolNames = tools.map((t: { name: string }) => t.name);
        return {
          output: `Connected to MCP server '${serverName}'. ${tools.length} tools available: ${toolNames.join(', ')}`,
        };
      } catch (err) {
        return {
          output: `Failed to connect to MCP server '${serverName}': ${err instanceof Error ? err.message : String(err)}`,
          isError: true,
        };
      }
    },
  };
}
