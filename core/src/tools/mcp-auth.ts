import { ToolDefinition, ToolResult, ToolContext } from '../types';

export function createMcpAuthTool(serverName: string): ToolDefinition {
  const normalized = serverName.replace(/[^a-zA-Z0-9_]/g, '_');
  return {
    name: `mcp__${normalized}__authenticate`,
    description: `Authenticate with MCP server "${serverName}". Call this when the server requires authentication.`,
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
    async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = (context as any)._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };
      try {
        await mcpManager.reconnect?.(serverName);
        return { output: `Authentication completed for "${serverName}". Server tools should now be available.` };
      } catch (err) {
        return { output: `Authentication failed: ${err instanceof Error ? err.message : String(err)}. Configure credentials in .superinference/mcp.json.`, isError: true };
      }
    },
  };
}
