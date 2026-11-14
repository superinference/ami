import { ToolDefinition, ToolResult, ToolContext } from '../types';

/** Does a (re)connect failure look like the server demanding authentication? */
export function isAuthRequiredError(message: string): boolean {
  return /\b401\b|\b403\b|unauthorized|authentication required|requires? auth|www-authenticate|\boauth\b|not authenticated|login required/i.test(message);
}

export function createMcpConnectTool(serverName: string, description: string): ToolDefinition {
  const normalizedServer = serverName.replace(/[^a-zA-Z0-9_]/g, '_');

  return {
    name: `mcp__${normalizedServer}__connect`,
    description: `Connect to MCP server '${serverName}': ${description}. This starts the server (pulling the container image if needed) and discovers available tools.`,
    inputSchema: {
      type: 'object',
      properties: {},
    } as ToolDefinition['inputSchema'],
    isReadOnly: true,
    async execute(_input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = context._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };

      try {
        await mcpManager.ensureConnected(serverName);
        const tools = mcpManager.getToolsForServer(serverName);
        const toolNames = tools.map((t: { name: string }) => t.name);
        if (tools.length === 0) {
          return {
            output: `Connected to MCP server '${serverName}', but it currently exposes no tools.`,
            metadata: { status: 'connected', toolCount: 0 },
          };
        }
        return {
          output: `Connected to MCP server '${serverName}'. ${tools.length} ${tools.length === 1 ? 'tool' : 'tools'} available: ${toolNames.join(', ')}`,
          metadata: { status: 'connected', toolCount: tools.length },
        };
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        // Discriminate a needs-authentication outcome from a hard failure so the
        // model is steered to the authenticate tool instead of blindly retrying.
        if (isAuthRequiredError(message)) {
          return {
            output: `MCP server '${serverName}' requires authentication. Run the mcp__${normalizedServer}__authenticate tool to sign in, then retry connecting.`,
            isError: true,
            metadata: { status: 'needs-auth' },
          };
        }
        return {
          output: `Failed to connect to MCP server '${serverName}': ${message}`,
          isError: true,
          metadata: { status: 'error' },
        };
      }
    },
  };
}
