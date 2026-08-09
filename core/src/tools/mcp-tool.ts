import { ToolDefinition, ToolResult, ToolContext } from '../types';

export function createMcpTool(serverName: string, toolName: string, description: string, inputSchema: Record<string, unknown>): ToolDefinition {
  const normalizedServer = serverName.replace(/[^a-zA-Z0-9_]/g, '_');
  const normalizedTool = toolName.replace(/[^a-zA-Z0-9_]/g, '_');

  return {
    name: `mcp__${normalizedServer}__${normalizedTool}`,
    description: `[MCP:${serverName}] ${description}`,
    inputSchema: inputSchema as any,
    isReadOnly: false,
    async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = context._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };
      const MCP_TOOL_TIMEOUT = 100_000_000; // ~27.8 hours
      let timer: ReturnType<typeof setTimeout>;
      const timeoutPromise = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('MCP tool timeout')), MCP_TOOL_TIMEOUT);
        timer.unref();
      });
      try {
        const result = await Promise.race([
          mcpManager.callTool(serverName + ':' + toolName, input),
          timeoutPromise,
        ]);
        clearTimeout(timer!);
        const text = typeof result === 'string' ? result : JSON.stringify(result, null, 2);
        return { output: text.slice(0, 100_000) };
      } catch (err) {
        clearTimeout(timer!);
        return { output: `MCP tool error: ${err instanceof Error ? err.message : String(err)}`, isError: true };
      }
    },
  };
}
