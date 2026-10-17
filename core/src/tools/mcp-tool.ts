import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { spillToolText } from './spill-output';

/** Max tool-name length accepted by the OpenAI/Anthropic APIs. A qualified MCP
 *  name longer than this poisons every request, so it must be shortened. */
export const MAX_MCP_TOOL_NAME_LEN = 128;

/** Deterministic, dependency-free FNV-1a hash → base36 (collision-resistant
 *  enough to disambiguate truncated tool names within a session). */
function shortHash(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(36);
}

/** Build the qualified MCP tool name, truncating with a hash suffix so it never
 *  exceeds the API tool-name length limit. */
export function qualifiedMcpToolName(serverName: string, toolName: string): string {
  const normalizedServer = serverName.replace(/[^a-zA-Z0-9_]/g, '_');
  const normalizedTool = toolName.replace(/[^a-zA-Z0-9_]/g, '_');
  const full = `mcp__${normalizedServer}__${normalizedTool}`;
  if (full.length <= MAX_MCP_TOOL_NAME_LEN) return full;
  const suffix = '_' + shortHash(full);
  return full.slice(0, MAX_MCP_TOOL_NAME_LEN - suffix.length) + suffix;
}

export function createMcpTool(serverName: string, toolName: string, description: string, inputSchema: Record<string, unknown>): ToolDefinition {
  return {
    name: qualifiedMcpToolName(serverName, toolName),
    description: `[MCP:${serverName}] ${description}`,
    inputSchema: inputSchema as any,
    isReadOnly: false,
    async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = context._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP not initialized', isError: true };

      if (mcpManager.isPending?.(serverName)) {
        try {
          await mcpManager.ensureConnected(serverName);
        } catch (err) {
          return { output: `Failed to start MCP server '${serverName}': ${err instanceof Error ? err.message : String(err)}`, isError: true };
        }
      }

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
        return { output: spillToolText(context.cwd, 'mcp', text, 100_000) };
      } catch (err) {
        clearTimeout(timer!);
        return { output: `MCP tool error: ${err instanceof Error ? err.message : String(err)}`, isError: true };
      }
    },
  };
}
