import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { spillToolText } from './spill-output';
import { boundMcpResponse } from '../mcp/client';

/** Default MCP tool call timeout: 5 minutes (300 000 ms). */
export const DEFAULT_MCP_TOOL_TIMEOUT_MS = 300_000;

/** Read the MCP tool timeout from the environment, falling back to the default.
 *  Invalid or non-positive values are silently replaced by the default. */
export function getMcpToolTimeout(): number {
  const raw = process.env.SUPERINFERENCE_MCP_TOOL_TIMEOUT_MS;
  if (raw === undefined || raw === '') return DEFAULT_MCP_TOOL_TIMEOUT_MS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return DEFAULT_MCP_TOOL_TIMEOUT_MS;
  return n;
}

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

      if (mcpManager.isDisabled?.(serverName)) {
        return { output: `MCP server '${serverName}' is disabled. Use /mcp start ${serverName} to re-enable it.`, isError: true };
      }

      if (mcpManager.isPending?.(serverName)) {
        try {
          await mcpManager.ensureConnected(serverName);
        } catch (err) {
          return { output: `Failed to start MCP server '${serverName}': ${err instanceof Error ? err.message : String(err)}`, isError: true };
        }
      }

      // Track whether an elicitation notification was received during this call
      let gotElicitation = false;
      const elicitListener = (_sn: string, method: string) => {
        if ((method === 'elicitation/create' || method === 'notifications/elicitation') && _sn === serverName) {
          gotElicitation = true;
        }
      };
      mcpManager.on?.('notification', elicitListener);

      const timeoutMs = getMcpToolTimeout();
      let timer: ReturnType<typeof setTimeout>;
      const timeoutPromise = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(
          `MCP tool call timed out after ${Math.round(timeoutMs / 1000)} seconds. The MCP server may be unresponsive.`
        )), timeoutMs);
        timer.unref();
      });
      try {
        const result = await Promise.race([
          mcpManager.callTool(serverName + ':' + toolName, input),
          timeoutPromise,
        ]);
        clearTimeout(timer!);
        mcpManager.removeListener?.('notification', elicitListener);
        const raw = typeof result === 'string' ? result : JSON.stringify(result, null, 2);
        const maxResultChars = extractMaxResultSizeChars(result);
        const bounded = boundMcpResponse(raw, maxResultChars);
        const spillCap = maxResultChars ? Math.min(maxResultChars, 500_000) : 100_000;
        return { output: spillToolText(context.cwd, 'mcp', bounded.content, spillCap), metadata: bounded.truncated ? { mcpTruncated: true, mcpOriginalBytes: bounded.originalBytes } : undefined };
      } catch (err) {
        clearTimeout(timer!);
        mcpManager.removeListener?.('notification', elicitListener);
        if (gotElicitation && context.detachedMode) {
          return { output: 'MCP tool call timed out because the server requested user input (elicitation), but no user was available to answer in non-interactive mode. Do not retry this tool call.', isError: true };
        }
        return { output: `MCP tool error: ${err instanceof Error ? err.message : String(err)}`, isError: true };
      }
    },
  };
}

const MAX_RESULT_SIZE_CAP = 500_000;

export function extractMaxResultSizeChars(result: unknown): number | undefined {
  if (result && typeof result === 'object' && !Array.isArray(result)) {
    const meta = (result as Record<string, unknown>)._meta as Record<string, unknown> | undefined;
    if (meta) {
      const val = meta['anthropic/maxResultSizeChars'] ?? meta['maxResultSizeChars'];
      if (typeof val === 'number' && val > 0) {
        return Math.min(val, MAX_RESULT_SIZE_CAP);
      }
    }
  }
  return undefined;
}
