import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { normalizeMcpNameSegment } from './mcp-tool';
import { errorMessage } from '../credential-guard';


/** Does a (re)connect failure look like the server demanding authentication? */
export function isAuthRequiredError(message: string): boolean {
  return /\b401\b|\b403\b|unauthorized|authentication required|requires? auth|www-authenticate|\boauth\b|not authenticated|login required/i.test(message);
}

/** Rejected by the abort race so a cancelled connect is distinguishable from a
 * genuine connection failure via identity (never leaks into user-facing text). */
const MCP_CONNECT_ABORTED = Symbol('mcp-connect-aborted');

export function createMcpConnectTool(serverName: string, description: string): ToolDefinition {
  const normalizedServer = normalizeMcpNameSegment(serverName);

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

      // Honor a user/engine interrupt during a slow connect (a container image
      // pull can hold the manager's 120s timeout open) instead of blocking until
      // that internal timeout fires. Mirrors the abort race in mcp-tool.ts.
      const abortSignal = context.abortSignal;
      const aborted = (): ToolResult => ({
        output: `Connection to MCP server '${serverName}' was cancelled.`,
        isError: true,
        metadata: { status: 'aborted' },
      });
      if (abortSignal?.aborted) return aborted();

      let onAbort: (() => void) | undefined;
      const abortPromise = new Promise<never>((_, reject) => {
        if (!abortSignal) return;
        onAbort = () => reject(MCP_CONNECT_ABORTED);
        abortSignal.addEventListener('abort', onAbort, { once: true });
      });

      try {
        await Promise.race([mcpManager.ensureConnected(serverName), abortPromise]);
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
        // A cancelled connect is reported as such, not as a connection failure.
        if (err === MCP_CONNECT_ABORTED) {
          // Actively tear down the connection the manager may still be
          // establishing in the background, mirroring the reference's
          // transport.close()/inProcessServer.close() on an aborted connect.
          // Without this, abandoning the ensureConnected() promise leaves the
          // half-open transport (and any spawned child process / container /
          // network socket) open until the manager's internal timeout (120s for
          // container servers) fires. stopServer() disconnects the client and
          // marks the disconnect intentional so it is not auto-reconnected.
          // Best-effort: teardown must never mask the cancellation result.
          if (typeof mcpManager.stopServer === 'function') {
            try {
              await mcpManager.stopServer(serverName);
            } catch {
              /* teardown is best-effort — the connect is already cancelled */
            }
          }
          return aborted();
        }
        const message = errorMessage(err);
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
      } finally {
        if (onAbort && abortSignal) abortSignal.removeEventListener('abort', onAbort);
      }
    },
  };
}
