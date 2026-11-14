import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { redactSecrets } from './tool-utils';

/** Transports that can carry an OAuth browser-consent flow. */
const OAUTH_TRANSPORTS = new Set(['sse', 'http']);

/** Structured status surfaced to the model via ToolResult.metadata. */
export type McpAuthStatus = 'ok' | 'unsupported' | 'error';

/** The minimal server config the auth tool needs to reason about OAuth. */
export interface McpAuthServerConfig {
  transport?: 'stdio' | 'sse' | 'http';
  url?: string;
}

/**
 * Build the per-server `authenticate` pseudo-tool. It is NOT read-only and NOT
 * concurrency-safe: it mutates connection/credential state and drives an
 * interactive browser-consent flow, so it must run exclusively and be gated by
 * permissions like any other side-effecting tool. `isMcp` marks it for the
 * MCP schema-budget accounting.
 *
 * Behavior:
 * - stdio (or any non-sse/http) transports cannot do OAuth → a distinct
 *   `unsupported` status with actionable guidance, never a false success.
 * - a reconnect that actually (re)establishes the session reports `ok`; a
 *   no-op on an already-ready server says so rather than claiming it just
 *   authenticated.
 * - failures are reported with the reason scrubbed of any secret material
 *   (tokens/keys) via the shared redactor before it reaches the model.
 *
 * The config is read at execute time from the manager when not supplied, so it
 * reflects the live server configuration.
 */
export function createMcpAuthTool(serverName: string, config?: McpAuthServerConfig): ToolDefinition {
  const normalized = serverName.replace(/[^a-zA-Z0-9_]/g, '_');
  return {
    name: `mcp__${normalized}__authenticate`,
    description:
      `Authenticate with the MCP server "${serverName}" via its OAuth 2.1 flow. ` +
      `Call this when the server's tools report that authentication is required. ` +
      `Only http/SSE servers support OAuth; stdio servers do not. A browser window ` +
      `opens for consent (with a manual callback-URL paste fallback for headless shells); ` +
      `once complete the server's tools become available.`,
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: false,
    isConcurrencySafe: false,
    isMcp: true,
    async execute(_input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const mcpManager = (context as unknown as { _mcpManager?: {
        reconnect?: (name: string) => Promise<void>;
        getServerConfig?: (name: string) => McpAuthServerConfig | undefined;
        getServerStatus?: (name: string) => { state: string } | null;
        getPendingAuthUrl?: (name: string) => string | undefined;
      } })._mcpManager;
      if (!mcpManager) return { output: 'Error: MCP is not initialized.', isError: true, metadata: { status: 'error' satisfies McpAuthStatus } };

      const cfg = config ?? mcpManager.getServerConfig?.(serverName);
      const transport = cfg?.transport ?? 'stdio';
      if (!OAUTH_TRANSPORTS.has(transport)) {
        return {
          output: `MCP server "${serverName}" uses the "${transport}" transport, which does not support OAuth authentication. ` +
            `OAuth is only available for http/SSE servers. Configure credentials directly (e.g. env vars in .ami/mcp.json) instead.`,
          isError: true,
          metadata: { status: 'unsupported' satisfies McpAuthStatus, transport },
        };
      }

      const before = mcpManager.getServerStatus?.(serverName)?.state;
      try {
        await mcpManager.reconnect?.(serverName);
      } catch (err) {
        const reason = redactSecrets(err instanceof Error ? err.message : String(err));
        // Surface the authorization URL (if the flow reached the consent leg) so a
        // detached/headless agent can relay it to the user to complete auth out of
        // band, instead of only reporting failure (H46/H48/H49).
        const authUrl = mcpManager.getPendingAuthUrl?.(serverName);
        const urlLine = authUrl
          ? ` To authorize, open this URL in a browser and complete consent, then re-run authenticate:\n${authUrl}`
          : ` If a browser did not open, re-run and paste the callback URL when prompted, or check the server's OAuth config in .ami/mcp.json.`;
        return {
          output: `Authentication failed for "${serverName}": ${reason}.${urlLine}`,
          isError: true,
          metadata: { status: 'error' satisfies McpAuthStatus, transport, ...(authUrl ? { auth_url: authUrl } : {}) },
        };
      }
      const after = mcpManager.getServerStatus?.(serverName)?.state;
      const reconnected = after === 'ready' && before !== 'ready';
      return {
        output: reconnected
          ? `Authentication completed for "${serverName}"; its tools are now available.`
          : `"${serverName}" is already connected — no re-authentication was needed.`,
        metadata: { status: 'ok' satisfies McpAuthStatus, transport, reconnected },
      };
    },
  };
}
