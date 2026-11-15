import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { redactSecrets } from './tool-utils';

/** Transports that can carry an OAuth browser-consent flow. */
const OAUTH_TRANSPORTS = new Set(['sse', 'http']);

/** Structured status surfaced to the model via ToolResult.metadata. */
export const MCP_AUTH_STATUS = ['ok', 'unsupported', 'error'] as const;
export type McpAuthStatus = (typeof MCP_AUTH_STATUS)[number];

/**
 * Coarse classification of a failed authentication attempt, surfaced via
 * `ToolResult.metadata.error_kind` so the model can decide whether to re-auth
 * (`auth`), wait/retry (`network`), fix configuration (`config`), or escalate
 * (`unknown`) — mirroring Claude Code's `/mcp` auth-state surfacing instead of
 * lumping every failure into one opaque bucket.
 */
export const MCP_AUTH_ERROR_KINDS = ['auth', 'network', 'config', 'unknown'] as const;
export type McpAuthErrorKind = (typeof MCP_AUTH_ERROR_KINDS)[number];

/** Query-parameter names whose VALUES are credentials and must never reach the model. */
const SENSITIVE_URL_PARAMS =
  'access_token|refresh_token|id_token|token|client_secret|secret|code|password|api_?key|assertion|client_assertion|private_?key';
// eslint-disable-next-line security/detect-non-literal-regexp -- built from a fixed constant, not user input
const SENSITIVE_URL_PARAM_RE = new RegExp(`([?&](?:${SENSITIVE_URL_PARAMS})=)[^&#\\s]+`, 'gi');

/**
 * Redact credential-bearing query-parameter VALUES from an authorization URL
 * before it is surfaced to the model (output + metadata).
 *
 * FUNCTIONAL FIX (parity/secret-leak): the shared `redactSecrets` scrubber does
 * NOT catch `?access_token=…`, `&client_secret=…`, or `&code=…` in a URL query
 * string — its unquoted-`KEY=value` pattern is anchored to a whitespace/start
 * boundary, which a `?`/`&` separator does not provide. A proper OAuth 2.1
 * *authorize* URL only carries non-secret params (client_id, redirect_uri,
 * scope, state, response_type, code_challenge), so redaction is a no-op on the
 * happy path; but a captured callback/misissued URL can embed a live token or
 * authorization code, which would otherwise leak verbatim into the transcript.
 * This redacts those values and then runs `redactSecrets` as a backstop.
 */
export function redactAuthUrl(url: string): string {
  return redactSecrets(url.replace(SENSITIVE_URL_PARAM_RE, '$1[REDACTED-SECRET]'));
}

/**
 * Classify an authentication failure reason (already secret-scrubbed) into a
 * coarse {@link McpAuthErrorKind}. Pure string heuristic; never throws.
 *
 * FUNCTIONAL FIX (parity): previously every reconnect failure was reported with
 * an undifferentiated `error` status, so the model could not tell a recoverable
 * connectivity blip from an actual authorization rejection that warrants a fresh
 * consent flow. The classification is additive (a new metadata field) and does
 * not change the existing `status`.
 */
export function classifyMcpAuthError(reason: string): McpAuthErrorKind {
  const r = reason.toLowerCase();
  if (/econnrefused|enotfound|etimedout|econnreset|eai_again|network|timed?\s?out|unreachable|socket hang up|connection (?:refused|reset)|dns/.test(r)) {
    return 'network';
  }
  if (/\b401\b|\b403\b|unauthor|forbidden|consent|invalid_grant|invalid_client|invalid_token|access[_ ]denied|re-?auth|oauth|token|credential|expired|login required/.test(r)) {
    return 'auth';
  }
  if (/not configured|missing (?:url|config|endpoint|client)|no (?:url|endpoint) configured|invalid url|malformed url|bad config/.test(r)) {
    return 'config';
  }
  return 'unknown';
}

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
        const errorKind = classifyMcpAuthError(reason);
        // Surface the authorization URL (if the flow reached the consent leg) so a
        // detached/headless agent can relay it to the user to complete auth out of
        // band, instead of only reporting failure (H46/H48/H49). The URL is scrubbed
        // of any embedded credential (token/secret/code) before it reaches the model.
        const rawAuthUrl = mcpManager.getPendingAuthUrl?.(serverName);
        const authUrl = rawAuthUrl ? redactAuthUrl(rawAuthUrl) : undefined;
        const urlLine = authUrl
          ? ` To authorize, open this URL in a browser and complete consent, then re-run authenticate:\n${authUrl}`
          : ` If a browser did not open, re-run and paste the callback URL when prompted, or check the server's OAuth config in .ami/mcp.json.`;
        // Kind-specific, actionable lead-in so the model picks the right next step.
        const kindHint =
          errorKind === 'auth'
            ? ' Re-authentication is required.'
            : errorKind === 'network'
              ? ' This looks like a connectivity problem; verify the server is reachable and retry.'
              : errorKind === 'config'
                ? ' Check the server\'s OAuth configuration in .ami/mcp.json.'
                : '';
        return {
          output: `Authentication failed for "${serverName}": ${reason}.${kindHint}${urlLine}`,
          isError: true,
          metadata: { status: 'error' satisfies McpAuthStatus, error_kind: errorKind, transport, ...(authUrl ? { auth_url: authUrl } : {}) },
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
