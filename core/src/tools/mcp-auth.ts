import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { redactSecrets } from './tool-utils';
import { normalizeMcpNameSegment } from './mcp-tool';

/** Transports that can carry an OAuth browser-consent flow. */
const OAUTH_TRANSPORTS = new Set(['sse', 'http']);

/**
 * Structured status surfaced to the model via ToolResult.metadata.
 *
 * - `ok`         — the server is (re)connected and its tools are available.
 * - `auth_url`   — interactive consent is still required; `metadata.auth_url`
 *                  carries the (secret-scrubbed) authorization URL to relay to
 *                  the user so a detached/headless run can complete auth out of
 *                  band (mirrors Claude Code's `McpAuthOutput.status:'auth_url'`).
 * - `unsupported`— the server's transport cannot carry an OAuth flow.
 * - `error`      — the attempt failed; see `metadata.error_kind`.
 */
export const MCP_AUTH_STATUS = ['ok', 'auth_url', 'unsupported', 'error'] as const;
export type McpAuthStatus = (typeof MCP_AUTH_STATUS)[number];

/** Run `/mcp` to manage, authenticate, or reconnect MCP servers interactively. */
const MCP_COMMAND_HINT = 'run /mcp to manage and authenticate MCP servers interactively';

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
  const normalized = normalizeMcpNameSegment(serverName);
  return {
    name: `mcp__${normalized}__authenticate`,
    description:
      `Authenticate with the MCP server "${serverName}" via its OAuth 2.1 flow. ` +
      `Call this when the server's tools report that authentication is required. ` +
      `Only http/SSE servers support OAuth; stdio servers do not. A browser window ` +
      `opens for consent (with a manual callback-URL paste fallback for headless shells); ` +
      `once complete the server's tools become available. In a detached/headless run ` +
      `where no browser is available, this returns the authorization URL in its result ` +
      `(status "auth_url") so you can share it with the user to complete consent out of band. ` +
      `Pass {"force": true} to discard a still-valid cached credential and force a fresh ` +
      `login — use this after the server revoked the session on its side.`,
    inputSchema: {
      type: 'object',
      properties: {
        force: {
          type: 'boolean',
          description:
            'Discard any cached/valid OAuth credential before re-authenticating, forcing a ' +
            'fresh consent flow (e.g. after a server-side token revocation). Without it, an ' +
            'already-authenticated server is reported as a no-op rather than re-prompting.',
        },
      },
    },
    isReadOnly: false,
    isConcurrencySafe: false,
    isMcp: true,
    async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
      const force = input?.force === true;
      const mcpManager = (context as unknown as { _mcpManager?: {
        reconnect?: (name: string) => Promise<void>;
        startAuth?: (name: string, opts?: { signal?: AbortSignal }) => Promise<{ authUrl?: string }>;
        forgetOAuthToken?: (name: string) => boolean;
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
            `OAuth is only available for http/SSE servers. Configure credentials directly (e.g. env vars in .ami/mcp.json), or ${MCP_COMMAND_HINT}.`,
          isError: true,
          metadata: { status: 'unsupported' satisfies McpAuthStatus, transport },
        };
      }

      // Force re-login: drop any cached/valid credential FIRST so the ensuing
      // (re)connect cannot short-circuit on a still-valid token and must re-run the
      // consent flow — parity with Claude Code's `mcp xaa login --force`. A no-op
      // for a server with no persisted credential (never authenticated / stdio).
      if (force) mcpManager.forgetOAuthToken?.(serverName);

      const before = mcpManager.getServerStatus?.(serverName)?.state;
      // Active, model-driven OAuth: when the manager exposes the non-blocking
      // `startAuth` entry point, start the flow and get the authorization URL back
      // as soon as the consent leg produces one (status "auth_url") WITHOUT hanging
      // on the interactive browser leg — the reconnect/tool-swap finishes in the
      // background. A detached/headless run can then relay the URL to the user,
      // mirroring Claude Code's skipBrowserOpen McpAuthTool. The call honors
      // `context.abortSignal` so an in-flight flow can be cancelled. Managers that
      // only expose `reconnect` fall back to the blocking re-establish path below.
      let activeAuthUrl: string | undefined;
      try {
        if (typeof mcpManager.startAuth === 'function') {
          const res = await mcpManager.startAuth(serverName, { signal: context.abortSignal });
          activeAuthUrl = res?.authUrl;
        } else {
          await mcpManager.reconnect?.(serverName);
        }
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
          ? ` To authorize, open this URL in a browser and complete consent, then re-run authenticate (or ${MCP_COMMAND_HINT}):\n${authUrl}`
          : ` If a browser did not open, re-run and paste the callback URL when prompted, check the server's OAuth config in .ami/mcp.json, or ${MCP_COMMAND_HINT}.`;
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
          metadata: { status: 'error' satisfies McpAuthStatus, error_kind: errorKind, transport, ...(authUrl ? { auth_url: authUrl } : {}), ...(force ? { forced: true } : {}) },
        };
      }
      // Active non-blocking flow surfaced a consent URL before the background
      // reconnect finished — relay it immediately (status "auth_url") so the model
      // can share it, exactly like Claude Code's skipBrowserOpen McpAuthTool. The
      // URL is scrubbed of any embedded credential before it reaches the model.
      if (activeAuthUrl) {
        const authUrl = redactAuthUrl(activeAuthUrl);
        return {
          output: `Authorization required for "${serverName}". Open this URL in a browser to complete consent, then re-run authenticate (or ${MCP_COMMAND_HINT}):\n${authUrl}`,
          metadata: { status: 'auth_url' satisfies McpAuthStatus, transport, auth_url: authUrl, ...(force ? { forced: true } : {}) },
        };
      }
      // Tri-state post-reconnect reporting (connected / needs-auth / failed):
      // reconnect() returned without throwing, but the server may still require
      // interactive consent — e.g. a headless authorizer captured an
      // authorization URL without completing the browser leg. In that case we
      // surface the URL (status "auth_url") instead of claiming a false success,
      // mirroring Claude Code's auth_url result and the "report success only when
      // auth actually happens" guarantee.
      const after = mcpManager.getServerStatus?.(serverName)?.state;
      if (after !== 'ready') {
        const rawAuthUrl = mcpManager.getPendingAuthUrl?.(serverName);
        const authUrl = rawAuthUrl ? redactAuthUrl(rawAuthUrl) : undefined;
        if (authUrl) {
          return {
            output: `Authorization required for "${serverName}". Open this URL in a browser to complete consent, then re-run authenticate (or ${MCP_COMMAND_HINT}):\n${authUrl}`,
            metadata: { status: 'auth_url' satisfies McpAuthStatus, transport, auth_url: authUrl, ...(force ? { forced: true } : {}) },
          };
        }
      }
      const reconnected = after === 'ready' && before !== 'ready';
      // Distinct completion messages for a fresh (re)auth, a forced re-login that
      // discarded a cached credential, and the already-connected no-op, so the
      // model can tell them apart.
      const output = reconnected
        ? `Authentication completed for "${serverName}"; its tools are now available.`
        : force
          ? `Re-authentication completed for "${serverName}"; the cached credential was discarded and a fresh one is now in use.`
          : `"${serverName}" is already connected — no re-authentication was needed. Pass {"force": true} to discard the cached credential and force a fresh login (e.g. after a server-side revocation).`;
      return {
        output,
        metadata: { status: 'ok' satisfies McpAuthStatus, transport, reconnected, ...(force ? { forced: true } : {}) },
      };
    },
  };
}
