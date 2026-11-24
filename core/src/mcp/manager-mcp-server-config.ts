// Split from manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { McpClient, McpClientState } from './client';
import type { XaaConfig } from './oauth';
import { type McpToolPollSpec } from './tool-polling';
import { execFile } from 'child_process';
import { type ContainerConfig } from './container';
import type { McpServerScope } from './manager-resolve-disabled-mcp-servers';

export interface McpServerConfig {
  command: string;
  args?: string[];
  env?: Record<string, string>;
  headers?: Record<string, string>;
  /**
   * A shell command run at connect time (http/sse) that prints a JSON object of
   * dynamic request headers on stdout — git-credential-helper style, for rotating
   * / short-lived auth tokens. Merged over the static `headers` (dynamic wins). A
   * failing or malformed helper is logged and ignored so it never blocks connect.
   */
  headersHelper?: string;
  requestTimeout?: number;
  connectTimeout?: number;
  autoConnect?: boolean;
  /**
   * When false, disable the client's automatic reconnect retry loop for this
   * server (a caller/supervisor owns recovery). Defaults to true. Parity with
   * the reference's `WebSocketTransportOptions.autoReconnect`.
   */
  autoReconnect?: boolean;
  /**
   * Transport discriminator. Beyond the base transports (stdio / sse / http /
   * ws / wss), AMI recognizes the reference-parity IDE discriminants `sse-ide`
   * (an SSE endpoint exposed by a code editor) and `ws-ide` (a WebSocket one),
   * plus `sdk` (an in-process / host-wired server). An IDE discriminant carries
   * a human-readable {@link McpServerConfig.ideName} and is connected over its
   * BASE transport ({@link canonicalMcpTransport}: `sse-ide`→`sse`,
   * `ws-ide`→`ws`), so no new dispatch path is needed — only the reserved `ide`
   * server name ({@link IDE_MCP_SERVER_NAME}) and the editor-name badge differ.
   */
  transport?: 'stdio' | 'sse' | 'http' | 'ws' | 'wss' | 'sse-ide' | 'ws-ide' | 'sdk';
  url?: string;
  /**
   * Human-readable name of the code editor backing an IDE MCP server (e.g.
   * "VS Code", "JetBrains"), surfaced on the `/mcp` IDE-connection badge
   * (the reference-parity `ideName`, useIdeConnectionStatus.ts:L20-24). Only meaningful
   * when {@link McpServerConfig.transport} is `sse-ide` / `ws-ide`;
   * {@link getMcpIdeName} gates on that so a stray `ideName` on a non-IDE server
   * is ignored.
   */
  ideName?: string;
  containerConfig?: ContainerConfig;
  /**
   * User-entered config values for a plugin/bundle-declared server, keyed by the
   * field name the plugin's `userConfig` schema declares. Referenced from the
   * server's `command`/`args`/`url`/`env`/`headers` via `${user_config.<key>}`
   * placeholders (the reference-parity `resolvePluginMcpEnvironment`,
   * mcpPluginIntegration.ts:L475-490). Substituted BEFORE general `${ENV}`
   * expansion so a user/plugin value wins over a same-named process env var.
   */
  userConfig?: Record<string, string>;
  /**
   * Install directory of the plugin that packaged this server. Injected into a
   * stdio server's env as `CLAUDE_PLUGIN_ROOT` and substitutable via
   * `${CLAUDE_PLUGIN_ROOT}` so a bundled stdio server can locate its own files
   * (the reference-parity, mcpPluginIntegration.ts:L510-521).
   */
  pluginRoot?: string;
  /**
   * Persistent per-plugin data directory. Injected into a stdio server's env as
   * `CLAUDE_PLUGIN_DATA` and substitutable via `${CLAUDE_PLUGIN_DATA}`. The two
   * injected env keys are literal paths — NOT run through `${ENV}` expansion
   * (the reference-parity, mcpPluginIntegration.ts:L517).
   */
  pluginDataDir?: string;
  /**
   * Provenance tag for a server contributed by a plugin/bundle (the bundle name or
   * install source). Set by {@link scopePluginMcpServers} alongside the
   * `plugin:<name>:<server>` name scoping so the `/mcp` view can attribute a
   * scoped server back to the plugin that shipped it (the reference-parity
   * `ScopedMcpServerConfig.pluginSource`, mcpPluginIntegration.ts:L341-360).
   */
  pluginSource?: string;
  /**
   * Tools on this server to poll periodically. Each spec names a bare (server-local)
   * tool, a poll cadence, and optional fixed arguments; whenever a polled tool's
   * output *changes* between polls the manager emits `tool-polling-update` and the
   * engine surfaces it to the model as an `<mcp-polling-update type="tool" …>` block
   * (parity: the reference's `UserResourceUpdateMessage` polling branch). Polling runs
   * only while the server is connected (started on `ready`, stopped on close/remove).
   */
  pollTools?: McpToolPollSpec[];
  /** Enable OAuth 2.1 for this (http/sse) server. The authorizer is wired at the manager level. */
  oauth?: {
    redirectUri?: string;
    clientId?: string;
    clientSecret?: string;
    clientMetadataUrl?: string;
    scope?: string;
    authServerMetadataUrl?: string;
    loginHint?: string;
    refreshBufferMs?: number;
    /**
     * Pin the OAuth loopback callback to this fixed port for THIS server (the reference-parity
     * `mcp add --callback-port`). Per-server override of the global
     * {@link getConfiguredOAuthCallbackPort} env knob: when set, the redirect URI uses
     * this port (no dynamic discovery) so a user behind a firewall can allow-list a
     * stable per-server callback port. An explicit `redirectUri` still wins over it.
     */
    callbackPort?: number;
    /**
     * Browserless Cross-App Access config (XAA / SEP-990) for an enterprise shared-IdP
     * server. When set AND the {@link MCP_ENABLE_XAA_ENV} gate is on, the server
     * authenticates via the RFC 8693/7523 token-exchange flow using the pre-obtained
     * `idpIdToken` instead of an interactive browser consent — the headless/enterprise
     * path. Ignored (normal consent flow) when the gate is off.
     */
    xaa?: XaaConfig;
  };
}

export const MCP_SERVER_STATES = ['pending', 'connecting', 'ready', 'error', 'stopped', 'disabled', 'needs-auth'] as const;

export type McpServerState = (typeof MCP_SERVER_STATES)[number];

/**
 * OAuth credential-state introspection for an `oauth`-configured server — the
 * read-only `/mcp` counterpart of the reference's `mcp xaa show` subcommand
 * (commands/mcp/xaaIdpCommand.ts:L220-240). Surfaces WHICH identity the server is
 * configured for (resolved `clientId`, `scope`, `callbackPort`) and whether its
 * secrets are in place (`hasClientSecret` = a client secret is stored vs PKCE-only,
 * `loggedIn` = a persisted OAuth token file already exists on disk) WITHOUT ever
 * exposing the secret or token values themselves. `undefined` on a server with no
 * `oauth` config.
 */
export interface McpOAuthCredentialState {
  /** Resolved OAuth client id (config.oauth.clientId), when statically configured. */
  clientId?: string;
  /** Requested OAuth scope (config.oauth.scope), when configured. */
  scope?: string;
  /** Pinned loopback callback port (config.oauth.callbackPort), when configured. */
  callbackPort?: number;
  /** True when a client secret is configured (confidential client) vs PKCE-only. The
   *  secret VALUE is never surfaced — only its presence (parity with `mcp xaa show`'s
   *  "Client secret: (stored in keychain)" vs "(not set — PKCE-only)"). */
  hasClientSecret: boolean;
  /** True when a persisted OAuth token file already exists for this server (it has been
   *  authenticated at least once), parity with `mcp xaa show`'s "Logged in: yes/no". */
  loggedIn: boolean;
}

export interface McpServerStatus {
  name: string;
  state: McpClientState | McpServerState;
  serverInfo: { name: string; version: string } | null;
  toolCount: number;
  resourceCount: number;
  /** Count of MCP prompts (server "commands") the server exposes. */
  promptCount?: number;
  uptime: number | null;
  imageAvailable?: boolean;
  containerBacked?: boolean;
  /**
   * Authentication status for a remote (sse/http) server: true when it holds a
   * working session (connected/ready with tools) — mirrors the reference's auth badge.
   * `undefined` for stdio servers (not auth-scoped) and non-ready remotes.
   */
  isAuthenticated?: boolean;
  /**
   * True when a remote (sse/http) server is in the `needs-auth` state — its OAuth
   * consent is required/unavailable so it is not connected. Mirrors the reference's
   * per-server `needsAuth` auth-status field (MCPAgentServerMenu.tsx:L108-158),
   * the negative counterpart of {@link isAuthenticated}, so a UI can render a
   * distinct "needs authentication" affordance. `undefined` when not applicable.
   */
  needsAuth?: boolean;
  /**
   * The count of consecutive failed reconnect attempts currently in flight for a
   * dropped server (the reference-parity `reconnectAttempt`, MCPListPanel.tsx:L315-325),
   * so a status view can render "reconnecting (n/{@link maxReconnectAttempts})…".
   * `undefined`/absent when no reconnect is in progress.
   */
  reconnectAttempt?: number;
  /** The reconnect-attempt ceiling ({@link MAX_MCP_RECONNECT_ATTEMPTS}); present
   *  only alongside an in-flight {@link reconnectAttempt} for the "n/max" display. */
  maxReconnectAttempts?: number;
  /**
   * The config-source scope this server was loaded from (the reference-parity scope
   * grouping). `undefined` only before scope has been recorded. See
   * {@link McpServerScope}.
   */
  scope?: McpServerScope;
  /** Absolute path of the config file this server was loaded from (if any), so a
   *  diagnostics/listing view can show the user which file to edit. */
  sourcePath?: string;
  /**
   * The skill/stance (agent) definitions that pulled this server in — the reverse
   * mapping of which `mcp-servers`-declaring stance(s) introduced a given MCP server
   * (the reference-parity `AgentMcpServerInfo.sourceAgents`, MCPAgentServerMenu.tsx:L139-142
   * "Used by: <agent list>"). De-duplicated + alphabetically sorted. `undefined` for a
   * server no stance declares (e.g. a plain config-file server), so a `/mcp` view can
   * render provenance only where it exists. Populated via
   * {@link McpManager.addServerSourceAgent}.
   */
  sourceAgents?: string[];
  /**
   * True when the connected server OPTED IN to relaying permission prompts over a
   * message channel by declaring BOTH experimental capabilities `claude/channel`
   * and `claude/channel/permission` (the reference-parity `filterPermissionRelayClients`,
   * channelPermissions.ts:L177-194). Lets a `/mcp` diagnostics view flag a server
   * as a permission-relay surface. `false` for a server that declares neither or
   * only one; derived from {@link McpClient.supportsChannelPermissionRelay}.
   */
  channelPermissionRelay?: boolean;
  /**
   * The resolved launch command of a stdio server (`config.command`), so a `/mcp`
   * drill can show the user exactly what process backs the server (the reference-parity
   * MCPStdioServerMenu "Command:" line, MCPStdioServerMenu.tsx:L115-118). `undefined`
   * for a remote (http/sse/ws) server, which has a URL rather than a command.
   */
  command?: string;
  /**
   * The resolved launch arguments of a stdio server (`config.args`), rendered on the
   * drill's "Args:" line alongside {@link command} (the reference-parity MCPStdioServerMenu
   * "Args:" line, MCPStdioServerMenu.tsx:L120-123). Absent when the server has no args.
   */
  args?: string[];
  /**
   * OAuth credential-state introspection for an `oauth`-configured server (resolved
   * identity + whether its secret/token are in place), the `/mcp` counterpart of
   * `mcp xaa show`. `undefined` for a server with no `oauth` config. See
   * {@link McpOAuthCredentialState}.
   */
  oauth?: McpOAuthCredentialState;
  error?: string;
}

/**
 * Group a flat list of MCP server statuses by their originating stance/agent
 * (the reference-parity `MCPListPanel` agent grouping, MCPListPanel.tsx:L166-170/L408): each
 * server is emitted once under EVERY agent in its {@link McpServerStatus.sourceAgents}
 * list, agent names are de-duplicated, and both the agent headings and the servers
 * within each group are alphabetically ordered for a stable `/mcp` rendering. Servers
 * with no `sourceAgents` are omitted (they appear in the ungrouped/by-scope view). The
 * single source of truth a `/mcp` "Used by @<agent>" section renders.
 */
export function groupMcpServersBySourceAgent(
  statuses: McpServerStatus[],
): Array<{ agent: string; servers: McpServerStatus[] }> {
  const byAgent = new Map<string, McpServerStatus[]>();
  for (const status of statuses) {
    for (const agent of status.sourceAgents ?? []) {
      const bucket = byAgent.get(agent);
      if (bucket) bucket.push(status);
      else byAgent.set(agent, [status]);
    }
  }
  return [...byAgent.entries()]
    .map(([agent, servers]) => ({
      agent,
      servers: servers.slice().sort((a, b) => a.name.localeCompare(b.name)),
    }))
    .sort((a, b) => a.agent.localeCompare(b.agent));
}

/**
 * A partition of the live MCP servers into the two actionable connectivity buckets
 * the reference surfaces as proactive notifications (useMcpConnectivityStatus.tsx:L24-75):
 * `failed` servers are in a hard `error` state, `needsAuth` servers are in the
 * recoverable `needs-auth` state (the user can re-authenticate). Each bucket is a
 * de-duplicated, display-ordered (alphabetical, inherited from
 * {@link McpManager.listAllServers}) list of server names. The single source of truth
 * a `/mcp` view / startup banner renders via {@link formatMcpConnectivitySummary}.
 */
export interface McpConnectivitySummary {
  failed: string[];
  needsAuth: string[];
}

/**
 * A tally of the live MCP servers bucketed by distinct connectivity state
 * (the reference-parity `buildMcpProperties`, status.tsx:L89-115): a compact
 * `{connected, pending, needsAuth, failed}` count for a status-pane summary line
 * that stays ONE row no matter how many servers are configured (20+ servers would
 * otherwise wrap over many rows and dominate the pane). The reserved `ide`
 * pseudo-client ({@link IDE_MCP_SERVER_NAME}) is excluded — it is surfaced
 * separately as the IDE badge ({@link McpManager.getIdeConnectionStatus}) — and
 * the intentional user states (`disabled`/`stopped`/`disconnected`) are counted
 * in NO bucket (they are not actionable connectivity states, not failures).
 * Feeds {@link formatMcpStateSummary}.
 */
export interface McpServerStateSummary {
  connected: number;
  pending: number;
  needsAuth: number;
  failed: number;
}

export interface PendingServerInfo {
  name: string;
  config: McpServerConfig;
  state: McpServerState;
  error?: string;
  imageAvailable?: boolean;
  /**
   * M14 — true when this is a PROJECT-tier server from a repo's config that has
   * not yet been approved by the user. Such a server is registered as pending and
   * is NOT auto-connected (a cloned repo could point MCP at anything); it only
   * connects after an explicit {@link McpManager.approvePendingServer} call.
   */
  needsApproval?: boolean;
}

/** Expand env-var references in header values. Delegates to the single canonical
 *  {@link expandEnvVars} so headers get the same `${VAR}` / `${VAR:-default}` /
 *  bare-`$VAR` semantics as command/args/url (no second expansion dialect). */
/** Max time a `headersHelper` script may run before it is aborted (ms). */
export const HEADERS_HELPER_TIMEOUT_MS = 10_000;

/** Runs a `headersHelper` shell command and resolves with its stdout. Injectable so
 *  tests exercise the parse/merge logic without spawning a real process. */
export type HeadersHelperRunner = (command: string, env: NodeJS.ProcessEnv) => Promise<string>;

export const defaultHeadersHelperRunner: HeadersHelperRunner = (command, env) =>
  new Promise<string>((resolve, reject) => {
    execFile(
      command,
      [],
      { shell: true, timeout: HEADERS_HELPER_TIMEOUT_MS, maxBuffer: 1024 * 1024, env },
      (err, stdout) => (err ? reject(err) : resolve(String(stdout))),
    );
  });

/**
 * Resolve the effective request headers for a server: its static `headers` (with
 * `${VAR}` expansion) merged with the dynamic headers produced by running its
 * `headersHelper` (dynamic overrides static). The helper runs with the server name
 * and URL in the environment (git-credential-helper style) so one script can serve
 * many servers. A failing / non-object / non-string-valued helper output is logged
 * and ignored — the static headers are still returned so a bad helper never blocks
 * the connection.
 */
/**
 * Introspection hooks for {@link resolveMcpServerHeaders} (the reference-parity
 * `logMCPDebug`/`logMCPError` around header retrieval). Injected by the manager so the
 * per-server MCP log records WHEN a helper ran and HOW MANY headers it produced — key
 * debuggability into what a server will actually send. Both default to no-ops so the
 * standalone function stays side-effect-free when called directly (tests/consumers).
 */
export interface HeadersHelperLogHooks {
  onDebug?: (message: string) => void;
  onError?: (error: unknown) => void;
}

/**
 * Factory for creating the underlying MCP client. Overridable on an
 * {@link McpManager} instance purely as a dependency-injection seam for tests
 * (so the resolved command/args/timeouts passed to the client can be observed);
 * production always uses the default `new McpClient(...)`.
 */
export type McpClientFactory = (options: ConstructorParameters<typeof McpClient>[0]) => McpClient;

export const MAX_MCP_TOOLS_PER_SERVER = 200;

export const MAX_MCP_TOOL_NAME_LENGTH = 128;

/**
 * A health-check tick arriving later than `interval × this factor` is treated as
 * evidence the host process was suspended (sleep/VM-pause/freeze) rather than merely
 * busy: `setInterval` coalesces the ticks it missed while paused into one late fire.
 */
export const MCP_SUSPENSION_TICK_FACTOR = 2;

/** Reconnect backoff bounds for a dropped MCP server (H44). */
export const MAX_MCP_RECONNECT_ATTEMPTS = 5;

export const MCP_RECONNECT_BASE_DELAY_MS = 1000;

export const MAX_MCP_RECONNECT_DELAY_MS = 30_000;

/**
 * A proactive MCP connectivity notification (the reference-parity useMcpConnectivityStatus
 * `addNotification`): a dedupe `key`, a `severity` (error for failed, warning for
 * needs-auth), and a pluralized `message` ending in the ` · /mcp` pointer. The
 * frontend renders these as transient status notifications.
 */
export interface McpConnectivityNotification {
  key: 'mcp-failed' | 'mcp-needs-auth';
  severity: 'error' | 'warning';
  message: string;
}

/**
 * Format a {@link McpConnectivitySummary} into the proactive notifications the reference
 * pushes (useMcpConnectivityStatus.tsx:L36-56): `N MCP server(s) failed · /mcp`
 * (error) and `N MCP server(s) need auth · /mcp` (warning), each pluralized and
 * dedupe-keyed. Returns `[]` when nothing is failed/needs-auth (the early-return
 * "nothing to notify" case), so a caller can render zero banners.
 */
export function formatMcpConnectivitySummary(summary: McpConnectivitySummary): McpConnectivityNotification[] {
  const out: McpConnectivityNotification[] = [];
  if (summary.failed.length > 0) {
    const n = summary.failed.length;
    out.push({ key: 'mcp-failed', severity: 'error', message: `${n} MCP ${n === 1 ? 'server' : 'servers'} failed · /mcp` });
  }
  if (summary.needsAuth.length > 0) {
    const n = summary.needsAuth.length;
    out.push({ key: 'mcp-needs-auth', severity: 'warning', message: `${n} MCP ${n === 1 ? 'server needs' : 'servers need'} auth · /mcp` });
  }
  return out;
}

/**
 * One non-zero state bucket of a {@link McpServerStateSummary}, in the fixed
 * severity-aware render order the status pane uses. `severity` is the color class a
 * frontend maps to a theme color; `label` is the pre-pluralized count phrase.
 */
export interface McpStateSummaryPart {
  state: 'connected' | 'needsAuth' | 'pending' | 'failed';
  count: number;
  label: string;
  severity: 'success' | 'warning' | 'inactive' | 'error';
}
