// Split from client.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { amiUserAgent } from '../version';
import { getMTLSConfig, getSecureTLSConnectOptions } from '../utils/mtls';

/**
 * TLS connect-options handed to a WebSocket factory for a `wss://` MCP server —
 * client cert/key/passphrase/CA plus a secure protocol floor (parity with the reference
 * Code's `getWebSocketTLSOptions` returning `tls.ConnectionOptions`). The runtime
 * global `WebSocket` ignores the extra argument; a `ws`-package-backed factory
 * (injected by a host that needs mTLS) applies it so mutual-TLS / custom-CA trust
 * reaches WebSocket transports, not just HTTP/SSE.
 */
export interface McpWebSocketTlsOptions {
  cert?: string;
  key?: string;
  passphrase?: string;
  ca?: string;
  minVersion: 'TLSv1' | 'TLSv1.1' | 'TLSv1.2' | 'TLSv1.3';
}

/**
 * Resolve the TLS connect options for a WebSocket MCP connection. A `wss://` URL
 * yields the mTLS material + a secure protocol floor (always at least
 * `{ minVersion }`), so a `ws`-package-backed factory presents the client cert and
 * trusts a private CA exactly like the HTTP/SSE transports. A plaintext `ws://`
 * URL carries no TLS, so this returns `undefined`. Parity with the reference's
 * `getWebSocketTLSOptions` (utils/mtls.ts), reusing AMI's single mTLS source
 * (`getMTLSConfig`/`getSecureTLSConnectOptions`) rather than a parallel loader.
 */
export function resolveMcpWebSocketTlsOptions(
  url: string,
  env: NodeJS.ProcessEnv = process.env,
): McpWebSocketTlsOptions | undefined {
  if (!url.startsWith('wss')) return undefined;
  return getSecureTLSConnectOptions(getMTLSConfig(env), env);
}

export const MCP_TRANSPORTS = ['stdio', 'sse', 'http', 'ws', 'wss', 'inprocess'] as const;

export type McpTransport = (typeof MCP_TRANSPORTS)[number];

/**
 * Minimal structural subset of the WHATWG `WebSocket` (shared by Node 20+/Bun's
 * global `WebSocket` and the `ws` package's instances) that the WebSocket MCP
 * transport needs — parity with the reference's `WebSocketLike` in
 * `utils/mcpWebSocketTransport.ts`. Modeling the subset (rather than the DOM lib
 * type) keeps the transport runtime-agnostic and injectable for tests.
 */
export interface McpWebSocketLike {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: (ev: unknown) => void): void;
  removeEventListener(type: string, listener: (ev: unknown) => void): void;
}

export type McpWebSocketFactory = (url: string, tlsOptions?: McpWebSocketTlsOptions) => McpWebSocketLike;

/** `WebSocket.OPEN` readyState (1) — same numeric value in every runtime. */
export const MCP_WS_OPEN = 1;

/**
 * Default token budget for a single MCP tool result (parity with the reference's
 * `DEFAULT_MAX_MCP_OUTPUT_TOKENS`). A verbose MCP tool can return megabytes of
 * JSON; capping by *tokens* (not just bytes) keeps a single result from
 * dominating the model's context window regardless of how many raw bytes it is.
 */
export const DEFAULT_MAX_MCP_OUTPUT_TOKENS = 25000;

/** Chars-per-token ratio used to convert the token cap into a byte/char cap. */
export const MCP_CHARS_PER_TOKEN = 4;

/**
 * Resolve the per-result MCP output token cap. A single global override —
 * `MAX_MCP_OUTPUT_TOKENS` (positive integer, matching the reference's
 * `getMaxMcpOutputTokens`) — lets an operator widen/narrow how much a single MCP
 * tool result may contribute to the context window. An absent/invalid/non-positive
 * value falls back to {@link DEFAULT_MAX_MCP_OUTPUT_TOKENS}. `MAX_MCP_OUTPUT_TOKENS`
 * is already allow-listed in env-protection (a tuning knob, no trust boundary).
 */
export function getMaxMcpOutputTokens(): number {
  const n = parseInt(process.env.MAX_MCP_OUTPUT_TOKENS ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_MAX_MCP_OUTPUT_TOKENS;
}

/**
 * The token cap expressed as a character/byte budget ({@link getMaxMcpOutputTokens}
 * × {@link MCP_CHARS_PER_TOKEN}). This is the default size passed to
 * {@link boundMcpResponse} when a server does not carry an explicit
 * `maxResultSizeChars` hint, so every MCP result is bounded by the token budget.
 */
export function getMaxMcpOutputChars(): number {
  return getMaxMcpOutputTokens() * MCP_CHARS_PER_TOKEN;
}

/**
 * Latest MCP protocol version we request in `initialize`. Servers that only
 * support an older revision reply with their own version; we store the
 * negotiated value and proceed, since our wire usage (initialize / tools.list /
 * tools.call / notifications.initialized) is identical across these revisions.
 * Single source of truth for every transport's initialize handshake.
 */
export const MCP_PROTOCOL_VERSION = '2025-06-18';

/**
 * Experimental MCP capability names a server declares under
 * `capabilities.experimental` to OPT IN to relaying permission prompts over a
 * message channel (the reference-parity `claude/channel` + `claude/channel/permission`,
 * channelPermissions.ts:L177-194). BOTH must be present before a server may be
 * treated as a permission-relay surface — a relay-only channel never becomes a
 * permission surface by accident. Inspected via
 * {@link McpClient.hasExperimentalCapability} /
 * {@link McpClient.supportsChannelPermissionRelay}.
 */
export const MCP_CHANNEL_CAPABILITY = 'claude/channel';

export const MCP_CHANNEL_PERMISSION_CAPABILITY = 'claude/channel/permission';

export const MIN_MCP_BUFFER = 1024;            // 1 KB

/** Fallback connection timeout (ms) when no explicit value and no env override are given. */
export const DEFAULT_MCP_CONNECT_TIMEOUT_MS = 10000;

/**
 * Resolve the default MCP connection timeout. A single global override —
 * `MCP_TIMEOUT` (milliseconds, matching the reference's `getConnectionTimeoutMs`) —
 * lets an operator widen the connect window for slow-to-start servers without
 * editing each server's config. An explicit `connectTimeout` option still wins;
 * an absent/invalid/non-positive env value falls back to
 * {@link DEFAULT_MCP_CONNECT_TIMEOUT_MS}. `MCP_TIMEOUT` is already allow-listed in
 * SAFE_ENV_VARS (tuning knob, no trust boundary crossed).
 */
export function getMcpConnectTimeout(): number {
  const n = parseInt(process.env.MCP_TIMEOUT ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_MCP_CONNECT_TIMEOUT_MS;
}

/**
 * Self-identifying `User-Agent` for MCP HTTP/SSE transport requests (parity with
 * the reference's `getMCPUserAgent`). Lets server operators recognize/allowlist AMI
 * traffic AND see which surface originated the request — the originating
 * entrypoint and any embedding SDK/client-app are appended via the single
 * {@link amiUserAgent} builder (`ami/<version> (<entrypoint>, agent-sdk/…,
 * client-app/…)`) rather than re-spelling the UA here. A caller-supplied
 * `User-Agent` header still overrides this default.
 */
export function getMcpUserAgent(): string {
  return amiUserAgent();
}

/** Hard cap on cursor-pagination pages per list call — a runaway-server backstop. */
export const MCP_MAX_PAGES = 1000;

/**
 * Default interval (ms) between self-generated "still running" heartbeats for an
 * in-flight MCP tool call (parity with the reference's 30s progress logging in
 * `services/mcp/client.ts`). A slow MCP tool runs under a long tool-call
 * timeout; without a heartbeat the UI/log goes silent for minutes. On each tick
 * the client emits a `tool-heartbeat` event carrying the elapsed time so a
 * frontend can show "<tool> still running (Ns elapsed)". Cleared the instant the
 * call settles.
 */
export const DEFAULT_MCP_TOOL_HEARTBEAT_INTERVAL_MS = 30000;

/**
 * Resolve the tool-call heartbeat interval. `MCP_TOOL_HEARTBEAT_INTERVAL_MS`
 * (positive integer) lets an operator tune how often the "still running"
 * heartbeat fires; absent/invalid falls back to
 * {@link DEFAULT_MCP_TOOL_HEARTBEAT_INTERVAL_MS}. Allow-listed in env-protection
 * (a tuning knob, no trust boundary crossed).
 */
export function getMcpToolHeartbeatIntervalMs(): number {
  const n = parseInt(process.env.MCP_TOOL_HEARTBEAT_INTERVAL_MS ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_MCP_TOOL_HEARTBEAT_INTERVAL_MS;
}

/**
 * Default interval (ms) between keep-alive pings on a `ws`/`wss` MCP transport
 * (parity with the reference's WebSocketTransport DEFAULT_KEEPALIVE_INTERVAL, 5min).
 * A WebSocket behind an idle-timeout proxy/load-balancer is silently dropped after
 * minutes of inactivity; a periodic MCP `ping` keeps the path warm AND doubles as
 * dead-connection detection (a failed ping tears the socket down so the normal
 * close→reconnect path runs). Cleared the instant the socket closes/disconnects.
 */
export const DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS = 300000;

/**
 * Resolve the WebSocket keep-alive ping interval. `MCP_WS_KEEPALIVE_INTERVAL_MS`
 * (positive integer) tunes how often the keep-alive ping fires; absent/invalid
 * falls back to {@link DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS}. A tuning knob, no
 * trust boundary crossed.
 */
export function getMcpWsKeepaliveIntervalMs(): number {
  const n = parseInt(process.env.MCP_WS_KEEPALIVE_INTERVAL_MS ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_MCP_WS_KEEPALIVE_INTERVAL_MS;
}

/**
 * Default interval (ms) between application-level keep-alive DATA frames on a
 * Streamable-HTTP MCP transport (parity with the reference's WebSocketTransport
 * DEFAULT_KEEPALIVE_INTERVAL, 5min). A long-idle HTTP/SSE connection behind an
 * idle-timeout intermediary (e.g. Cloudflare's 5-min window) is silently RST —
 * and, unlike TCP keepalives or SSE comment frames the proxy may ignore, a real
 * MCP `ping` REQUEST (a data frame over the POST leg) resets the proxy's idle
 * timer. AMI previously only RECOVERED from such a drop reactively (the GET-stream
 * liveness watchdog + {@link scheduleHttpStreamReconnect}); this keeps the path
 * warm PROACTIVELY so the drop never happens. Activity-gated: a tick is a no-op
 * when real traffic already flowed within the interval.
 */
export const DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS = 300000;

/**
 * Resolve the Streamable-HTTP keep-alive interval. `MCP_HTTP_KEEPALIVE_INTERVAL_MS`
 * (positive integer) tunes how often the keep-alive data frame fires; absent/invalid
 * falls back to {@link DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS}. Set it BELOW the
 * intermediary's idle timeout (a "sub-idle-timeout interval"). A tuning knob, no
 * trust boundary crossed (allow-listed in env-protection).
 */
export function getMcpHttpKeepaliveIntervalMs(): number {
  const n = parseInt(process.env.MCP_HTTP_KEEPALIVE_INTERVAL_MS ?? '', 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_MCP_HTTP_KEEPALIVE_INTERVAL_MS;
}

/**
 * Env gate that SUPPRESSES the WebSocket keep-alive ping when set truthy (parity
 * with the reference's `isEnvTruthy(CLAUDE_CODE_REMOTE)` skipping keepalive frames
 * because an outer session-activity heartbeat already covers idle-timeout). When a
 * host embeds AMI behind its own heartbeat it sets `AMI_REMOTE=1` so the transport
 * does not add a redundant ping. Truthy = `1`/`true`/`yes`/`on` (case-insensitive).
 */
export function isMcpWsKeepaliveSuppressed(env: NodeJS.ProcessEnv = process.env): boolean {
  const v = env.AMI_REMOTE?.trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

export interface McpToolAnnotations {
  title?: string;
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
  openWorldHint?: boolean;
  idempotentHint?: boolean;
}

export interface McpToolSchema {
  name: string;
  /** Human-display name (2025-06-18); falls back to `name` when absent. */
  title?: string;
  description: string;
  inputSchema: Record<string, unknown>;
  /** JSON Schema for the tool's structured result (2025-06-18). */
  outputSchema?: Record<string, unknown>;
  annotations?: McpToolAnnotations;
  _meta?: Record<string, unknown>;
}

export interface McpResource {
  uri: string;
  name: string;
  /** Human-display name (2025-06-18). */
  title?: string;
  mimeType?: string;
  /** Human-readable description the model uses to pick the right resource. */
  description?: string;
}

/** A parameterized resource URI (RFC 6570 template), from resources/templates/list. */
export interface McpResourceTemplate {
  uriTemplate: string;
  name: string;
  title?: string;
  description?: string;
  mimeType?: string;
}

export interface McpPrompt {
  name: string;
  title?: string;
  description?: string;
  arguments?: Array<{ name: string; description?: string; required?: boolean }>;
}

// ---------------------------------------------------------------------------
// 2025-06-18 client-capability request/response shapes
// ---------------------------------------------------------------------------

/**
 * Elicitation (2025-06-18): a server asks the user, via the client, for
 * structured input mid-operation. The client MUST reply with exactly one of
 * three actions and MUST NOT block indefinitely — absent a handler, or on a
 * handler error, it declines/cancels so the server can proceed.
 */
export const MCP_ELICITATION_ACTIONS = ['accept', 'decline', 'cancel'] as const;

export type McpElicitationAction = (typeof MCP_ELICITATION_ACTIONS)[number];

/**
 * Elicitation presentation modes (2025-06-18 `mode` discriminator): `'form'`
 * collects structured field input; `'url'` hands the user a URL to open and runs
 * a two-phase open→waiting lifecycle. Absent/unknown values are treated as
 * `'form'` by the frontend.
 */
export const MCP_ELICITATION_MODES = ['form', 'url'] as const;

export type McpElicitationMode = (typeof MCP_ELICITATION_MODES)[number];

export interface McpElicitationRequest {
  /** Human-readable prompt the server wants shown to the user. */
  message: string;
  /** Flat JSON Schema of primitive fields the server is requesting. */
  requestedSchema: Record<string, unknown>;
  /**
   * Aborts when the SERVER cancels this elicitation (inbound
   * `notifications/cancelled`). A frontend prompt should observe it and resolve
   * as a cancel so a request the server abandoned does not hang the UI (H08).
   */
  signal?: AbortSignal;
  /**
   * Elicitation mode (2025-06-18 `mode` discriminator). `'form'` (default, and
   * when absent) collects structured field input against `requestedSchema`;
   * `'url'` hands the user a `url` to open in a browser and runs a two-phase
   * open→waiting lifecycle. A frontend reads this to branch its UI.
   */
  mode?: McpElicitationMode;
  /** URL-mode only: the URL the server wants opened in the user's browser. */
  url?: string;
  /**
   * URL-mode only: label for the proceed-without-waiting affordance during the
   * waiting phase (e.g. 'Skip confirmation'); defaults applied by the frontend.
   */
  actionLabel?: string;
  /** URL-mode only: whether to offer a visible Cancel during the waiting phase. */
  showCancel?: boolean;
  /** URL-mode only: set true once the server posts a completion notification. */
  completed?: boolean;
  /**
   * URL-mode only: the server-assigned id of this elicitation. Set when the
   * elicitation arrives inside a tool call's `-32042` (URL-elicitation-required)
   * error so a frontend can correlate the server's completion notification; absent
   * for server-initiated `elicitation/create` requests.
   */
  elicitationId?: string;
}

export interface McpElicitationResult {
  action: McpElicitationAction;
  /** Present only when action === 'accept'. */
  content?: Record<string, unknown>;
}

/**
 * Sampling (`sampling/createMessage`): a server asks the client to run an LLM
 * turn on its behalf. AMI is uniquely able to satisfy this because it owns the
 * engine. Absent a handler the request is refused with a JSON-RPC error.
 */
export interface McpSamplingMessage {
  role: 'user' | 'assistant';
  content: { type: string; text?: string; data?: string; mimeType?: string; [key: string]: unknown };
}

export interface McpSamplingRequest {
  messages: McpSamplingMessage[];
  systemPrompt?: string;
  maxTokens?: number;
  temperature?: number;
  stopSequences?: string[];
  modelPreferences?: Record<string, unknown>;
  includeContext?: 'none' | 'thisServer' | 'allServers';
  metadata?: Record<string, unknown>;
}

export interface McpSamplingResult {
  role: 'assistant';
  content: { type: 'text'; text: string } | { type: string; [key: string]: unknown };
  model: string;
  stopReason?: string;
}

/** MCP log levels (RFC 5424 severities) for logging/setLevel. */
export const MCP_LOG_LEVELS = ['debug', 'info', 'notice', 'warning', 'error', 'critical', 'alert', 'emergency'] as const;

export type McpLogLevel = (typeof MCP_LOG_LEVELS)[number];

export interface McpLogMessage {
  level: McpLogLevel;
  logger?: string;
  data: unknown;
}

/** completion/complete — argument autocompletion for prompts & resource templates. */
export interface McpCompletionRef {
  type: 'ref/prompt' | 'ref/resource';
  name?: string;
  uri?: string;
}

export interface McpCompletionResult {
  values: string[];
  total?: number;
  hasMore?: boolean;
}
