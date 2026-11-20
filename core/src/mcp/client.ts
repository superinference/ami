import { spawn, ChildProcess } from 'child_process';
import { EventEmitter } from 'events';
import { AMI_CLIENT_INFO, amiUserAgent } from '../version';
import type { McpOAuthProvider } from './oauth';
import { recursivelySanitizeUnicode } from '../tools/input-sanitizer';
import { sanitizeEnvForSubprocess } from '../tools/bash';
import { findExecutable } from '../utils/which';
import { boundedCall } from '../utils/bounded-call';
import { getMTLSConfig, getSecureTLSConnectOptions } from '../utils/mtls';
import type { InProcessTransport } from './in-process-transport';

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

export const DEFAULT_MCP_MAX_RESPONSE_BYTES = 10 * 1024 * 1024; // 10 MB

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

const MIN_MCP_BUFFER = 1024;            // 1 KB
const MAX_MCP_BUFFER = 100 * 1024 * 1024; // 100 MB

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

/**
 * Hard cap on the stdio read buffer (L31). A stdio MCP server streams
 * newline-delimited JSON over stdout and we accumulate bytes in `this.buffer`
 * until a newline completes a message. A malicious/buggy server that never
 * sends a newline (or emits one gigantic line) would otherwise grow this buffer
 * without bound → memory-exhaustion DoS. When the un-terminated remainder left
 * after extracting complete messages exceeds this cap, the buffer is dropped and
 * a `protocol-error` is emitted; complete messages parsed on the same pass are
 * unaffected, so normal framing keeps working.
 */
export const MCP_MAX_STDIO_BUFFER = 16 * 1024 * 1024; // 16 MiB

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

export function boundMcpResponse(
  content: string,
  maxBytes: number = DEFAULT_MCP_MAX_RESPONSE_BYTES,
): { content: string; truncated: boolean; originalBytes: number } {
  const originalBytes = Buffer.byteLength(content, 'utf-8');
  if (originalBytes <= maxBytes) {
    return { content, truncated: false, originalBytes };
  }
  const buf = Buffer.from(content, 'utf-8');
  let end = maxBytes;
  // Walk back to avoid splitting a multi-byte UTF-8 character
  while (end > 0 && (buf[end] & 0xc0) === 0x80) end--;
  const truncated = buf.subarray(0, end).toString('utf-8');
  return {
    content: `${truncated}\n\n[Response truncated: ${originalBytes} bytes → ${maxBytes} bytes. Set MCP_MAX_RESPONSE_BYTES to increase limit.]\n\nThe tool output was truncated. If this MCP server provides pagination or filtering tools, use them to retrieve specific portions of the data. If pagination is not available, inform the user that you are working with truncated output and results may be incomplete.`,
    truncated: true,
    originalBytes,
  };
}

export function validateMcpBufferSize(size: unknown): number {
  if (size === undefined || size === null) return DEFAULT_MCP_MAX_RESPONSE_BYTES;
  const n = typeof size === 'string' ? Number(size) : Number(size);
  if (!Number.isFinite(n)) return DEFAULT_MCP_MAX_RESPONSE_BYTES;
  if (n < MIN_MCP_BUFFER) return MIN_MCP_BUFFER;
  if (n > MAX_MCP_BUFFER) return MAX_MCP_BUFFER;
  return n;
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

export interface McpPromptMessage {
  role: 'user' | 'assistant';
  content: { type: string; text?: string; [key: string]: unknown };
}

interface JsonRpcRequest {
  jsonrpc: '2.0';
  id: number;
  method: string;
  params?: Record<string, unknown>;
}

interface JsonRpcResponse {
  jsonrpc: '2.0';
  id: number;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

interface JsonRpcNotification {
  jsonrpc: '2.0';
  method: string;
  params?: Record<string, unknown>;
}

/**
 * Validate that a parsed value is a well-formed JSON-RPC 2.0 message envelope
 * before it is dispatched. A compliant message is a non-null, non-array object
 * carrying `jsonrpc: '2.0'` and EITHER a usable `id` (number|string — a response,
 * or a server→client request/notification that must be answered) OR a string
 * `method` (a notification / request). Untrusted MCP servers (and the raw WebSocket
 * /SSE/stdio framing they travel over) can emit arbitrary JSON: routing a
 * non-conforming shape would either silently no-op (a plain object with neither id
 * nor method falls through every branch) or throw inside the dispatcher (the `in`
 * operator on a primitive). Running this envelope check first lets the transport
 * reject + surface a malformed frame as a `protocol-error` instead of swallowing or
 * crashing on it — parity with the reference's schema-validated inbound transport
 * messages (mcpWebSocketTransport validateJsonRpcMessage / zod-parsed frames). Note
 * this validates only the ENVELOPE; per-method `params` remain intentionally lenient
 * (optional fields are guarded at each handler) so a server omitting an optional
 * field is tolerated rather than dropped.
 */
export function isValidJsonRpcMessage(msg: unknown): msg is Record<string, unknown> {
  if (typeof msg !== 'object' || msg === null || Array.isArray(msg)) return false;
  const m = msg as Record<string, unknown>;
  if (m.jsonrpc !== '2.0') return false;
  const hasId = typeof m.id === 'number' || typeof m.id === 'string';
  const hasMethod = typeof m.method === 'string';
  return hasId || hasMethod;
}

/**
 * Validate the `params` of an inbound server→client NOTIFICATION against the shape
 * AMI actually dereferences, at the trust boundary, BEFORE any handler touches a
 * field — a runtime schema check on untrusted MCP-server payloads. Parity with
 * the reference validating inbound notification params through a zod schema in
 * `setNotificationHandler` before dispatch (e.g. `useIdeSelection`'s
 * `SelectionChangedSchema`, `usePromptsFromClaudeInChrome`'s notification schema);
 * AMI mirrors zod with a plain-TS structural validator to match the no-zod-dependency
 * convention used throughout `core/` (see `cron.ts`/`plugins.ts` "mirrors the reference's
 * zod" validators). Where {@link isValidJsonRpcMessage} checks only the ENVELOPE,
 * this checks the per-method `params`.
 *
 * Only the methods whose params AMI reads are enforced; an unrecognized method has
 * no params contract and is accepted. Spec-required fields (a `resources/updated`
 * `uri`, a `progress` `progressToken`, a `cancelled` `requestId`) MUST be present
 * and correctly typed; genuinely optional fields are rejected only when present but
 * wrong-typed, so a server omitting an optional field is still tolerated. A rejected
 * notification is surfaced as a `notification-error` and dropped by the dispatcher
 * instead of feeding a malformed, wrong-typed payload to a handler or an emitted
 * event (which could propagate e.g. a non-string `uri` or an out-of-range log level
 * to downstream consumers).
 */
export function validateMcpNotificationParams(
  method: string,
  params: unknown,
): { ok: true } | { ok: false; error: string } {
  const isObj = (v: unknown): v is Record<string, unknown> =>
    typeof v === 'object' && v !== null && !Array.isArray(v);
  const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);
  const isTokenId = (v: unknown): boolean => typeof v === 'string' || isNum(v);
  switch (method) {
    case 'notifications/resources/updated':
      if (!isObj(params) || typeof params.uri !== 'string' || params.uri.length === 0)
        return { ok: false, error: `${method}: params.uri must be a non-empty string` };
      return { ok: true };
    case 'notifications/progress':
      if (!isObj(params) || !isTokenId(params.progressToken))
        return { ok: false, error: `${method}: params.progressToken must be a string or number` };
      if (params.progress !== undefined && !isNum(params.progress))
        return { ok: false, error: `${method}: params.progress must be a number` };
      if (params.total !== undefined && !isNum(params.total))
        return { ok: false, error: `${method}: params.total must be a number` };
      if (params.message !== undefined && typeof params.message !== 'string')
        return { ok: false, error: `${method}: params.message must be a string` };
      return { ok: true };
    case 'notifications/message':
      if (!isObj(params))
        return { ok: false, error: `${method}: params must be an object` };
      if (params.level !== undefined && !(MCP_LOG_LEVELS as readonly string[]).includes(params.level as string))
        return { ok: false, error: `${method}: params.level must be one of ${MCP_LOG_LEVELS.join(', ')}` };
      if (params.logger !== undefined && typeof params.logger !== 'string')
        return { ok: false, error: `${method}: params.logger must be a string` };
      return { ok: true };
    case 'notifications/cancelled':
      if (!isObj(params) || !isTokenId(params.requestId))
        return { ok: false, error: `${method}: params.requestId must be a string or number` };
      return { ok: true };
    case 'notifications/elicitation/complete':
      if (!isObj(params) || typeof params.elicitationId !== 'string' || params.elicitationId.length === 0)
        return { ok: false, error: `${method}: params.elicitationId must be a non-empty string` };
      return { ok: true };
    case 'selection_changed': {
      // IDE-as-MCP-server editor selection (parity with the reference's
      // `SelectionChangedSchema` in hooks/useIdeSelection.ts). `selection` is
      // nullable/optional; when present both endpoints must carry numeric line &
      // character. `text`/`filePath` are optional strings.
      if (!isObj(params)) return { ok: false, error: `${method}: params must be an object` };
      const sel = params.selection;
      if (sel !== undefined && sel !== null) {
        if (!isObj(sel)) return { ok: false, error: `${method}: params.selection must be an object or null` };
        for (const key of ['start', 'end'] as const) {
          const pt = sel[key];
          if (!isObj(pt) || !isNum(pt.line) || !isNum(pt.character))
            return { ok: false, error: `${method}: params.selection.${key} must have numeric line and character` };
        }
      }
      if (params.text !== undefined && typeof params.text !== 'string')
        return { ok: false, error: `${method}: params.text must be a string` };
      if (params.filePath !== undefined && typeof params.filePath !== 'string')
        return { ok: false, error: `${method}: params.filePath must be a string` };
      return { ok: true };
    }
    case 'at_mentioned': {
      // IDE-as-MCP-server at-mention (parity with the reference's `AtMentionedSchema`
      // in hooks/useIdeAtMentioned.ts): a required `filePath` plus optional 0-based
      // `lineStart`/`lineEnd` the handler normalizes to 1-based.
      if (!isObj(params) || typeof params.filePath !== 'string' || params.filePath.length === 0)
        return { ok: false, error: `${method}: params.filePath must be a non-empty string` };
      if (params.lineStart !== undefined && !isNum(params.lineStart))
        return { ok: false, error: `${method}: params.lineStart must be a number` };
      if (params.lineEnd !== undefined && !isNum(params.lineEnd))
        return { ok: false, error: `${method}: params.lineEnd must be a number` };
      return { ok: true };
    }
    default:
      return { ok: true };
  }
}

/**
 * A point in an IDE editor selection: 0-based `line` and `character`, exactly as
 * an IDE-as-MCP-server emits them in a `selection_changed` notification (parity
 * with the reference's `SelectionPoint` in hooks/useIdeSelection.ts).
 */
export interface McpSelectionPoint {
  line: number;
  character: number;
}

/**
 * Raw `params` of a `selection_changed` IDE notification. `selection` is `null`
 * when the editor has no active range. Mirrors the reference's `SelectionData`.
 */
export interface McpSelectionChangedParams {
  selection: { start: McpSelectionPoint; end: McpSelectionPoint } | null;
  text?: string;
  filePath?: string;
}

/**
 * Normalized IDE selection emitted on the client's `ide-selection` event (parity
 * with the reference's `IDESelection`). `lineStart` stays 0-based (matching the reference's
 * `selectionChangeHandler`); `lineCount` is the number of fully-selected lines.
 */
export interface McpIdeSelection {
  lineCount: number;
  lineStart?: number;
  text?: string;
  filePath?: string;
}

/**
 * Normalized IDE at-mention emitted on the client's `ide-at-mentioned` event with
 * 1-based line numbers (parity with the reference's `IDEAtMentioned`).
 */
export interface McpIdeAtMention {
  filePath: string;
  lineStart?: number;
  lineEnd?: number;
}

/**
 * Convert a validated `selection_changed` payload into a normalized
 * {@link McpIdeSelection}, mirroring the reference's `selectionChangeHandler`
 * (hooks/useIdeSelection.ts): `lineCount = end.line - start.line + 1`, decremented
 * by one when the selection ends on the first character of a line (so a trailing
 * caret at column 0 does not count its line). Returns `null` when the payload
 * carries no concrete start/end range, so a caret-only/empty event emits nothing.
 */
export function normalizeIdeSelection(params: McpSelectionChangedParams): McpIdeSelection | null {
  const sel = params.selection;
  if (sel?.start && sel?.end) {
    const { start, end } = sel;
    let lineCount = end.line - start.line + 1;
    if (end.character === 0) lineCount--;
    return { lineCount, lineStart: start.line, text: params.text, filePath: params.filePath };
  }
  return null;
}

/**
 * Normalize a validated `at_mentioned` payload, converting the IDE's 0-based
 * `lineStart`/`lineEnd` to the 1-based line numbers AMI presents to the user/model
 * — adding 1 only to a defined value (parity with the reference's
 * hooks/useIdeAtMentioned.ts:L57-61).
 */
export function normalizeIdeAtMention(
  params: { filePath: string; lineStart?: number; lineEnd?: number },
): McpIdeAtMention {
  return {
    filePath: params.filePath,
    lineStart: params.lineStart !== undefined ? params.lineStart + 1 : undefined,
    lineEnd: params.lineEnd !== undefined ? params.lineEnd + 1 : undefined,
  };
}

/**
 * Detect a 403 that signals the bearer token was REVOKED/invalidated server-side,
 * as opposed to an `insufficient_scope` step-up. A revoked or otherwise invalid
 * token must drive a FULL re-authorization (`onUnauthorized`) — a scope step-up
 * (`onForbidden`) or a silent refresh cannot resurrect a revoked grant and would
 * loop. Matches the reference's `also403Revoked` (utils/http.ts): a 403 whose
 * `WWW-Authenticate` challenge or response body marks the token invalid/revoked is
 * treated as a re-auth trigger. Both sources are checked case-insensitively; an
 * ordinary `access_denied`/`insufficient_scope` 403 returns false so the existing
 * step-up path still runs.
 */
export function is403RevokedToken(wwwAuthenticate: string | null, body: string): boolean {
  const hay = `${wwwAuthenticate ?? ''}\n${body ?? ''}`.toLowerCase();
  return hay.includes('invalid_token') || hay.includes('revoked');
}

export const MCP_CLIENT_STATES = ['disconnected', 'connecting', 'ready', 'error', 'needs-auth'] as const;
export type McpClientState = (typeof MCP_CLIENT_STATES)[number];

/**
 * JSON-RPC server error code that signals a tool call cannot proceed until the
 * user completes one or more URL elicitations (opens a URL in a browser). The
 * server returns `-32042` with `data.elicitations: [{ mode:'url', url,
 * elicitationId, message }, …]`; {@link McpClient.callTool} drives each
 * elicitation then retries the call (parity with the reference's
 * UrlElicitationRequiredError / callMCPToolWithUrlElicitationRetry).
 */
export const MCP_URL_ELICITATION_REQUIRED_CODE = -32042;
/** Max times a single {@link McpClient.callTool} retries after a `-32042`. */
export const MAX_MCP_URL_ELICITATION_RETRIES = 3;

/**
 * A JSON-RPC error returned by an MCP server, preserving the numeric `code` and
 * optional `data` payload (not just a flattened message) so callers can branch on
 * well-known codes — e.g. the URL-elicitation-required retry loop keys off
 * {@link MCP_URL_ELICITATION_REQUIRED_CODE} and reads `data.elicitations`.
 */
export class McpRpcError extends Error {
  readonly code: number;
  readonly data?: unknown;
  constructor(code: number, message: string, data?: unknown) {
    super(`MCP error ${code}: ${message}`);
    this.name = 'McpRpcError';
    this.code = code;
    this.data = data;
  }
}

/**
 * An unrecoverable MCP authentication failure — the bearer token is gone/revoked
 * and (re)authorization did not succeed. Thrown from the HTTP transport's 401 /
 * 403-revoked paths after a re-auth attempt fails; the client transitions to the
 * `needs-auth` state so the manager/UI can surface a "requires authentication,
 * use /mcp to authenticate" affordance (parity with the reference flipping a client
 * to `needs-auth` on McpAuthError).
 */
export class McpAuthError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'McpAuthError';
  }
}

/**
 * URL-elicitation descriptor carried in a `-32042` error's `data.elicitations`.
 * Extract + validate the url-mode entries so the tool-call retry loop only acts
 * on well-formed server requests (parity with the reference's
 * `ElicitRequestURLParams` runtime filter).
 */
export interface McpUrlElicitation {
  mode: 'url';
  url: string;
  elicitationId: string;
  message: string;
}

export function extractUrlElicitations(data: unknown): McpUrlElicitation[] {
  if (data == null || typeof data !== 'object') return [];
  const raw = (data as { elicitations?: unknown }).elicitations;
  if (!Array.isArray(raw)) return [];
  return raw.filter((e): e is McpUrlElicitation => {
    if (e == null || typeof e !== 'object') return false;
    const o = e as Record<string, unknown>;
    return o.mode === 'url'
      && typeof o.url === 'string'
      && typeof o.elicitationId === 'string'
      && typeof o.message === 'string';
  });
}

// Only codes classifyError() can actually PRODUCE are advertised here (audit #8: the old
// 'SESSION_EXPIRED' was never produced — no MCP transport emitted it — so it was removed to
// keep the --introspect manifest honest. Re-add it only alongside code that produces it.)
export const MCP_TERMINAL_ERROR_CODES = ['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ECONNREFUSED', 'ESRCH', 'SPAWN_ERROR', 'UNKNOWN'] as const;
export type McpTerminalErrorCode = (typeof MCP_TERMINAL_ERROR_CODES)[number];

export interface McpTerminalError {
  code: McpTerminalErrorCode;
  message: string;
  original?: Error;
}

export class McpSessionExpiredError extends Error {
  constructor(message: string = 'MCP session expired') {
    super(message);
    this.name = 'McpSessionExpiredError';
  }
}

const TERMINAL_ERROR_CODES = new Set([
  'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'EHOSTUNREACH', 'ECONNREFUSED', 'ESRCH',
]);

const SHUTDOWN_SIGINT_MS = 100;
const SHUTDOWN_SIGTERM_MS = 400;
const SHUTDOWN_TOTAL_MS = 500;
const SHUTDOWN_POLL_MS = 50;

/** Backoff bounds for re-opening the Streamable-HTTP server→client GET stream. */
const HTTP_STREAM_RECONNECT_BASE_MS = 1000;
const HTTP_STREAM_RECONNECT_MAX_MS = 30000;

/**
 * ±25% proportional jitter applied to the capped exponential reconnect delay so
 * many clients dropped by a shared outage do not retry in lockstep (thundering
 * herd). Matches the fraction the reference's transports use.
 */
const RECONNECT_JITTER_FRAC = 0.25;

/**
 * Liveness/idle timeout for the Streamable-HTTP server→client GET SSE stream.
 * A compliant server sends keepalives (SSE comment frames) periodically; if NO
 * bytes at all arrive for this long the connection is treated as dead (a
 * half-open TCP socket silently dropped by a NAT/proxy/load-balancer with no
 * FIN/RST). The timer is reset on EVERY incoming chunk (including keepalive
 * comment frames), and on expiry the GET request is destroyed and a reconnect
 * is scheduled. Mirrors the reference's SSETransport LIVENESS_TIMEOUT_MS (45s).
 */
const HTTP_STREAM_LIVENESS_TIMEOUT_MS = 45000;

/**
 * Wall-clock budget for re-opening the GET stream before giving up (10 minutes),
 * measured from the first failure of a run of consecutive failures. A successful
 * (re)connect resets the clock, so the stream survives any number of brief
 * transient drops as long as no single outage lasts past the budget — unlike a
 * small fixed attempt count that gives up after a few seconds of backoff.
 * Mirrors the reference's SSETransport RECONNECT_GIVE_UP_MS.
 */
const HTTP_STREAM_RECONNECT_GIVE_UP_MS = 600000;

/**
 * HTTP status codes on the GET stream that indicate a PERMANENT server-side
 * rejection of the credential — retrying would just re-send the same rejected
 * bearer/session in a loop. On any of these the stream closes immediately
 * without reconnecting. (404 is handled separately as "stream unsupported →
 * degrade to POST-only", so it is NOT in this set.) Mirrors the reference's
 * SSETransport PERMANENT_HTTP_CODES minus the 404 special-case.
 */
const PERMANENT_HTTP_STREAM_CODES = new Set([401, 403]);

/**
 * Cap on the dedup set of seen SSE sequence numbers before pruning, and how far
 * below the high-water mark a pruned entry must be. Only sequence numbers near
 * the high-water mark matter for duplicate detection, so older ones are dropped
 * to keep the set bounded. Mirrors the reference's SSETransport pruning strategy.
 */
const HTTP_STREAM_SEEN_SEQ_CAP = 1000;
const HTTP_STREAM_SEEN_SEQ_PRUNE_MARGIN = 200;

/**
 * How many trailing stderr lines a stdio MCP server's output is retained for. On
 * an abnormal exit the tail is appended to the failure message so a consumer can
 * see WHY the child died (e.g. a stack trace it printed right before exiting)
 * instead of only a bare exit code. Bounded so a chatty server can't grow it.
 */
export const MCP_STDERR_TAIL_LINES = 10;

export class McpClient extends EventEmitter {
  private process: ChildProcess | null = null;
  private processPid: number | null = null;
  private requestId = 0;
  private pendingRequests = new Map<number, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    /** Undefined when the request is deliberately unbounded (handshake — bounded by the outer connect guard instead). */
    timer: ReturnType<typeof setTimeout> | undefined;
    abortHandler?: () => void;
  }>();
  private buffer = '';
  private _state: McpClientState = 'disconnected';
  private _serverInfo: { name: string; version: string } | null = null;
  private _instructions: string | null = null;
  /** In-flight server→client requests (by their id) that a 'notifications/cancelled' can abort. */
  private _inboundAborts = new Map<number | string, AbortController>();
  private _serverCapabilities: Record<string, unknown> = {};
  private _protocolVersion: string | null = null;
  private _tools: McpToolSchema[] = [];
  private _resources: McpResource[] = [];
  private _prompts: McpPrompt[] = [];
  private transport: McpTransport = 'stdio';
  private sseUrl: string | null = null;
  /** The live WebSocket for a `ws`/`wss` transport (null on other transports). */
  private ws: McpWebSocketLike | null = null;
  /** The linked client-side transport for an `inprocess` transport (null otherwise). */
  private inProcessTransport: InProcessTransport | null = null;
  /** Factory for the WebSocket, injectable for tests; defaults to the runtime global. */
  private readonly webSocketFactory?: McpWebSocketFactory;
  private readonly command: string;
  private readonly args: string[];
  private readonly env: Record<string, string>;
  private readonly requestTimeout: number;
  private readonly connectTimeout: number;
  private rootPaths: string[];
  /** Static + dynamically-resolved request headers, refreshable via {@link setHeaders}. */
  private headers: Record<string, string>;
  /** Optional OAuth 2.1 layer for the HTTP transport (bearer token + 401 re-auth). */
  private readonly oauth?: McpOAuthProvider;

  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;
  /**
   * When false, {@link reconnect} does NOT run its automatic retry loop — it tears
   * the connection down and throws at once so a caller that owns its own recovery
   * (e.g. an external supervisor/poll loop) is not fighting a second reconnect
   * driver. Defaults to true. Parity with the reference's
   * `WebSocketTransportOptions.autoReconnect`.
   */
  private readonly autoReconnectEnabled: boolean;
  /** Keep-alive ping timer for a live `ws`/`wss` transport (null otherwise). */
  private wsKeepaliveTimer: ReturnType<typeof setInterval> | null = null;
  /** Wall-clock give-up budget (ms) for the client-level {@link reconnect} loop. */
  private readonly reconnectGiveUpMs: number;
  /**
   * Set true by {@link disconnect} so an in-flight {@link reconnect} loop can be
   * cancelled mid-retry (e.g. the server was stopped/disabled while a prior
   * attempt was sleeping) instead of burning the whole attempt budget. Reset at
   * the start of each reconnect run (after its own initial teardown).
   */
  private reconnectAborted = false;
  /**
   * True only while {@link reconnect} is inside its OWN connect attempt. A failed
   * attempt calls {@link disconnect} to tear down the half-open connection; this
   * flag lets disconnect() tell that internal teardown apart from an EXTERNAL
   * disconnect/stopServer (which must abort the loop).
   */
  private reconnectConnecting = false;
  /**
   * The resolver + timer of the reconnect loop's in-flight backoff sleep, so an
   * EXTERNAL {@link disconnect}/stopServer landing mid-backoff can wake it
   * immediately ({@link wakeReconnectSleep}) instead of the loop burning the full
   * (up to {@link HTTP_STREAM_RECONNECT_MAX_MS}) delay before noticing the abort.
   * Parity with the reference's wakePollLoop cancelling an in-flight at-capacity
   * sleep so a queued fault/reconnect can proceed at once (bridge/bridgeDebug.ts).
   */
  private reconnectSleepWake: (() => void) | null = null;
  private reconnectSleepTimer: ReturnType<typeof setTimeout> | null = null;
  private hasTriggeredClose = false;
  private connectionStartTime: number | null = null;
  /** Rolling tail of the last {@link MCP_STDERR_TAIL_LINES} stdio-server stderr lines. */
  private _stderrTail: string[] = [];

  private progressCallbacks = new Map<string, (progress: { percent?: number; message?: string }) => void>();
  /** Monotonic source for a per-tool-call `_meta.progressToken` (see {@link callTool}). */
  private progressTokenSeq = 0;
  /** URIs this client has an active resources/subscribe for. */
  private subscribedResources = new Set<string>();

  // --- Streamable-HTTP server→client GET SSE stream state -------------------
  /** Session id returned by the server (Mcp-Session-Id), echoed on later calls. */
  private httpSessionId: string | null = null;
  /** The in-flight GET stream request, so disconnect can tear it down. */
  private httpStreamReq: { destroy: () => void } | null = null;
  /** Partial SSE buffer for the GET stream (events split on a blank line). */
  private httpStreamBuffer = '';
  /** A trailing CR held back across SSE chunks: it may be the CR of a CRLF split at
   *  a chunk boundary, so it is resolved against the next chunk's first byte instead
   *  of re-normalizing the whole accumulated buffer each chunk (see processHttpStreamBuffer). */
  private httpStreamCarryCr = false;
  /** Set when the stream was closed intentionally (disconnect) — stops reconnect. */
  private httpStreamClosed = false;
  private httpStreamReconnectAttempts = 0;
  /** Timestamp (ms) of the first failure in the current run of consecutive GET-stream failures; null while connected. */
  private httpStreamReconnectStartTime: number | null = null;
  /** Per-chunk liveness timer; fires if the GET stream goes silent past {@link httpStreamLivenessTimeout}. */
  private httpStreamLivenessTimer: ReturnType<typeof setTimeout> | null = null;
  /** Idle budget (ms) for the GET stream before it is treated as half-open and reconnected. */
  private readonly httpStreamLivenessTimeout: number;
  /** High-water mark of SSE `id:` sequence numbers seen on the GET stream (for Last-Event-ID / from_sequence_num resumption). */
  private httpStreamLastSequenceNum = 0;
  /** Recently-seen SSE sequence numbers, for duplicate-frame detection across a resume/replay. */
  private httpStreamSeenSequenceNums = new Set<number>();
  /**
   * Wall-clock (ms) of the last DATA frame on the Streamable-HTTP transport — an
   * inbound SSE chunk OR an outbound POST ({@link sendHttpRequest}). Drives the
   * activity-gated keepalive: a frame within the interval makes the next keepalive
   * tick a no-op. Excludes control bookkeeping (it is set only by real data).
   */
  private httpStreamLastDataActivity = 0;
  /** Proactive keepalive timer: fires an MCP `ping` data frame when the HTTP transport has been idle a full interval, defeating proxy idle-timeout RSTs. */
  private httpKeepaliveTimer: ReturnType<typeof setInterval> | null = null;
  /** Keepalive interval (ms) for the Streamable-HTTP transport (see {@link getMcpHttpKeepaliveIntervalMs}). */
  private readonly httpKeepaliveInterval: number;

  /** Injected by the manager/engine: resolve a server elicitation request. */
  onElicitation?: (request: McpElicitationRequest) => Promise<McpElicitationResult>;
  /**
   * Injected by the manager/engine: run an LLM turn for a sampling request. The
   * optional `signal` is the per-request abort the client forwards — it fires
   * when the server sends `notifications/cancelled` for this request or the
   * connection drops, so an in-flight server-initiated model turn is cancellable
   * (parity with the reference threading the SDK's `extra.signal`).
   */
  onSampling?: (request: McpSamplingRequest, signal?: AbortSignal) => Promise<McpSamplingResult>;

  constructor(options: {
    command: string;
    args?: string[];
    env?: Record<string, string>;
    headers?: Record<string, string>;
    requestTimeout?: number;
    connectTimeout?: number;
    rootPaths?: string[];
    oauth?: McpOAuthProvider;
    /** Override the GET-stream liveness/idle timeout (ms). Defaults to {@link HTTP_STREAM_LIVENESS_TIMEOUT_MS} (45s). */
    httpStreamLivenessTimeout?: number;
    /**
     * Override the Streamable-HTTP keep-alive interval (ms) — how often an idle
     * HTTP transport sends an application-level `ping` DATA frame to keep an
     * intermediary proxy's idle timer from RSTing the connection. Defaults to
     * {@link getMcpHttpKeepaliveIntervalMs} (`MCP_HTTP_KEEPALIVE_INTERVAL_MS` or 5min).
     * Set below the proxy's idle window. Suppressed entirely when the host runs its
     * own heartbeat (`AMI_REMOTE`, see {@link isMcpWsKeepaliveSuppressed}).
     */
    httpStreamKeepaliveInterval?: number;
    /**
     * Wall-clock budget (ms) for the client-level {@link reconnect} loop before it
     * gives up, measured from the first attempt. Applied ALONGSIDE the attempt cap
     * (whichever trips first ends the loop), so a flaky link keeps retrying for a
     * bounded real-time window instead of only a few seconds of fixed-count
     * backoff. Defaults to {@link HTTP_STREAM_RECONNECT_GIVE_UP_MS} (10 min),
     * matching the GET-stream give-up budget.
     */
    reconnectGiveUpMs?: number;
    /**
     * When false, disable the automatic {@link reconnect} retry loop (a caller with
     * its own recovery mechanism opts out). Defaults to true. Parity with the reference
     * Code's `WebSocketTransportOptions.autoReconnect`.
     */
    autoReconnect?: boolean;
    /**
     * Factory for the WebSocket used by the `ws`/`wss` transport. Defaults to the
     * runtime's global `WebSocket` (Node 20+/Bun/undici). Injectable so tests can
     * drive the transport without a real socket. Receives resolved
     * {@link McpWebSocketTlsOptions} for a `wss://` URL so a `ws`-package factory can
     * apply mTLS/custom-CA trust.
     */
    webSocketFactory?: McpWebSocketFactory;
  }) {
    super();
    this.command = options.command;
    this.args = options.args ?? [];
    this.env = options.env ?? {};
    this.headers = options.headers ?? {};
    this.requestTimeout = options.requestTimeout ?? 30000;
    this.connectTimeout = options.connectTimeout ?? getMcpConnectTimeout();
    this.rootPaths = options.rootPaths ?? [];
    this.oauth = options.oauth;
    this.httpStreamLivenessTimeout = options.httpStreamLivenessTimeout ?? HTTP_STREAM_LIVENESS_TIMEOUT_MS;
    this.httpKeepaliveInterval = options.httpStreamKeepaliveInterval ?? getMcpHttpKeepaliveIntervalMs();
    this.reconnectGiveUpMs = options.reconnectGiveUpMs ?? HTTP_STREAM_RECONNECT_GIVE_UP_MS;
    this.autoReconnectEnabled = options.autoReconnect ?? true;
    this.webSocketFactory = options.webSocketFactory;
  }

  /**
   * Replace the client's request headers (static + dynamically-resolved). The
   * manager calls this at connect time for servers with a `headersHelper` so
   * rotating/short-lived auth headers are refreshed before the HTTP/SSE handshake.
   */
  setHeaders(headers: Record<string, string>): void {
    this.headers = { ...headers };
  }

  get state(): McpClientState { return this._state; }
  get serverInfo() { return this._serverInfo; }
  /** Server-authored usage instructions from InitializeResult (raw, untrimmed), or null. */
  get instructions(): string | null { return this._instructions; }
  get serverCapabilities(): Record<string, unknown> { return { ...this._serverCapabilities }; }
  /** The protocol version negotiated with the server (null until connected). */
  get protocolVersion(): string | null { return this._protocolVersion; }
  get tools(): McpToolSchema[] { return [...this._tools]; }
  get resources(): McpResource[] { return [...this._resources]; }
  get prompts(): McpPrompt[] { return [...this._prompts]; }
  get uptime(): number | null {
    return this.connectionStartTime ? Date.now() - this.connectionStartTime : null;
  }
  /** The retained tail of the stdio server's stderr (most recent lines last). */
  get stderrTail(): string[] { return [...this._stderrTail]; }
  /** False when the automatic {@link reconnect} retry loop is disabled (caller-owned recovery). */
  get autoReconnect(): boolean { return this.autoReconnectEnabled; }

  /**
   * Resolve how the stdio server process is launched. When
   * SUPERINFERENCE_MCP_SHELL_PREFIX is set (non-empty), the server is run THROUGH
   * that wrapper command instead of spawned directly: the prefix becomes the
   * executable and the whole `command arg1 arg2 …` line is handed to it as a
   * single argument, so a sandbox/shim (firejail, a seccomp wrapper, a login
   * shell) can mediate every stdio MCP server without editing each server config.
   * Absent the env var the configured command + args are spawned verbatim.
   */
  private resolveStdioLaunch(): { command: string; args: string[] } {
    const prefix = process.env.SUPERINFERENCE_MCP_SHELL_PREFIX?.trim();
    if (prefix) {
      return { command: prefix, args: [[this.command, ...this.args].join(' ')] };
    }
    // Resolve the executable name to its full PATH path up front so the launch is
    // deterministic across platforms (Windows PATHEXT especially); falls back to
    // the original name — spawn then applies its own PATH lookup — when unresolved.
    const { cmd, args } = findExecutable(this.command, this.args);
    return { command: cmd, args };
  }

  async connect(): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.hasTriggeredClose = false;

    // Scrub secrets (API keys, tokens, …) from the INHERITED environment before
    // handing it to an untrusted MCP server process; the server's own explicitly
    // configured env is intentional and layered on top (H31).
    const { command, args } = this.resolveStdioLaunch();
    this.process = spawn(command, args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...sanitizeEnvForSubprocess(process.env), ...this.env },
    });
    this.processPid = this.process.pid ?? null;

    this.process.stdin?.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code !== 'EPIPE') {
        this.emit('stderr', `MCP stdin error: ${err.message}`);
      }
    });

    this.process.stdout!.on('data', (chunk: Buffer) => {
      const s = chunk.toString();
      this.buffer += s;
      // Only re-split the accumulated buffer when this chunk actually introduces a
      // newline: the post-processBuffer remainder never contains one (lines.pop()),
      // so a newline-free chunk can add no complete line. This avoids the O(n²)
      // full-buffer split when a single large message arrives across many chunks.
      // The overflow cap still runs unconditionally so an endless newline-free
      // stream is still bounded.
      if (s.includes('\n')) this.processBuffer();
      else this.enforceStdioBufferCap();
    });

    this.process.stderr!.on('data', (chunk: Buffer) => {
      const text = chunk.toString();
      this.appendStderrTail(text);
      this.emit('stderr', text);
    });

    this.process.on('error', (err) => {
      const classified = this.classifyError(err);
      this._state = 'error';
      this.rejectAllPending(err);
      this.emit('error', err);
      this.emit('terminal-error', classified);
    });

    this.process.on('close', (code, signal) => {
      if (this.hasTriggeredClose) return;
      this.hasTriggeredClose = true;
      const uptimeMs = this.uptime;
      this._state = 'disconnected';
      this.rejectAllPending(new Error(this.formatExitMessage(code, signal)));
      this.emit('close', code, uptimeMs);
    });

    // Bound the initialize handshake with the shared canonical `boundedCall` guard
    // (clears + unrefs the timer, swallows a post-deadline orphan rejection, rejects
    // with a descriptive message) instead of an inline Promise.race copy.
    try {
      await boundedCall(() => this.performInitialize(), {
        timeoutMs: this.connectTimeout,
        message: `MCP connection timed out after ${this.connectTimeout}ms`,
      });
    } catch (err) {
      this._state = 'error';
      this.disconnect();
      throw err;
    }
  }

  /**
   * WebSocket transport (`ws://` / `wss://`) — parity with the reference's
   * `WebSocketTransport` (`utils/mcpWebSocketTransport.ts`). Opens a WebSocket via
   * the injectable factory (defaulting to the runtime global, which exists on
   * Node 20+/Bun/undici), then runs the standard initialize handshake over it.
   * Inbound frames (each a complete JSON-RPC message) route through the SAME
   * dispatcher the stdio/http transports use, so server→client requests and
   * notifications are handled identically. The `wss://` scheme is driven by the
   * same code path (TLS is handled by the socket), satisfying dual ws/wss support.
   */
  async connectWebSocket(url: string): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.transport = url.startsWith('wss') ? 'wss' : 'ws';
    this.sseUrl = url;
    this.hasTriggeredClose = false;

    const factory = this.webSocketFactory
      ?? (globalThis as { WebSocket?: new (u: string) => McpWebSocketLike }).WebSocket;
    if (!factory) {
      this._state = 'error';
      throw new Error('MCP WebSocket transport unavailable: no global WebSocket in this runtime');
    }
    // Resolve TLS connect options (client cert/CA + secure floor) for a wss:// URL
    // so a ws-package-backed factory applies mTLS exactly like the HTTP transports;
    // the runtime global WebSocket ignores the extra argument.
    const tlsOptions = resolveMcpWebSocketTlsOptions(url);
    const ws = this.webSocketFactory ? this.webSocketFactory(url, tlsOptions) : new (factory as new (u: string) => McpWebSocketLike)(url);
    this.ws = ws;

    // Phase 1 — wait for the socket to open (bounded by the connect timeout).
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(`MCP WebSocket connection timed out after ${this.connectTimeout}ms`));
      }, this.connectTimeout);
      timer.unref?.();
      const onOpen = () => { clearTimeout(timer); cleanup(); resolve(); };
      const onError = () => { clearTimeout(timer); cleanup(); reject(new Error('MCP WebSocket connection error')); };
      const cleanup = () => { ws.removeEventListener('open', onOpen); ws.removeEventListener('error', onError); };
      if (ws.readyState === MCP_WS_OPEN) { clearTimeout(timer); resolve(); return; }
      ws.addEventListener('open', onOpen);
      ws.addEventListener('error', onError);
    }).catch((err) => {
      this._state = 'error';
      this.disconnect();
      throw err;
    });

    // Attach the persistent message/close handlers before the handshake so the
    // initialize RESPONSE (which arrives as a ws frame) is routed to the pending
    // request. A frame-level error is surfaced as a redacted diagnostic that NEVER
    // echoes the untrusted payload (parity with the reference's logForDiagnosticsNoPII).
    ws.addEventListener('message', (ev) => this.handleWsMessage((ev as { data?: unknown })?.data));
    ws.addEventListener('close', () => this.handleWsClose());

    // Phase 2 — run the initialize handshake over the socket, bounded by the same
    // timeout via the shared `boundedCall` guard (orphan-rejection-safe). The
    // handshake sends/receives through writeMessage/routeMessage.
    try {
      await boundedCall(() => this.performInitialize(), {
        timeoutMs: this.connectTimeout,
        message: `MCP WebSocket connection timed out after ${this.connectTimeout}ms`,
      });
    } catch (err) {
      this._state = 'error';
      this.disconnect();
      throw err;
    }
    // The socket is live + handshaked: start the keep-alive ping so an idle-timeout
    // proxy/LB does not silently drop it (and a failed ping forces a reconnect).
    this.startWsKeepalive();
  }

  /**
   * In-process transport (`inprocess`) — parity with the reference's
   * InProcessTransport / SdkControlTransport pair. Drives the standard initialize
   * handshake over a caller-provided {@link InProcessTransport} (typically the
   * client end of a {@link createLinkedTransportPair} whose server end is wired to
   * an in-process MCP server object), so a host embedding AMI can register a tool
   * server that runs in the SAME process — no subprocess, no socket. Inbound
   * messages route through the SAME dispatcher the stdio/http/ws transports use, so
   * server→client requests and notifications are handled identically.
   */
  async connectInProcess(transport: InProcessTransport): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.transport = 'inprocess';
    this.hasTriggeredClose = false;
    this.inProcessTransport = transport;

    // Route inbound frames (each already a parsed JSON-RPC message object) through
    // the shared dispatcher; a peer close tears this client down like any transport.
    transport.onmessage = (msg) => this.routeMessage(msg as JsonRpcResponse | JsonRpcNotification);
    transport.onclose = () => this.handleInProcessClose();
    transport.onerror = (err) => this.emit('stderr', `MCP in-process transport error: ${err.message}`);
    await transport.start();

    // Bound the handshake via the shared `boundedCall` guard (orphan-rejection-safe).
    try {
      await boundedCall(() => this.performInitialize(), {
        timeoutMs: this.connectTimeout,
        message: `MCP connection timed out after ${this.connectTimeout}ms`,
      });
    } catch (err) {
      this._state = 'error';
      this.disconnect();
      throw err;
    }
  }

  /** Handle a peer close of the in-process transport: reject pending work, emit `close`. */
  private handleInProcessClose(): void {
    if (this.hasTriggeredClose) return;
    this.hasTriggeredClose = true;
    const uptimeMs = this.uptime;
    this._state = 'disconnected';
    this.inProcessTransport = null;
    this.connectionStartTime = null;
    this.rejectAllPending(new Error('MCP in-process transport closed'));
    this.emit('close', null, uptimeMs);
  }

  /**
   * Start a periodic keep-alive ping on the live WebSocket transport. Each tick
   * sends an MCP `ping` request (bounded by the request timeout) and on failure
   * tears the socket down so the normal close→reconnect path runs. Suppressed when
   * {@link isMcpWsKeepaliveSuppressed} (an outer heartbeat already covers idle
   * timeout). The interval is unref'd so it never keeps the event loop alive.
   */
  private startWsKeepalive(): void {
    this.stopWsKeepalive();
    if (isMcpWsKeepaliveSuppressed()) return;
    const timer = setInterval(() => {
      if (this._state !== 'ready' || this.ws?.readyState !== MCP_WS_OPEN) return;
      this.ping().catch(() => {
        // A failed keep-alive ping means the socket is dead; close it so the
        // close handler's reconnect path runs instead of pinging into the void.
        const sock = this.ws;
        this.ws = null;
        if (sock) { try { sock.close(); } catch { /* already gone */ } }
      });
    }, getMcpWsKeepaliveIntervalMs());
    timer.unref?.();
    this.wsKeepaliveTimer = timer;
  }

  /** Stop the WebSocket keep-alive ping timer (idempotent). */
  private stopWsKeepalive(): void {
    if (this.wsKeepaliveTimer) {
      clearInterval(this.wsKeepaliveTimer);
      this.wsKeepaliveTimer = null;
    }
  }

  /**
   * Start the proactive Streamable-HTTP keep-alive. On each tick, if the transport
   * has been idle for a full {@link httpKeepaliveInterval} (no inbound SSE chunk and
   * no outbound POST in that window), send an MCP `ping` REQUEST — a real DATA frame
   * over the POST leg — to reset an intermediary proxy's idle timer (ping/pong and
   * SSE comment frames a proxy may ignore; a request body it cannot). This keeps a
   * long-idle connection warm PROACTIVELY instead of only recovering after the proxy
   * RSTs it ({@link scheduleHttpStreamReconnect}). Activity-gated so real traffic
   * makes a tick a no-op. Suppressed when an outer host heartbeat already covers
   * idle-timeout ({@link isMcpWsKeepaliveSuppressed} / `AMI_REMOTE`). The interval is
   * unref'd so it never keeps the event loop alive. Parity with the reference's
   * WebSocketTransport startKeepaliveInterval() keep_alive data frame.
   */
  private startHttpKeepalive(): void {
    this.stopHttpKeepalive();
    if (isMcpWsKeepaliveSuppressed()) return;
    const interval = this.httpKeepaliveInterval;
    const timer = setInterval(() => {
      if (this._state !== 'ready' || this.transport !== 'http' || this.httpStreamClosed) return;
      const idleMs = Date.now() - this.httpStreamLastDataActivity;
      // Real traffic within the interval already kept the path warm — skip.
      if (idleMs < interval) return;
      this.emit('http-stream-keepalive', idleMs);
      this.ping().catch(() => {
        // A failed keep-alive ping means the POST leg is down; the GET-stream
        // liveness watchdog + reconnect path recover it, so nothing to do here.
      });
    }, interval);
    timer.unref?.();
    this.httpKeepaliveTimer = timer;
  }

  /** Stop the Streamable-HTTP keep-alive timer (idempotent). */
  private stopHttpKeepalive(): void {
    if (this.httpKeepaliveTimer) {
      clearInterval(this.httpKeepaliveTimer);
      this.httpKeepaliveTimer = null;
    }
  }

  /** Route one inbound WebSocket frame (a complete JSON-RPC message or batch). */
  private handleWsMessage(data: unknown): void {
    const text = typeof data === 'string' ? data : String(data);
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        for (const m of parsed) this.routeMessage(m as JsonRpcResponse | JsonRpcNotification);
      } else {
        this.routeMessage(parsed as JsonRpcResponse | JsonRpcNotification);
      }
    } catch {
      // No-PII diagnostic: a malformed frame from an untrusted server is surfaced
      // as a stable marker WITHOUT echoing the raw payload (parity with the reference
      // Code's mcpWebSocketTransport logForDiagnosticsNoPII).
      this.emit('parse-error', '[mcp websocket frame parse error]');
    }
  }

  /** Handle an unexpected WebSocket close: reject pending work and emit `close`. */
  private handleWsClose(): void {
    if (this.hasTriggeredClose) return;
    this.hasTriggeredClose = true;
    this.stopWsKeepalive();
    const uptimeMs = this.uptime;
    this._state = 'disconnected';
    this.ws = null;
    this.connectionStartTime = null;
    this.rejectAllPending(new Error('MCP WebSocket closed'));
    this.emit('close', null, uptimeMs);
  }

  /**
   * The client capabilities AMI advertises at `initialize`. Single source of
   * truth for all three transports so a server sees identical support
   * regardless of how it was launched: elicitation + sampling always (AMI can
   * fulfill both, gracefully declining/erroring when no handler is wired), and
   * roots (with listChanged) only when root paths were configured.
   */
  private buildClientCapabilities(): Record<string, unknown> {
    const caps: Record<string, unknown> = { elicitation: {}, sampling: {} };
    if (this.rootPaths.length > 0) {
      caps.roots = { listChanged: true };
    }
    return caps;
  }

  private async performInitialize(): Promise<void> {
    // The initialize handshake is bounded SOLELY by the outer `boundedCall(connectTimeout)`
    // guard at every connect call site. Pass a non-finite inner timeout so `sendRequest`
    // does not arm its own (`requestTimeout`-based) deadline — a short `requestTimeout`
    // must never abort the handshake before the connect budget, which otherwise raced the
    // wall clock under load. `connectTimeout` stays the single authority here.
    const initResult = await this.sendRequest('initialize', {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities: this.buildClientCapabilities(),
      clientInfo: AMI_CLIENT_INFO,
    }, undefined, Number.POSITIVE_INFINITY) as { serverInfo?: { name: string; version: string }; capabilities?: Record<string, unknown>; protocolVersion?: string; instructions?: string };

    this._serverInfo = initResult.serverInfo ?? { name: 'unknown', version: '0.0.0' };
    this._serverCapabilities = initResult.capabilities ?? {};
    this._protocolVersion = initResult.protocolVersion ?? MCP_PROTOCOL_VERSION;
    this._instructions = typeof initResult.instructions === 'string' ? initResult.instructions : null;

    await this.sendNotification('notifications/initialized', {});

    this.connectionStartTime = Date.now();
    this._state = 'ready';
    this.emit('ready');
  }

  async connectSSE(url: string): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.transport = 'sse';
    this.sseUrl = url;
    this.hasTriggeredClose = false;

    const http = url.startsWith('https') ? require('https') : require('http');
    const urlObj = new URL(url);

    const initBody = JSON.stringify({
      jsonrpc: '2.0',
      id: 1,
      method: 'initialize',
      params: {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: this.buildClientCapabilities(),
        clientInfo: AMI_CLIENT_INFO,
      },
    });

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        req.destroy();
        this._state = 'error';
        reject(new Error(`MCP SSE connection timed out after ${this.connectTimeout}ms`));
      }, this.connectTimeout);
      timer.unref?.();

      const req = http.request(urlObj, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json, text/event-stream',
          'User-Agent': getMcpUserAgent(),
          ...this.headers,
        },
        timeout: this.requestTimeout,
      }, (res: any) => {
        const statusCode = res.statusCode;
        let body = '';
        res.on('data', (d: Buffer) => body += d);
        res.on('end', () => {
          clearTimeout(timer);
          if (statusCode === 404 && this.isSessionExpiredBody(body)) {
            this._state = 'error';
            reject(new McpSessionExpiredError());
            return;
          }
          try {
            const result = JSON.parse(body);
            if (result.error) {
              if (result.error.code === -32001) {
                this._state = 'error';
                reject(new McpSessionExpiredError());
                return;
              }
              this._state = 'error';
              reject(new Error(`MCP SSE init error ${result.error.code}: ${result.error.message}`));
              return;
            }
            this._serverInfo = result.result?.serverInfo ?? { name: 'unknown', version: '0.0.0' };
            this._serverCapabilities = result.result?.capabilities ?? {};
            this._protocolVersion = result.result?.protocolVersion ?? MCP_PROTOCOL_VERSION;
            this._instructions = typeof result.result?.instructions === 'string' ? result.result.instructions : null;
            this.connectionStartTime = Date.now();
            this._state = 'ready';
            this.emit('ready');
            resolve();
          } catch {
            this._state = 'error';
            reject(new Error('Invalid SSE init response'));
          }
        });
      });
      req.on('error', (err: Error) => {
        clearTimeout(timer);
        const classified = this.classifyError(err);
        this._state = 'error';
        this.emit('terminal-error', classified);
        reject(err);
      });
      req.write(initBody);
      req.end();
    });
  }

  async connectHTTP(url: string): Promise<void> {
    if (this._state === 'ready') return;
    this._state = 'connecting';
    this.transport = 'http';
    this.sseUrl = url;
    this.hasTriggeredClose = false;

    const initResult = await this.sendHttpRequest('initialize', {
      protocolVersion: MCP_PROTOCOL_VERSION,
      capabilities: this.buildClientCapabilities(),
      clientInfo: AMI_CLIENT_INFO,
    }) as { serverInfo?: { name: string; version: string }; capabilities?: Record<string, unknown>; protocolVersion?: string; instructions?: string };

    this._serverInfo = initResult?.serverInfo ?? { name: 'unknown', version: '0.0.0' };
    this._serverCapabilities = initResult?.capabilities ?? {};
    this._protocolVersion = initResult?.protocolVersion ?? MCP_PROTOCOL_VERSION;
    this._instructions = typeof initResult?.instructions === 'string' ? initResult.instructions : null;

    await this.sendHttpRequest('notifications/initialized', {});

    this.connectionStartTime = Date.now();
    this._state = 'ready';
    this.emit('ready');

    // Streamable-HTTP: open the server→client GET SSE stream so the server can
    // push notifications and requests (sampling, elicitation, progress, roots).
    // Best-effort — a server that declines (405) leaves us on the POST-only path.
    this.httpStreamClosed = false;
    this.httpStreamReconnectAttempts = 0;
    this.openHttpStream().catch(() => { /* POST path remains fully usable */ });

    // Proactively keep the (possibly proxied) connection warm: an application-level
    // `ping` data frame on a sub-idle-timeout interval defeats idle-timeout RSTs
    // the GET-stream liveness watchdog would otherwise only recover from reactively.
    this.httpStreamLastDataActivity = Date.now();
    this.startHttpKeepalive();
  }

  private async sendWithRetry<T>(method: string, params: Record<string, unknown>, maxAttempts: number = 3): Promise<T> {
    const { isTransientConnectError } = require('./manager');
    let lastErr: unknown;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        return await this.sendRequestAuto(method, params) as T;
      } catch (err) {
        lastErr = err;
        if (err instanceof McpSessionExpiredError || !isTransientConnectError(err)) throw err;
        if (attempt < maxAttempts - 1) {
          await new Promise(r => setTimeout(r, 1000 * (attempt + 1)));
        }
      }
    }
    throw lastErr;
  }

  /**
   * Drive a cursor-paginated list method to completion (2025-06-18). Follows
   * `nextCursor` until the server stops returning one, concatenating the array
   * under `key`. Bounded by MCP_MAX_PAGES so a server that never drops the
   * cursor cannot loop forever (it logs and stops rather than hanging).
   */
  private async collectPaginated<T>(method: string, key: string): Promise<T[]> {
    const items: T[] = [];
    let cursor: string | undefined;
    for (let page = 0; page < MCP_MAX_PAGES; page++) {
      const params: Record<string, unknown> = cursor !== undefined ? { cursor } : {};
      const result = await this.sendWithRetry<Record<string, unknown>>(method, params);
      const batch = (result?.[key] as T[] | undefined) ?? [];
      items.push(...batch);
      const next = result?.nextCursor;
      if (typeof next !== 'string' || next.length === 0) return items;
      cursor = next;
    }
    this.emit('pagination-capped', method, items.length);
    return items;
  }

  async listTools(): Promise<McpToolSchema[]> {
    this.assertReady();
    // Sanitize untrusted server-supplied tool metadata (H26).
    this._tools = recursivelySanitizeUnicode(await this.collectPaginated<McpToolSchema>('tools/list', 'tools'));
    return this._tools;
  }

  async callTool(name: string, args: Record<string, unknown> = {}, options?: { signal?: AbortSignal; timeoutMs?: number }): Promise<unknown> {
    this.assertReady();
    if (options?.signal?.aborted) {
      throw new Error('MCP tool call aborted');
    }
    // Tool calls use a separate (long) timeout so a slow tool is not killed by the
    // short connect/request timeout (H25/H30). A slow tool would otherwise go
    // silent for minutes; emit a periodic heartbeat so a frontend/log can show
    // "<tool> still running (Ns elapsed)" (parity with the reference).
    const startTime = Date.now();
    const heartbeat = setInterval(() => {
      const elapsedMs = Date.now() - startTime;
      this.emit('tool-heartbeat', {
        name,
        elapsedMs,
        elapsedSeconds: Math.floor(elapsedMs / 1000),
      });
    }, getMcpToolHeartbeatIntervalMs());
    // Never let the heartbeat timer keep the event loop alive on its own.
    heartbeat.unref?.();
    // Attach a unique `_meta.progressToken` to the call so the server can push
    // `notifications/progress` for THIS tool call; route those to a `tool-progress`
    // event carrying the determinate percent (when the server sends total) and any
    // message, so a frontend can render a live progress bar for the in-flight call
    // (parity with the reference's per-tool-call onProgress / progressToken). Cleared
    // the instant the call settles so the token map stays bounded.
    const progressToken = `mcp-${++this.progressTokenSeq}`;
    this.progressCallbacks.set(progressToken, (progress) => {
      this.emit('tool-progress', { name, percent: progress.percent, message: progress.message });
    });
    try {
      // URL-elicitation-required (-32042) retry loop: a server can refuse a tool
      // call until the user opens one or more URLs, returning the elicitations in
      // the error's `data`. Drive each elicitation (url mode) via the injected
      // handler, then re-run the call — bounded to MAX_MCP_URL_ELICITATION_RETRIES
      // so a server that keeps re-requesting cannot loop forever (parity with
      // the reference's callMCPToolWithUrlElicitationRetry).
      for (let attempt = 0; ; attempt++) {
        try {
          const result = await this.sendRequestAuto('tools/call', { name, arguments: args, _meta: { progressToken } }, options?.signal, options?.timeoutMs);
          // Sanitize untrusted tool result content before it reaches the model (H26).
          return recursivelySanitizeUnicode(result);
        } catch (err) {
          if (!(err instanceof McpRpcError) || err.code !== MCP_URL_ELICITATION_REQUIRED_CODE) throw err;
          if (attempt >= MAX_MCP_URL_ELICITATION_RETRIES) throw err;
          const elicitations = extractUrlElicitations(err.data);
          // A -32042 with no well-formed url elicitations is unactionable — surface
          // the original error rather than silently retrying an identical call.
          if (elicitations.length === 0 || !this.onElicitation) throw err;
          const aborted = await this.resolveUrlElicitations(elicitations, options?.signal);
          // A declined/cancelled elicitation ends the call with an informative
          // (non-throwing) result — the user chose not to complete the step.
          if (aborted) return aborted;
          // All elicitations accepted — loop retries the tool call.
        }
      }
    } finally {
      clearInterval(heartbeat);
      this.progressCallbacks.delete(progressToken);
    }
  }

  /**
   * Present each URL elicitation from a `-32042` tool-call error to the user via
   * the injected {@link onElicitation} handler. Returns `undefined` when every
   * elicitation is accepted (caller retries the tool call), or a terminal MCP tool
   * result describing the decline/cancel otherwise (parity with the reference
   * returning an informative content block instead of throwing).
   */
  private async resolveUrlElicitations(
    elicitations: McpUrlElicitation[],
    signal?: AbortSignal,
  ): Promise<{ content: Array<{ type: 'text'; text: string }>; isError: true } | undefined> {
    for (const e of elicitations) {
      if (signal?.aborted) throw new Error('MCP tool call aborted');
      const result = await this.onElicitation!({
        message: e.message,
        requestedSchema: {},
        mode: 'url',
        url: e.url,
        elicitationId: e.elicitationId,
        signal,
      });
      if (result.action !== 'accept') {
        const verb = result.action === 'decline' ? 'declined' : 'cancelled';
        return {
          content: [{
            type: 'text',
            text: `URL elicitation was ${verb}. The tool could not complete because it requires opening ${e.url}.`,
          }],
          isError: true,
        };
      }
    }
    return undefined;
  }

  async listResources(): Promise<McpResource[]> {
    this.assertReady();
    // Serve the warm cache when populated — it is invalidated on
    // notifications/resources/list_changed and on disconnect, so a cache hit here
    // means the server's resource list has not changed since the last fetch
    // (parity with the reference's LRU-cached fetchResourcesForClient). A server
    // with zero resources leaves the cache empty and simply re-queries.
    if (this._resources.length > 0) return [...this._resources];
    this._resources = recursivelySanitizeUnicode(await this.collectPaginated<McpResource>('resources/list', 'resources'));
    return this._resources;
  }

  /** resources/templates/list — parameterized resource URIs (2025-06-18). */
  async listResourceTemplates(): Promise<McpResourceTemplate[]> {
    this.assertReady();
    return recursivelySanitizeUnicode(await this.collectPaginated<McpResourceTemplate>('resources/templates/list', 'resourceTemplates'));
  }

  async readResource(uri: string, signal?: AbortSignal): Promise<unknown> {
    this.assertReady();
    if (signal?.aborted) {
      throw new Error('MCP request aborted');
    }
    return recursivelySanitizeUnicode(await this.sendRequestAuto('resources/read', { uri }, signal));
  }

  /** resources/subscribe — ask the server to emit notifications/resources/updated for a URI. */
  async subscribeResource(uri: string): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('resources/subscribe', { uri });
    this.subscribedResources.add(uri);
  }

  async unsubscribeResource(uri: string): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('resources/unsubscribe', { uri });
    this.subscribedResources.delete(uri);
  }

  async listPrompts(): Promise<McpPrompt[]> {
    this.assertReady();
    this._prompts = recursivelySanitizeUnicode(await this.collectPaginated<McpPrompt>('prompts/list', 'prompts'));
    return this._prompts;
  }

  async getPrompt(name: string, args: Record<string, string> = {}): Promise<{ description?: string; messages: McpPromptMessage[] }> {
    this.assertReady();
    const result = await this.sendRequestAuto('prompts/get', { name, arguments: args }) as {
      description?: string;
      messages: McpPromptMessage[];
    };
    return recursivelySanitizeUnicode(result);
  }

  /** logging/setLevel — set the minimum severity the server should emit. */
  async setLogLevel(level: McpLogLevel): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('logging/setLevel', { level });
  }

  /** completion/complete — argument autocompletion for a prompt or resource template. */
  async complete(
    ref: McpCompletionRef,
    argument: { name: string; value: string },
    context?: { arguments?: Record<string, string> },
  ): Promise<McpCompletionResult> {
    this.assertReady();
    const params: Record<string, unknown> = { ref, argument };
    if (context) params.context = context;
    const result = await this.sendRequestAuto('completion/complete', params) as { completion?: McpCompletionResult };
    return result?.completion ?? { values: [] };
  }

  async ping(): Promise<void> {
    this.assertReady();
    await this.sendRequestAuto('ping', {});
  }

  async reconnect(): Promise<void> {
    this.disconnect();
    // autoReconnect disabled: the caller owns recovery, so do not run the retry
    // loop — fail fast after the teardown above (parity with the reference going
    // straight to 'closed' when WebSocketTransportOptions.autoReconnect is false).
    if (!this.autoReconnectEnabled) {
      throw new Error('MCP automatic reconnection is disabled (autoReconnect=false)');
    }
    // Our own teardown above flips the abort flag; clear it so THIS run can retry.
    this.reconnectAborted = false;
    const startTime = Date.now();
    while (this.reconnectAttempts < this.maxReconnectAttempts) {
      await this.reconnectBackoffSleep(this.reconnectDelay(this.reconnectAttempts));
      // A disconnect()/stopServer landed while we were sleeping — abort gracefully
      // instead of continuing to retry a server the caller no longer wants.
      if (this.reconnectAborted) return;
      // Give up once the continuous outage exceeds the wall-clock budget, even if
      // attempts remain — a bounded real-time window rather than a fixed count.
      if (Date.now() - startTime >= this.reconnectGiveUpMs) {
        throw new Error(`MCP reconnection gave up after ${this.reconnectGiveUpMs}ms`);
      }
      this.reconnectConnecting = true;
      try {
        if ((this.transport === 'ws' || this.transport === 'wss') && this.sseUrl) {
          await this.connectWebSocket(this.sseUrl);
        } else if (this.transport === 'sse' && this.sseUrl) {
          await this.connectSSE(this.sseUrl);
        } else if (this.transport === 'http' && this.sseUrl) {
          // HTTP transport must re-establish over HTTP; falling through to
          // connect() would spawn an (empty) stdio command and never reconnect.
          await this.connectHTTP(this.sseUrl);
        } else {
          await this.connect();
        }
        this.reconnectAttempts = 0;
        return;
      } catch {
        this.reconnectAttempts++;
      } finally {
        this.reconnectConnecting = false;
      }
    }
    throw new Error(`MCP reconnection failed after ${this.maxReconnectAttempts} attempts`);
  }

  onToolProgress(callId: string, callback: (progress: { percent?: number; message?: string }) => void): void {
    this.progressCallbacks.set(callId, callback);
  }

  hasCapability(name: string): boolean {
    return name in this._serverCapabilities;
  }

  /**
   * Whether the connected server declared a given EXPERIMENTAL capability under
   * `capabilities.experimental` (MCP spec's extension namespace). Unlike
   * {@link hasCapability} (top-level standard capabilities), this inspects the
   * nested `experimental` object a server uses to opt into non-standard
   * extensions such as `claude/channel` (the reference-parity). Presence is keyed on a
   * declared value — a key mapped to `undefined` is treated as absent, mirroring
   * the reference's `experimental?.[name] !== undefined` check
   * (channelPermissions.ts:L191-192).
   */
  hasExperimentalCapability(name: string): boolean {
    const experimental = this._serverCapabilities.experimental;
    if (!experimental || typeof experimental !== 'object') return false;
    return (experimental as Record<string, unknown>)[name] !== undefined;
  }

  /**
   * Whether this server has OPTED IN to relaying permission prompts over a
   * message channel. Requires BOTH experimental capabilities to be declared —
   * {@link MCP_CHANNEL_CAPABILITY} AND {@link MCP_CHANNEL_PERMISSION_CAPABILITY}
   * — so a relay-only channel never becomes a permission surface by accident
   * (the reference-parity `filterPermissionRelayClients`, channelPermissions.ts:L177-194).
   * A server that declares neither, or only one, is NOT a permission-relay
   * surface. Surfaced via {@link McpServerStatus.channelPermissionRelay}.
   */
  supportsChannelPermissionRelay(): boolean {
    return (
      this.hasExperimentalCapability(MCP_CHANNEL_CAPABILITY) &&
      this.hasExperimentalCapability(MCP_CHANNEL_PERMISSION_CAPABILITY)
    );
  }

  getRoots(): Array<{ uri: string; name?: string }> {
    return this.rootPaths.map(p => ({
      uri: `file://${p}`,
      name: p.split('/').pop() || p,
    }));
  }

  /**
   * Update this client's workspace roots after connect and, when ready, push a
   * `notifications/roots/list_changed` so the server re-requests `roots/list`
   * (MCP spec: a client advertising `roots.listChanged` MUST notify on change).
   * Built on the public {@link notify} outbound API. A no-op (no notification) when
   * the roots are unchanged, so a redundant `setRoots` never spams the server.
   */
  async setRoots(paths: string[]): Promise<void> {
    const changed = paths.length !== this.rootPaths.length || paths.some((p, i) => p !== this.rootPaths[i]);
    this.rootPaths = [...paths];
    if (!changed || this._state !== 'ready') return;
    await this.notify('notifications/roots/list_changed');
  }

  /**
   * Revoke this server's OAuth tokens server-side (RFC 7009) and drop local
   * credential state — a user-initiated "sign out of this MCP server". Best-effort
   * and a no-op for a server without an OAuth provider. The manager calls this on
   * {@link McpManager.removeServer} (forget/reject), NOT on a transient stop, so a
   * temporarily-stopped server keeps its tokens.
   */
  async revokeAuth(): Promise<void> {
    await this.oauth?.revoke?.();
    // A forget is a FULL sign-out: beyond the server-side token revocation above,
    // drop ALL local auth state — the dynamically-registered client id/secret and
    // cached discovery, not just the tokens revoke() already cleared — so a later
    // re-add re-registers and re-discovers from scratch rather than reusing a stale
    // dynamic registration.
    await this.oauth?.invalidateCredentials?.('all');
  }

  disconnect(): void {
    // Cancel any in-flight reconnect loop — but NOT when this disconnect is the
    // loop's own teardown of a failed connect attempt (reconnectConnecting), which
    // must keep retrying. An external disconnect/stopServer while the loop is
    // sleeping aborts it. Invalidate the warm resource cache so a reconnected
    // client re-fetches rather than serving a list captured before the outage
    // (parity with the reference's onclose cache invalidation).
    if (!this.reconnectConnecting) {
      this.reconnectAborted = true;
      // Wake an in-flight backoff sleep so the loop sees the abort at once instead
      // of waiting out the remaining (up to 30s) delay.
      this.wakeReconnectSleep();
    }
    this._resources = [];
    if (this.hasTriggeredClose && !this.process) return;
    const wasConnected = this._state === 'ready' || this._state === 'connecting';
    this.hasTriggeredClose = true;
    this.stopWsKeepalive();
    this.closeHttpStream();

    // Tear down a WebSocket transport's live socket.
    if (this.ws) {
      const sock = this.ws;
      this.ws = null;
      try { sock.close(); } catch { /* already closed */ }
    }

    // Tear down an in-process transport's linked pair (symmetric + idempotent).
    if (this.inProcessTransport) {
      const tr = this.inProcessTransport;
      this.inProcessTransport = null;
      void tr.close().catch(() => { /* already closed */ });
    }

    const proc = this.process;
    const pid = this.processPid;
    const uptimeMs = this.uptime;
    this.process = null;
    this.processPid = null;
    this.connectionStartTime = null;

    if (proc) {
      this.gracefulShutdown(proc, pid);
    }

    this._state = 'disconnected';
    this.rejectAllPending(new Error('Client disconnected'));
    // Abort any in-flight server→client prompts so their handlers unblock.
    for (const controller of this._inboundAborts.values()) controller.abort();
    this._inboundAborts.clear();

    if (wasConnected) {
      this.emit('close', null, uptimeMs);
    }
  }

  private gracefulShutdown(proc: ChildProcess, pid: number | null): void {
    if (!pid) {
      try { proc.kill('SIGKILL'); } catch {}
      return;
    }

    const processExists = (): boolean => {
      try { process.kill(pid, 0); return true; } catch { return false; }
    };

    if (!processExists()) return;

    try { proc.kill('SIGINT'); } catch {}

    const checkAfterSigint = setTimeout(() => {
      if (!processExists()) return;
      try { proc.kill('SIGTERM'); } catch {}

      const checkAfterSigterm = setTimeout(() => {
        if (!processExists()) return;
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }, SHUTDOWN_SIGTERM_MS);
      checkAfterSigterm.unref();
    }, SHUTDOWN_SIGINT_MS);
    checkAfterSigint.unref();

    const failsafe = setTimeout(() => {
      if (processExists()) {
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }
    }, SHUTDOWN_TOTAL_MS);
    failsafe.unref();
  }

  classifyError(err: Error): McpTerminalError {
    const errWithCode = err as NodeJS.ErrnoException;
    const code = errWithCode.code;
    const msg = err.message?.toLowerCase() ?? '';

    if (code && TERMINAL_ERROR_CODES.has(code)) {
      return { code: code as McpTerminalErrorCode, message: this.errorContext(code), original: err };
    }
    if (msg.includes('spawn')) {
      return { code: 'SPAWN_ERROR', message: 'Failed to spawn MCP server process', original: err };
    }
    if (msg.includes('terminated') || msg.includes('killed')) {
      return { code: 'ESRCH', message: 'MCP server process terminated', original: err };
    }
    return { code: 'UNKNOWN', message: err.message, original: err };
  }

  private errorContext(code: string): string {
    switch (code) {
      case 'ECONNRESET': return 'Connection reset by peer — server crashed or restarted';
      case 'ETIMEDOUT': return 'Connection timed out — network issue or server unresponsive';
      case 'EPIPE': return 'Broken pipe — server closed unexpectedly';
      case 'EHOSTUNREACH': return 'Host unreachable — network connectivity issue';
      case 'ECONNREFUSED': return 'Connection refused — server is down';
      case 'ESRCH': return 'Process not found — server terminated';
      default: return `System error: ${code}`;
    }
  }

  private isSessionExpiredBody(body: string): boolean {
    return body.includes('"code":-32001') || body.includes('"code": -32001');
  }

  /**
   * A Streamable-HTTP session rotation tears the transport down, so the pending
   * tool call rejects with a JSON-RPC -32000 whose message is 'Connection closed'
   * rather than the original 404/-32001. Classify that shape as session-expiry on
   * the HTTP/SSE transport so the manager reconnects and retries the call once,
   * keeping a routine rotation invisible to the model (parity with the reference's
   * -32000 'Connection closed' → session-expired mapping).
   */
  private isConnectionClosedError(code: unknown, message: unknown): boolean {
    return code === -32000 && typeof message === 'string' && message.toLowerCase().includes('connection closed');
  }

  private assertReady(): void {
    if (this._state !== 'ready') {
      throw new Error(`MCP client not ready (state: ${this._state})`);
    }
  }

  /**
   * Transition to the `needs-auth` state and emit `needs-auth` after an
   * unrecoverable authentication failure (401 / 403-revoked that re-auth could not
   * fix), returning the {@link McpAuthError} to reject with. The manager surfaces
   * this as the per-server `needs-auth` status so the UI can prompt the user to
   * re-authenticate via `/mcp` (parity with the reference's needs-auth client
   * state). A subsequent successful connect/reconnect flips the state back to
   * `ready`, clearing the flag.
   */
  private flagNeedsAuth(message: string): McpAuthError {
    this._state = 'needs-auth';
    this.emit('needs-auth');
    return new McpAuthError(message);
  }

  private async sendRequestAuto(method: string, params: Record<string, unknown>, signal?: AbortSignal, timeoutMs?: number): Promise<unknown> {
    if ((this.transport === 'sse' || this.transport === 'http') && this.sseUrl) {
      return this.sendHttpRequest(method, params, signal, false, timeoutMs);
    }
    return this.sendRequest(method, params, signal, timeoutMs);
  }

  private async sendHttpRequest(method: string, params: Record<string, unknown>, signal?: AbortSignal, retried = false, timeoutMs?: number, forceNotification = false): Promise<unknown> {
    // A method may be sent as a JSON-RPC notification (no id, fire-and-forget) either
    // because it is a protocol `notifications/*` message OR because a caller (the public
    // {@link notify}) explicitly asked for notification semantics on a custom method.
    const isNotification = method.startsWith('notifications/') || forceNotification;
    const msg: Record<string, unknown> = { jsonrpc: '2.0', method, params };
    if (!isNotification) {
      msg.id = ++this.requestId;
    }
    const body = JSON.stringify(msg);
    const http = this.sseUrl!.startsWith('https') ? require('https') : require('http');
    // OAuth 2.1 (2025-06-18): attach the current bearer token, if any, before the request.
    const authHeader = this.oauth ? await this.oauth.authorizationHeader() : null;
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(new Error('MCP request aborted'));
        return;
      }

      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/event-stream',
        'User-Agent': getMcpUserAgent(),
        ...this.headers,
      };
      // 2025-06-18: every HTTP request after the initialize handshake MUST carry
      // the negotiated protocol version so the server pins compatible framing.
      if (method !== 'initialize' && this._protocolVersion) {
        headers['MCP-Protocol-Version'] = this._protocolVersion;
      }
      // Echo the server-assigned session id (if any) on every subsequent request.
      if (this.httpSessionId) {
        headers['Mcp-Session-Id'] = this.httpSessionId;
      }
      if (authHeader && !headers.Authorization && !headers.authorization) {
        headers.Authorization = authHeader;
      }
      const req = http.request(new URL(this.sseUrl!), {
        method: 'POST',
        headers,
        timeout: timeoutMs ?? this.requestTimeout,
      }, (res: any) => {
        const statusCode = res.statusCode;
        // Capture the session id the server assigns at initialize (2025 spec).
        const sid = res.headers?.['mcp-session-id'];
        if (typeof sid === 'string' && sid) this.httpSessionId = sid;
        // Collect raw chunks and decode ONCE on end: the old `data += d` coerced
        // every Buffer chunk to a UTF-8 string and built a cons-string (a redundant
        // per-chunk decode plus a multi-byte-boundary corruption hazard). A single
        // Buffer.concat + toString is behavior-identical for well-formed UTF-8.
        const chunks: Buffer[] = [];
        res.on('data', (d: Buffer) => chunks.push(d));
        res.on('end', () => {
          const data = Buffer.concat(chunks).toString('utf-8');
          // Notifications: server responds with 202 Accepted (empty body)
          if (isNotification) {
            resolve(undefined);
            return;
          }
          // OAuth 2.1: a 401 triggers (re)authorization; on success retry once.
          if (statusCode === 401 && this.oauth && !retried) {
            const www = (res.headers?.['www-authenticate'] as string | undefined) ?? null;
            this.oauth.onUnauthorized(www).then((ok: boolean) => {
              if (ok) {
                this.sendHttpRequest(method, params, signal, true, timeoutMs, forceNotification).then(resolve, reject);
              } else {
                reject(this.flagNeedsAuth('MCP HTTP 401 Unauthorized (authorization failed)'));
              }
            }).catch(reject);
            return;
          }
          // OAuth 2.1 step-up: a 403 insufficient_scope means the token is valid
          // but lacks a required scope — drive a full re-consent for the wider
          // scope and retry once on success (a non-step-up 403 is a real denial).
          if (statusCode === 403 && this.oauth && !retried) {
            const www = (res.headers?.['www-authenticate'] as string | undefined) ?? null;
            // A 403 whose challenge/body marks the token revoked/invalid is NOT a
            // scope step-up — the grant is gone, so drive a FULL re-authorization
            // (onUnauthorized) and retry once, mirroring the reference's also403Revoked.
            if (is403RevokedToken(www, data)) {
              this.oauth.onUnauthorized(www).then((ok: boolean) => {
                if (ok) {
                  this.sendHttpRequest(method, params, signal, true, timeoutMs, forceNotification).then(resolve, reject);
                } else {
                  reject(this.flagNeedsAuth('MCP HTTP 403 Forbidden (token revoked — re-authorization failed)'));
                }
              }).catch(reject);
              return;
            }
            this.oauth.onForbidden(www).then((ok: boolean) => {
              if (ok) {
                this.sendHttpRequest(method, params, signal, true, timeoutMs, forceNotification).then(resolve, reject);
              } else {
                reject(new Error('MCP HTTP 403 Forbidden (insufficient scope — authorization failed)'));
              }
            }).catch(reject);
            return;
          }
          if (statusCode === 404 && this.isSessionExpiredBody(data)) {
            this._state = 'error';
            this.emit('session-expired');
            reject(new McpSessionExpiredError());
            return;
          }
          try {
            // Try direct JSON parse first; if that fails, extract from
            // SSE framing (text/event-stream: "event: message\ndata: {…}")
            let jsonStr = data;
            try {
              JSON.parse(jsonStr);
            } catch {
              const dataLine = data.split('\n').find(l => l.startsWith('data: '));
              if (dataLine) {
                jsonStr = dataLine.slice(6);
              }
            }
            const parsed = JSON.parse(jsonStr);
            if (parsed.error) {
              if (parsed.error.code === -32001 || this.isConnectionClosedError(parsed.error.code, parsed.error.message)) {
                this._state = 'error';
                this.emit('session-expired');
                reject(new McpSessionExpiredError());
                return;
              }
              reject(new McpRpcError(parsed.error.code, parsed.error.message, parsed.error.data));
            } else {
              resolve(parsed.result);
            }
          } catch {
            reject(new Error('Invalid JSON response from MCP SSE server'));
          }
        });
      });

      const abortHandler = () => {
        req.destroy();
        reject(new Error('MCP request aborted'));
      };
      if (signal) {
        signal.addEventListener('abort', abortHandler, { once: true });
        req.on('close', () => signal.removeEventListener('abort', abortHandler));
      }

      req.on('error', (err: Error) => {
        const classified = this.classifyError(err);
        if (classified.code !== 'UNKNOWN') {
          this.emit('terminal-error', classified);
        }
        reject(err);
      });
      req.on('timeout', () => { req.destroy(); reject(new Error(this.formatTimeoutMessage(method, params, timeoutMs ?? this.requestTimeout))); });
      // Outbound POST is a data frame over the client→server leg — record it so the
      // proactive keepalive stays idle while real requests are already flowing.
      this.httpStreamLastDataActivity = Date.now();
      req.write(body);
      req.end();
    });
  }

  /**
   * Streamable-HTTP server→client channel (MCP spec): after initialization open a
   * long-lived GET with `Accept: text/event-stream` (carrying the negotiated
   * protocol version + any `Mcp-Session-Id`) so the server can push JSON-RPC
   * notifications and requests (sampling, elicitation, progress, roots). Incoming
   * messages route through the SAME dispatcher the stdio/sse transports use, and
   * server→client requests are answered via respondToServerRequest. A server that
   * does not offer the stream replies 405 (Method Not Allowed) — we then degrade
   * to POST-only without erroring. An unexpected close reconnects with bounded
   * exponential backoff (mirroring the client's reconnect policy).
   */
  private async openHttpStream(): Promise<void> {
    if (this.transport !== 'http' || !this.sseUrl || this.httpStreamClosed) return;
    const http = this.sseUrl.startsWith('https') ? require('https') : require('http');
    const headers = await this.buildStreamHeaders('text/event-stream');
    // Resume from where the last stream left off: the server replays only the
    // missed tail (by sequence number) instead of from zero or dropping events
    // across the gap. Sent both as a query param and the standard Last-Event-ID
    // header so either server convention works.
    const streamUrl = new URL(this.sseUrl);
    if (this.httpStreamLastSequenceNum > 0) {
      streamUrl.searchParams.set('from_sequence_num', String(this.httpStreamLastSequenceNum));
      headers['Last-Event-ID'] = String(this.httpStreamLastSequenceNum);
    }
    if (this.httpStreamClosed) return;
    this.httpStreamBuffer = '';
    this.httpStreamCarryCr = false;
    const req = http.request(streamUrl, { method: 'GET', headers }, (res: any) => {
      const statusCode = res.statusCode;
      // 405 / 404 / 501 → the server does not offer the GET stream: POST-only.
      if (statusCode === 405 || statusCode === 404 || statusCode === 501) {
        res.resume();
        this.httpStreamReq = null;
        this.emit('http-stream-unsupported', statusCode);
        return;
      }
      // 401 / 403 → the credential is permanently rejected; retrying would just
      // re-send the same rejected bearer in a loop. Stop for good.
      if (PERMANENT_HTTP_STREAM_CODES.has(statusCode)) {
        res.resume();
        this.httpStreamReq = null;
        this.httpStreamClosed = true;
        this.emit('http-stream-closed', statusCode);
        return;
      }
      if (statusCode !== 200) {
        res.resume();
        this.httpStreamReq = null;
        this.scheduleHttpStreamReconnect();
        return;
      }
      // A successful (re)connect clears the failure clock + attempt counter so a
      // later brief drop gets the full wall-clock budget again.
      this.httpStreamReconnectAttempts = 0;
      this.httpStreamReconnectStartTime = null;
      this.resetHttpStreamLiveness();
      res.setEncoding('utf-8');
      res.on('data', (chunk: string) => {
        // Any byte (including a `:keepalive` comment) proves the socket is alive.
        this.resetHttpStreamLiveness();
        // Inbound traffic also warms the proxy path, so gate the proactive
        // keepalive off the most recent data frame in EITHER direction.
        this.httpStreamLastDataActivity = Date.now();
        this.processHttpStreamBuffer(chunk);
      });
      res.on('end', () => { this.clearHttpStreamLiveness(); this.httpStreamReq = null; this.scheduleHttpStreamReconnect(); });
      res.on('error', () => { this.clearHttpStreamLiveness(); this.httpStreamReq = null; this.scheduleHttpStreamReconnect(); });
    });
    req.on('error', () => { this.clearHttpStreamLiveness(); this.httpStreamReq = null; this.scheduleHttpStreamReconnect(); });
    this.httpStreamReq = req;
    req.end();
  }

  /** Shared header builder for the GET stream + POSTed server-request responses. */
  private async buildStreamHeaders(accept: string): Promise<Record<string, string>> {
    const authHeader = this.oauth ? await this.oauth.authorizationHeader() : null;
    const headers: Record<string, string> = { Accept: accept, 'User-Agent': getMcpUserAgent(), ...this.headers };
    if (this._protocolVersion) headers['MCP-Protocol-Version'] = this._protocolVersion;
    if (this.httpSessionId) headers['Mcp-Session-Id'] = this.httpSessionId;
    if (authHeader && !headers.Authorization && !headers.authorization) headers.Authorization = authHeader;
    return headers;
  }

  /**
   * Append an SSE chunk and drain every complete event. CRLF→LF normalization is
   * applied INCREMENTALLY to the newly appended chunk (carrying a trailing lone CR
   * across chunks to resolve a CRLF split at a chunk boundary) instead of
   * re-normalizing the ENTIRE accumulated buffer on every chunk — the old code made
   * a single multi-chunk SSE event O(n²). Lone CRs are preserved exactly as before.
   */
  private processHttpStreamBuffer(chunk = ''): void {
    let s = chunk;
    // Put back a CR held from the previous chunk; the replace below collapses it
    // into \n if this chunk's first byte is \n (a CRLF that straddled the boundary),
    // otherwise it survives as a lone CR exactly like the whole-buffer replace did.
    if (this.httpStreamCarryCr) {
      this.httpStreamCarryCr = false;
      s = '\r' + s;
    }
    // Hold back a trailing CR: it may be the CR of a CRLF whose LF is in the next chunk.
    if (s.endsWith('\r')) {
      this.httpStreamCarryCr = true;
      s = s.slice(0, -1);
    }
    this.httpStreamBuffer += s.replace(/\r\n/g, '\n');
    let sep: number;
    while ((sep = this.httpStreamBuffer.indexOf('\n\n')) !== -1) {
      const rawEvent = this.httpStreamBuffer.slice(0, sep);
      this.httpStreamBuffer = this.httpStreamBuffer.slice(sep + 2);
      this.handleSseStreamEvent(rawEvent);
    }
  }

  /** Parse one SSE event's `data:` payload(s) and route the JSON-RPC message. */
  private handleSseStreamEvent(rawEvent: string): void {
    const dataParts: string[] = [];
    let eventId: string | null = null;
    for (const line of rawEvent.split('\n')) {
      if (line.startsWith('data:')) {
        dataParts.push(line.slice(line.startsWith('data: ') ? 6 : 5));
      } else if (line.startsWith('id:')) {
        eventId = line.slice(line.startsWith('id: ') ? 4 : 3);
      }
    }
    // Track the event's sequence number for resumption + dedup. A sequence
    // already seen (a replay after a resume) is dropped so the server->client
    // message is not processed twice.
    if (eventId !== null) {
      const seqNum = parseInt(eventId, 10);
      if (!Number.isNaN(seqNum)) {
        if (this.httpStreamSeenSequenceNums.has(seqNum)) {
          this.emit('http-stream-duplicate', seqNum);
          return;
        }
        this.httpStreamSeenSequenceNums.add(seqNum);
        // Bound the dedup set: once large, drop sequence numbers well below the
        // high-water mark (only recent ones can still arrive as duplicates).
        if (this.httpStreamSeenSequenceNums.size > HTTP_STREAM_SEEN_SEQ_CAP) {
          const threshold = this.httpStreamLastSequenceNum - HTTP_STREAM_SEEN_SEQ_PRUNE_MARGIN;
          for (const s of this.httpStreamSeenSequenceNums) {
            if (s < threshold) this.httpStreamSeenSequenceNums.delete(s);
          }
        }
        if (seqNum > this.httpStreamLastSequenceNum) this.httpStreamLastSequenceNum = seqNum;
      }
    }
    if (dataParts.length === 0) return;
    const payload = dataParts.join('\n');
    try {
      const parsed = JSON.parse(payload);
      if (Array.isArray(parsed)) {
        for (const m of parsed) this.routeMessage(m as JsonRpcResponse | JsonRpcNotification);
      } else {
        this.routeMessage(parsed as JsonRpcResponse | JsonRpcNotification);
      }
    } catch {
      this.emit('parse-error', payload);
    }
  }

  private scheduleHttpStreamReconnect(): void {
    if (this.httpStreamClosed || this._state !== 'ready') return;
    const now = Date.now();
    if (this.httpStreamReconnectStartTime === null) this.httpStreamReconnectStartTime = now;
    // Give up only after a continuous outage longer than the wall-clock budget —
    // not after a small fixed attempt count — so brief transient drops recover.
    if (now - this.httpStreamReconnectStartTime >= HTTP_STREAM_RECONNECT_GIVE_UP_MS) {
      this.emit('http-stream-closed');
      return;
    }
    const attempt = this.httpStreamReconnectAttempts++;
    const delay = this.reconnectDelay(attempt);
    const timer = setTimeout(() => {
      if (this.httpStreamClosed || this._state !== 'ready') return;
      this.openHttpStream().catch(() => { /* a later close retries, bounded */ });
    }, delay);
    timer.unref?.();
  }

  /**
   * Arm/refresh the GET-stream liveness timer. Called on every incoming chunk;
   * if the stream falls silent past {@link HTTP_STREAM_LIVENESS_TIMEOUT_MS} the
   * (likely half-open) connection is destroyed and a reconnect is scheduled.
   */
  private resetHttpStreamLiveness(): void {
    this.clearHttpStreamLiveness();
    const timer = setTimeout(() => {
      this.httpStreamLivenessTimer = null;
      this.emit('http-stream-liveness-timeout');
      const req = this.httpStreamReq;
      this.httpStreamReq = null;
      if (req) { try { req.destroy(); } catch { /* already gone */ } }
      this.scheduleHttpStreamReconnect();
    }, this.httpStreamLivenessTimeout);
    timer.unref?.();
    this.httpStreamLivenessTimer = timer;
  }

  private clearHttpStreamLiveness(): void {
    if (this.httpStreamLivenessTimer) {
      clearTimeout(this.httpStreamLivenessTimer);
      this.httpStreamLivenessTimer = null;
    }
  }

  /** Tear down the GET stream and stop any reconnect (called from disconnect). */
  private closeHttpStream(): void {
    this.httpStreamClosed = true;
    this.stopHttpKeepalive();
    this.clearHttpStreamLiveness();
    const req = this.httpStreamReq;
    this.httpStreamReq = null;
    if (req) {
      try { req.destroy(); } catch { /* already gone */ }
    }
  }

  /** POST a JSON-RPC response back to the HTTP endpoint for a server→client request. */
  private postServerResponse(id: number, payload: { result: unknown } | { error: { code: number; message: string } }): void {
    const url = this.sseUrl;
    if (!url) return;
    const http = url.startsWith('https') ? require('https') : require('http');
    this.buildStreamHeaders('application/json, text/event-stream').then((streamHeaders) => {
      const headers = { 'Content-Type': 'application/json', ...streamHeaders };
      const req = http.request(new URL(url), { method: 'POST', headers, timeout: this.requestTimeout }, (res: any) => res.resume());
      req.on('error', () => { /* best-effort: the server times out its own request */ });
      req.on('timeout', () => { try { req.destroy(); } catch { /* ignore */ } });
      req.write(JSON.stringify({ jsonrpc: '2.0', id, ...payload }));
      req.end();
    }).catch(() => { /* best-effort */ });
  }

  /**
   * Human-facing timeout message. For a `tools/call` it names the server and the
   * tool and uses whole seconds — `MCP server "<name>" tool "<tool>" timed out
   * after Ns` — so the model/user sees which MCP tool stalled (parity with the reference
   * Code's tool-timeout wording). Non-tool requests keep the generic method/ms form.
   */
  private formatTimeoutMessage(method: string, params: Record<string, unknown>, timeoutMs: number): string {
    if (method === 'tools/call') {
      const server = this._serverInfo?.name ?? 'unknown';
      const tool = typeof params.name === 'string' ? params.name : 'unknown';
      return `MCP server "${server}" tool "${tool}" timed out after ${Math.max(1, Math.floor(timeoutMs / 1000))}s`;
    }
    return `MCP request '${method}' timed out after ${timeoutMs}ms`;
  }

  /** Retain the last {@link MCP_STDERR_TAIL_LINES} non-empty stderr lines. */
  private appendStderrTail(text: string): void {
    for (const line of text.split('\n')) {
      if (line.length === 0) continue;
      this._stderrTail.push(line);
    }
    if (this._stderrTail.length > MCP_STDERR_TAIL_LINES) {
      this._stderrTail = this._stderrTail.slice(-MCP_STDERR_TAIL_LINES);
    }
  }

  /**
   * Human-facing message for a stdio server exit. When the process was killed by
   * a signal the exit code is null, so fall back to naming the signal
   * (`signal SIGKILL`) instead of a confusing `code null`. On an ABNORMAL exit
   * (non-zero code, or any signal) the retained stderr tail is appended so the
   * reason the child died is visible, not just the bare exit status.
   */
  private formatExitMessage(code: number | null, signal: NodeJS.Signals | null): string {
    const cause = code !== null ? `code ${code}` : `signal ${signal}`;
    let msg = `MCP server exited with ${cause}`;
    const failed = code !== 0; // non-zero exit, or killed by signal (code === null)
    if (failed && this._stderrTail.length > 0) {
      msg += `\nServer stderr (${cause}):\n${this._stderrTail.join('\n')}`;
    }
    return msg;
  }

  /**
   * Capped exponential reconnect delay (ms) with ±25% jitter. Single source of
   * truth for BOTH reconnect paths — the client-level {@link reconnect} and the
   * Streamable-HTTP GET-stream {@link scheduleHttpStreamReconnect} — so a shared
   * outage does not make every client retry in lockstep (thundering herd).
   */
  /**
   * A reconnect-loop backoff sleep that can be woken early. Stores its resolver +
   * timer so an external {@link disconnect} landing mid-backoff can resolve it at
   * once via {@link wakeReconnectSleep} (the loop then sees `reconnectAborted` and
   * returns), instead of waiting out a delay that can be as long as
   * {@link HTTP_STREAM_RECONNECT_MAX_MS}. The timer is unref'd so it never keeps
   * the event loop alive on its own.
   */
  private reconnectBackoffSleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      this.reconnectSleepWake = resolve;
      const timer = setTimeout(() => {
        this.reconnectSleepTimer = null;
        this.reconnectSleepWake = null;
        resolve();
      }, ms);
      timer.unref?.();
      this.reconnectSleepTimer = timer;
    });
  }

  /**
   * Wake an in-flight {@link reconnectBackoffSleep} immediately. Called from
   * {@link disconnect} when it aborts the reconnect loop so a queued stop/reconnect
   * proceeds without burning the remaining backoff. A no-op when no backoff sleep
   * is pending.
   */
  private wakeReconnectSleep(): void {
    if (this.reconnectSleepTimer) {
      clearTimeout(this.reconnectSleepTimer);
      this.reconnectSleepTimer = null;
    }
    const wake = this.reconnectSleepWake;
    this.reconnectSleepWake = null;
    if (wake) wake();
  }

  private reconnectDelay(attempt: number): number {
    const base = Math.min(HTTP_STREAM_RECONNECT_BASE_MS * Math.pow(2, attempt), HTTP_STREAM_RECONNECT_MAX_MS);
    const jitter = base * RECONNECT_JITTER_FRAC * (2 * Math.random() - 1);
    return Math.max(0, Math.round(base + jitter));
  }

  private sendRequest(method: string, params: Record<string, unknown>, signal?: AbortSignal, timeoutMs?: number): Promise<unknown> {
    return new Promise((resolve, reject) => {
      if (signal?.aborted) {
        reject(new Error('MCP request aborted'));
        return;
      }

      const id = ++this.requestId;
      const request: JsonRpcRequest = {
        jsonrpc: '2.0',
        id,
        method,
        params,
      };

      const effectiveTimeout = timeoutMs ?? this.requestTimeout;
      // A non-finite `timeoutMs` means "no per-request deadline" — the caller bounds
      // this request itself (e.g. the initialize handshake, which is wrapped by the
      // outer `boundedCall(connectTimeout)` guard). Arming a second, shorter inner
      // timer there would let a small `requestTimeout` abort the handshake before the
      // connect budget elapses (a wall-clock race under load). Leaving it unbounded
      // keeps the connect timeout the single authority over the handshake deadline.
      const timer = Number.isFinite(effectiveTimeout)
        ? setTimeout(() => {
            this.pendingRequests.delete(id);
            reject(new Error(this.formatTimeoutMessage(method, params, effectiveTimeout)));
          }, effectiveTimeout)
        : undefined;

      let abortHandler: (() => void) | undefined;
      if (signal) {
        abortHandler = () => {
          clearTimeout(timer);
          this.pendingRequests.delete(id);
          this.sendCancelled(id, 'client aborted');
          reject(new Error('MCP request aborted'));
        };
        signal.addEventListener('abort', abortHandler, { once: true });
      }

      this.pendingRequests.set(id, { resolve, reject, timer, abortHandler });
      this.writeMessage(request);
    });
  }

  /** notifications/cancelled — tell the server we abandoned an in-flight request. */
  private sendCancelled(requestId: number, reason: string): void {
    try {
      if (this.process?.stdin?.writable) {
        this.writeMessage({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId, reason } });
      }
    } catch {
      /* best-effort: the connection may already be gone */
    }
  }

  private async sendNotification(method: string, params: Record<string, unknown>): Promise<void> {
    // Remote (http/sse) transports have no persistent write pipe — a notification is
    // a POST with no id (fire-and-forget, 202 Accepted). Local (stdio/ws) transports
    // frame it directly on the live connection.
    if ((this.transport === 'http' || this.transport === 'sse') && this.sseUrl) {
      await this.sendHttpRequest(method, params, undefined, false, undefined, true);
      return;
    }
    const notification: JsonRpcNotification = {
      jsonrpc: '2.0',
      method,
      params,
    };
    this.writeMessage(notification);
  }

  /**
   * Public outbound-notification API (the reference-parity `client.notification({method, params})`,
   * services/mcp/vscodeSdkMcp.ts) — push an application-level JSON-RPC notification to a
   * connected server, e.g. `notify('file_updated', { filePath, oldContent, newContent })`
   * or `notify('experiment_gates', { gates })`. Unlike the protocol-standard
   * {@link sendNotification} call sites (`notifications/initialized`), this accepts an
   * ARBITRARY method + params so a host embedding AMI can keep an MCP server informed of
   * host-side state. Transport-agnostic (stdio/ws frame it; http/sse POST it with no id).
   * Rejects when the client is not ready so a message is never silently dropped onto a
   * dead connection.
   */
  async notify(method: string, params: Record<string, unknown> = {}): Promise<void> {
    if (!method.trim()) throw new Error('notify() requires a non-empty method name');
    this.assertReady();
    await this.sendNotification(method, params);
  }

  private writeMessage(msg: JsonRpcRequest | JsonRpcNotification | JsonRpcResponse): void {
    // In-process transport: hand the message object straight to the linked peer.
    if (this.transport === 'inprocess') {
      if (!this.inProcessTransport || this.inProcessTransport.closed) {
        throw new Error('MCP in-process transport is closed');
      }
      // send() delivers on a microtask and surfaces a delivery fault via onerror;
      // the sync closed-guard above already covers the "not writable" contract.
      void this.inProcessTransport.send(msg).catch(() => { /* surfaced via onerror */ });
      return;
    }
    // WebSocket transport: each message is one discrete frame (no newline framing).
    if ((this.transport === 'ws' || this.transport === 'wss') && this.ws) {
      if (this.ws.readyState !== MCP_WS_OPEN) {
        throw new Error('MCP WebSocket is not open');
      }
      this.ws.send(JSON.stringify(msg));
      return;
    }
    if (!this.process?.stdin?.writable) {
      throw new Error('MCP server stdin not writable');
    }
    const json = JSON.stringify(msg);
    this.process.stdin.write(json + '\n');
  }

  /**
   * Reply to a server→client JSON-RPC request. Used by the ping/roots/
   * elicitation/sampling handlers so no server request is ever left unanswered.
   * Stdio writes the response to the child's stdin; the Streamable-HTTP transport
   * (whose server→client requests arrive on the GET SSE stream) POSTs it back to
   * the same endpoint. With neither available this is a safe no-op.
   */
  private respondToServerRequest(id: number, payload: { result: unknown } | { error: { code: number; message: string } }): void {
    // In-process: the reply is delivered to the linked peer.
    if (this.transport === 'inprocess' && this.inProcessTransport && !this.inProcessTransport.closed) {
      try { this.writeMessage({ jsonrpc: '2.0', id, ...payload } as JsonRpcResponse); } catch { /* peer gone */ }
      return;
    }
    // WebSocket: the reply is a frame on the same socket.
    if ((this.transport === 'ws' || this.transport === 'wss') && this.ws) {
      try { this.writeMessage({ jsonrpc: '2.0', id, ...payload } as JsonRpcResponse); } catch { /* socket gone */ }
      return;
    }
    if (this.process?.stdin?.writable) {
      this.writeMessage({ jsonrpc: '2.0', id, ...payload } as JsonRpcResponse);
      return;
    }
    if ((this.transport === 'http' || this.transport === 'sse') && this.sseUrl) {
      this.postServerResponse(id, payload);
    }
  }

  private processBuffer(): void {
    const lines = this.buffer.split('\n');
    this.buffer = lines.pop() ?? '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        const msg = JSON.parse(trimmed) as JsonRpcResponse | JsonRpcNotification;
        this.routeMessage(msg);
      } catch {
        this.emit('parse-error', trimmed);
      }
    }

    this.enforceStdioBufferCap();
  }

  /**
   * L31: bound the accumulated read buffer. Every complete (newline-delimited)
   * message has already been routed; what remains is a single not-yet-terminated
   * line. If that remainder exceeds the cap, a server is either never sending a
   * newline or streaming one oversized line — drop it and signal the overflow
   * rather than letting the buffer grow without bound. Runs on every chunk (even
   * newline-free ones) so the bound holds regardless of framing.
   */
  private enforceStdioBufferCap(): void {
    if (this.buffer.length > MCP_MAX_STDIO_BUFFER) {
      const droppedBytes = this.buffer.length;
      this.buffer = '';
      this.emit(
        'protocol-error',
        `MCP stdio read buffer exceeded ${MCP_MAX_STDIO_BUFFER} bytes without a complete message; dropped ${droppedBytes} buffered bytes`,
      );
    }
  }

  /**
   * Dispatch a parsed JSON-RPC message to the right handler. Shared by the stdio
   * read loop (processBuffer) and the Streamable-HTTP GET stream so server→client
   * requests and notifications are handled identically across transports.
   */
  private routeMessage(msg: JsonRpcResponse | JsonRpcNotification): void {
    // Reject a malformed JSON-RPC envelope before dispatch: an untrusted server can
    // push arbitrary JSON (a bare primitive, an array where an object is expected, an
    // object with neither id nor method). Such a frame would otherwise silently
    // vanish (no branch matches) or throw inside a branch — surface it instead.
    if (!isValidJsonRpcMessage(msg)) {
      this.emit('protocol-error', "MCP server sent a malformed JSON-RPC message (expected jsonrpc:'2.0' with an id or method)");
      return;
    }
    if ('id' in msg && msg.id !== undefined && 'method' in msg) {
      this.handleNotification(msg as JsonRpcNotification);
    } else if ('id' in msg && msg.id !== undefined) {
      this.handleResponse(msg as JsonRpcResponse);
    } else if ('method' in msg) {
      this.handleNotification(msg as JsonRpcNotification);
    }
  }

  private handleResponse(response: JsonRpcResponse): void {
    const pending = this.pendingRequests.get(response.id);
    if (!pending) return;

    clearTimeout(pending.timer);
    if (pending.abortHandler) {
      // Can't remove from signal directly, but the once:true handles cleanup
    }
    this.pendingRequests.delete(response.id);

    if (response.error) {
      if (response.error.code === -32001) {
        this.emit('session-expired');
        pending.reject(new McpSessionExpiredError());
        return;
      }
      pending.reject(new McpRpcError(response.error.code, response.error.message, response.error.data));
    } else {
      pending.resolve(response.result);
    }
  }

  private handleNotification(notification: JsonRpcNotification): void {
    try {
      this.dispatchNotification(notification);
    } catch (err) {
      // Per-notification error boundary: a single malformed notification or a
      // throwing 'notification' listener must not crash or stall the shared read
      // loop that processes every subsequent message. Isolate and surface it as a
      // dedicated diagnostic (NOT the EventEmitter 'error' event, which would
      // rethrow when unhandled).
      this.emit('notification-error', notification.method, err instanceof Error ? err : new Error(String(err)));
    }
  }

  private dispatchNotification(notification: JsonRpcNotification): void {
    const id = (notification as JsonRpcNotification & { id?: number }).id;

    // Trust boundary: validate the untrusted server's notification params against
    // the shape AMI dereferences BEFORE any handler runs. A malformed/wrong-typed
    // payload is surfaced as a `notification-error` and dropped rather than fed to a
    // handler or re-emitted on the generic `notification` channel (parity with
    // the reference's schema-validated inbound notification handlers).
    const paramsCheck = validateMcpNotificationParams(notification.method, notification.params);
    if (!paramsCheck.ok) {
      this.emit('notification-error', notification.method, new Error(paramsCheck.error));
      return;
    }

    // --- Server→client REQUESTS (carry an id; MUST be answered) -------------
    if (notification.method === 'elicitation/create' || notification.method === 'notifications/elicitation') {
      void this.handleElicitation(notification as JsonRpcNotification & { id?: number });
    }

    if (notification.method === 'sampling/createMessage') {
      void this.handleSampling(notification as JsonRpcNotification & { id?: number });
    }

    // Server abandoned a request it sent us (e.g. a pending elicitation) — abort
    // the matching in-flight handler so a frontend prompt resolves cancel (H08).
    if (notification.method === 'notifications/cancelled') {
      const params = notification.params as { requestId?: number | string } | undefined;
      if (params?.requestId !== undefined) {
        const controller = this._inboundAborts.get(params.requestId);
        if (controller) controller.abort();
      }
    }

    if (notification.method === 'ping' && id !== undefined) {
      this.respondToServerRequest(id, { result: {} });
    }

    if (notification.method === 'roots/list' && id !== undefined) {
      this.respondToServerRequest(id, { result: { roots: this.getRoots() } });
    }

    // --- Server→client NOTIFICATIONS (no reply) ----------------------------
    if (notification.method === 'notifications/tools/list_changed') {
      this._tools = [];
      this.emit('tools-changed');
    }

    if (notification.method === 'notifications/resources/list_changed') {
      this._resources = [];
      this.emit('resources-changed');
    }

    if (notification.method === 'notifications/resources/updated') {
      const params = notification.params as { uri?: string } | undefined;
      if (params?.uri) this.emit('resource-updated', params.uri);
    }

    if (notification.method === 'notifications/prompts/list_changed') {
      this._prompts = [];
      this.emit('prompts-changed');
    }

    // Out-of-band URL elicitation completed: the server confirms the user finished
    // the browser step for a `-32042`/url-mode elicitation. Surface the id so a
    // frontend can flip the matching waiting-state prompt to completed (parity with
    // the reference's ElicitationCompleteNotificationSchema handler setting
    // `completed: true` on the queued elicitation).
    if (notification.method === 'notifications/elicitation/complete') {
      const params = notification.params as { elicitationId?: string } | undefined;
      if (params?.elicitationId) this.emit('elicitation-complete', params.elicitationId);
    }

    if (notification.method === 'notifications/message') {
      const params = notification.params as Partial<McpLogMessage> | undefined;
      if (params) {
        this.emit('log', { level: params.level ?? 'info', logger: params.logger, data: params.data } as McpLogMessage);
      }
    }

    if (notification.method === 'notifications/progress') {
      const params = notification.params as { progressToken?: string; progress?: number; total?: number; message?: string } | undefined;
      if (params?.progressToken) {
        const cb = this.progressCallbacks.get(String(params.progressToken));
        if (cb) {
          const percent = params.progress != null && params.total
            ? Math.round((params.progress / params.total) * 100)
            : undefined;
          cb({ percent, message: params.message });
        }
      }
    }

    // IDE-as-MCP-server editor-context notifications (parity with the reference's
    // useIdeSelection / useIdeAtMentioned). Params are already validated above;
    // normalize and surface as typed events a frontend/engine can consume to add
    // the user's current editor selection / at-mention to the turn context.
    if (notification.method === 'selection_changed') {
      const sel = normalizeIdeSelection(notification.params as unknown as McpSelectionChangedParams);
      if (sel) this.emit('ide-selection', sel);
    }

    if (notification.method === 'at_mentioned') {
      this.emit(
        'ide-at-mentioned',
        normalizeIdeAtMention(notification.params as unknown as { filePath: string; lineStart?: number; lineEnd?: number }),
      );
    }

    this.emit('notification', notification.method, notification.params);
  }

  /**
   * Resolve a server elicitation request: ask the user via the injected handler,
   * then reply with the chosen action. Never blocks the read loop and never
   * leaves the request unanswered — absent a handler it declines, on a handler
   * error it cancels, so the server can always proceed.
   */
  private async handleElicitation(notification: JsonRpcNotification & { id?: number }): Promise<void> {
    const raw = (notification.params ?? {}) as Partial<McpElicitationRequest>;
    const id = notification.id;
    // Register an abort controller so an inbound notifications/cancelled for this
    // request id can cancel the pending prompt (H08).
    let controller: AbortController | undefined;
    if (id !== undefined && id !== null) {
      controller = new AbortController();
      this._inboundAborts.set(id, controller);
    }
    const request: McpElicitationRequest = {
      message: typeof raw.message === 'string' ? raw.message : '',
      requestedSchema:
        raw.requestedSchema && typeof raw.requestedSchema === 'object'
          ? (raw.requestedSchema as Record<string, unknown>)
          : {},
      signal: controller?.signal,
      // Pass the URL-mode fields through (do NOT strip): a `mode:'url'` server
      // elicitation hands off a `url` for the browser and runs a two-phase
      // open→waiting lifecycle with an optional `actionLabel`/`showCancel` and a
      // server-driven `completed` flag. A form-mode request simply carries none
      // of these, so the frontend defaults to form handling.
      ...(raw.mode === 'url' ? { mode: 'url' as const } : {}),
      ...(typeof raw.url === 'string' ? { url: raw.url } : {}),
      ...(typeof raw.actionLabel === 'string' ? { actionLabel: raw.actionLabel } : {}),
      ...(raw.showCancel === true ? { showCancel: true } : {}),
      ...(raw.completed === true ? { completed: true } : {}),
    };
    let result: McpElicitationResult;
    let serverCancelled = false;
    if (this.onElicitation) {
      try {
        // Race the handler against server cancellation so a frontend that does not
        // observe the signal still unblocks the read loop.
        const abortPromise = controller
          ? new Promise<McpElicitationResult>((resolve) => {
              controller!.signal.addEventListener('abort', () => { serverCancelled = true; resolve({ action: 'cancel' }); }, { once: true });
            })
          : undefined;
        result = abortPromise
          ? await Promise.race([this.onElicitation(request), abortPromise])
          : await this.onElicitation(request);
      } catch {
        result = { action: 'cancel' };
      }
    } else {
      result = { action: 'decline' };
    }
    if (id !== undefined && id !== null) {
      this._inboundAborts.delete(id);
      // Per MCP spec a cancelled request should not be answered; only respond when
      // the server did NOT abandon it.
      if (!serverCancelled) this.respondToServerRequest(id, { result });
    }
  }

  /**
   * Fulfill a server sampling request by running an LLM turn through the injected
   * handler, then reply with the completion. Absent a handler, or on error, it
   * replies with a JSON-RPC error (−32601 / −32603) so the server is not left
   * hanging and can fall back.
   */
  private async handleSampling(notification: JsonRpcNotification & { id?: number }): Promise<void> {
    const id = notification.id;
    if (id === undefined || id === null) return;
    if (!this.onSampling) {
      this.respondToServerRequest(id, { error: { code: -32601, message: 'sampling not supported by this client' } });
      return;
    }
    // Register an abort controller so an inbound notifications/cancelled for this
    // request id (or a connection drop, which aborts all _inboundAborts) cancels
    // the in-flight model turn instead of letting it run to completion and burn
    // tokens — parity with the reference threading the SDK's per-request
    // `extra.signal` into the sampling handler.
    const controller = new AbortController();
    this._inboundAborts.set(id, controller);
    let serverCancelled = false;
    const params = (notification.params ?? {}) as unknown as McpSamplingRequest;
    try {
      const samplingPromise = this.onSampling(params, controller.signal);
      // Race the handler against server cancellation so a handler that does not
      // observe the signal still unblocks the read loop. Swallow the handler's
      // late settlement if cancellation wins the race (avoid an unhandled
      // rejection). Per MCP spec a cancelled request is NOT answered.
      samplingPromise.catch(() => {});
      const abortPromise = new Promise<never>((_, reject) => {
        controller.signal.addEventListener('abort', () => {
          serverCancelled = true;
          reject(new Error('sampling request cancelled by server'));
        }, { once: true });
      });
      const result = await Promise.race([samplingPromise, abortPromise]);
      if (!serverCancelled) this.respondToServerRequest(id, { result });
    } catch (err) {
      if (!serverCancelled) this.respondToServerRequest(id, { error: { code: -32603, message: err instanceof Error ? err.message : 'sampling failed' } });
    } finally {
      this._inboundAborts.delete(id);
    }
  }

  private rejectAllPending(error: Error): void {
    for (const [id, pending] of this.pendingRequests) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pendingRequests.clear();
  }
}
