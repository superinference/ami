// Split from client.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { MCP_LOG_LEVELS } from './client-mcp-elicitation-request';

export interface McpPromptMessage {
  role: 'user' | 'assistant';
  content: { type: string; text?: string; [key: string]: unknown };
}

export interface JsonRpcRequest {
  jsonrpc: '2.0';
  id: number;
  method: string;
  params?: Record<string, unknown>;
}

export interface JsonRpcResponse {
  jsonrpc: '2.0';
  id: number;
  result?: unknown;
  error?: { code: number; message: string; data?: unknown };
}

export interface JsonRpcNotification {
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

export const SHUTDOWN_SIGINT_MS = 100;

export const SHUTDOWN_SIGTERM_MS = 400;

export const SHUTDOWN_TOTAL_MS = 500;

export const SHUTDOWN_POLL_MS = 50;

/** Backoff bounds for re-opening the Streamable-HTTP server→client GET stream. */
export const HTTP_STREAM_RECONNECT_BASE_MS = 1000;

export const HTTP_STREAM_RECONNECT_MAX_MS = 30000;

/**
 * ±25% proportional jitter applied to the capped exponential reconnect delay so
 * many clients dropped by a shared outage do not retry in lockstep (thundering
 * herd). Matches the fraction the reference's transports use.
 */
export const RECONNECT_JITTER_FRAC = 0.25;

/**
 * Liveness/idle timeout for the Streamable-HTTP server→client GET SSE stream.
 * A compliant server sends keepalives (SSE comment frames) periodically; if NO
 * bytes at all arrive for this long the connection is treated as dead (a
 * half-open TCP socket silently dropped by a NAT/proxy/load-balancer with no
 * FIN/RST). The timer is reset on EVERY incoming chunk (including keepalive
 * comment frames), and on expiry the GET request is destroyed and a reconnect
 * is scheduled. Mirrors the reference's SSETransport LIVENESS_TIMEOUT_MS (45s).
 */
export const HTTP_STREAM_LIVENESS_TIMEOUT_MS = 45000;

/**
 * Wall-clock budget for re-opening the GET stream before giving up (10 minutes),
 * measured from the first failure of a run of consecutive failures. A successful
 * (re)connect resets the clock, so the stream survives any number of brief
 * transient drops as long as no single outage lasts past the budget — unlike a
 * small fixed attempt count that gives up after a few seconds of backoff.
 * Mirrors the reference's SSETransport RECONNECT_GIVE_UP_MS.
 */
export const HTTP_STREAM_RECONNECT_GIVE_UP_MS = 600000;

/**
 * Cap on the dedup set of seen SSE sequence numbers before pruning, and how far
 * below the high-water mark a pruned entry must be. Only sequence numbers near
 * the high-water mark matter for duplicate detection, so older ones are dropped
 * to keep the set bounded. Mirrors the reference's SSETransport pruning strategy.
 */
export const HTTP_STREAM_SEEN_SEQ_CAP = 1000;

export const HTTP_STREAM_SEEN_SEQ_PRUNE_MARGIN = 200;

/**
 * How many trailing stderr lines a stdio MCP server's output is retained for. On
 * an abnormal exit the tail is appended to the failure message so a consumer can
 * see WHY the child died (e.g. a stack trace it printed right before exiting)
 * instead of only a bare exit code. Bounded so a chatty server can't grow it.
 */
export const MCP_STDERR_TAIL_LINES = 10;
