// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { streamText, generateText } from 'ai';
import { resolveThinkingBudget } from './model-capabilities';
import { findErrorCode, SSL_ERROR_CODES, MOCK_RATE_LIMIT_ENV, type MockRateLimitConfig } from './error-classifier';
import { MTLS_ENV } from './utils/mtls';
import { isEnvTruthy } from './analytics';
import type { ProviderConfig, StreamChunk, ThinkingConfig } from './types';

export function extractStatusCode(errorText: string): number | null {
  // Match "HTTP {status}" or "status code {status}" patterns commonly
  // found in AI SDK error messages and our own error formatting.
  const match = errorText.match(/(?:HTTP|status\s+code)\s+(\d+)/i);
  if (match) {
    return parseInt(match[1], 10);
  }
  return null;
}

// FUNCTIONAL: the Anthropic SDK sometimes fails to propagate the 529 status code
// during streaming, so an overload surfaces as an error whose body carries
// `"type":"overloaded_error"` but no parseable HTTP status. Treat that body signal
// as a retryable overload (equivalent to 529) regardless of the parsed status, so
// a status-less overload is retried instead of terminating the turn.
export const OVERLOADED_ERROR_STATUS = 529;

export function isOverloadedError(errorText: string): boolean {
  return /"type"\s*:\s*"overloaded_error"/i.test(errorText);
}

/**
 * Build a synthetic 429 rate-limit error chunk for the {@link MOCK_RATE_LIMIT_ENV}
 * dev/test knob. The error text carries a parseable `HTTP 429` so the retry loop's
 * `extractStatusCode` classifies it as rate-limited, and the Retry-After / reset
 * headers drive the real Retry-After backoff + rate-limit-header display path
 * exactly as a live provider 429 would. Lets the whole rate-limit UX be exercised
 * deterministically without a real throttle (parity: the reference's mock-limits facility).
 */
export function buildMockRateLimitChunk(cfg: MockRateLimitConfig): StreamChunk {
  const retryAfterSeconds = Math.ceil(cfg.retryAfterMs / 1000);
  const resetEpoch = Math.floor(Date.now() / 1000) + retryAfterSeconds;
  return {
    type: 'error',
    error: `API Error: HTTP ${cfg.status} rate_limit_error — synthetic rate limit injected via ${MOCK_RATE_LIMIT_ENV}`,
    retryAfter: retryAfterSeconds,
    responseHeaders: {
      'retry-after': String(retryAfterSeconds),
      'x-ratelimit-reset-requests': String(resetEpoch),
      'x-ratelimit-remaining-requests': '0',
    },
  };
}

// ---------------------------------------------------------------------------
// Query-source tagging + foreground/background overload (529) retry gating
// ---------------------------------------------------------------------------

/**
 * Origin tag for a model request, mirroring the reference's `QuerySource`
 * (constants/querySource.ts). It classifies WHO is blocking on the result so the
 * retry layer can treat a capacity cascade (HTTP 529 overload) differently for a
 * foreground turn — the user (or an in-flight turn) is waiting on it — versus a
 * background/auxiliary call (summaries, memory extraction, titles, critic scoring)
 * the user never sees fail. During an overload every background retry is pure
 * gateway amplification (3-10× per attempt), so a KNOWN background source bails
 * immediately on a 529 instead of hammering an already-overloaded endpoint.
 *
 * The union is open to AMI's own call sites; an untagged (`undefined`) source is
 * treated conservatively as foreground (retried), so this is additive — existing
 * callers that pass no source keep their current retry behavior.
 */
export type QuerySource =
  // Foreground — a human or an in-flight turn is blocking on the result.
  | 'repl_main_thread'   // interactive TUI turn
  | 'detached'           // --prompt / autonomous run blocking on the turn
  | 'sdk'                // programmatic SDK caller
  | 'agent'              // a sub-agent turn the parent turn awaits
  | 'compact'            // context compaction required to continue the turn
  | 'side_question'      // forked side question the user explicitly asked for
  | 'auto_mode'          // permission auto-mode classifier (must complete for correctness)
  // Background — auxiliary work the user never directly waits on.
  | 'session_memory'     // background fact extraction
  | 'title'              // background conversation-title generation
  | 'insights'           // background session-insights generation
  | 'critic'             // background POMDP critic scoring
  | 'skill_judge';       // background skill/stance selection

/**
 * Best-effort identification of the proxy/gateway in front of the API from a
 * response's headers, so a retry/terminal diagnostic can name WHICH intermediary is
 * failing — a Cloudflare 529 vs an Envoy 503 vs a bare origin error point at very
 * different fixes. Returns a short label, or `undefined` when no known fingerprint
 * matches. Header lookup is case-insensitive. Mirrors the reference's
 * gateway-identification debug logging (services/api/logging.ts).
 */
export function identifyGateway(headers: Record<string, string> | undefined): string | undefined {
  if (!headers) return undefined;
  const h: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) h[k.toLowerCase()] = v;
  const server = (h['server'] ?? '').toLowerCase();
  if ('cf-ray' in h || server.includes('cloudflare')) return 'cloudflare';
  if ('x-amz-cf-id' in h || 'x-amzn-requestid' in h || 'x-amzn-trace-id' in h || server.includes('awselb')) return 'aws';
  if ('x-envoy-upstream-service-time' in h || server.includes('envoy')) return 'envoy';
  if (server.includes('nginx')) return 'nginx';
  if (server.includes('gunicorn')) return 'gunicorn';
  const via = h['via'];
  if (via) {
    if (via.toLowerCase().includes('google')) return 'google-frontend';
    return `via:${via.split(/[\s,]/).filter(Boolean)[0] ?? via}`;
  }
  return undefined;
}

// Context-overflow 400 auto-recovery (parity: the reference's withRetry.ts
// parseMaxTokensContextOverflowError + maxTokensOverride). When the server reports
// `input length and max_tokens exceed context limit: A + B > C`, recompute a smaller
// max_tokens from its numbers and retry the SAME request instead of failing the turn
// — AMI preempts this with fitOutputTokens, but when the provider's token accounting
// disagrees (tool schemas, system-prompt growth, different counting) the request can
// still 400 with no recovery.
/** Safety margin (tokens) left below the context limit when recomputing max_tokens. */
export const CONTEXT_OVERFLOW_SAFETY_BUFFER = 1000;

/** Absolute floor for the recomputed output-token budget. */
export const FLOOR_OUTPUT_TOKENS = 3000;

/**
 * Parse an Anthropic context-overflow 400 body into its {input, max, limit} token
 * counts, or `null` when the error is not that specific overflow. The caller gates
 * on `statusCode === 400`; the message substring + numeric triple are matched here.
 */
export function parseMaxTokensContextOverflowError(
  errorText: string | undefined,
): { inputTokens: number; maxTokens: number; contextLimit: number } | null {
  if (!errorText || !errorText.includes('input length and `max_tokens` exceed context limit')) return null;
  const match = errorText.match(/input length and `max_tokens` exceed context limit:\s*(\d+)\s*\+\s*(\d+)\s*>\s*(\d+)/);
  if (!match) return null;
  const inputTokens = parseInt(match[1], 10);
  const maxTokens = parseInt(match[2], 10);
  const contextLimit = parseInt(match[3], 10);
  if (Number.isNaN(inputTokens) || Number.isNaN(maxTokens) || Number.isNaN(contextLimit)) return null;
  return { inputTokens, maxTokens, contextLimit };
}

/**
 * Recompute a reduced max_tokens after a context-overflow 400, or `null` when even
 * the FLOOR won't fit (unrecoverable ⇒ surface the error). availableContext =
 * contextLimit − inputTokens − safetyBuffer, clamped up to {@link FLOOR_OUTPUT_TOKENS}
 * and the thinking-budget floor (+1 output token). Mirrors the reference's maxTokensOverride.
 */
export function recoverMaxTokensFromContextOverflow(
  overflow: { inputTokens: number; contextLimit: number },
  thinking?: ThinkingConfig,
  modelId?: string,
): number | null {
  const availableContext = Math.max(0, overflow.contextLimit - overflow.inputTokens - CONTEXT_OVERFLOW_SAFETY_BUFFER);
  if (availableContext < FLOOR_OUTPUT_TOKENS) return null;
  const thinkingBudget = thinking?.enabled
    ? (thinking.budgetTokens ?? resolveThinkingBudget(thinking.level, modelId))
    : 0;
  const minRequired = thinkingBudget + 1;
  return Math.max(FLOOR_OUTPUT_TOKENS, availableContext, minRequired);
}

// ---------------------------------------------------------------------------
// SSL/TLS certificate error detection — fail fast with actionable hints
// ---------------------------------------------------------------------------

// OpenSSL/TLS failure codes are the single canonical SSL_ERROR_CODES set in
// error-classifier.ts (imported above) so provider.ts and classifyError share
// one source of truth — no parallel table that can drift.

export function isSslError(err: unknown): boolean {
  // Walk the cause chain: TLS errors (CERT_HAS_EXPIRED, …) are nested under
  // `.cause` beneath a generic top-level wrapper.
  const code = findErrorCode(err);
  if (code && SSL_ERROR_CODES.has(code)) return true;
  const msg = err instanceof Error ? err.message : '';
  return /self signed certificate|certificate has expired|unable to get local issuer certificate/i.test(msg);
}

/**
 * True when the error is a STALE pooled keep-alive socket (ECONNRESET/EPIPE) —
 * the connection was reused after the far end had already dropped it. Mirrors
 * the reference's `isStaleConnectionError` (services/api/withRetry.ts:L112-118).
 * Distinguished from a generic network error because the recovery is different:
 * the dead socket must be dropped (keep-alive disabled, fresh client) before the
 * next attempt, or the retry grabs the same poisoned socket and fails again.
 */
export function isStaleConnectionError(err: unknown): boolean {
  const code = findErrorCode(err);
  if (code === 'ECONNRESET' || code === 'EPIPE') return true;
  const msg = err instanceof Error ? err.message : (typeof err === 'string' ? err : '');
  return /ECONNRESET|EPIPE/i.test(msg);
}

export const DEFAULT_REQUEST_TIMEOUT_MS = 300_000; // 5 minutes

/**
 * Inter-stream-event gap (ms) above which a stream is counted as having STALLED —
 * slow but still alive — distinct from the hard idle-abort. Mirrors the reference's
 * `STALL_THRESHOLD_MS` (30s, services/api/claude.ts streaming-stall telemetry).
 * A gap past this is logged as a diagnostic breadcrumb and summarized at stream
 * end; it never aborts the stream (only the idle watchdog does that).
 */
export const STREAM_STALL_THRESHOLD_MS = 30_000;

/**
 * Resolve the per-request timeout (ms) from the environment. Prefers AMI's own
 * SUPERINFERENCE_API_TIMEOUT_MS and falls back to the reference's API_TIMEOUT_MS
 * name for parity. Returns a non-positive value (0 / NaN-coerced) when neither
 * is a usable number so the caller applies DEFAULT_REQUEST_TIMEOUT_MS.
 */
export function envApiTimeoutMs(env: NodeJS.ProcessEnv): number {
  const raw = env.SUPERINFERENCE_API_TIMEOUT_MS ?? env.API_TIMEOUT_MS;
  return Number(raw);
}

/**
 * Build the user-facing request-timeout error, naming the configurable env var
 * and the current limit so the message is actionable (mirrors the reference's
 * "try increasing it" affordance) instead of a bare duration.
 */
export function formatTimeoutError(timeoutMs: number, partial: boolean): string {
  const partialNote = partial ? ' (partial content received)' : '';
  return `Request timed out after ${timeoutMs}ms${partialNote} `
    + `(SUPERINFERENCE_API_TIMEOUT_MS=${timeoutMs}ms, try increasing it)`;
}

/**
 * Build the actionable SSL/TLS error message. Beyond the custom-CA-file hint
 * (NODE_EXTRA_CA_CERTS), it names AMI's own trust/auth affordances so the fix is
 * self-service: a custom CA bundle ({@link MTLS_ENV.ca}) for a corporate CA already
 * trusted by the OS but not exported as a PEM file, and client-certificate / mutual
 * TLS ({@link MTLS_ENV.cert}/{@link MTLS_ENV.key}) for a gateway that rejects the
 * handshake for lack of a client cert. Mirrors the reference's caCerts.ts / mtls.ts
 * remedy enumeration.
 */
export function formatSslError(original: string): string {
  return (
    `SSL/TLS certificate error: ${original}. Possible fixes: ` +
    '(1) Set NODE_EXTRA_CA_CERTS=/path/to/ca-bundle.crt for corporate proxies ' +
    '(2) Check if a TLS-inspecting proxy needs its CA certificate added ' +
    "(3) Verify server certificates haven't expired " +
    `(4) Trust a corporate/private CA already in your OS store by exporting it and setting ${MTLS_ENV.ca} to its PEM (path or inline) ` +
    `(5) If the server requires a CLIENT certificate (mutual TLS), set ${MTLS_ENV.cert} and ${MTLS_ENV.key} (optionally ${MTLS_ENV.passphrase})`
  );
}

// ---------------------------------------------------------------------------
// Non-streaming request fallback (the reference parity: executeNonStreamingRequest).
//
// Some gateways/proxies break SSE streaming — they answer a stream request with a
// 200 + non-SSE body (surfaced as EMPTY_STREAM_ERROR) or a 404 on the streaming
// endpoint — yet serve an ordinary non-streaming request fine. When the fallback
// is enabled, streamChatCompletionWithRetry recovers such a turn by making ONE
// bounded, token-capped non-streaming request via the AI SDK `generateText` and
// replaying its result as the normal stream chunks (reusing ALL of
// streamChatCompletion's request-building and chunk-emission — no duplication).
// ---------------------------------------------------------------------------

/**
 * Hard cap on `max_tokens` for a non-streaming fallback request (the reference parity:
 * `MAX_NON_STREAMING_TOKENS`). A single non-streaming response has no incremental
 * delivery, so a huge budget risks a long opaque wait or a server-side cap; the
 * fallback is a recovery path, not the primary long-generation path, so its output
 * is bounded. {@link adjustParamsForNonStreaming} clamps the request to this.
 */
export const MAX_NON_STREAMING_TOKENS = 21_333;

/**
 * Whether the non-streaming fallback is enabled for a request. An explicit
 * `config.nonStreamingFallback` boolean wins; otherwise the process-wide
 * `SUPERINFERENCE_NONSTREAMING_FALLBACK` / `CLAUDE_CODE_NONSTREAMING_FALLBACK`
 * env knob is honored. Off by default (the streaming-only retry path is unchanged).
 */
export function nonStreamingFallbackEnabled(
  config: ProviderConfig,
  env: NodeJS.ProcessEnv = process.env,
): boolean {
  if (typeof config.nonStreamingFallback === 'boolean') return config.nonStreamingFallback;
  return isEnvTruthy(env.SUPERINFERENCE_NONSTREAMING_FALLBACK)
    || isEnvTruthy(env.CLAUDE_CODE_NONSTREAMING_FALLBACK);
}

/**
 * Clamp a request's `maxTokens` to {@link MAX_NON_STREAMING_TOKENS} for the
 * non-streaming fallback (the reference parity: `adjustParamsForNonStreaming`). Returns a
 * NEW config (never mutates the caller's). An absent/larger budget is capped; a
 * smaller one is left as-is so a deliberately tiny request is respected.
 */
export function adjustParamsForNonStreaming(config: ProviderConfig): ProviderConfig {
  const requested = config.maxTokens;
  const capped = requested === undefined
    ? MAX_NON_STREAMING_TOKENS
    : Math.min(requested, MAX_NON_STREAMING_TOKENS);
  return { ...config, maxTokens: capped };
}

/**
 * Adapt the AI SDK `generateText` (a single non-streaming call) into a
 * `streamText`-shaped impl so {@link streamChatCompletion} can drive the
 * non-streaming path through its EXISTING request-building and part-handling loop
 * with zero duplication. The returned fake exposes a `.stream` that replays the
 * completed result as the same fullStream parts streamChatCompletion already
 * understands — reasoning, text, each tool-call, then a terminal finish-step
 * carrying the usage + providerMetadata the token/cache accounting reads. A thrown
 * generateText error propagates out of the replay (caught by streamChatCompletion's
 * try/catch and surfaced as an error chunk), exactly like a streaming failure.
 */
export function generateTextToStreamAdapter(
  generateTextFn: typeof generateText = generateText,
): typeof streamText {
  return ((params: Record<string, unknown>) => {
    async function* replay(): AsyncGenerator<Record<string, unknown>> {
      // `onError` is a streamText-only callback; a non-streaming call throws
      // instead, so strip it before forwarding the identical params.
      const { onError: _onError, ...rest } = params;
      const result = await generateTextFn(rest as Parameters<typeof generateText>[0]);
      const reasoning = (result as { reasoningText?: unknown }).reasoningText;
      if (typeof reasoning === 'string' && reasoning.length > 0) {
        yield { type: 'reasoning-delta', text: reasoning };
      }
      if (typeof result.text === 'string' && result.text.length > 0) {
        yield { type: 'text-delta', text: result.text };
      }
      for (const tc of result.toolCalls ?? []) {
        yield {
          type: 'tool-call',
          toolCallId: (tc as { toolCallId: string }).toolCallId,
          toolName: (tc as { toolName: string }).toolName,
          input: (tc as { input: unknown }).input,
        };
      }
      yield {
        type: 'finish-step',
        finishReason: result.finishReason,
        usage: (result as { totalUsage?: unknown }).totalUsage ?? result.usage ?? {},
        providerMetadata: result.providerMetadata,
        response: result.response,
      };
    }
    return { stream: replay() };
  }) as unknown as typeof streamText;
}

// ---------------------------------------------------------------------------
// Credential sanitization — strip secrets from error messages
// ---------------------------------------------------------------------------

export const CREDENTIAL_PATTERNS: RegExp[] = [
  /sk-[a-zA-Z0-9_-]{10,}/g,
  /sk-ant-[a-zA-Z0-9_-]{10,}/g,
  /sk-or-[a-zA-Z0-9_-]{10,}/g,
  /key-[a-zA-Z0-9_-]{10,}/g,
  /Bearer\s+[a-zA-Z0-9_.\-/+=]{10,}/gi,
  /\b[a-zA-Z0-9._%+-]+:[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+/g,
  /(?:x-api-key|authorization|x-goog-api-key)[:\s]+[^\s,;]{10,}/gi,
  /AIzaSy[a-zA-Z0-9_-]{30,}/g,
  /gsk_[a-zA-Z0-9_-]{10,}/g,
  /xai-[a-zA-Z0-9_-]{10,}/g,
  /hf_[a-zA-Z0-9_-]{10,}/g,
  // FUNCTIONAL: credentials AMI itself provisions but the error path would leak.
  // AWS access-key IDs (amazon-bedrock via AWS_ACCESS_KEY_ID): long-term AKIA plus
  // temporary/STS and other canonical prefixes, each followed by 16 upper-alnum.
  /\b(?:AKIA|ASIA|AGPA|AIDA|AROA|ANPA|ANVA|ABIA|ACCA)[0-9A-Z]{16}\b/g,
  // GCP service-account identities (google-vertex via GOOGLE_APPLICATION_CREDENTIALS,
  // a service-account JSON): <name>@<project>.iam.gserviceaccount.com.
  /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.iam\.gserviceaccount\.com\b/g,
  // AWS_* / GOOGLE_* secret env assignments — redact only the value (lookbehind keeps
  // the variable name intact for debuggability), e.g. `AWS_SECRET_ACCESS_KEY=…`.
  /(?<=\bAWS[_-][A-Za-z0-9_]+\s*[=:]\s*)[^\s,;"']+/gi,
  /(?<=\bGOOGLE[_-][A-Za-z0-9_]+\s*[=:]\s*)[^\s,;"']+/gi,
  // Generic secret-named assignment fallback: API[-_]KEY / TOKEN / SECRET / PASSWORD = value.
  /(?<=\b(?:API[-_]?KEY|TOKEN|SECRET|PASSWORD)\s*[=:]\s*)[^\s,;"']+/gi,
];

export function sanitizeErrorMessage(error: Error | string): string {
  const msg = error instanceof Error ? error.message : error;
  let sanitized = msg;
  for (const pattern of CREDENTIAL_PATTERNS) {
    sanitized = sanitized.replace(new RegExp(pattern.source, pattern.flags), '[REDACTED]');
  }
  return sanitized;
}

// ---------------------------------------------------------------------------
// Retry-After header parsing
// ---------------------------------------------------------------------------

export const MAX_RETRY_AFTER_MS = 3_600_000;

export const DEFAULT_RETRY_AFTER_MS = 60_000;

export const MIN_RETRY_AFTER_MS = 1_000;

export function parseRetryAfterHeader(header: string | null | undefined): number {
  if (!header || !header.trim()) return DEFAULT_RETRY_AFTER_MS;
  const trimmed = header.trim();
  const numeric = Number(trimmed);
  if (!isNaN(numeric) && isFinite(numeric)) {
    const ms = numeric * 1000;
    return Math.max(MIN_RETRY_AFTER_MS, Math.min(ms, MAX_RETRY_AFTER_MS));
  }
  const date = Date.parse(trimmed);
  if (!isNaN(date)) {
    const delayMs = date - Date.now();
    if (delayMs <= 0) return MIN_RETRY_AFTER_MS;
    return Math.max(MIN_RETRY_AFTER_MS, Math.min(delayMs, MAX_RETRY_AFTER_MS));
  }
  return DEFAULT_RETRY_AFTER_MS;
}

// ---------------------------------------------------------------------------
// Prompt cache tracking — detect cache thrashing from mid-session tool loads
// ---------------------------------------------------------------------------

export const CACHE_MISS_WARN_THRESHOLD = 3;
