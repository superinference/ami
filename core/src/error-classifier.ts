/**
 * Error classification for smart recovery and model fallback.
 *
 * Provides a structured taxonomy of API errors and a classification
 * function that determines the correct recovery action (retry, compact
 * context, fall back to another model, or abort).
 *
 * Inspired by the Hermes agent's error_classifier.py — centralizes
 * scattered inline string-matching into a single classifier that the
 * engine loop consults for every API failure.
 */

import { APICallError, LoadAPIKeyError, NoSuchModelError, InvalidPromptError, NoContentGeneratedError, EmptyResponseBodyError } from 'ai';
import { classifyZdrError } from './openrouter-zdr';

/**
 * Walk an error's `.cause` chain (depth-limited, loop-guarded) and return the
 * first string `code` found. The Anthropic/undici SDK wraps the real
 * network/TLS error (ETIMEDOUT, CERT_HAS_EXPIRED, …) inside nested `.cause`
 * objects, so the top-level error is often a generic "Connection error." with no
 * `code` of its own — inspecting only the top level misses it. Mirrors Claude's
 * extractConnectionErrorDetails cause walk. `maxDepth` bounds the descent and a
 * visited set guards against self-referential / cyclic causes.
 */
export function findErrorCode(err: unknown, maxDepth = 5): string | undefined {
  const seen = new Set<unknown>();
  let current: unknown = err;
  for (let depth = 0; depth <= maxDepth; depth++) {
    if (current === null || typeof current !== 'object' || seen.has(current)) break;
    seen.add(current);
    const code = (current as { code?: unknown }).code;
    if (typeof code === 'string' && code.length > 0) return code;
    current = (current as { cause?: unknown }).cause;
  }
  return undefined;
}

/**
 * Minify-safe abort/cancellation detection. An aborted fetch/stream surfaces as
 * a DOMException whose `name` is 'AbortError' (code 20) in Node's
 * fetch/AbortController path, but bundlers can rename concrete classes, so an
 * `instanceof SomeAbortClass` check is brittle. Union the stable signals: the
 * 'AbortError' name, a DOMException ABORT_ERR code, and a message fallback —
 * while never mistaking an ECONNABORTED network drop for a user cancellation.
 * Mirrors Claude's isAbortError. Exported so the engine/provider can share one
 * definition instead of ad-hoc `message.includes('abort')` checks.
 */
/**
 * Canonical OpenSSL/TLS failure-code set (used by both Node.js and Bun),
 * mirroring Claude Code's `SSL_ERROR_CODES` breadth (services/api/errorUtils.ts).
 * The SINGLE source of the TLS-code knowledge for the whole codebase: both this
 * module's `classifyError` ssl branch AND `provider.ts`'s `isSslError` match
 * against it, so a wrong-version / handshake-timeout / revoked / untrusted-chain
 * failure is classified as SSL rather than a generic network error — in one
 * place, with no parallel tables that can drift.
 */
export const SSL_ERROR_CODES: ReadonlySet<string> = new Set([
  // Certificate verification
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'UNABLE_TO_GET_ISSUER_CERT',
  'UNABLE_TO_GET_ISSUER_CERT_LOCALLY',
  'CERT_SIGNATURE_FAILURE',
  'CERT_NOT_YET_VALID',
  'CERT_HAS_EXPIRED',
  'CERT_REVOKED',
  'CERT_REJECTED',
  'CERT_UNTRUSTED',
  // Self-signed
  'DEPTH_ZERO_SELF_SIGNED_CERT',
  'SELF_SIGNED_CERT_IN_CHAIN',
  // Chain
  'CERT_CHAIN_TOO_LONG',
  'PATH_LENGTH_EXCEEDED',
  // Hostname / altname
  'ERR_TLS_CERT_ALTNAME_INVALID',
  'HOSTNAME_MISMATCH',
  // TLS handshake
  'ERR_TLS_HANDSHAKE_TIMEOUT',
  'ERR_SSL_WRONG_VERSION_NUMBER',
  'ERR_SSL_DECRYPTION_FAILED_OR_BAD_RECORD_MAC',
]);

const SSL_ERROR_CODES_LOWER: readonly string[] = [...SSL_ERROR_CODES].map(c => c.toLowerCase());

/**
 * True when a (lower-cased) error string contains any canonical OpenSSL/TLS code.
 * Keeps the `classifyError` ssl branch in lock-step with `SSL_ERROR_CODES`
 * (adding a code to the set is picked up here with no second edit).
 */
export function matchesSslErrorCode(lowerMessage: string): boolean {
  return SSL_ERROR_CODES_LOWER.some(code => lowerMessage.includes(code));
}

export function isAbortError(err: unknown): boolean {
  if (typeof err === 'string') {
    const m = err.toLowerCase();
    return (m.includes('abort') || m.includes('cancel')) && !m.includes('econnaborted');
  }
  if (err === null || typeof err !== 'object') return false;
  if ((err as { name?: unknown }).name === 'AbortError') return true;
  if (typeof DOMException !== 'undefined' && err instanceof DOMException && err.code === DOMException.ABORT_ERR) {
    return true;
  }
  const message = (err as { message?: unknown }).message;
  if (typeof message === 'string') {
    const m = message.toLowerCase();
    return (m.includes('abort') || m.includes('cancel')) && !m.includes('econnaborted');
  }
  return false;
}

export const ERROR_CATEGORIES = [
  'rate_limited', 'plan_limit', 'context_overflow', 'model_unavailable',
  'auth_error', 'output_too_large', 'content_filter', 'server_error',
  'network_error', 'ssl_error', 'abort', 'repetition', 'gateway_rejection',
  'tool_pairing', 'unknown',
] as const;

export type ErrorCategory = (typeof ERROR_CATEGORIES)[number];

export interface ClassifiedError {
  category: ErrorCategory;
  message: string;
  retryable: boolean;
  shouldCompact: boolean;
  shouldFallback: boolean;
  suggestedDelay?: number;
  /**
   * For context_overflow: how many tokens the prompt is over the limit, parsed
   * from the server's authoritative "… N tokens > M maximum" message. Lets
   * reactive compaction jump past multiple groups in one retry.
   */
  tokenGap?: number;
}

// ---------------------------------------------------------------------------
// Message normalization — HTML stripping + deserialized-error recovery
// ---------------------------------------------------------------------------

/**
 * Strip an HTML error-page body (e.g. a CloudFlare/proxy 5xx interstitial
 * returned instead of JSON) down to its <title> text, or '' when there is no
 * title. Plain text is returned unchanged. Mirrors Claude's sanitizeMessageHTML
 * so the user never sees a multi-KB raw HTML dump.
 */
export function sanitizeMessageHTML(message: string): string {
  if (typeof message !== 'string') return '';
  if (message.includes('<!DOCTYPE html') || message.includes('<html')) {
    const titleMatch = message.match(/<title>([^<]+)<\/title>/);
    if (titleMatch && titleMatch[1]) return titleMatch[1].trim();
    return '';
  }
  return message;
}

/**
 * Recover a human-readable message from a deserialized API error that lost its
 * top-level `.message` after JSONL round-tripping (session resume). The real
 * text lives nested: `error.error.error.message` (standard Anthropic response
 * body wrapping the API error) or `error.error.message` (Bedrock/proxy). Probes
 * deepest-first, sanitizing HTML at each level. Returns null when neither holds
 * a usable string. Mirrors Claude's extractNestedErrorMessage.
 */
export function extractNestedErrorMessage(error: unknown): string | null {
  if (typeof error !== 'object' || error === null) return null;
  const outer = (error as { error?: unknown }).error;
  if (typeof outer !== 'object' || outer === null) return null;
  const deep = (outer as { error?: { message?: unknown } }).error?.message;
  if (typeof deep === 'string' && deep.length > 0) {
    const sanitized = sanitizeMessageHTML(deep);
    if (sanitized.length > 0) return sanitized;
  }
  const mid = (outer as { message?: unknown }).message;
  if (typeof mid === 'string' && mid.length > 0) {
    const sanitized = sanitizeMessageHTML(mid);
    if (sanitized.length > 0) return sanitized;
  }
  return null;
}

/**
 * Dig a usable error type out of a non-standard provider error envelope that
 * arrives as a plain object rather than an `Error` — specifically an Amazon
 * Bedrock routing error, which nests its type under `Output.__type` (e.g.
 * `{ Output: { __type: 'ThrottlingException' } }`) inside an otherwise
 * well-formed-looking body. Without this the object stringifies to
 * `[object Object]` and classifies as a silent `unknown`. Mirrors Claude's
 * extractUnknownErrorFormat.
 */
export function extractUnknownErrorFormat(value: unknown): string | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const type = (value as { Output?: { __type?: unknown } }).Output?.__type;
  return typeof type === 'string' && type.length > 0 ? type : undefined;
}

/**
 * Best human-readable message for any error value: a plain string, an Error's
 * `.message`, or a nested/deserialized API-error shape — always HTML-sanitized.
 */
export function normalizeErrorMessage(error: unknown): string {
  if (typeof error === 'string') return sanitizeMessageHTML(error);
  if (error instanceof Error) {
    const nested = extractNestedErrorMessage(error);
    return nested ?? sanitizeMessageHTML(error.message);
  }
  const nested = extractNestedErrorMessage(error);
  if (nested) return nested;
  // A non-standard provider envelope (e.g. Bedrock Output.__type) carries its
  // type here when there is no message — fold it in so classification keywords
  // can match instead of falling to a meaningless String(error).
  const envelope = extractUnknownErrorFormat(error);
  if (envelope) return envelope;
  return String(error ?? '');
}

// ---------------------------------------------------------------------------
// Token-count parsing — precise overflow recovery
// ---------------------------------------------------------------------------

/**
 * Parse actual/limit token counts from a raw "prompt is too long: 137500 tokens
 * > 135000 maximum" error. Lenient: tolerant of SDK prefixes, JSON envelopes,
 * and Vertex casing. Mirrors Claude's parsePromptTooLongTokenCounts.
 */
export function parsePromptTooLongTokenCounts(rawMessage: string): {
  actualTokens: number | undefined;
  limitTokens: number | undefined;
} {
  const match = typeof rawMessage === 'string'
    ? rawMessage.match(/prompt is too long[^0-9]*(\d+)\s*tokens?\s*>\s*(\d+)/i)
    : null;
  return {
    actualTokens: match ? parseInt(match[1], 10) : undefined,
    limitTokens: match ? parseInt(match[2], 10) : undefined,
  };
}

/** How many tokens over the limit a prompt-too-long error reports, or undefined. */
export function getPromptTooLongTokenGap(rawMessage: string): number | undefined {
  const { actualTokens, limitTokens } = parsePromptTooLongTokenCounts(rawMessage);
  if (actualTokens === undefined || limitTokens === undefined) return undefined;
  const gap = actualTokens - limitTokens;
  return gap > 0 ? gap : undefined;
}

/**
 * Parse the exact 400 "input length and `max_tokens` exceed context limit: A + B
 * > C" message into its three counts, or null. Mirrors Claude's
 * parseMaxTokensContextOverflowError — lets callers compute a precise reduced
 * max_tokens for the next attempt instead of guessing.
 */
export function parseMaxTokensContextOverflow(rawMessage: string): {
  inputTokens: number;
  maxTokens: number;
  contextLimit: number;
} | null {
  if (typeof rawMessage !== 'string') return null;
  if (!rawMessage.includes('input length and `max_tokens` exceed context limit')) return null;
  const m = rawMessage.match(/input length and `max_tokens` exceed context limit:\s*(\d+)\s*\+\s*(\d+)\s*>\s*(\d+)/);
  if (!m) return null;
  return { inputTokens: parseInt(m[1], 10), maxTokens: parseInt(m[2], 10), contextLimit: parseInt(m[3], 10) };
}

// ---------------------------------------------------------------------------
// Gateway rejection detection — API gateways that reject beta headers
// ---------------------------------------------------------------------------

/**
 * Detect gateway/proxy errors that reject Anthropic beta headers.
 * These are 400 errors caused by gateways that don't forward
 * `anthropic-beta` or reject unknown beta versions. The fix is to
 * retry without the beta header.
 */
export function isGatewayRejection(lowerMsg: string): boolean {
  // Reject patterns: unsupported/unknown/invalid/unrecognized header or beta version
  if (lowerMsg.includes('anthropic-beta') &&
      (lowerMsg.includes('unsupported') || lowerMsg.includes('unknown') ||
       lowerMsg.includes('invalid') || lowerMsg.includes('unrecognized') ||
       lowerMsg.includes('not supported') || lowerMsg.includes('not recognized'))) {
    return true;
  }
  if (lowerMsg.includes('unknown beta') || lowerMsg.includes('unsupported beta') ||
      lowerMsg.includes('unrecognized beta') || lowerMsg.includes('invalid beta')) {
    return true;
  }
  if ((lowerMsg.includes('unsupported header') || lowerMsg.includes('unrecognized header') ||
       lowerMsg.includes('invalid header') || lowerMsg.includes('unknown header')) &&
      lowerMsg.includes('beta')) {
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Tool-pairing corruption detection — recoverable only via /rewind
// ---------------------------------------------------------------------------

/**
 * Detect the three distinct 400 errors the API returns when the `tool_use` /
 * `tool_result` blocks in the conversation history are mispaired:
 *   - `tool_use` ids found without a `tool_result` block immediately after,
 *   - an unexpected `tool_use_id` in a `tool_result`,
 *   - duplicate (non-unique) `tool_use` ids.
 * AMI heals these proactively (provider-transform.ts healOrphanedToolCalls), but
 * if a corruption still reaches the API, retrying the same history just loops —
 * the only recovery is to rewind the conversation. Mirrors Claude's reactive
 * tool-use-concurrency detection. Backtick-tolerant (the API wraps the tokens in
 * backticks). `lowerMsg` must already be lower-cased.
 */
export function isToolPairingError(lowerMsg: string): boolean {
  const m = typeof lowerMsg === 'string' ? lowerMsg.replace(/`/g, '') : '';
  return (
    (m.includes('tool_use') && m.includes('without') && m.includes('tool_result')) ||
    m.includes('unexpected tool_use_id found in tool_result') ||
    m.includes('tool_use ids must be unique')
  );
}

// ---------------------------------------------------------------------------
// SDK typed-error fast path — classify by error class + status code
// ---------------------------------------------------------------------------

function classifySDKError(error: Error): ClassifiedError | null {
  const message = error.message;

  // Abort/cancel — check early before SDK type checks, since an abort
  // may wrap an SDK error. Uses the shared minify-safe detector (which also
  // declines to treat an ECONNABORTED network drop as a cancellation).
  if (isAbortError(error)) {
    return { category: 'abort', message, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // APICallError carries a numeric statusCode — classify by it
  if (APICallError.isInstance(error)) {
    const status = error.statusCode;
    const msg = message;

    // Check for plan/billing limits before rate_limited (billing 429 is not transient)
    const lowerMsg = msg.toLowerCase();
    if (lowerMsg.includes('billing') || lowerMsg.includes('credit') ||
        lowerMsg.includes('usage limit') || lowerMsg.includes('plan limit') ||
        lowerMsg.includes('spending limit') ||
        (lowerMsg.includes('budget') && lowerMsg.includes('exceeded'))) {
      return { category: 'plan_limit', message: msg, retryable: false, shouldCompact: false, shouldFallback: false };
    }

    switch (status) {
      case 401:
        return { category: 'auth_error', message: msg, retryable: false, shouldCompact: false, shouldFallback: false };
      case 403:
        // A 403 whose body signals OAuth token revocation is recoverable by a
        // token refresh + retry (Claude's also403Revoked path), so it is
        // retryable — unlike an ordinary permanent permission denial.
        if (lowerMsg.includes('oauth token has been revoked') || lowerMsg.includes('token has been revoked')) {
          return { category: 'auth_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: false };
        }
        // 403 is otherwise a permanent permission denial — never retryable.
        // Covers: invalid API key, unauthorized, no model access, project restrictions.
        return { category: 'auth_error', message: msg, retryable: false, shouldCompact: false, shouldFallback: false };
      case 404:
        // Model does not exist / no access. Non-retryable, but a different model
        // may work — surface actionable /model guidance (Claude's 404 branch).
        return { category: 'model_unavailable', message: msg, retryable: false, shouldCompact: false, shouldFallback: true };
      case 408:
        // Request timeout — transient, retry (Claude shouldRetry 408).
        return { category: 'network_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
      case 409:
        // Lock timeout / conflict — transient, retry (Claude shouldRetry 409).
        return { category: 'server_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 3000 };
      case 429:
        return { category: 'rate_limited', message: msg, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
      case 413:
        return { category: 'context_overflow', message: msg, retryable: true, shouldCompact: true, shouldFallback: false, tokenGap: getPromptTooLongTokenGap(msg) };
      case 400:
        // 400 from corrupted tool_use/tool_result pairing — retrying the same
        // history loops; the user must /rewind. Checked before the gateway/
        // context branches so a pairing message is never mis-bucketed.
        if (isToolPairingError(lowerMsg)) {
          return { category: 'tool_pairing', message: msg, retryable: false, shouldCompact: false, shouldFallback: false };
        }
        // 400 from gateway rejecting beta headers (e.g. anthropic-beta)
        if (isGatewayRejection(lowerMsg)) {
          return { category: 'gateway_rejection', message: msg, retryable: true, shouldCompact: false, shouldFallback: false };
        }
        // 400 with context/token-related message → context_overflow
        // Use specific phrases to avoid false positives on unrelated messages
        // like 'authentication token expired' or 'field value is too long'.
        if (lowerMsg.includes('context_length') || lowerMsg.includes('context window') || lowerMsg.includes('context limit') ||
            lowerMsg.includes('context length') || lowerMsg.includes('maximum context') ||
            lowerMsg.includes('token limit') || lowerMsg.includes('too many tokens') || lowerMsg.includes('max_tokens') ||
            lowerMsg.includes('token count') ||
            lowerMsg.includes('prompt_too_long') || lowerMsg.includes('prompt is too long') || lowerMsg.includes('input is too long') ||
            lowerMsg.includes('reduce the length')) {
          return { category: 'context_overflow', message: msg, retryable: true, shouldCompact: true, shouldFallback: false, tokenGap: getPromptTooLongTokenGap(msg) };
        }
        return null; // fall through to regex
      case 500:
      case 502:
        return { category: 'server_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
      case 503:
      case 529:
        return { category: 'model_unavailable', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 10000 };
      default:
        // Generic status fallback: any other 5xx (504 gateway timeout, 507/508,
        // Cloudflare 520-527, 599, …) is a transient server/gateway failure —
        // retryable with fallback, mirroring Claude's `status >= 500` rule.
        // Keeps the explicit cases above (500/502 server, 503/529 overload)
        // authoritative while no longer dropping uncommon 5xx codes to 'unknown'.
        if (typeof status === 'number' && status >= 500) {
          return { category: 'server_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
        }
        return null; // fall through to regex
    }
  }

  // LoadAPIKeyError — missing or unloadable API key
  if (LoadAPIKeyError.isInstance(error)) {
    return { category: 'auth_error', message, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // NoSuchModelError — model does not exist
  if (NoSuchModelError.isInstance(error)) {
    return { category: 'model_unavailable', message, retryable: false, shouldCompact: false, shouldFallback: true };
  }

  // InvalidPromptError — prompt validation failure (often context overflow)
  if (InvalidPromptError.isInstance(error)) {
    return { category: 'context_overflow', message, retryable: true, shouldCompact: true, shouldFallback: false };
  }

  // NoContentGeneratedError — model returned empty response
  if (NoContentGeneratedError.isInstance(error)) {
    return { category: 'server_error', message, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
  }

  // EmptyResponseBodyError — empty HTTP response body
  if (EmptyResponseBodyError.isInstance(error)) {
    return { category: 'server_error', message, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
  }

  return null; // not a recognized SDK error type
}

/**
 * Classify an error into a structured recovery recommendation.
 *
 * Accepts either a string (legacy) or an Error object (SDK errors).
 * When given an Error object, the SDK fast path tries to classify by
 * error type and status code before falling through to regex matching.
 *
 * The regex checks are priority-ordered: more specific patterns (abort,
 * auth) are tested before broad ones (server_error, unknown).
 */
export function classifyError(error: string | Error | unknown): ClassifiedError {
  // SDK fast path: classify by error type + status code
  if (error instanceof Error) {
    const sdkResult = classifySDKError(error);
    if (sdkResult) return sdkResult;
    // Fall through to regex matching on the message string
  }

  // Resolve the best message for any value: plain string, Error.message, or a
  // nested/deserialized API-error shape — HTML-sanitized so a CloudFlare/proxy
  // error page never reaches the user or the classifier's keyword matching.
  const errorString = normalizeErrorMessage(error);
  // Fold the root `.cause` code (e.g. a nested ETIMEDOUT / CERT_HAS_EXPIRED that
  // the SDK hides under a generic "Connection error.") into the matched text, so
  // network/SSL classification works even when the top-level message has no code.
  const causeCode = error instanceof Error ? findErrorCode(error) : undefined;
  const lower = (causeCode ? `${errorString} ${causeCode}` : errorString).toLowerCase();

  // Abort — not retryable, checked first so it short-circuits. The shared
  // detector excludes ECONNABORTED, which is a network error, not a
  // user-initiated abort.
  if (isAbortError(lower)) {
    return { category: 'abort', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Auth — not retryable, no point falling back with bad credentials
  // Checked before 403 so that "HTTP 403 - invalid api key" is correctly non-retryable.
  if (
    /\bHTTP\s+401\b/i.test(errorString) ||
    lower.includes('api key') ||
    lower.includes('unauthorized') ||
    lower.includes('invalid_api_key') ||
    lower.includes('authentication')
  ) {
    return { category: 'auth_error', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // 403 — permanent permission denial (no model access, project restrictions).
  // Never retryable — retrying burns max-turns on a permanent error.
  if (/\bHTTP\s+403\b/i.test(errorString) || lower.includes('forbidden')) {
    return { category: 'auth_error', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Plan/usage limit — not retryable (billing or plan cap, not transient)
  if (
    lower.includes('billing') ||
    lower.includes('credit') ||
    lower.includes('usage limit') ||
    lower.includes('plan limit') ||
    lower.includes('spending limit') ||
    lower.includes('budget') && lower.includes('exceeded')
  ) {
    return { category: 'plan_limit', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Tool-pairing corruption — not retryable; recover via /rewind. Checked before
  // the broad buckets so a 400 pairing error is never swallowed as 'unknown'.
  if (isToolPairingError(lower)) {
    return { category: 'tool_pairing', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Gateway rejection — a proxy/gateway rejected beta headers; retryable without them
  if (isGatewayRejection(lower)) {
    return { category: 'gateway_rejection', message: errorString, retryable: true, shouldCompact: false, shouldFallback: false };
  }

  // Rate limit — retryable after a delay. `throttling` covers AWS Bedrock's
  // ThrottlingException envelope type extracted by extractUnknownErrorFormat.
  if (
    /\bHTTP\s+429\b/i.test(errorString) ||
    lower.includes('rate limit') ||
    lower.includes('rate_limit') ||
    lower.includes('quota') ||
    lower.includes('too many requests') ||
    lower.includes('throttled') ||
    lower.includes('throttling')
  ) {
    return { category: 'rate_limited', message: errorString, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
  }

  // Context overflow — retryable after compaction
  if (
    (lower.includes('context') && (lower.includes('too long') || lower.includes('overflow') || lower.includes('maximum'))) ||
    /\bHTTP\s+413\b/i.test(errorString) ||
    lower.includes('prompt_too_long') ||
    lower.includes('prompt is too long') ||
    (lower.includes('max_tokens') && !lower.includes('output') && !lower.includes('maximum number of output')) ||
    lower.includes('context length') ||
    lower.includes('token limit') ||
    lower.includes('too many tokens') ||
    lower.includes('reduce the length')
  ) {
    return { category: 'context_overflow', message: errorString, retryable: true, shouldCompact: true, shouldFallback: false, tokenGap: getPromptTooLongTokenGap(errorString) };
  }

  // Output too large — retryable (the engine can request fewer output tokens)
  if (
    lower.includes('output too large') ||
    lower.includes('output is too') ||
    lower.includes('max_output') ||
    lower.includes('output_too_large')
  ) {
    return { category: 'output_too_large', message: errorString, retryable: true, shouldCompact: false, shouldFallback: false };
  }

  // Content filter — not retryable; these are policy decisions, not transient errors
  if (
    lower.includes('content_filter') ||
    lower.includes('content_management') ||
    lower.includes('content policy')
  ) {
    return { category: 'content_filter', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Model unavailable / overloaded — retryable, should try fallback model
  if (
    /\bHTTP\s+503\b/i.test(errorString) ||
    /\bHTTP\s+529\b/i.test(errorString) ||
    lower.includes('overloaded') ||
    lower.includes('capacity') ||
    lower.includes('model_not_found') ||
    lower.includes('model not found')
  ) {
    return { category: 'model_unavailable', message: errorString, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 10000 };
  }

  // Server error — retryable, may benefit from fallback.
  // Use word-boundary matching for status codes to avoid false positives (port
  // numbers, token counts). The generic `HTTP 5xx` arm catches uncommon 5xx
  // codes (504/507/508/599, …); 503/529 overload is already handled above as
  // model_unavailable, so it never reaches here.
  if (
    /\bHTTP\s+500\b|\b500\s+(internal|server)/i.test(errorString) ||
    /\bHTTP\s+502\b/i.test(errorString) ||
    /\bHTTP\s+5\d\d\b/i.test(errorString) ||
    lower.includes('internal server error')
  ) {
    return { category: 'server_error', message: errorString, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
  }

  // SSL/TLS certificate error — not retryable (configuration issue, not transient).
  // Codes are matched from the single canonical SSL_ERROR_CODES set so the branch
  // stays in lock-step with provider.ts; the message heuristics catch the
  // human-readable (spaced) forms the SDK sometimes surfaces without a code.
  if (
    matchesSslErrorCode(lower) ||
    lower.includes('unable to get local issuer certificate') ||
    (lower.includes('certificate') &&
      (lower.includes('expired') || lower.includes('invalid') || lower.includes('self-signed') ||
       lower.includes('verify') || lower.includes('not yet valid') || lower.includes('revoked') ||
       lower.includes('hostname') || lower.includes('untrusted') || lower.includes('rejected')))
  ) {
    return { category: 'ssl_error', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Network error — retryable after a delay
  if (
    lower.includes('econnrefused') ||
    lower.includes('etimedout') ||
    lower.includes('econnreset') ||
    lower.includes('econnaborted') ||
    lower.includes('enotfound') ||
    lower.includes('ehostunreach') ||
    lower.includes('enetunreach') ||
    lower.includes('epipe') ||
    lower.includes('timed out') ||
    lower.includes('network') ||
    lower.includes('fetch failed') ||
    lower.includes('socket hang up') ||
    lower.includes('dns')
  ) {
    return { category: 'network_error', message: errorString, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
  }

  // Repetition — model output is degenerate; not retryable at the API level
  if (lower.includes('repetitive')) {
    return { category: 'repetition', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Unknown — not retryable by default; the engine can decide
  return { category: 'unknown', message: errorString, retryable: false, shouldCompact: false, shouldFallback: false };
}

const FRIENDLY_MESSAGES: Partial<Record<ErrorCategory, string>> = {
  rate_limited: 'Rate limited by the provider — retrying after a delay.',
  plan_limit: 'Plan or billing limit reached — check your account usage and billing settings.',
  context_overflow: 'Context limit reached — compacting conversation. Run /compact or /clear to continue.',
  model_unavailable: 'Model temporarily unavailable — retrying.',
  auth_error: 'Authentication error — check your API key.',
  output_too_large: 'Model output exceeded size limit — retrying with smaller output.',
  content_filter: 'Response blocked by content policy.',
  ssl_error: 'SSL/TLS certificate error — check your provider URL or set NODE_TLS_REJECT_UNAUTHORIZED=0 for self-signed certs.',
  server_error: 'Provider server error — retrying. Check provider status page if this persists.',
  network_error: 'Network error — retrying connection.',
  repetition: 'Repetitive output detected.',
  gateway_rejection: 'Gateway rejected beta header — retrying without it.',
  tool_pairing: 'Conversation history has a tool-call pairing error — run /rewind to recover the conversation.',
};

/**
 * Build a richer, actionable network-error message.
 *
 * A dropped connection is far more often a local proxy/gateway in the way
 * than a VPN/firewall, so the hint names the proxy/gateway first and
 * VPN/firewall second. DNS and timeout signatures get a more specific lead.
 */
export function networkErrorHint(lowerMsg: string): string {
  if (lowerMsg.includes('enotfound') || lowerMsg.includes('getaddrinfo') || lowerMsg.includes('dns')) {
    return 'Network error (DNS lookup failed) — check your proxy/gateway and DNS first, then VPN/firewall.';
  }
  if (lowerMsg.includes('etimedout') || lowerMsg.includes('timed out')) {
    // Introspect the active timeout knob so the user can fix it directly.
    const configured = process.env.SUPERINFERENCE_API_TIMEOUT_MS;
    const knob = configured
      ? ` (SUPERINFERENCE_API_TIMEOUT_MS=${configured}ms, try increasing it)`
      : '';
    return `Network error (connection timed out) — check your proxy/gateway first, then VPN/firewall.${knob}`;
  }
  return 'Network error — retrying connection. Check your proxy/gateway first, then VPN/firewall.';
}

/**
 * Build a per-code, actionable SSL/TLS certificate hint. A bare "SSL error"
 * leaves the user guessing; distinguishing expired / not-yet-valid / revoked /
 * self-signed / hostname-mismatch / untrusted-chain cases tells them exactly
 * what to fix — and points corporate-proxy users at NODE_EXTRA_CA_CERTS rather
 * than the blunt NODE_TLS_REJECT_UNAUTHORIZED=0. Every branch mentions
 * "certificate" so downstream renderers can key off it. Mirrors Claude's
 * per-code SSL guidance.
 */
export function getSSLErrorHint(message: string): string {
  const lower = (typeof message === 'string' ? message : '').toLowerCase();
  if (lower.includes('cert_has_expired') || (lower.includes('certificate') && lower.includes('expired'))) {
    return "SSL/TLS certificate has expired — the server's certificate is no longer valid. Verify your system clock and the provider's certificate.";
  }
  if (lower.includes('cert_not_yet_valid') || (lower.includes('certificate') && lower.includes('not yet valid'))) {
    return 'SSL/TLS certificate is not yet valid — check that your system clock is set correctly.';
  }
  if (lower.includes('cert_revoked') || (lower.includes('certificate') && lower.includes('revoked'))) {
    return 'SSL/TLS certificate has been revoked — do not bypass this; contact your provider.';
  }
  if (lower.includes('err_tls_cert_altname_invalid') || (lower.includes('certificate') && lower.includes('hostname'))) {
    return 'SSL/TLS certificate hostname mismatch — the certificate does not match the server name. Check your provider base URL.';
  }
  if (lower.includes('depth_zero_self_signed_cert') || lower.includes('self_signed_cert_in_chain') ||
      (lower.includes('certificate') && lower.includes('self-signed'))) {
    return 'Self-signed SSL/TLS certificate — if this is a corporate proxy, set NODE_EXTRA_CA_CERTS to your CA bundle (or NODE_TLS_REJECT_UNAUTHORIZED=0 only for a trusted self-signed cert).';
  }
  if (lower.includes('unable_to_get_issuer_cert') || lower.includes('unable_to_get_local_issuer_cert') ||
      lower.includes('unable_to_verify_leaf_signature') || lower.includes('unable to get local issuer certificate') ||
      lower.includes('unable to verify')) {
    return 'SSL/TLS certificate chain could not be verified — if you are behind a corporate proxy, set NODE_EXTRA_CA_CERTS to your CA bundle.';
  }
  if (lower.includes('cert_signature_failure')) {
    return "SSL/TLS certificate signature is invalid — the certificate failed signature verification against its issuer. Verify the server's certificate chain or your CA bundle (NODE_EXTRA_CA_CERTS).";
  }
  if (lower.includes('cert_revoked')) {
    return 'SSL/TLS certificate has been revoked — do not bypass this; contact your provider.';
  }
  if (lower.includes('cert_rejected') || lower.includes('cert_untrusted') ||
      (lower.includes('certificate') && (lower.includes('rejected') || lower.includes('untrusted')))) {
    return 'SSL/TLS certificate was rejected / is untrusted — add the issuing CA to your trust store (NODE_EXTRA_CA_CERTS) or check your provider base URL.';
  }
  if (lower.includes('cert_chain_too_long') || lower.includes('path_length_exceeded')) {
    return 'SSL/TLS certificate chain is too long (path-length constraint exceeded) — the server is presenting an invalid chain. Contact your provider or proxy administrator.';
  }
  if (lower.includes('hostname_mismatch')) {
    return 'SSL/TLS certificate hostname mismatch — the certificate does not match the server name. Check your provider base URL.';
  }
  if (lower.includes('err_tls_handshake_timeout')) {
    return 'SSL/TLS handshake timed out — the connection stalled during the TLS handshake. Check your proxy/gateway and network path, then retry.';
  }
  if (lower.includes('err_ssl_wrong_version_number')) {
    return 'SSL/TLS wrong version number — a plain-HTTP endpoint is being reached over HTTPS (or vice versa). Check that your provider base URL uses the correct scheme and port.';
  }
  if (lower.includes('err_ssl_decryption_failed_or_bad_record_mac')) {
    return 'SSL/TLS decryption failed (bad record MAC) — the TLS stream is corrupt, usually a mis-configured TLS-inspecting proxy. Check your proxy/gateway TLS settings.';
  }
  return FRIENDLY_MESSAGES.ssl_error!;
}

export function friendlyErrorMessage(classified: ClassifiedError): string {
  // OpenRouter ZDR-violation 404 masquerades as a generic error (often 'unknown'
  // or 'model_unavailable'); surface actionable guidance before the category
  // fallbacks so users learn it is an account data-policy setting, not a bad model.
  const zdr = classifyZdrError(classified.message);
  if (zdr.isZdr) return zdr.guidance!;
  if (classified.category === 'unknown' || classified.category === 'abort') {
    // A bare/empty error body (e.g. a generic API-error prefix with no detail)
    // is useless to the user — substitute an actionable fallback instead.
    if (classified.message.trim().length === 0) {
      return 'An unexpected error occurred. Please wait a moment and try again.';
    }
    return classified.message;
  }
  // A non-retryable model_unavailable is a 404 model-not-found (vs a retryable
  // 503/529 overload): tell the user how to recover instead of "retrying".
  if (classified.category === 'model_unavailable' && !classified.retryable) {
    return `Model not found or no access: ${classified.message}. Run /model to pick a different one.`;
  }
  if (classified.category === 'network_error') {
    return networkErrorHint(classified.message.toLowerCase());
  }
  // SSL/TLS: dispatch a per-code, actionable hint instead of one generic line.
  if (classified.category === 'ssl_error') {
    return getSSLErrorHint(classified.message);
  }
  const friendly = FRIENDLY_MESSAGES[classified.category];
  return friendly ?? classified.message;
}
