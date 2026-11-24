// Split from error-classifier.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/**
 * Walk an error's `.cause` chain (depth-limited, loop-guarded) and return the
 * first string `code` found. The Anthropic/undici SDK wraps the real
 * network/TLS error (ETIMEDOUT, CERT_HAS_EXPIRED, …) inside nested `.cause`
 * objects, so the top-level error is often a generic "Connection error." with no
 * `code` of its own — inspecting only the top level misses it. Mirrors the reference's
 * extractConnectionErrorDetails cause walk. `maxDepth` bounds the descent and a
 * visited set guards against self-referential / cyclic causes.
 */
/**
 * Shared, depth-limited, loop-guarded traversal of an error's `.cause` chain,
 * yielding each object node in order (self/outermost first). The SINGLE canonical
 * cause-walk — `findErrorCode` and `extractConnectionErrorDetails` both consume it
 * so there is no parallel traversal that can drift. `maxDepth` bounds the descent
 * and a visited set guards against self-referential / cyclic causes.
 */
export function* walkCauseChain(err: unknown, maxDepth = 5): Generator<object> {
  const seen = new Set<unknown>();
  let current: unknown = err;
  for (let depth = 0; depth <= maxDepth; depth++) {
    if (current === null || typeof current !== 'object' || seen.has(current)) break;
    seen.add(current);
    yield current as object;
    current = (current as { cause?: unknown }).cause;
  }
}

export function findErrorCode(err: unknown, maxDepth = 5): string | undefined {
  for (const node of walkCauseChain(err, maxDepth)) {
    const code = (node as { code?: unknown }).code;
    if (typeof code === 'string' && code.length > 0) return code;
  }
  return undefined;
}

/**
 * Structured root-cause details for a connection failure: the first string `.code`
 * found walking the error's `.cause` chain, the message carried at that same node,
 * and whether the code is a known OpenSSL/TLS failure. The undici/SDK wrappers
 * bury the real errno (ETIMEDOUT, CERT_HAS_EXPIRED, …) under nested `.cause`
 * beneath a generic code-less "Connection error.", so the structured view must
 * walk the chain. Built on the SAME `walkCauseChain` as `findErrorCode` (no
 * parallel traversal) and the SAME canonical `SSL_ERROR_CODES` set as the ssl
 * classification branch. Mirrors the reference's extractConnectionErrorDetails.
 */
export interface ConnectionErrorDetails {
  code: string;
  message: string;
  isSSLError: boolean;
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

/**
 * Dedicated typed cancellation error. A fetch/stream/subagent acquisition aborted
 * via an AbortSignal surfaces as an error whose stable `.name` is 'AbortError'
 * (matching Node's fetch path and DOMException), so both `instanceof AbortError`
 * and the minify-safe {@link isAbortError} / {@link isAbort} name checks recognize
 * it. Centralizes the ad-hoc `const e = new Error(...); e.name = 'AbortError'`
 * pattern that was duplicated across the MCP container pull and the subagent
 * limiter. Mirrors the reference's dedicated `AbortError` class so callers discriminate
 * user/host cancellation from a genuine failure by type, not by substring.
 */
export class AbortError extends Error {
  constructor(message = 'The operation was aborted') {
    super(message);
    this.name = 'AbortError';
  }
}

/**
 * Canonical factory for the abort rejection of a cancelled operation. When the
 * signal already carries an Error `reason` (an explicit `AbortController.abort(reason)`)
 * that reason is preserved; otherwise a typed {@link AbortError} with
 * `fallbackMessage` is returned. One factory so the container pull, subagent
 * acquisition, and any future cancellable op reject with the same recognizable
 * type instead of re-deriving `err.name = 'AbortError'` at each site.
 */
export function makeAbortError(signal?: { reason?: unknown } | null, fallbackMessage = 'The operation was aborted'): Error {
  const reason = signal?.reason;
  if (reason instanceof Error) return reason;
  return new AbortError(fallbackMessage);
}

/**
 * Robust cancellation check that distinguishes a user/host abort from an unrelated
 * failure. Prefers the authoritative signal (`signal.aborted`) when one is passed,
 * then falls back to the typed/name/DOMException detection in {@link isAbortError}
 * (which never treats an ECONNABORTED network drop as a cancellation). Supersedes
 * scattered `msg.includes('aborted')` checks at the provider/engine boundary.
 * Mirrors the reference's `isAbortError`, extended with the signal fast path.
 */
export function isAbort(err: unknown, signal?: { aborted?: boolean } | null): boolean {
  if (signal?.aborted) return true;
  return isAbortError(err);
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
   * True only for a transient capacity OVERLOAD (HTTP 503/529, "overloaded",
   * "capacity") — distinct from a genuine model-unavailable (e.g. `model_not_found`),
   * which is also `model_unavailable`/`shouldFallback` but is NOT an overload.
   * An overload usually clears on its own, so the engine retries the SAME model a
   * few times (config `max529RetriesBeforeFallback`, default 3) before switching to
   * a fallback model, instead of switching on the first 529. Parity: the reference's
   * MAX_529_RETRIES consecutive-529 threshold before FallbackTriggeredError.
   */
  overloaded?: boolean;
  /**
   * For context_overflow: how many tokens the prompt is over the limit, parsed
   * from the server's authoritative "… N tokens > M maximum" message. Lets
   * reactive compaction jump past multiple groups in one retry.
   */
  tokenGap?: number;
  /**
   * For context_overflow: the actual prompt token count the server reported
   * (the `N` in "N tokens > M maximum"), when parseable. Surfaced so callers and
   * the user-facing message can show the exact overflow, not just "too long".
   */
  actualTokens?: number;
  /**
   * For context_overflow: the server-reported token limit (the `M` in
   * "N tokens > M maximum"), when parseable.
   */
  limitTokens?: number;
}

/**
 * Parsed token fields (gap + actual/limit) for a context-overflow error, with
 * only the keys that could be parsed present. Shared by every context_overflow
 * classification branch so the structured counts stay consistent.
 *
 * Recognizes BOTH authoritative overflow wordings so magnitude is carried on the
 * ClassifiedError regardless of which one the API returns:
 *   1. "prompt is too long: N tokens > M maximum" (the INPUT alone overflows) —
 *      actual = N, limit = M.
 *   2. "input length and `max_tokens` exceed context limit: A + B > C" (input +
 *      requested max_tokens overflow) — actual = A + B, limit = C.
 * In both cases tokenGap is the positive (actual − limit), so the engine's
 * recovery loop can size compaction / a max_tokens reduction in one shot and the
 * user-facing message can show the exact overflow instead of a vague "too long".
 */
export function overflowTokenFields(raw: string): Pick<ClassifiedError, 'tokenGap' | 'actualTokens' | 'limitTokens'> {
  const fields: Pick<ClassifiedError, 'tokenGap' | 'actualTokens' | 'limitTokens'> = {};
  const { actualTokens, limitTokens } = parsePromptTooLongTokenCounts(raw);
  if (actualTokens !== undefined && limitTokens !== undefined) {
    const gap = getPromptTooLongTokenGap(raw);
    if (gap !== undefined) fields.tokenGap = gap;
    fields.actualTokens = actualTokens;
    fields.limitTokens = limitTokens;
    return fields;
  }
  // Fall back to the "input length and `max_tokens` exceed context limit" form,
  // where the overflowing total is the sum of the input and requested max_tokens.
  const maxTokens = parseMaxTokensContextOverflow(raw);
  if (maxTokens) {
    const actual = maxTokens.inputTokens + maxTokens.maxTokens;
    fields.actualTokens = actual;
    fields.limitTokens = maxTokens.contextLimit;
    const gap = actual - maxTokens.contextLimit;
    if (gap > 0) fields.tokenGap = gap;
  }
  return fields;
}

// ---------------------------------------------------------------------------
// Message normalization — HTML stripping + deserialized-error recovery
// ---------------------------------------------------------------------------

/**
 * Strip an HTML error-page body (e.g. a CloudFlare/proxy 5xx interstitial
 * returned instead of JSON) down to its <title> text, or '' when there is no
 * title. Plain text is returned unchanged. Mirrors the reference's sanitizeMessageHTML
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
 * a usable string. Mirrors the reference's extractNestedErrorMessage.
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
 * `[object Object]` and classifies as a silent `unknown`. Mirrors the reference's
 * extractUnknownErrorFormat.
 */
export function extractUnknownErrorFormat(value: unknown): string | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const type = (value as { Output?: { __type?: unknown } }).Output?.__type;
  return typeof type === 'string' && type.length > 0 ? type : undefined;
}

// ---------------------------------------------------------------------------
// AWS Bedrock credential-chain failure — recognize CredentialsProviderError
// ---------------------------------------------------------------------------

/**
 * True when an error is the AWS SDK's `CredentialsProviderError` (thrown when no
 * AWS credentials can be resolved from any provider in the chain). The AWS SDK
 * sets `error.name` to this stable string, so matching on the name is minify-safe
 * and does not require importing the SDK. Mirrors the reference's
 * `isAwsCredentialsProviderError`, which gives a clear "no AWS credentials
 * configured" branch instead of surfacing a raw 401/500 from the Bedrock call.
 */
export function isAwsCredentialsProviderError(err: unknown): boolean {
  if (err === null || typeof err !== 'object') return false;
  return (err as { name?: unknown }).name === 'CredentialsProviderError';
}

/**
 * Actionable guidance for a Bedrock run that has no resolvable AWS credentials.
 * Contains the literal "AWS credentials" so `friendlyErrorMessage` can key off it
 * and surface this instead of the generic API-key auth hint (which is wrong for
 * the Bedrock/AWS credential chain).
 */
export function getAwsCredentialsErrorMessage(): string {
  return 'No AWS credentials found — configure AWS credentials (set AWS_PROFILE, populate ~/.aws/credentials, or run `aws sso login`) before using the Bedrock provider.';
}

/**
 * Stable fallback message for a deserialized/plain-object API error that carries
 * no usable message — `API error (status N)` (or `status unknown`). Reads a
 * numeric `.status` or `.statusCode`. Mirrors the reference's `formatAPIError` guard so
 * a JSONL-round-tripped error (e.g. after `--resume`) never collapses to an empty
 * string or `[object Object]` that would mislead the user and the classifier.
 */
export function apiErrorStatusFallback(error: unknown): string {
  const raw = (error ?? {}) as { status?: unknown; statusCode?: unknown };
  const status = typeof raw.status === 'number' ? raw.status
    : typeof raw.statusCode === 'number' ? raw.statusCode
    : undefined;
  return `API error (status ${status ?? 'unknown'})`;
}

/**
 * Best human-readable message for any error value: a plain string, an Error's
 * `.message`, or a nested/deserialized API-error shape — always HTML-sanitized.
 * When no usable message survives, falls back to `API error (status N)` rather
 * than an empty string / `[object Object]`.
 */
export function normalizeErrorMessage(error: unknown): string {
  if (typeof error === 'string') return sanitizeMessageHTML(error);
  if (error instanceof Error) {
    const nested = extractNestedErrorMessage(error);
    if (nested) return nested;
    const sanitized = sanitizeMessageHTML(error.message);
    if (sanitized.length > 0) return sanitized;
    // Deserialized SDK error that lost its `.message` (e.g. JSONL `--resume`):
    // surface a stable status fallback instead of an empty string.
    return apiErrorStatusFallback(error);
  }
  const nested = extractNestedErrorMessage(error);
  if (nested) return nested;
  // A non-standard provider envelope (e.g. Bedrock Output.__type) carries its
  // type here when there is no message — fold it in so classification keywords
  // can match instead of falling to a meaningless String(error).
  const envelope = extractUnknownErrorFormat(error);
  if (envelope) return envelope;
  if (error !== null && typeof error === 'object') {
    const direct = (error as { message?: unknown }).message;
    if (typeof direct === 'string' && direct.length > 0) return sanitizeMessageHTML(direct);
    // A plain-object API error (deserialized, no message/nested/envelope) still
    // carries a status — surface `API error (status N)` rather than
    // `[object Object]`. Mirrors the reference's formatAPIError status fallback.
    return apiErrorStatusFallback(error);
  }
  return String(error ?? '');
}

// ---------------------------------------------------------------------------
// Token-count parsing — precise overflow recovery
// ---------------------------------------------------------------------------

/**
 * Parse actual/limit token counts from a raw "prompt is too long: 137500 tokens
 * > 135000 maximum" error. Lenient: tolerant of SDK prefixes, JSON envelopes,
 * and Vertex casing. Mirrors the reference's parsePromptTooLongTokenCounts.
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
 * > C" message into its three counts, or null. Mirrors the reference's
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

/**
 * Detect a DETERMINISTIC 400 "prompt is too long: N tokens > M maximum" error and
 * return its parsed token counts, or undefined for any other error. This is the
 * narrow do-not-retry variant: the INPUT (prompt) itself exceeds the context
 * window, so — unlike the recoverable "input length and `max_tokens` exceed context
 * limit" 400 (see {@link parseMaxTokensContextOverflow}, fixed by shrinking
 * max_tokens) or a transient 429/5xx — re-sending the SAME request loops forever.
 * Accepts an Error (its `.message`) or a raw string. Keyed on the literal
 * "prompt is too long" phrase so it never swallows the broader context-overflow
 * phrasings {@link isPromptTooLongMessage} recognizes. Mirrors the reference's
 * `detectPromptTooLong` (utils/permissions/yoloClassifier.ts) — a distinct outcome
 * so a caller can stop retrying a deterministic failure instead of hammering it.
 */
export function detectPromptTooLong(
  error: string | Error | unknown,
): { actualTokens: number | undefined; limitTokens: number | undefined } | undefined {
  const message = error instanceof Error ? error.message : typeof error === 'string' ? error : undefined;
  if (typeof message !== 'string') return undefined;
  if (!message.toLowerCase().includes('prompt is too long')) return undefined;
  return parsePromptTooLongTokenCounts(message);
}

// ---------------------------------------------------------------------------
// Message-level predicates — re-detect an already-rendered error by its text
// ---------------------------------------------------------------------------

/**
 * Stable prefix prepended when a classified API error is rendered into the
 * transcript. Kept constant on purpose so downstream UI / recovery code can
 * recognize an error line already in history via {@link startsWithApiErrorPrefix}
 * and re-classify its underlying text. Mirrors the reference's API_ERROR_MESSAGE_PREFIX.
 */
export const API_ERROR_MESSAGE_PREFIX = 'API Error';
