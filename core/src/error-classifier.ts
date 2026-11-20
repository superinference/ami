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
import { formatFileSize } from './utils/format';
import { API_PDF_MAX_PAGES, PDF_TARGET_RAW_SIZE } from './media-limits';

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
function* walkCauseChain(err: unknown, maxDepth = 5): Generator<object> {
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
 * Minify-safe abort/cancellation detection. An aborted fetch/stream surfaces as
 * a DOMException whose `name` is 'AbortError' (code 20) in Node's
 * fetch/AbortController path, but bundlers can rename concrete classes, so an
 * `instanceof SomeAbortClass` check is brittle. Union the stable signals: the
 * 'AbortError' name, a DOMException ABORT_ERR code, and a message fallback —
 * while never mistaking an ECONNABORTED network drop for a user cancellation.
 * Mirrors the reference's isAbortError. Exported so the engine/provider can share one
 * definition instead of ad-hoc `message.includes('abort')` checks.
 */
/**
 * Canonical OpenSSL/TLS failure-code set (used by both Node.js and Bun),
 * mirroring the reference's `SSL_ERROR_CODES` breadth (services/api/errorUtils.ts).
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

export function extractConnectionErrorDetails(err: unknown): ConnectionErrorDetails | null {
  for (const node of walkCauseChain(err)) {
    const code = (node as { code?: unknown }).code;
    if (typeof code === 'string' && code.length > 0) {
      const rawMsg = (node as { message?: unknown }).message;
      return {
        code,
        message: typeof rawMsg === 'string' ? rawMsg : '',
        isSSLError: SSL_ERROR_CODES.has(code),
      };
    }
  }
  return null;
}

/**
 * One-line connection-error diagnostic for the debug log:
 * `Connection error details: code=<code> (SSL error), message=<message>`.
 * Returns null when no structured `.code` is present (nothing useful to record).
 * Mirrors the reference's logging.ts connection-error diagnostic so a dropped connection
 * records its root errno + an explicit SSL flag — diagnosable with debug logging
 * on — instead of leaving only a code-less "Connection error.".
 */
export function connectionErrorDiagnostic(err: unknown): string | null {
  const details = extractConnectionErrorDetails(err);
  if (!details) return null;
  const sslLabel = details.isSSLError ? ' (SSL error)' : '';
  return `Connection error details: code=${details.code}${sslLabel}, message=${details.message}`;
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
function overflowTokenFields(raw: string): Pick<ClassifiedError, 'tokenGap' | 'actualTokens' | 'limitTokens'> {
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

/** True when a message is (or begins with) a rendered `API Error …` line. */
export function startsWithApiErrorPrefix(message: string): boolean {
  return typeof message === 'string' && message.startsWith(API_ERROR_MESSAGE_PREFIX);
}

/**
 * Strip a leading rendered `API Error: …` prefix so a message pulled back out of
 * the transcript re-classifies on its underlying text. No-op when the prefix is
 * absent. The single place that knows the prefix's punctuation shape.
 */
function stripApiErrorPrefix(message: string): string {
  if (!startsWithApiErrorPrefix(message)) return message;
  return message.slice(API_ERROR_MESSAGE_PREFIX.length).replace(/^[:\s-]+/, '');
}

/**
 * Message-level predicate: is this (raw or already-rendered) text a prompt-too-long
 * / context-overflow error? Recognizes the authoritative "… N tokens > M maximum"
 * form (via {@link parsePromptTooLongTokenCounts}) plus the common phrasings, so UI
 * and recovery code can re-detect an overflow error already sitting in history
 * without re-running the full {@link classifyError} taxonomy. Mirrors the reference's
 * isPromptTooLongMessage.
 */
export function isPromptTooLongMessage(message: string): boolean {
  if (typeof message !== 'string') return false;
  const { actualTokens, limitTokens } = parsePromptTooLongTokenCounts(message);
  if (actualTokens !== undefined && limitTokens !== undefined) return true;
  const lower = message.toLowerCase();
  return (
    lower.includes('prompt is too long') ||
    lower.includes('prompt_too_long') ||
    lower.includes('input is too long') ||
    (lower.includes('context') && (lower.includes('too long') || lower.includes('overflow')))
  );
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
 * the only recovery is to rewind the conversation. Mirrors the reference's reactive
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
// Model refusal / content-filter (AUP) — stop_reason 'refusal'
// ---------------------------------------------------------------------------

/** Usage-policy URL cited in a refusal notice. */
export const AUP_POLICY_URL = 'https://www.anthropic.com/legal/aup';

/**
 * Model this provider's refusals suggest switching to. A refusal is often
 * model-specific, so pointing the user at a known-good model is the fastest
 * recovery. Mirrors the reference's getErrorMessageIfRefusal model-switch affordance.
 */
export const REFUSAL_FALLBACK_MODEL = 'claude-sonnet-4-5';

/**
 * Build the actionable user-facing notice for a model refusal / content-filter
 * stop reason (the API returned `stop_reason: 'refusal'`, surfaced by the SDK as
 * `finishReason: 'content-filter'`). Explains it is an AUP/policy decision (not a
 * transient error), tells the user how to proceed, and — unless already on the
 * suggested model — names a `/model` switch to try. The wording adapts to
 * non-interactive (detached) sessions, which cannot "press esc to edit".
 * Mirrors the reference's `getErrorMessageIfRefusal`.
 */
export function getRefusalMessage(model: string, nonInteractive = false): string {
  const base = nonInteractive
    ? `The model refused this request, which appears to violate the provider's usage policy (${AUP_POLICY_URL}). Rephrase the request or try a different approach.`
    : `The model refused this request, which appears to violate the provider's usage policy (${AUP_POLICY_URL}). Edit your last message or start a new session to continue with a different task.`;
  const suggestion = typeof model === 'string' && model !== REFUSAL_FALLBACK_MODEL
    ? ` If you see this refusal repeatedly, run /model ${REFUSAL_FALLBACK_MODEL} to switch models.`
    : '';
  return base + suggestion;
}

// ---------------------------------------------------------------------------
// Per-model-family fallback suggestion — name a concrete model to switch to
// ---------------------------------------------------------------------------

/**
 * Suggest a concrete fallback model when the selected one is unavailable
 * (404 model-not-found), mapping each model family to its previous generation so
 * a deployment that lacks the newest model still has a working option. Returns
 * undefined when there is no sensible suggestion (the caller then falls back to
 * generic "/model to pick another"). Mirrors the reference's get3PModelFallbackSuggestion
 * family→previous-version chain.
 */
export function suggestFallbackModel(model: string): string | undefined {
  if (typeof model !== 'string') return undefined;
  const m = model.toLowerCase();
  if (m.includes('opus-4-6') || m.includes('opus_4_6')) return 'claude-opus-4-1';
  if (m.includes('opus-4-5') || m.includes('opus_4_5')) return 'claude-opus-4-1';
  if (m.includes('sonnet-4-6') || m.includes('sonnet_4_6')) return 'claude-sonnet-4-5';
  if (m.includes('sonnet-4-5') || m.includes('sonnet_4_5')) return 'claude-sonnet-4';
  return undefined;
}

// ---------------------------------------------------------------------------
// Media-size rejections — actionable PDF / image / request-too-large messages
// ---------------------------------------------------------------------------

/**
 * Is this raw API error text a media-size rejection (oversized/invalid PDF or
 * image, or a request that exceeds the raw payload ceiling)? Used both to decide
 * whether stripping media and retrying can help and to dispatch a per-variant
 * actionable message. Mirrors the reference's `isMediaSizeError`, broadened to the PDF
 * password/invalid variants that share the same recovery path. Case-insensitive.
 */
export function isMediaSizeError(raw: string): boolean {
  const r = typeof raw === 'string' ? raw.toLowerCase() : '';
  if (!r) return false;
  return (
    (r.includes('image exceeds') && r.includes('maximum')) ||
    r.includes('image dimensions exceed') ||
    /maximum of \d+ pdf pages/.test(r) ||
    r.includes('pdf is too large') || r.includes('pdf too large') ||
    r.includes('password protected') || r.includes('password-protected') ||
    (r.includes('pdf') && (r.includes('not valid') || r.includes('invalid') || r.includes('could not be parsed'))) ||
    r.includes('image was too large') || r.includes('image is too large') || r.includes('image too large') ||
    r.includes('request too large') || r.includes('request is too large') || r.includes('request exceeds')
  );
}

function pdfLimitsBlurb(): string {
  return `max ${API_PDF_MAX_PAGES} pages, ${formatFileSize(PDF_TARGET_RAW_SIZE)}`;
}

/** "PDF too large" guidance (pages + size ceiling), interactive/non-interactive aware. */
export function getPdfTooLargeErrorMessage(nonInteractive = false): string {
  const limits = pdfLimitsBlurb();
  return nonInteractive
    ? `PDF too large (${limits}). Try reading the file a different way (e.g. extract text with pdftotext).`
    : `PDF too large (${limits}). Edit your message and try again, or use pdftotext to convert it to text first.`;
}

/** Password-protected PDF guidance. */
export function getPdfPasswordProtectedErrorMessage(nonInteractive = false): string {
  return nonInteractive
    ? 'PDF is password protected. Use a CLI tool to extract or convert the PDF first.'
    : 'PDF is password protected. Edit your message and try again with an unprotected copy.';
}

/** Invalid/corrupt PDF guidance. */
export function getPdfInvalidErrorMessage(nonInteractive = false): string {
  return nonInteractive
    ? 'The PDF file was not valid. Convert it to text first (e.g. pdftotext).'
    : 'The PDF file was not valid. Edit your message and try again with a different file.';
}

/** Oversized image guidance. */
export function getImageTooLargeErrorMessage(nonInteractive = false): string {
  return nonInteractive
    ? 'Image was too large. Resize the image or use a different approach.'
    : 'Image was too large. Edit your message and try again with a smaller image.';
}

/** Oversized request-payload guidance (size ceiling), interactive/non-interactive aware. */
export function getRequestTooLargeErrorMessage(nonInteractive = false): string {
  const limits = `max ${formatFileSize(PDF_TARGET_RAW_SIZE)}`;
  return nonInteractive
    ? `Request too large (${limits}). Try again with a smaller file.`
    : `Request too large (${limits}). Edit your message and try again with a smaller file.`;
}

/**
 * Dispatch the correct actionable media-size message for a raw error, in
 * specificity order (password → invalid → PDF-too-large → image → request).
 * Returns undefined when the text is not a media-size rejection, so callers can
 * fall through. Centralizes the per-variant wording so no call site re-derives it.
 */
export function getMediaSizeErrorMessage(raw: string, nonInteractive = false): string | undefined {
  const r = typeof raw === 'string' ? raw.toLowerCase() : '';
  if (!isMediaSizeError(r)) return undefined;
  if (r.includes('password')) return getPdfPasswordProtectedErrorMessage(nonInteractive);
  if (r.includes('pdf') && (r.includes('not valid') || r.includes('invalid') || r.includes('could not be parsed'))) {
    return getPdfInvalidErrorMessage(nonInteractive);
  }
  if (/maximum of \d+ pdf pages/.test(r) || r.includes('pdf is too large') || r.includes('pdf too large')) {
    return getPdfTooLargeErrorMessage(nonInteractive);
  }
  if (r.includes('image')) return getImageTooLargeErrorMessage(nonInteractive);
  return getRequestTooLargeErrorMessage(nonInteractive);
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

  // AWS Bedrock credential-chain failure: the AWS SDK throws CredentialsProviderError
  // (by name) when no credentials resolve. Classify it as a non-retryable auth error
  // with Bedrock-specific guidance, rather than letting it surface as a raw 401/500.
  if (isAwsCredentialsProviderError(error)) {
    return { category: 'auth_error', message: getAwsCredentialsErrorMessage(), retryable: false, shouldCompact: false, shouldFallback: false };
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
        // token refresh + retry (the reference's also403Revoked path), so it is
        // retryable — unlike an ordinary permanent permission denial.
        if (lowerMsg.includes('oauth token has been revoked') || lowerMsg.includes('token has been revoked')) {
          return { category: 'auth_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: false };
        }
        // 403 is otherwise a permanent permission denial — never retryable.
        // Covers: invalid API key, unauthorized, no model access, project restrictions.
        return { category: 'auth_error', message: msg, retryable: false, shouldCompact: false, shouldFallback: false };
      case 404:
        // Model does not exist / no access. Non-retryable, but a different model
        // may work — surface actionable /model guidance (the reference's 404 branch).
        return { category: 'model_unavailable', message: msg, retryable: false, shouldCompact: false, shouldFallback: true };
      case 408:
        // Request timeout — transient, retry (the reference shouldRetry 408).
        return { category: 'network_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
      case 409:
        // Lock timeout / conflict — transient, retry (the reference shouldRetry 409).
        return { category: 'server_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 3000 };
      case 429:
        return { category: 'rate_limited', message: msg, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
      case 413:
        return { category: 'context_overflow', message: msg, retryable: true, shouldCompact: true, shouldFallback: false, ...overflowTokenFields(msg) };
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
          return { category: 'context_overflow', message: msg, retryable: true, shouldCompact: true, shouldFallback: false, ...overflowTokenFields(msg) };
        }
        return null; // fall through to regex
      case 500:
      case 502:
        return { category: 'server_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
      case 503:
      case 529:
        return { category: 'model_unavailable', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 10000, overloaded: true };
      default:
        // Generic status fallback: any other 5xx (504 gateway timeout, 507/508,
        // Cloudflare 520-527, 599, …) is a transient server/gateway failure —
        // retryable with fallback, mirroring the reference's `status >= 500` rule.
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
  // A message pulled back out of the transcript may still carry the rendered
  // `API Error: …` prefix — strip it for matching so an already-rendered error
  // re-classifies on its underlying text (the reference's startsWithApiErrorPrefix path).
  const matchText = stripApiErrorPrefix(errorString);
  // Fold the root `.cause` code (e.g. a nested ETIMEDOUT / CERT_HAS_EXPIRED that
  // the SDK hides under a generic "Connection error.") into the matched text, so
  // network/SSL classification works even when the top-level message has no code.
  const causeCode = error instanceof Error ? findErrorCode(error) : undefined;
  const lower = (causeCode ? `${matchText} ${causeCode}` : matchText).toLowerCase();

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
    isPromptTooLongMessage(lower) ||
    (lower.includes('context') && lower.includes('maximum')) ||
    /\bHTTP\s+413\b/i.test(errorString) ||
    (lower.includes('max_tokens') && !lower.includes('output') && !lower.includes('maximum number of output')) ||
    lower.includes('context length') ||
    lower.includes('token limit') ||
    lower.includes('too many tokens') ||
    lower.includes('reduce the length')
  ) {
    return { category: 'context_overflow', message: errorString, retryable: true, shouldCompact: true, shouldFallback: false, ...overflowTokenFields(errorString) };
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

  // Model unavailable / overloaded — retryable, should try fallback model. A
  // transient capacity overload (503/529/"overloaded"/"capacity") is flagged
  // `overloaded` so the engine retries the same model a few times before switching;
  // a genuine `model_not_found` is NOT an overload and still falls back immediately.
  const overloadSignal =
    /\bHTTP\s+503\b/i.test(errorString) ||
    /\bHTTP\s+529\b/i.test(errorString) ||
    lower.includes('overloaded') ||
    lower.includes('capacity');
  const modelNotFound =
    lower.includes('model_not_found') ||
    lower.includes('model not found');
  if (overloadSignal || modelNotFound) {
    return { category: 'model_unavailable', message: errorString, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 10000, ...(overloadSignal ? { overloaded: true } : {}) };
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

// ---------------------------------------------------------------------------
// Safety-classifier failure — fail-safe fallback for an async check that CANNOT
// produce a verdict (API error / timeout / transcript overflow)
// ---------------------------------------------------------------------------

/**
 * Structured outcome of a SAFETY/auto-mode classifier call that FAILED — i.e. an
 * async check (e.g. the auto-mode security classifier, see {@link ./auto-mode})
 * that could not produce an allow/deny verdict. Mirrors the failure-carrying
 * fields of the reference's `YoloClassifierResult` (`unavailable`,
 * `transcriptTooLong`, `durationMs`) and encodes the documented fail-safe
 * contract: a check that cannot decide must fall back to normal PROMPTING, never
 * retry a DETERMINISTIC failure, and never silently auto-approve or hard-deny.
 *
 * This is deliberately distinct from {@link ClassifiedError} (which classifies a
 * MAIN-model API error for the engine's retry/compact/fallback loop): the retry
 * semantics differ. For the main model a context overflow is retryable (compact
 * then re-send); for a CLASSIFIER call the SAME transcript overflows every time,
 * so {@link classifySafetyCheckFailure} forces `retryable=false` on a transcript
 * overflow — the single place that encodes that nuance.
 */
export interface SafetyClassifierFailure {
  /** Always true: the classifier produced no verdict (it is "unavailable"). */
  unavailable: true;
  /**
   * The failure is a DETERMINISTIC transcript/context overflow (the classifier
   * prompt exceeded the context window). Same input → same error, so callers must
   * fall back to prompting rather than retry. Parity: `transcriptTooLong`.
   */
  transcriptTooLong: boolean;
  /** Underlying {@link ErrorCategory} of the failure, for logging/telemetry. */
  category: ErrorCategory;
  /** Human-readable, HTML-sanitized reason (the classified message). */
  reason: string;
  /** Duration of the failed classifier call in ms, when the caller tracked it. */
  durationMs?: number;
  /**
   * Whether RETRYING the classifier call could plausibly succeed. True only for a
   * TRANSIENT failure (rate limit / network / 5xx). A deterministic transcript
   * overflow is always `false` even though {@link classifyError} marks
   * context_overflow retryable for the main model.
   */
  retryable: boolean;
}

/**
 * Classify a failed SAFETY/auto-mode classifier call into the fail-safe
 * {@link SafetyClassifierFailure} contract. Runs the shared {@link classifyError}
 * taxonomy, then applies the classifier-specific override: a transcript/context
 * overflow is DETERMINISTIC for a fixed classifier prompt, so it is reported as
 * `transcriptTooLong` and NON-retryable (retrying loops forever) — the opposite of
 * the main-model "compact then retry" path. `opts.durationMs` records call latency
 * for timeout/overhead telemetry (the reference's `durationMs`). The caller uses the
 * result to fall back to normal prompting; it never auto-approves a failed check.
 */
export function classifySafetyCheckFailure(
  error: string | Error | unknown,
  opts?: { durationMs?: number },
): SafetyClassifierFailure {
  const classified = classifyError(error);
  const transcriptTooLong = classified.category === 'context_overflow';
  // Deterministic overflow → never retry (override classifyError's retryable=true
  // for the main-model compaction path). Otherwise inherit transient retryability.
  const retryable = transcriptTooLong ? false : classified.retryable;
  const failure: SafetyClassifierFailure = {
    unavailable: true,
    transcriptTooLong,
    category: classified.category,
    reason: classified.message,
    retryable,
  };
  if (opts?.durationMs !== undefined) failure.durationMs = opts.durationMs;
  return failure;
}

// ---------------------------------------------------------------------------
// StopFailure error-class taxonomy — a matchable CLASS for the StopFailure hook
// ---------------------------------------------------------------------------

/**
 * The enumerated error classes a StopFailure hook is matched against, mirroring
 * the reference's StopFailure `error` matcher taxonomy (hooksConfigManager
 * `matcherMetadata.values`). A StopFailure fires when an API error ends the turn
 * (or a Stop hook itself errors) and a hook scopes on WHICH class of failure it
 * was, so the set MUST stay 1:1 with the reference values. This is the SINGLE
 * source: the hooks catalog's advertised `stopFailure` matcher values import it,
 * and {@link stopFailureErrorClass} maps any classified error onto it, so the
 * value advertised to `/hooks` / `--introspect` and the value actually emitted at
 * the StopFailure callsite can never drift.
 */
export const STOP_FAILURE_ERROR_CLASSES = [
  'rate_limit', 'authentication_failed', 'billing_error', 'invalid_request',
  'server_error', 'max_output_tokens', 'unknown',
] as const;

export type StopFailureErrorClass = (typeof STOP_FAILURE_ERROR_CLASSES)[number];

/**
 * Map every internal {@link ErrorCategory} onto its StopFailure taxonomy class.
 * Exhaustive `Record` on purpose: a newly added `ErrorCategory` must be assigned a
 * taxonomy class here (a compile error otherwise) rather than silently defaulting
 * at a callsite.
 */
const ERROR_CATEGORY_TO_STOP_FAILURE_CLASS: Record<ErrorCategory, StopFailureErrorClass> = {
  rate_limited: 'rate_limit',
  plan_limit: 'billing_error',
  auth_error: 'authentication_failed',
  context_overflow: 'invalid_request',
  output_too_large: 'max_output_tokens',
  content_filter: 'invalid_request',
  gateway_rejection: 'invalid_request',
  tool_pairing: 'invalid_request',
  model_unavailable: 'server_error',
  server_error: 'server_error',
  network_error: 'server_error',
  ssl_error: 'server_error',
  abort: 'unknown',
  repetition: 'unknown',
  unknown: 'unknown',
};

/**
 * Classify an error into the StopFailure matcher taxonomy so a StopFailure hook can
 * scope on the FAILURE CLASS (`rate_limit` / `authentication_failed` / …) instead
 * of the raw, unmatchable error text. Runs the full {@link classifyError} taxonomy
 * and folds its category onto the reference class set. Mirrors the reference's
 * StopFailure `error` discriminator so a matcher configured with one of the
 * advertised {@link STOP_FAILURE_ERROR_CLASSES} actually matches the emitted event.
 */
export function stopFailureErrorClass(error: string | Error | unknown): StopFailureErrorClass {
  return ERROR_CATEGORY_TO_STOP_FAILURE_CLASS[classifyError(error).category];
}

// ---------------------------------------------------------------------------
// Config / plugin load-failure categorization — actionable remediation hints
// ---------------------------------------------------------------------------

/**
 * The category a config/plugin LOAD failure (clone, read, or parse) falls into,
 * driving which remediation hint the user sees. Distinct from {@link ErrorCategory}
 * (which classifies live API errors): a load failure is a local filesystem / git /
 * JSON problem, not a provider response. Mirrors the network/permissions/config
 * triage the reference performs in `sessionStart.ts` when plugin-hook loading fails.
 */
export type LoadFailureCategory = 'network' | 'permissions' | 'config' | 'unknown';

export interface LoadFailureGuidance {
  category: LoadFailureCategory;
  /** One-line, actionable remediation hint for the categorized failure. */
  guidance: string;
}

/**
 * Categorize a config/plugin-bundle load failure and return an actionable
 * remediation hint. A failed clone / network errno (ETIMEDOUT/ENOTFOUND) →
 * `network`; a filesystem permission errno (EACCES/EPERM / "permission denied") →
 * `permissions`; a JSON/parse/schema/"invalid" failure → `config`; anything else →
 * `unknown` with generic guidance. Lets a load catch surface WHY it failed and HOW
 * to fix it instead of silently dropping the source. Mirrors the reference's
 * sessionStart plugin-hook-load categorized guidance.
 */
export function categorizeLoadFailure(error: unknown): LoadFailureGuidance {
  const raw = (error instanceof Error ? error.message : String(error ?? '')).toLowerCase();
  if (
    raw.includes('failed to clone') || raw.includes('network') ||
    raw.includes('etimedout') || raw.includes('enotfound') ||
    raw.includes('econnrefused') || raw.includes('econnreset')
  ) {
    return { category: 'network', guidance: 'This looks like a network issue — check your internet connection (and any proxy) and try again.' };
  }
  if (
    raw.includes('permission denied') || raw.includes('eacces') || raw.includes('eperm')
  ) {
    return { category: 'permissions', guidance: 'This looks like a permissions issue — check the file permissions on the plugins directory and your config files.' };
  }
  if (
    raw.includes('invalid') || raw.includes('parse') || raw.includes('json') ||
    raw.includes('schema') || raw.includes('unexpected token') || raw.includes('unexpected end')
  ) {
    return { category: 'config', guidance: 'This looks like a configuration issue — fix the malformed JSON (or schema) in the offending config file.' };
  }
  return { category: 'unknown', guidance: 'Fix the plugin/hook configuration or remove the problematic source from your settings.' };
}

// ---------------------------------------------------------------------------
// Subprocess stderr classification — recoverable reason + remediation steps
// ---------------------------------------------------------------------------

/**
 * A recognized, RECOVERABLE external-subprocess failure mode, derived from the
 * raw stderr a non-zero child process wrote. Distinct from {@link ErrorCategory}
 * (live API responses) and {@link LoadFailureCategory} (a thrown load Error → a
 * one-line hint): this inspects the stderr TEXT an external tool emitted and names
 * the specific actionable condition so a caller can both branch on a typed
 * discriminator AND show the user concrete remediation STEPS.
 *
 * Mirrors the reference's `verifyIt2Setup` (utils/swarm/backends/it2Setup.ts), which
 * on a non-zero exit inspects stderr for signatures ('api', 'python',
 * 'connection refused', 'not enabled') to distinguish ONE recoverable mode (the
 * Python API being disabled) and returns a typed `needsPythonApiEnabled`
 * discriminator paired with the `getPythonApiInstructions` remediation step list —
 * generalized here across the subprocess-launching call sites AMI already has (the
 * LSP server launch, external tool detection, git/clone paths) so a known
 * subprocess error becomes an actionable recovery path instead of an opaque
 * "crashed with exit code N".
 */
export type SubprocessErrorReason =
  | 'missing-dependency'
  | 'permission-denied'
  | 'api-not-enabled'
  | 'connection-refused'
  | 'out-of-memory';

/** Typed classification of a subprocess failure, returned by {@link classifySubprocessError}. */
export interface SubprocessErrorClassification {
  /** The recognized recoverable failure mode, or `null` when no known signature matched. */
  reason: SubprocessErrorReason | null;
  /** True iff a known recoverable signature matched (i.e. `reason !== null`). */
  recoverable: boolean;
  /**
   * Ordered, human-readable remediation steps for `reason` (the AMI analogue of
   * `getPythonApiInstructions`), or an empty array when the stderr matched no
   * known signature. Never contains secrets — derived only from stderr keywords.
   */
  remediation: string[];
}

/**
 * Ordered signature table: each known recoverable reason, the lowercased stderr
 * substrings that identify it, and its remediation step list. Checked top-to-bottom
 * so the FIRST matching (most specific) reason wins; keep more-specific signatures
 * above broader ones. Centralizing the table (vs scattering `stderr.includes(...)`
 * across every subprocess caller) is the whole point — one canonical place that the
 * LSP launch, tool detection, and git paths all consult.
 */
const SUBPROCESS_ERROR_SIGNATURES: ReadonlyArray<{
  reason: SubprocessErrorReason;
  signatures: readonly string[];
  remediation: readonly string[];
}> = [
  {
    reason: 'missing-dependency',
    signatures: [
      'modulenotfounderror', 'no module named', 'cannot find module', 'importerror',
      'command not found', 'is not recognized as an internal', 'executable not found',
    ],
    remediation: [
      'A required dependency is missing.',
      'Install the missing package/binary the tool reported, then make sure it is on your PATH.',
      'Re-run once it is installed.',
    ],
  },
  {
    reason: 'permission-denied',
    signatures: ['permission denied', 'eacces', 'operation not permitted', 'eperm', 'access is denied'],
    remediation: [
      'The tool was denied permission to run or to access a file.',
      'Check the executable and file permissions (and any sandbox/ACL), then grant the needed access.',
      'Re-run once permissions are fixed.',
    ],
  },
  {
    reason: 'api-not-enabled',
    signatures: ['not enabled', 'is disabled', 'feature is disabled', 'api is not', 'enable the api'],
    remediation: [
      'A required API or feature is disabled.',
      'Enable it in the tool\'s settings/preferences.',
      'Restart the tool after enabling, then re-run.',
    ],
  },
  {
    reason: 'connection-refused',
    signatures: ['connection refused', 'econnrefused', 'could not connect', 'failed to connect', 'econnreset', 'connection reset'],
    remediation: [
      'Could not reach the service the tool depends on.',
      'Make sure it is running and listening, and check the host/port and any firewall/proxy.',
      'Re-run once it is reachable.',
    ],
  },
  {
    reason: 'out-of-memory',
    signatures: ['out of memory', 'enomem', 'heap out of memory', 'cannot allocate memory', 'oom-killer'],
    remediation: [
      'The tool ran out of memory.',
      'Free up memory (close other processes) or raise the tool\'s memory limit.',
      'Re-run once more memory is available.',
    ],
  },
];

/** A subprocess failure the signature table did not recognize — reusable empty result. */
const UNRECOGNIZED_SUBPROCESS_ERROR: SubprocessErrorClassification = Object.freeze({
  reason: null,
  recoverable: false,
  remediation: [],
});

/**
 * Classify an external subprocess's raw stderr into a typed, recoverable reason
 * plus concrete remediation steps (or an unrecognized result when nothing matches).
 * The ONE canonical subprocess-stderr classifier — subprocess-launching call sites
 * (the LSP server launch, external tool detection, git/clone) feed it the stderr a
 * non-zero child wrote instead of each re-implementing `stderr.includes(...)`.
 *
 * Non-string / empty / whitespace input returns the unrecognized result without a
 * match (fail-safe: an unreadable stderr is never mis-reported as a known reason).
 */
export function classifySubprocessError(stderr: unknown): SubprocessErrorClassification {
  const text = (typeof stderr === 'string' ? stderr : '').toLowerCase();
  if (text.trim().length === 0) return UNRECOGNIZED_SUBPROCESS_ERROR;
  for (const entry of SUBPROCESS_ERROR_SIGNATURES) {
    if (entry.signatures.some((sig) => text.includes(sig))) {
      return { reason: entry.reason, recoverable: true, remediation: [...entry.remediation] };
    }
  }
  return UNRECOGNIZED_SUBPROCESS_ERROR;
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
 * "certificate" so downstream renderers can key off it. Mirrors the reference's
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

export function friendlyErrorMessage(
  classified: ClassifiedError,
  opts?: { model?: string; nonInteractive?: boolean },
): string {
  // OpenRouter ZDR-violation 404 masquerades as a generic error (often 'unknown'
  // or 'model_unavailable'); surface actionable guidance before the category
  // fallbacks so users learn it is an account data-policy setting, not a bad model.
  const zdr = classifyZdrError(classified.message);
  if (zdr.isZdr) return zdr.guidance!;
  // Media-size rejection (oversized/invalid/password-protected PDF, oversized
  // image, request-too-large): these classify as a generic 'unknown', so dispatch
  // the per-variant actionable message before the category fallbacks.
  const media = getMediaSizeErrorMessage(classified.message, opts?.nonInteractive);
  if (media) return media;
  if (classified.category === 'unknown' || classified.category === 'abort') {
    // A bare/empty error body (e.g. a generic API-error prefix with no detail)
    // is useless to the user — substitute an actionable fallback instead.
    if (classified.message.trim().length === 0) {
      return 'An unexpected error occurred. Please wait a moment and try again.';
    }
    return classified.message;
  }
  // AWS Bedrock credential-chain failure classifies as auth_error but needs its
  // own actionable guidance (the generic "check your API key" is wrong for the AWS
  // credential chain). The dedicated message carries the literal "AWS credentials",
  // so surface it verbatim instead of the generic auth fallback.
  if (classified.category === 'auth_error' && classified.message.includes('AWS credentials')) {
    return classified.message;
  }
  // A non-retryable model_unavailable is a 404 model-not-found (vs a retryable
  // 503/529 overload): name a concrete fallback model to switch to when the
  // family maps to a known previous generation, else generic /model guidance.
  if (classified.category === 'model_unavailable' && !classified.retryable) {
    const suggestion = opts?.model ? suggestFallbackModel(opts.model) : undefined;
    const switchHint = suggestion
      ? `Run /model ${suggestion} to switch, or /model to pick another.`
      : 'Run /model to pick a different one.';
    return `Model not found or no access: ${classified.message}. ${switchHint}`;
  }
  if (classified.category === 'network_error') {
    return networkErrorHint(classified.message.toLowerCase());
  }
  // SSL/TLS: dispatch a per-code, actionable hint instead of one generic line.
  if (classified.category === 'ssl_error') {
    return getSSLErrorHint(classified.message);
  }
  // Context overflow with server-reported counts: show the exact overflow
  // (actual > limit) so the user sees how far over they are, not just "too long".
  if (classified.category === 'context_overflow' &&
      classified.actualTokens !== undefined && classified.limitTokens !== undefined) {
    return `Context limit reached (${classified.actualTokens} > ${classified.limitTokens} tokens) — compacting conversation. Run /compact or /clear to continue.`;
  }
  // Rate limited on a model that has a lighter sibling (per-model limits are
  // common — a premium tier can be throttled while its previous generation is
  // not): name the concrete model to drop to so the user has a one-command
  // recovery instead of only waiting. Reuses the single `suggestFallbackModel`
  // family map. Mirrors the reference's model-tier-aware rate-limit fallback.
  if (classified.category === 'rate_limited') {
    const suggestion = opts?.model ? suggestFallbackModel(opts.model) : undefined;
    if (suggestion) {
      return `${FRIENDLY_MESSAGES.rate_limited} If the limit persists, run /model ${suggestion} to switch to a model with separate limits.`;
    }
  }
  const friendly = FRIENDLY_MESSAGES[classified.category];
  return friendly ?? classified.message;
}

// ---------------------------------------------------------------------------
// Auto-compaction failure wording
// ---------------------------------------------------------------------------

/**
 * Actionable user-facing wording when AUTOMATIC (background) compaction fails or
 * is interrupted. The manual `/compact` path already surfaces an error line, but
 * the auto path was previously opaque — a failed background summary was logged to
 * analytics and silently swallowed, so the user kept hitting the context wall with
 * no guidance. Classifies the underlying failure and returns copy that says what
 * happened AND what to do next (retry on a transient/network/abort failure; trim
 * context manually when the summary could not be produced or could not shrink the
 * conversation). Mirrors the reference's ERROR_MESSAGE_INCOMPLETE_RESPONSE
 * ("Compaction interrupted · This may be due to network issues — please try again")
 * and ERROR_MESSAGE_PROMPT_TOO_LONG guidance (services/compact/compact.ts),
 * adapted to AMI's own `/compact` and `/clear` recovery commands.
 */
export function compactionFailureMessage(error: string | Error | unknown): string {
  const classified = classifyError(error);
  switch (classified.category) {
    case 'abort':
      return 'Automatic compaction was interrupted — run /compact to trim the conversation manually, or continue (it will retry on the next turn).';
    case 'network_error':
    case 'ssl_error':
    case 'server_error':
    case 'gateway_rejection':
      return 'Automatic compaction interrupted · this may be due to network issues — please try again, or run /compact to trim the conversation manually.';
    case 'context_overflow':
    case 'output_too_large':
      return 'Automatic compaction could not shrink the conversation — run /compact to trim it manually, or /clear to start fresh.';
    case 'rate_limited':
    case 'plan_limit':
      return 'Automatic compaction was rate limited — it will retry shortly; run /compact to trim the conversation manually if it persists.';
    default:
      return 'Automatic compaction failed — run /compact to trim the conversation manually, or /clear to start fresh.';
  }
}

// ---------------------------------------------------------------------------
// Rate-limit simulation knob (dev/test)
// ---------------------------------------------------------------------------

/** Env var that enables synthetic rate-limit injection (see {@link parseMockRateLimit}). */
export const MOCK_RATE_LIMIT_ENV = 'AMI_MOCK_RATE_LIMIT';

/** Default Retry-After advertised on a synthetic rate-limit response, in milliseconds. */
export const DEFAULT_MOCK_RATE_LIMIT_RETRY_AFTER_MS = 2000;

/** Parsed configuration of the {@link MOCK_RATE_LIMIT_ENV} dev/test knob. */
export interface MockRateLimitConfig {
  /** Number of synthetic rate-limit responses to inject before the real request runs. */
  count: number;
  /** Retry-After value, in milliseconds, advertised on each synthetic response. */
  retryAfterMs: number;
  /** HTTP status stamped on the synthetic response. */
  status: 429;
}

/**
 * Parse the {@link MOCK_RATE_LIMIT_ENV} dev/test knob into a config, or null when
 * unset / disabled. This is AMI's provider-neutral equivalent of the reference's
 * `shouldProcessMockLimits()` / `/mock-limits` facility (RateLimitMessage.tsx): it
 * lets a developer or test deterministically drive the WHOLE rate-limit code path
 * (classification, the approaching/at-limit warning, Retry-After backoff, the retry
 * loop) WITHOUT provoking a real 429 from a live provider. Deliberately excludes the
 * vendor-specific upsell/subscription/billing surface from that reference file.
 *
 * Accepted forms (trimmed, case-insensitive):
 *   - unset / '' / '0' / 'false' / 'off' / 'no'  → null (disabled)
 *   - 'true' / 'on' / 'yes' / '1'                → one synthetic 429 (default delay)
 *   - 'N'   (positive integer)                   → N synthetic 429s (default delay)
 *   - 'N:MS' (count : retry-after in ms)         → N synthetic 429s, each Retry-After MS
 * Any other (non-numeric, non-boolean) token is treated as disabled — fail-safe, so a
 * typo never silently throttles a real session.
 */
export function parseMockRateLimit(raw: string | undefined): MockRateLimitConfig | null {
  if (!raw) return null;
  const trimmed = raw.trim().toLowerCase();
  if (trimmed === '' || trimmed === '0' || trimmed === 'false' || trimmed === 'off' || trimmed === 'no') {
    return null;
  }
  if (trimmed === 'true' || trimmed === 'on' || trimmed === 'yes') {
    return { count: 1, retryAfterMs: DEFAULT_MOCK_RATE_LIMIT_RETRY_AFTER_MS, status: 429 };
  }
  const [countPart, msPart] = trimmed.split(':');
  const count = Number.parseInt(countPart, 10);
  if (!Number.isFinite(count) || count <= 0) return null;
  let retryAfterMs = DEFAULT_MOCK_RATE_LIMIT_RETRY_AFTER_MS;
  if (msPart !== undefined) {
    const ms = Number.parseInt(msPart, 10);
    if (Number.isFinite(ms) && ms >= 0) retryAfterMs = ms;
  }
  return { count, retryAfterMs, status: 429 };
}
