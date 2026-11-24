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

import 'ai';
import './openrouter-zdr';
import './utils/format';
import './media-limits';
import { findErrorCode, isAbortError, normalizeErrorMessage, overflowTokenFields, walkCauseChain } from './error-classifier-classified-error';
import type { ClassifiedError, ConnectionErrorDetails } from './error-classifier-classified-error';
import { classifySDKError, isGatewayRejection, isPromptTooLongMessage, isToolPairingError, stripApiErrorPrefix } from './error-classifier-safety-classifier-failure';
import type { SafetyClassifierFailure } from './error-classifier-safety-classifier-failure';
import { ERROR_CATEGORY_TO_STOP_FAILURE_CLASS, SUBPROCESS_ERROR_SIGNATURES } from './error-classifier-friendly-error-message';
import type { StopFailureErrorClass, SubprocessErrorClassification } from './error-classifier-friendly-error-message';
export { API_ERROR_MESSAGE_PREFIX, AbortError, ERROR_CATEGORIES, apiErrorStatusFallback, detectPromptTooLong, extractNestedErrorMessage, extractUnknownErrorFormat, findErrorCode, getAwsCredentialsErrorMessage, getPromptTooLongTokenGap, isAbort, isAbortError, isAwsCredentialsProviderError, makeAbortError, normalizeErrorMessage, parseMaxTokensContextOverflow, parsePromptTooLongTokenCounts, sanitizeMessageHTML } from './error-classifier-classified-error';
export type { ClassifiedError, ConnectionErrorDetails, ErrorCategory } from './error-classifier-classified-error';
export { AUP_POLICY_URL, REFUSAL_FALLBACK_MODEL, getImageTooLargeErrorMessage, getMediaSizeErrorMessage, getPdfInvalidErrorMessage, getPdfPasswordProtectedErrorMessage, getPdfTooLargeErrorMessage, getRefusalMessage, getRequestTooLargeErrorMessage, isGatewayRejection, isMediaSizeError, isPromptTooLongMessage, isToolPairingError, startsWithApiErrorPrefix, suggestFallbackModel } from './error-classifier-safety-classifier-failure';
export type { SafetyClassifierFailure } from './error-classifier-safety-classifier-failure';
export { LOAD_FAILURE_CATEGORYS, STOP_FAILURE_ERROR_CLASSES, SUBPROCESS_ERROR_REASONS, categorizeLoadFailure, friendlyErrorMessage, getSSLErrorHint, networkErrorHint } from './error-classifier-friendly-error-message';
export type { LoadFailureCategory, LoadFailureGuidance, StopFailureErrorClass, SubprocessErrorClassification, SubprocessErrorReason } from './error-classifier-friendly-error-message';

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
