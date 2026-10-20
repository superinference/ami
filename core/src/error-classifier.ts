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

export type ErrorCategory =
  | 'rate_limited'
  | 'plan_limit'
  | 'context_overflow'
  | 'model_unavailable'
  | 'auth_error'
  | 'output_too_large'
  | 'content_filter'
  | 'server_error'
  | 'network_error'
  | 'ssl_error'
  | 'abort'
  | 'repetition'
  | 'unknown';

export interface ClassifiedError {
  category: ErrorCategory;
  message: string;
  retryable: boolean;
  shouldCompact: boolean;
  shouldFallback: boolean;
  suggestedDelay?: number;
}

// ---------------------------------------------------------------------------
// SDK typed-error fast path — classify by error class + status code
// ---------------------------------------------------------------------------

function classifySDKError(error: Error): ClassifiedError | null {
  const message = error.message;

  // Abort/cancel — check early before SDK type checks, since an abort
  // may wrap an SDK error
  if (error.name === 'AbortError' || message.includes('abort') || message.includes('cancel')) {
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
        // 403 with auth keywords is non-retryable; otherwise transient
        if (lowerMsg.includes('api key') || lowerMsg.includes('unauthorized') ||
            lowerMsg.includes('invalid_api_key') || lowerMsg.includes('authentication')) {
          return { category: 'auth_error', message: msg, retryable: false, shouldCompact: false, shouldFallback: false };
        }
        return { category: 'auth_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 5000 };
      case 429:
        return { category: 'rate_limited', message: msg, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
      case 413:
        return { category: 'context_overflow', message: msg, retryable: true, shouldCompact: true, shouldFallback: false };
      case 400:
        // 400 with context/token-related message → context_overflow
        if (lowerMsg.includes('context') || lowerMsg.includes('token') ||
            lowerMsg.includes('prompt_too_long') || lowerMsg.includes('too long') ||
            lowerMsg.includes('reduce the length')) {
          return { category: 'context_overflow', message: msg, retryable: true, shouldCompact: true, shouldFallback: false };
        }
        return null; // fall through to regex
      case 500:
      case 502:
        return { category: 'server_error', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
      case 503:
      case 529:
        return { category: 'model_unavailable', message: msg, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 10000 };
      default:
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
export function classifyError(error: string | Error): ClassifiedError {
  // SDK fast path: classify by error type + status code
  if (error instanceof Error) {
    const sdkResult = classifySDKError(error);
    if (sdkResult) return sdkResult;
    // Fall through to regex matching on the message string
  }

  const errorString = error instanceof Error ? error.message : error;
  const lower = errorString.toLowerCase();

  // Abort — not retryable, checked first so it short-circuits.
  // Exclude ECONNABORTED which is a network error, not a user-initiated abort.
  if (
    (lower.includes('abort') && !lower.includes('econnaborted')) ||
    lower.includes('aborterror') ||
    lower.includes('cancel')
  ) {
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

  // 403 — often transient (quota enforcement, intermittent key restrictions).
  // Retry with delay and allow fallback to another provider.
  if (/\bHTTP\s+403\b/i.test(errorString) || lower.includes('forbidden')) {
    return { category: 'auth_error', message: errorString, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 5000 };
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

  // Rate limit — retryable after a delay
  if (
    /\bHTTP\s+429\b/i.test(errorString) ||
    lower.includes('rate limit') ||
    lower.includes('rate_limit') ||
    lower.includes('quota') ||
    lower.includes('too many requests') ||
    lower.includes('throttled')
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
    return { category: 'context_overflow', message: errorString, retryable: true, shouldCompact: true, shouldFallback: false };
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

  // Server error — retryable, may benefit from fallback
  // Use word boundary matching for status codes to avoid false positives (port numbers, token counts)
  if (/\bHTTP\s+500\b|\b500\s+(internal|server)/i.test(errorString) || /\bHTTP\s+502\b/i.test(errorString) || lower.includes('internal server error')) {
    return { category: 'server_error', message: errorString, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
  }

  // SSL/TLS certificate error — not retryable (configuration issue, not transient)
  if (
    lower.includes('unable_to_verify_leaf_signature') ||
    lower.includes('depth_zero_self_signed_cert') ||
    lower.includes('self_signed_cert_in_chain') ||
    lower.includes('cert_has_expired') ||
    lower.includes('err_tls_cert_altname_invalid') ||
    lower.includes('certificate') && (lower.includes('expired') || lower.includes('invalid') || lower.includes('self-signed') || lower.includes('verify'))
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
  context_overflow: 'Response context too large — compacting conversation.',
  model_unavailable: 'Model temporarily unavailable — retrying.',
  auth_error: 'Authentication error — check your API key.',
  output_too_large: 'Model output exceeded size limit — retrying with smaller output.',
  content_filter: 'Response blocked by content policy.',
  ssl_error: 'SSL/TLS certificate error — check your provider URL or set NODE_TLS_REJECT_UNAUTHORIZED=0 for self-signed certs.',
  server_error: 'Provider server error — retrying. Check provider status page if this persists.',
  network_error: 'Network error — retrying connection.',
  repetition: 'Repetitive output detected.',
};

export function friendlyErrorMessage(classified: ClassifiedError): string {
  if (classified.category === 'unknown' || classified.category === 'abort') {
    return classified.message;
  }
  const friendly = FRIENDLY_MESSAGES[classified.category];
  return friendly ?? classified.message;
}
