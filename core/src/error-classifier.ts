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

export type ErrorCategory =
  | 'rate_limited'
  | 'context_overflow'
  | 'model_unavailable'
  | 'auth_error'
  | 'output_too_large'
  | 'content_filter'
  | 'server_error'
  | 'network_error'
  | 'abort'
  | 'unknown';

export interface ClassifiedError {
  category: ErrorCategory;
  message: string;
  retryable: boolean;
  shouldCompact: boolean;
  shouldFallback: boolean;
  suggestedDelay?: number;
}

/**
 * Classify an error string into a structured recovery recommendation.
 *
 * The checks are priority-ordered: more specific patterns (abort, auth)
 * are tested before broad ones (server_error, unknown).
 */
export function classifyError(error: string): ClassifiedError {
  const lower = error.toLowerCase();

  // Abort — not retryable, checked first so it short-circuits
  if (lower.includes('abort') || lower.includes('aborterror') || lower.includes('cancel')) {
    return { category: 'abort', message: error, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Auth — not retryable, no point falling back with bad credentials
  if (
    /\bHTTP\s+401\b/i.test(error) ||
    /\bHTTP\s+403\b/i.test(error) ||
    lower.includes('api key') ||
    lower.includes('unauthorized') ||
    lower.includes('forbidden') ||
    lower.includes('invalid_api_key') ||
    lower.includes('authentication')
  ) {
    return { category: 'auth_error', message: error, retryable: false, shouldCompact: false, shouldFallback: false };
  }

  // Rate limit — retryable after a delay
  if (
    /\bHTTP\s+429\b/i.test(error) ||
    lower.includes('rate limit') ||
    lower.includes('rate_limit') ||
    lower.includes('quota') ||
    lower.includes('too many requests') ||
    lower.includes('throttled')
  ) {
    return { category: 'rate_limited', message: error, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
  }

  // Context overflow — retryable after compaction
  if (
    (lower.includes('context') && (lower.includes('too long') || lower.includes('overflow') || lower.includes('maximum'))) ||
    /\bHTTP\s+413\b/i.test(error) ||
    lower.includes('prompt_too_long') ||
    lower.includes('prompt is too long') ||
    lower.includes('max_tokens') ||
    lower.includes('context length') ||
    lower.includes('token limit') ||
    lower.includes('too many tokens') ||
    lower.includes('reduce the length')
  ) {
    return { category: 'context_overflow', message: error, retryable: true, shouldCompact: true, shouldFallback: false };
  }

  // Output too large — retryable (the engine can request fewer output tokens)
  if (
    lower.includes('output too large') ||
    lower.includes('output is too') ||
    lower.includes('max_output') ||
    lower.includes('output_too_large')
  ) {
    return { category: 'output_too_large', message: error, retryable: true, shouldCompact: false, shouldFallback: false };
  }

  // Content filter — retryable, the model can rephrase to avoid the trigger
  if (
    lower.includes('content_filter') ||
    lower.includes('content_management') ||
    lower.includes('content policy')
  ) {
    return { category: 'content_filter', message: error, retryable: true, shouldCompact: false, shouldFallback: false };
  }

  // Model unavailable / overloaded — retryable, should try fallback model
  if (
    /\bHTTP\s+503\b/i.test(error) ||
    /\bHTTP\s+529\b/i.test(error) ||
    lower.includes('overloaded') ||
    lower.includes('capacity') ||
    lower.includes('model_not_found') ||
    lower.includes('model not found')
  ) {
    return { category: 'model_unavailable', message: error, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 10000 };
  }

  // Server error — retryable, may benefit from fallback
  // Use word boundary matching for status codes to avoid false positives (port numbers, token counts)
  if (/\bHTTP\s+500\b|\b500\s+(internal|server)/i.test(error) || /\bHTTP\s+502\b/i.test(error) || lower.includes('internal server error')) {
    return { category: 'server_error', message: error, retryable: true, shouldCompact: false, shouldFallback: true, suggestedDelay: 3000 };
  }

  // Network error — retryable after a delay
  if (
    lower.includes('econnrefused') ||
    lower.includes('etimedout') ||
    lower.includes('econnreset') ||
    lower.includes('network') ||
    lower.includes('fetch failed') ||
    lower.includes('dns')
  ) {
    return { category: 'network_error', message: error, retryable: true, shouldCompact: false, shouldFallback: false, suggestedDelay: 5000 };
  }

  // Unknown — not retryable by default; the engine can decide
  return { category: 'unknown', message: error, retryable: false, shouldCompact: false, shouldFallback: false };
}
