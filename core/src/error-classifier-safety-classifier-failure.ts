// Split from error-classifier.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { APICallError, LoadAPIKeyError, NoSuchModelError, InvalidPromptError, NoContentGeneratedError, EmptyResponseBodyError } from 'ai';
import { formatFileSize } from './utils/format';
import { API_PDF_MAX_PAGES, PDF_TARGET_RAW_SIZE } from './media-limits';
import { API_ERROR_MESSAGE_PREFIX, getAwsCredentialsErrorMessage, isAbortError, isAwsCredentialsProviderError, overflowTokenFields, parsePromptTooLongTokenCounts } from './error-classifier-classified-error';
import type { ClassifiedError, ErrorCategory } from './error-classifier-classified-error';

/** True when a message is (or begins with) a rendered `API Error …` line. */
export function startsWithApiErrorPrefix(message: string): boolean {
  return typeof message === 'string' && message.startsWith(API_ERROR_MESSAGE_PREFIX);
}

/**
 * Strip a leading rendered `API Error: …` prefix so a message pulled back out of
 * the transcript re-classifies on its underlying text. No-op when the prefix is
 * absent. The single place that knows the prefix's punctuation shape.
 */
export function stripApiErrorPrefix(message: string): string {
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

export function pdfLimitsBlurb(): string {
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

export function classifySDKError(error: Error): ClassifiedError | null {
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
