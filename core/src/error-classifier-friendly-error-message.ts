// Split from error-classifier.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { classifyZdrError } from './openrouter-zdr';
import type { ClassifiedError, ErrorCategory } from './error-classifier-classified-error';
import { getMediaSizeErrorMessage, suggestFallbackModel } from './error-classifier-safety-classifier-failure';

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
export const ERROR_CATEGORY_TO_STOP_FAILURE_CLASS: Record<ErrorCategory, StopFailureErrorClass> = {
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
export const LOAD_FAILURE_CATEGORYS = ['network', 'permissions', 'config', 'unknown'] as const;

export type LoadFailureCategory = (typeof LOAD_FAILURE_CATEGORYS)[number];

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
export const SUBPROCESS_ERROR_REASONS = ['missing-dependency', 'permission-denied', 'api-not-enabled', 'connection-refused', 'out-of-memory'] as const;

export type SubprocessErrorReason = (typeof SUBPROCESS_ERROR_REASONS)[number];

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
export const SUBPROCESS_ERROR_SIGNATURES: ReadonlyArray<{
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

export const FRIENDLY_MESSAGES: Partial<Record<ErrorCategory, string>> = {
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
