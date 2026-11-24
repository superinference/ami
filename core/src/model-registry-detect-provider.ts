// Split from model-registry.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { ProviderConfig } from './types';
import { MODEL_PREFERENCE } from './model-preference';
import { redactSecrets } from './tools/tool-utils';
import { validateBoundedIntEnvVar } from './config';

/**
 * Strip header-injection bytes (CR, LF, NUL) from a value before it is placed in
 * an outbound HTTP request header. A secret or base URL that carries a stray
 * `\r\n` (e.g. a mis-pasted `OPENAI_API_KEY="sk-...\r\nX-Evil: 1"`) would
 * otherwise let an attacker smuggle an extra header line into the model-list
 * request. Mirrors the reference's sanitizeHeaderValue (utils/hooks/execHttpHook.ts):
 * the single canonical header-value cleaner for every auth header this module
 * builds (Authorization / x-goog-api-key / x-api-key). Returns '' for a nullish
 * value so an absent key never becomes the literal string "undefined".
 */
export function sanitizeHeaderValue(value: string | undefined): string {
  if (typeof value !== 'string') return '';
  return value.replace(/[\r\n\x00]/g, '');
}

/**
 * Pull a human-readable message out of an API error-response body: checks
 * `data.message` first, then `data.error.message`. Mirrors the reference's
 * extractErrorDetail (bridge/debugUtils.ts) so an HTTP failure can surface the
 * server's own reason (e.g. "model X not found, available: ...") instead of a
 * bare status code. The single canonical body-detail extractor for every reject
 * path in this module.
 */
export function extractErrorDetail(data: unknown): string | undefined {
  if (!data || typeof data !== 'object') return undefined;
  const d = data as Record<string, unknown>;
  if (typeof d.message === 'string' && d.message.trim() !== '') return d.message;
  const err = d.error;
  if (err && typeof err === 'object') {
    const m = (err as Record<string, unknown>).message;
    if (typeof m === 'string' && m.trim() !== '') return m;
  }
  return undefined;
}

// Default 10s; overridable per-call via env for tests and constrained
// environments (mirrors SUPERINFERENCE_API_TIMEOUT_MS / HF_ENDPOINT seams).
// Read at call time (not module load) so an override takes effect post-import.
export function modelListTimeoutMs(): number {
  // Bounded: reject NaN/non-positive (→ 10s default) and cap at 10 minutes so a
  // mistaken/hostile SI_MODEL_LIST_TIMEOUT_MS cannot hang model discovery forever.
  return validateBoundedIntEnvVar('SI_MODEL_LIST_TIMEOUT_MS', process.env.SI_MODEL_LIST_TIMEOUT_MS, {
    default: 10000,
    max: 600000,
  }).effective;
}

export interface ModelInfo {
  id: string;
  name?: string;
  owned_by?: string;
}

/**
 * Abort error for a cancelled model-list / connectivity request. Carries a
 * recognizable `code` ('ABORT_ERR') so {@link isTransientModelListError} never
 * treats a deliberate cancellation as a transient (retryable) failure — a user
 * who aborts `/model` listing must not trigger the retry loop.
 */
export function makeAbortError(): Error {
  const e = new Error('Aborted') as Error & { code?: string };
  e.code = 'ABORT_ERR';
  return e;
}

// ---------------------------------------------------------------------------
// Transient-failure retry for model-list discovery — a small bounded retry loop
// around httpGetJSON so a momentary rate-limit (HTTP 429), a provider restart
// (HTTP 503), or a dropped/half-open socket (ECONNRESET / ETIMEDOUT) does not
// make listModels fall straight through to the static fallback list on the first
// blip. Mirrors the reference's withRetry (services/api/withRetry.ts): retry only
// genuinely transient conditions, honor a server Retry-After when present, and
// otherwise back off exponentially with equal jitter. Scoped to the discovery
// fetch path (listOpenAIModels / listGeminiModels); the point-in-time
// connectivity probe deliberately does NOT retry so its latency reading and
// reachable/unreachable verdict stay single-shot.
// ---------------------------------------------------------------------------

/** Up-to-N attempts total (1 initial + N-1 retries) for a model-list fetch. */
export const MODEL_LIST_MAX_ATTEMPTS = 3;

/** Base blind-backoff delay; doubled per attempt before jitter. */
export const MODEL_LIST_BASE_DELAY_MS = 300;

/**
 * True when a model-list fetch error is worth another attempt: a transient
 * rate-limit/unavailable status (429/503) or a transient socket error
 * (ECONNRESET / ETIMEDOUT). A 4xx other than 429 (bad key, wrong path) and a
 * malformed-JSON body are NOT retried — they will not fix themselves.
 */
export function isTransientModelListError(err: unknown): boolean {
  const status = (err as { statusCode?: unknown })?.statusCode;
  if (status === 429 || status === 503) return true;
  const code = (err as { code?: unknown })?.code;
  return code === 'ECONNRESET' || code === 'ETIMEDOUT';
}

export function detectProvider(config: ProviderConfig): string {
  if (config.provider) return config.provider;

  const key = config.apiKey || '';
  const url = config.baseUrl || '';

  // Explicit cloud-routing opt-in toggle for Bedrock (parity with the reference's
  // getAPIProvider, utils/model/providers.ts:L6-L14): route to the managed cloud
  // provider purely from the env toggle, independent of whether an AWS credential
  // happens to be exported — so a user who runs via Bedrock is never mis-detected
  // as a first-party/OpenAI endpoint by credential sniffing. Honors both the
  // Claude-compatible var and AMI's own convention (mirrors the vertex toggle below
  // and the cloud-provider list in common/engine-factory's preconnect skip).
  if (process.env.CLAUDE_CODE_USE_BEDROCK || process.env.SUPERINFERENCE_USE_BEDROCK) return 'amazon-bedrock';

  // Env-based detection for vertex
  if (process.env.ANTHROPIC_VERTEX_PROJECT_ID || process.env.CLAUDE_CODE_USE_VERTEX) return 'anthropic-vertex';

  // Explicit opt-in toggle for the Azure/Microsoft Foundry route (parity with the reference
  // Code's getAPIProvider CLAUDE_CODE_USE_FOUNDRY, utils/model/providers.ts:L11-L12).
  // AMI represents Foundry as the `azure-openai` provider — model-capabilities'
  // getApiProviderClass maps azure-openai → the 'foundry' provider class. engine-factory
  // already recognizes these vars for the API-preconnect skip; routing here makes the
  // toggle actually SELECT the provider instead of only disabling preconnect, closing
  // the inconsistency. Honors both the Claude-compatible var and AMI's own convention
  // (mirrors the bedrock/vertex toggles above).
  if (process.env.CLAUDE_CODE_USE_FOUNDRY || process.env.SUPERINFERENCE_USE_FOUNDRY) return 'azure-openai';

  // API key patterns
  if (key.startsWith('sk-ant-')) return 'anthropic';
  if (key.startsWith('AIza')) return 'google';
  if (key.startsWith('sk-or-')) return 'openrouter';
  if (key.startsWith('gsk_')) return 'groq';
  if (key.startsWith('xai-')) return 'xai';
  if (key.startsWith('pplx-')) return 'perplexity';

  // URL patterns
  if (url.includes('generativelanguage.googleapis.com')) return 'google';
  if (url.includes('aiplatform.googleapis.com')) return 'google-vertex';
  if (url.includes('api.anthropic.com')) return 'anthropic';
  if (url.includes('openrouter.ai')) return 'openrouter';
  if (url.includes('api.groq.com')) return 'groq';
  if (url.includes('api.mistral.ai')) return 'mistral';
  if (url.includes('api.x.ai')) return 'xai';
  if (url.includes('api.deepseek.com')) return 'deepseek';
  if (url.includes('api.together.xyz')) return 'togetherai';
  if (url.includes('localhost') || url.includes('127.0.0.1')) return 'ollama';
  if (url.includes('api.openai.com')) return 'openai';

  // Default
  if (key.startsWith('sk-')) return 'openai';
  return 'openai';
}

export function listProviders(): Array<{ id: string; models: string[] }> {
  return Object.entries(MODEL_PREFERENCE).map(([id, models]) => ({ id, models }));
}

/**
 * Built-in static fallback model list for a provider (from MODEL_PREFERENCE), as
 * ModelInfo rows. Used when a live listing is unavailable OR suppressed by the
 * essential-traffic-only privacy opt-out. The single source of the offline list.
 */
export function staticModelList(provider: string): ModelInfo[] {
  return (MODEL_PREFERENCE[provider] || []).map(id => ({ id, name: id }));
}

/**
 * Human-readable env-var hint for a provider's credential, used in empty-state
 * diagnostics. Falls back to a generic phrase for providers without a single
 * well-known key var.
 */
export function providerKeyHint(provider: string): string {
  switch (provider) {
    case 'openai': return 'OPENAI_API_KEY';
    case 'google': return 'GOOGLE_API_KEY';
    case 'openrouter': return 'OPENROUTER_API_KEY';
    case 'ollama': return 'AI_BASE_URL (Ollama needs no API key)';
    default: return `the ${provider} API key environment variable`;
  }
}

/**
 * Classify a model-discovery failure into a specific, actionable diagnostic.
 *
 * Distinguishes rejected credentials (401/403), a wrong base URL (404), an
 * unreachable endpoint (timeout / connection refused / DNS), and missing
 * configuration. Any secret that leaked into the underlying error message is
 * scrubbed via redactSecrets so keys never surface in the UI.
 */
export function diagnoseModelDiscovery(err: unknown, provider: string): string {
  const status = typeof (err as { statusCode?: unknown })?.statusCode === 'number'
    ? (err as { statusCode: number }).statusCode
    : undefined;
  const code = typeof (err as { code?: unknown })?.code === 'string'
    ? (err as { code: string }).code
    : undefined;
  const message = err instanceof Error ? err.message : String(err ?? '');

  // The server's own error-body message (if any), scrubbed of secrets, so the
  // user sees the concrete reason (e.g. "model X not found, available: ...")
  // appended to the status-code line instead of only a bare HTTP code.
  const rawDetail = typeof (err as { detail?: unknown })?.detail === 'string'
    ? (err as { detail: string }).detail
    : undefined;
  const detail = rawDetail ? redactSecrets(rawDetail) : undefined;
  const suffix = detail ? ` — ${detail}` : '';

  if (status === 401 || status === 403) {
    return `Authentication failed (HTTP ${status}): ${provider} rejected the API key. Check ${providerKeyHint(provider)}.${suffix}`;
  }
  if (status === 404) {
    return `Model list endpoint not found (HTTP 404) for ${provider}. Check the base URL includes the API path (e.g. ".../v1").${suffix}`;
  }
  if (typeof status === 'number') {
    return `${provider} returned HTTP ${status} when listing models.${suffix}`;
  }
  if (message.includes('baseUrl is required')) {
    return `No base URL configured for ${provider}. Set AI_BASE_URL (or pass --base-url).`;
  }
  if (message.includes('Timeout')) {
    return `Timed out reaching ${provider}. The endpoint may be unreachable — check the base URL and your network.`;
  }
  if (code === 'ECONNREFUSED' || code === 'ENOTFOUND' || code === 'EAI_AGAIN') {
    return `Could not reach ${provider} (${code}). Check the base URL and your network.`;
  }
  if (message.includes('Invalid JSON')) {
    return `${provider} returned a non-JSON response from the model list endpoint — the base URL may be wrong.`;
  }
  return `Could not list models for ${provider}: ${redactSecrets(message) || 'unknown error'}.`;
}

export interface ValidateModelResult {
  valid: boolean;
  available: string[];
  suggestion?: string;
  /** Why validation could not confirm the model (when valid is false, or a
   *  soft/transient failure that was allowed through). */
  reason?: string;
  /** Classified failure kind for context-specific messaging. */
  errorKind?: 'auth' | 'network' | 'api' | 'none';
}

/** Classify a diagnoseModelDiscovery message into a failure kind. */
export function classifyDiscoveryDiagnostic(diagnostic: string | null): 'auth' | 'network' | 'api' | 'none' {
  if (!diagnostic) return 'none';
  if (/^Authentication failed/.test(diagnostic)) return 'auth';
  if (/^Timed out reaching|^Could not reach/.test(diagnostic)) return 'network';
  if (/endpoint not found|returned HTTP|non-JSON response/.test(diagnostic)) return 'api';
  return 'none';
}

// ---------------------------------------------------------------------------
// Live provider-connectivity probe — an ACTIVE boundary/health check that hits
// the provider's model-list endpoint and classifies the outcome, so a diagnostic
// surface (/doctor connectivity) can tell the user the endpoint is unreachable,
// the credentials are rejected, or the base URL path is wrong — instead of only
// echoing the configured provider/model string. Mirrors the reference's
// checkEndpoints() preflight (utils/preflightChecks.tsx:L17-L74): it reaches the
// endpoint, picks the concrete failure, and distinguishes a connectivity error
// (the whole probe threw / timed out / DNS / refused) from a reachable endpoint
// that answered with an auth/path error. REUSES the single canonical
// {@link httpGetJSON} transport and {@link diagnoseModelDiscovery} /
// {@link classifyDiscoveryDiagnostic} classifiers — no parallel probe/classifier.
// ---------------------------------------------------------------------------

/** Structured outcome of {@link probeProviderConnectivity}. */
export interface ProviderConnectivityResult {
  /** The provider the probe targeted (from {@link detectProvider}). */
  provider: string;
  /** True when the endpoint ANSWERED (even with an auth/path error) — i.e. the
   *  network path is up. False only for a true connectivity failure (timeout /
   *  DNS / connection refused) or when no endpoint could be derived. */
  reachable: boolean;
  /** Probe status: `ok` (answered 2xx), `warn` (answered but auth/path error —
   *  reachable), `error` (unreachable), or `skipped` (nonessential traffic off). */
  status: 'ok' | 'warn' | 'error' | 'skipped';
  /** Human-readable detail (reused diagnostic copy, secrets already redacted). */
  detail: string;
  /** Classified failure kind when the probe did not return 2xx. */
  errorKind?: 'auth' | 'network' | 'api' | 'none';
  /** Round-trip latency of the probe request in milliseconds, when one was made. */
  latencyMs?: number;
}

/**
 * Derive the model-list endpoint + auth headers to probe for `provider`. Returns
 * undefined when no endpoint can be derived (a cloud-SDK provider with no base URL
 * configured) so the caller can report a configuration error rather than a false
 * "unreachable". Keeps provider-specific auth-header knowledge in ONE place.
 */
export function connectivityProbeTarget(
  provider: string,
  config: ProviderConfig,
): { url: string; headers: Record<string, string> } | undefined {
  const base = config.baseUrl ? config.baseUrl.replace(/\/+$/, '') : '';
  if (provider === 'google') {
    const url = base
      ? `${base}/models`
      : (process.env.SI_GEMINI_MODELS_URL || 'https://generativelanguage.googleapis.com/v1beta/models');
    const headers: Record<string, string> = {};
    if (config.apiKey) headers['x-goog-api-key'] = sanitizeHeaderValue(config.apiKey);
    return { url, headers };
  }
  if (provider === 'anthropic') {
    const root = base || 'https://api.anthropic.com/v1';
    const headers: Record<string, string> = { 'anthropic-version': '2023-06-01' };
    if (config.apiKey) headers['x-api-key'] = sanitizeHeaderValue(config.apiKey);
    return { url: `${root}/models`, headers };
  }
  // Generic OpenAI-compatible endpoint (and any provider with an explicit base URL).
  if (!base) return undefined;
  const headers: Record<string, string> = {};
  if (config.apiKey) headers['Authorization'] = `Bearer ${sanitizeHeaderValue(config.apiKey)}`;
  return { url: `${base}/models`, headers };
}

/**
 * User-declared trusted model ids (comma-separated SI_CUSTOM_MODEL) that bypass
 * provider listing entirely. Mirrors the reference's ANTHROPIC_CUSTOM_MODEL_OPTION: a
 * self-hosted / private model id that a live /models listing will never return
 * can still be force-accepted so validateModel does not reject it. Read at call
 * time so an override set after import takes effect.
 */
export function customModelIds(): string[] {
  return (process.env.SI_CUSTOM_MODEL || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

/**
 * Deprecated / renamed model-id patterns mapped to a currently-recommended
 * successor. Advisory only: feeds `suggestion` when a requested id has NO
 * substring overlap with any live model (e.g. a fully removed id), so the user
 * still gets a concrete "try this instead" target. Mirrors the reference's
 * get3PFallbackSuggestion replacement chain.
 */
export const DEPRECATED_REPLACEMENTS: ReadonlyArray<{ pattern: RegExp; replacement: string }> = [
  { pattern: /claude-?3[._-]?5[._-]?sonnet/i, replacement: 'claude-sonnet-4-5' },
  { pattern: /claude-?3[._-]?5[._-]?haiku/i, replacement: 'claude-haiku-4-5' },
  { pattern: /claude-?3[._-]?opus/i, replacement: 'claude-opus-4-1' },
  { pattern: /gpt-?4(?:[._-]?turbo|[._-]?0613|[._-]?1106)/i, replacement: 'gpt-4o' },
  { pattern: /gpt-?3[._-]?5/i, replacement: 'gpt-4o-mini' },
];

/**
 * Map a deprecated/removed/renamed model id to its currently-recommended successor,
 * preferring a successor the provider actually lists (`available`) and otherwise the
 * canonical successor id. Returns undefined when `modelId` is not a known-deprecated
 * id. The SINGLE source of the deprecated→successor knowledge (feeds both the
 * advisory `validateModel` suggestion and the persisted-settings startup migration —
 * no parallel deprecation tables). Pass `[]` for `available` to get the canonical
 * successor without a provider round-trip.
 */
export function suggestDeprecatedReplacement(modelId: string, available: string[]): string | undefined {
  // Opt-out: CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP=1 suppresses the
  // deprecated→successor remap entirely (parity with the reference's
  // useModelMigrationNotifications opt-out env, hooks/notifs/
  // useModelMigrationNotifications.tsx:L24-L29). With it set, a user who pins a
  // legacy id is neither auto-migrated (reference-journal's startup migration) nor
  // nudged with a "did you mean" suggestion — their explicit choice stands.
  if (isLegacyModelRemapDisabled()) return undefined;
  for (const { pattern, replacement } of DEPRECATED_REPLACEMENTS) {
    if (pattern.test(modelId)) {
      // Prefer the successor the provider actually lists; otherwise suggest the
      // canonical successor id so the user still has a concrete target.
      const live = available.find(id => id.toLowerCase().includes(replacement.toLowerCase()));
      return live ?? replacement;
    }
  }
  return undefined;
}

/**
 * The SINGLE canonical name of the env var that opts out of the deprecated→successor
 * model remap (parity with the reference's useModelMigrationNotifications.tsx:L24-L29
 * opt-out). Exported so every consumer — the remap decision ({@link
 * isLegacyModelRemapDisabled}) AND the one-time startup-migration notice that
 * advertises how to disable the auto-remap — references ONE literal instead of
 * re-typing the variable name (no divergent copies).
 */
export const LEGACY_MODEL_REMAP_OPT_OUT_ENV = 'CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP';

/**
 * True when the legacy-model remap is opted out via
 * {@link LEGACY_MODEL_REMAP_OPT_OUT_ENV} (parity with the reference's env opt-out,
 * useModelMigrationNotifications.tsx:L24-L29). Accepts the usual truthy spellings
 * (1/true/yes/on, case-insensitive, trimmed). The SINGLE source of the opt-out
 * decision, consumed by both the advisory {@link suggestDeprecatedReplacement}
 * suggestion and the persisted-settings startup auto-migration
 * (reference-journal's migrateDeprecatedModelAlias) so the knob governs every
 * legacy-id remap path identically.
 */
export function isLegacyModelRemapDisabled(): boolean {
  const v = (process.env[LEGACY_MODEL_REMAP_OPT_OUT_ENV] ?? '').trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/**
 * Family-version fallback: map a FAILING too-new model id DOWN to the previous
 * generation in its own family (e.g. opus-4-6 → opus-4-1, sonnet-4-6 → sonnet-4-5,
 * sonnet-4-5 → sonnet-4, haiku-4-5 → claude-3-5-haiku). Mirrors the reference's
 * get3PModelFallbackSuggestion (services/api/errors.ts:L936-L959): when a
 * third-party / self-hosted provider does not (yet) carry the newest generation,
 * the model-id error names the prior sibling it is likely to have — a concrete
 * "try this instead". This is DISTINCT from {@link suggestDeprecatedReplacement}
 * (which maps a RETIRED old id UP to its current successor); here the id is too new
 * for the endpoint, so we step DOWN. Returns undefined for an unrecognized id.
 */
export const FAMILY_VERSION_FALLBACKS: ReadonlyArray<{ pattern: RegExp; fallback: string }> = [
  { pattern: /opus-?4[._-]?6/i, fallback: 'claude-opus-4-1' },
  { pattern: /sonnet-?4[._-]?6/i, fallback: 'claude-sonnet-4-5' },
  { pattern: /sonnet-?4[._-]?5/i, fallback: 'claude-sonnet-4' },
  { pattern: /opus-?4[._-]?5/i, fallback: 'claude-opus-4-1' },
  { pattern: /haiku-?4[._-]?5/i, fallback: 'claude-3-5-haiku' },
];
