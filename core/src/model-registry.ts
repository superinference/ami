import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import type { ProviderConfig } from './types';
import { MODEL_PREFERENCE } from './provider';
import {
  discoverFromModelList,
  modelCapabilitiesCacheDir,
  warmModelCapabilitiesFromCache,
  writeModelCapabilitiesCache,
  discoveryToCacheEntries,
  isFirstPartyProvider,
  toApiProviderClass,
} from './model-capabilities';
import { resolveBedrockModelId, getBedrockInferenceProfiles, getBedrockRegionPrefix } from './bedrock';
import { redactSecrets } from './tools/tool-utils';
import { isEssentialTrafficOnly } from './analytics';
import { log } from './logger';
import { validateBoundedIntEnvVar } from './config';
import { parseRetryAfterHeader } from './rate-limit-parser';

/**
 * Strip header-injection bytes (CR, LF, NUL) from a value before it is placed in
 * an outbound HTTP request header. A secret or base URL that carries a stray
 * `\r\n` (e.g. a mis-pasted `OPENAI_API_KEY="sk-...\r\nX-Evil: 1"`) would
 * otherwise let an attacker smuggle an extra header line into the model-list
 * request. Mirrors Claude Code's sanitizeHeaderValue (utils/hooks/execHttpHook.ts):
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
 * `data.message` first, then `data.error.message`. Mirrors Claude Code's
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
function modelListTimeoutMs(): number {
  // Bounded: reject NaN/non-positive (→ 10s default) and cap at 10 minutes so a
  // mistaken/hostile SI_MODEL_LIST_TIMEOUT_MS cannot hang model discovery forever.
  return validateBoundedIntEnvVar('SI_MODEL_LIST_TIMEOUT_MS', process.env.SI_MODEL_LIST_TIMEOUT_MS, {
    default: 10000,
    max: 600000,
  }).effective;
}

interface ModelInfo {
  id: string;
  name?: string;
  owned_by?: string;
}

const STATIC_MODEL_PROVIDERS = new Set([
  'anthropic', 'anthropic-vertex', 'google-vertex', 'azure-openai',
  'amazon-bedrock', 'groq', 'mistral', 'xai', 'deepseek',
  'togetherai', 'cohere', 'fireworks', 'perplexity', 'deepinfra',
  'cerebras', 'alibaba', 'luma', 'huggingface',
]);

/**
 * Abort error for a cancelled model-list / connectivity request. Carries a
 * recognizable `code` ('ABORT_ERR') so {@link isTransientModelListError} never
 * treats a deliberate cancellation as a transient (retryable) failure — a user
 * who aborts `/model` listing must not trigger the retry loop.
 */
function makeAbortError(): Error {
  const e = new Error('Aborted') as Error & { code?: string };
  e.code = 'ABORT_ERR';
  return e;
}

/**
 * Low-level JSON GET for model discovery / connectivity probing.
 *
 * FUNCTIONAL (additive): accepts an optional {@link AbortSignal}. When supplied,
 * the in-flight request is torn down the instant the signal fires (and an already
 * -aborted signal rejects synchronously before any socket is opened) so a cancelled
 * `/model` listing / `/doctor connectivity` probe stops immediately instead of
 * running to the 10s timeout. Mirrors Claude Code's signal-threaded network waits
 * (services/api/withRetry.ts). A single `settled` latch + listener teardown ensures
 * the promise never double-settles when abort races the response/timeout.
 */
function httpGetJSON(
  url: string,
  headers: Record<string, string>,
  signal?: AbortSignal,
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) { reject(makeAbortError()); return; }
    const parsed = new URL(url);
    const requester = parsed.protocol === 'https:' ? https : http;

    let settled = false;
    let onAbort: (() => void) | undefined;
    const finish = (apply: () => void): void => {
      if (settled) return;
      settled = true;
      if (onAbort) signal?.removeEventListener('abort', onAbort);
      apply();
    };

    const req = requester.get(url, {
      headers: { ...headers, Accept: 'application/json' },
      timeout: modelListTimeoutMs(),
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => {
        const status = res.statusCode ?? 0;
        const bodyText = Buffer.concat(chunks).toString('utf-8');
        if (status < 200 || status >= 300) {
          // Parse the error body and surface the server's own human-readable
          // reason (data.message / data.error.message) on the thrown error, so
          // the diagnostic can tell the user WHY instead of a bare "HTTP 404".
          let detail: string | undefined;
          try { detail = extractErrorDetail(JSON.parse(bodyText)); } catch { /* non-JSON error body */ }
          const httpErr = new Error(`HTTP ${status}`) as Error & { statusCode: number; detail?: string; retryAfterMs?: number };
          httpErr.statusCode = status;
          if (detail) httpErr.detail = detail;
          // Capture a server-provided Retry-After so the discovery retry loop can
          // honor the advertised cool-down instead of blind backoff (parity with
          // Claude Code's withRetry Retry-After handling). Header names are
          // case-insensitive; Node lower-cases them.
          const retryAfterRaw = res.headers['retry-after'];
          const retryAfterMs = parseRetryAfterHeader(
            Array.isArray(retryAfterRaw) ? retryAfterRaw[0] : retryAfterRaw,
          );
          if (retryAfterMs !== null) httpErr.retryAfterMs = retryAfterMs;
          finish(() => reject(httpErr));
          return;
        }
        let payload: unknown;
        try {
          payload = JSON.parse(bodyText);
        } catch {
          finish(() => reject(new Error('Invalid JSON response')));
          return;
        }
        finish(() => resolve(payload));
      });
      res.on('error', (e) => finish(() => reject(e)));
    });

    if (signal) {
      onAbort = () => { req.destroy(); finish(() => reject(makeAbortError())); };
      signal.addEventListener('abort', onAbort, { once: true });
    }

    req.on('timeout', () => { req.destroy(); finish(() => reject(new Error('Timeout'))); });
    req.on('error', (e) => finish(() => reject(e)));
  });
}

// ---------------------------------------------------------------------------
// Transient-failure retry for model-list discovery — a small bounded retry loop
// around httpGetJSON so a momentary rate-limit (HTTP 429), a provider restart
// (HTTP 503), or a dropped/half-open socket (ECONNRESET / ETIMEDOUT) does not
// make listModels fall straight through to the static fallback list on the first
// blip. Mirrors Claude Code's withRetry (services/api/withRetry.ts): retry only
// genuinely transient conditions, honor a server Retry-After when present, and
// otherwise back off exponentially with equal jitter. Scoped to the discovery
// fetch path (listOpenAIModels / listGeminiModels); the point-in-time
// connectivity probe deliberately does NOT retry so its latency reading and
// reachable/unreachable verdict stay single-shot.
// ---------------------------------------------------------------------------

/** Up-to-N attempts total (1 initial + N-1 retries) for a model-list fetch. */
const MODEL_LIST_MAX_ATTEMPTS = 3;
/** Base blind-backoff delay; doubled per attempt before jitter. */
const MODEL_LIST_BASE_DELAY_MS = 300;

/**
 * Test seam for the discovery retry loop: an injectable PRNG (for deterministic
 * jitter) and sleep (so tests don't actually wait). Mirrors provider.ts'
 * retryHooks seam so backoff timing is controllable without real delays.
 */
export const modelListRetryHooks: { random: () => number; sleep: (ms: number) => Promise<void> } = {
  random: Math.random,
  sleep: (ms: number) => new Promise(resolve => setTimeout(resolve, ms)),
};

/**
 * True when a model-list fetch error is worth another attempt: a transient
 * rate-limit/unavailable status (429/503) or a transient socket error
 * (ECONNRESET / ETIMEDOUT). A 4xx other than 429 (bad key, wrong path) and a
 * malformed-JSON body are NOT retried — they will not fix themselves.
 */
function isTransientModelListError(err: unknown): boolean {
  const status = (err as { statusCode?: unknown })?.statusCode;
  if (status === 429 || status === 503) return true;
  const code = (err as { code?: unknown })?.code;
  return code === 'ECONNRESET' || code === 'ETIMEDOUT';
}

/**
 * httpGetJSON with a bounded transient-error retry. Honors a server Retry-After
 * (captured onto the error by httpGetJSON) over blind backoff; otherwise waits
 * {@link MODEL_LIST_BASE_DELAY_MS} × 2^attempt with equal jitter (50–100%) to
 * de-synchronize concurrent callers. Rethrows the last error once attempts are
 * exhausted or the failure is non-transient, so the existing listModels fallback
 * to the static list still fires.
 */
async function httpGetJSONRetrying(
  url: string,
  headers: Record<string, string>,
  signal?: AbortSignal,
): Promise<unknown> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < MODEL_LIST_MAX_ATTEMPTS; attempt++) {
    try {
      return await httpGetJSON(url, headers, signal);
    } catch (err) {
      lastErr = err;
      if (attempt === MODEL_LIST_MAX_ATTEMPTS - 1 || !isTransientModelListError(err)) {
        throw err;
      }
      const retryAfterMs = (err as { retryAfterMs?: unknown })?.retryAfterMs;
      const waitMs = typeof retryAfterMs === 'number' && retryAfterMs > 0
        ? retryAfterMs
        : Math.round(MODEL_LIST_BASE_DELAY_MS * 2 ** attempt * (0.5 + 0.5 * modelListRetryHooks.random()));
      log('modelCapabilities', `[modelCapabilities] transient model-list error, retrying in ${waitMs}ms (attempt ${attempt + 1}/${MODEL_LIST_MAX_ATTEMPTS})`);
      await modelListRetryHooks.sleep(waitMs);
    }
  }
  throw lastErr;
}

export function detectProvider(config: ProviderConfig): string {
  if (config.provider) return config.provider;

  const key = config.apiKey || '';
  const url = config.baseUrl || '';

  // Explicit cloud-routing opt-in toggle for Bedrock (parity with Claude Code's
  // getAPIProvider, utils/model/providers.ts:L6-L14): route to the managed cloud
  // provider purely from the env toggle, independent of whether an AWS credential
  // happens to be exported — so a user who runs via Bedrock is never mis-detected
  // as a first-party/OpenAI endpoint by credential sniffing. Honors both the
  // Claude-compatible var and AMI's own convention (mirrors the vertex toggle below
  // and the cloud-provider list in common/engine-factory's preconnect skip).
  if (process.env.CLAUDE_CODE_USE_BEDROCK || process.env.SUPERINFERENCE_USE_BEDROCK) return 'amazon-bedrock';

  // Env-based detection for vertex
  if (process.env.ANTHROPIC_VERTEX_PROJECT_ID || process.env.CLAUDE_CODE_USE_VERTEX) return 'anthropic-vertex';

  // Explicit opt-in toggle for the Azure/Microsoft Foundry route (parity with Claude
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
function staticModelList(provider: string): ModelInfo[] {
  return (MODEL_PREFERENCE[provider] || []).map(id => ({ id, name: id }));
}

export async function listModels(config: ProviderConfig, signal?: AbortSignal): Promise<ModelInfo[]> {
  const provider = detectProvider(config);

  // Warm the in-memory context-window / output-cap tables from the persisted
  // capability cache (opt-in via SUPERINFERENCE_MODEL_CACHE_DIR) so even a cold
  // start — or a run whose live /models fetch is skipped/fails below — is sized
  // from the last-known advertised windows. A successful live discovery still wins.
  const cacheDir = modelCapabilitiesCacheDir();
  if (cacheDir) warmModelCapabilitiesFromCache(cacheDir);

  // Providers with known static model lists (no API listing endpoint)
  if (STATIC_MODEL_PROVIDERS.has(provider)) {
    // Bedrock: await the model-string init barrier so the region/profile-correct
    // strings are resolved before the option list is built/returned, instead of
    // interim defaults (parity: Claude's ensureModelStringsInitialized before
    // generating model options). A no-op await for every other static provider.
    if (toApiProviderClass(provider) === 'bedrock') await ensureModelStringsInitialized(config);
    return staticModelList(provider);
  }

  // Respect the essential-traffic-only privacy opt-out: a live model-list
  // discovery request is nonessential background traffic, so fall back to the
  // built-in static list instead of reaching the network. Mirrors Claude Code's
  // modelCapabilities honoring *_DISABLE_NONESSENTIAL_TRAFFIC before discovery.
  if (isEssentialTrafficOnly()) {
    log('modelCapabilities', '[modelCapabilities] model-list discovery skipped (essential-traffic-only)');
    return staticModelList(provider);
  }

  try {
    // Coalesced: concurrent callers for the same endpoint share one /models walk.
    return await fetchProviderModels(provider, config, signal);
  } catch (err) {
    // Fallback to static list if API call fails
    log('modelCapabilities', `[modelCapabilities] fetch failed: ${diagnoseModelDiscovery(err, provider)}`);
    const models = MODEL_PREFERENCE[provider] || [];
    if (models.length > 0) return models.map(id => ({ id, name: id }));
    return [];
  }
}

/**
 * Human-readable env-var hint for a provider's credential, used in empty-state
 * diagnostics. Falls back to a generic phrase for providers without a single
 * well-known key var.
 */
function providerKeyHint(provider: string): string {
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

/**
 * Like listModels, but surfaces a specific empty-state diagnostic instead of
 * silently swallowing failures. The success path is unchanged (diagnostic is
 * null whenever at least one model is returned), so callers that only need the
 * list can keep using listModels.
 */
export async function listModelsDiagnostic(
  config: ProviderConfig,
  signal?: AbortSignal,
): Promise<{ models: ModelInfo[]; diagnostic: string | null }> {
  const provider = detectProvider(config);

  if (STATIC_MODEL_PROVIDERS.has(provider)) {
    // Bedrock: ensure region/profile-correct model strings are resolved before the
    // option list is built (parity: Claude's ensureModelStringsInitialized).
    if (toApiProviderClass(provider) === 'bedrock') await ensureModelStringsInitialized(config);
    const models = staticModelList(provider);
    return {
      models,
      diagnostic: models.length > 0 ? null : `No built-in model list for ${provider}.`,
    };
  }

  // Essential-traffic-only opt-out: skip the live discovery request (nonessential
  // background traffic) and surface the built-in static list, with a diagnostic
  // naming the knob to unset when no offline list exists for the provider.
  if (isEssentialTrafficOnly()) {
    const models = staticModelList(provider);
    return {
      models,
      diagnostic: models.length > 0
        ? null
        : `Model-list discovery skipped for ${provider}: nonessential network traffic is disabled. Unset SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC to enable live discovery.`,
    };
  }

  try {
    // Coalesced: concurrent callers for the same endpoint share one /models walk.
    const models = await fetchProviderModels(provider, config, signal);
    return {
      models,
      diagnostic: models.length > 0
        ? null
        : `${provider} returned an empty model list. Verify ${providerKeyHint(provider)} has model-list access.`,
    };
  } catch (err) {
    const diagnostic = diagnoseModelDiscovery(err, provider);
    log('modelCapabilities', `[modelCapabilities] fetch failed: ${diagnostic}`);
    return { models: [], diagnostic };
  }
}

async function listOpenAIModels(config: ProviderConfig, signal?: AbortSignal): Promise<ModelInfo[]> {
  if (!config.baseUrl) throw new Error('baseUrl is required for OpenAI-compatible model listing');
  const baseUrl = config.baseUrl.replace(/\/+$/, '');
  const url = `${baseUrl}/models`;
  const headers: Record<string, string> = {};
  if (config.apiKey) headers['Authorization'] = `Bearer ${sanitizeHeaderValue(config.apiKey)}`;

  const data = await httpGetJSONRetrying(url, headers, signal) as { data?: Array<Record<string, unknown>> };
  const raw = data.data || [];
  const models: ModelInfo[] = raw.map(m => ({
    id: String(m.id ?? ''),
    name: String(m.id ?? ''),
    owned_by: typeof m.owned_by === 'string' ? m.owned_by : undefined,
  }));
  // Learn capabilities from the RAW records (not the stripped {id,name,owned_by}
  // projection) so a provider-advertised context window / output cap
  // (context_window, max_output_tokens, …) is actually captured and persisted —
  // the mapped rows carry no such fields, so discovery must see the originals.
  const discovery = discoverFromModelList(raw);
  // Persist the freshly-advertised windows/caps to the on-disk cache (opt-in via
  // SUPERINFERENCE_MODEL_CACHE_DIR) so the next cold start can warm them without a
  // round-trip. Skipped automatically when the model set is unchanged (no mtime churn).
  const cacheDir = modelCapabilitiesCacheDir();
  if (cacheDir) {
    const entries = discoveryToCacheEntries(discovery);
    if (entries.length > 0) {
      // writeModelCapabilitiesCache returns false when the freshly-advertised set
      // is byte-identical to what is already on disk (no mtime churn). Report that
      // skip so operators can see the cache was reused unchanged — the third of
      // Claude Code's model-capability debug lines ('cache unchanged, skipping
      // write'), alongside 'cached N models' and 'fetch failed' below/above.
      const wrote = writeModelCapabilitiesCache(cacheDir, entries);
      if (!wrote) log('modelCapabilities', '[modelCapabilities] cache unchanged, skipping write');
    }
  }
  // Debug/introspection: report the outcome of capability discovery (how many
  // models the endpoint returned and how many advertised a context window), so
  // operators can see which models/windows were learned. Mirrors Claude Code's
  // `[modelCapabilities] cached N models` debug line. Gated by log level.
  log('modelCapabilities',
    `[modelCapabilities] cached ${models.length} models`,
    {
      registered: discovery.registered.length,
      contextWindows: Object.keys(discovery.contextWindows).length,
    },
  );
  return models;
}

async function listGeminiModels(config: ProviderConfig, signal?: AbortSignal): Promise<ModelInfo[]> {
  // Default to the public Gemini endpoint; overridable via env (e.g. an offline
  // mirror, or a test server) without changing the default behavior.
  const url = process.env.SI_GEMINI_MODELS_URL || 'https://generativelanguage.googleapis.com/v1beta/models';
  const data = await httpGetJSONRetrying(url, { 'x-goog-api-key': sanitizeHeaderValue(config.apiKey) }, signal) as { models?: Array<{ name: string; displayName: string }> };
  return (data.models || []).map(m => ({
    id: m.name.replace('models/', ''),
    name: m.displayName,
  }));
}

// ---------------------------------------------------------------------------
// In-flight listing coalescing — mirrors Claude Code's `sequential()`-guarded,
// null-rechecked background model-string init (utils/model/modelStrings.ts:
// L102-L134): multiple concurrent callers (a startup preconnect AND a `/model`
// invocation, say) must not each fire a provider `/models` request. A fetch that
// is still pending for the SAME provider+baseUrl+apiKey is shared by every
// concurrent caller and cleared the instant it settles — so the network walk (and
// its on-disk capability-cache write) runs ONCE per concurrent burst, while a call
// that arrives AFTER it settles still re-fetches (coalescing in-flight work only,
// never a stale permanent memoization). Both listModels and listModelsDiagnostic
// route through this single guard so the de-duplication is consistent everywhere.
// ---------------------------------------------------------------------------
const inFlightListings = new Map<string, Promise<ModelInfo[]>>();

function fetchProviderModels(provider: string, config: ProviderConfig, signal?: AbortSignal): Promise<ModelInfo[]> {
  const run = (): Promise<ModelInfo[]> =>
    provider === 'google' ? listGeminiModels(config, signal) : listOpenAIModels(config, signal);
  // A cancellable listing runs on its OWN request (never coalesced): aborting it
  // must tear down only that caller's socket, never a listing shared with other
  // concurrent (non-cancellable) callers. Uncancellable calls still coalesce.
  if (signal) return run();
  // Key on the full credential+endpoint tuple so different providers, base URLs,
  // or keys never share a listing (correctness over aggressiveness).
  const key = `${provider}:::${config.baseUrl ?? ''}:::${config.apiKey ?? ''}`;
  const existing = inFlightListings.get(key);
  if (existing) return existing;
  const pending = run().finally(() => { inFlightListings.delete(key); });
  inFlightListings.set(key, pending);
  return pending;
}

// Models that have already validated (provider+baseUrl+id) are never re-probed.
// Mirrors Claude's module-level validModelCache.
const validModelCache = new Set<string>();

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
function classifyDiscoveryDiagnostic(diagnostic: string | null): 'auth' | 'network' | 'api' | 'none' {
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
// echoing the configured provider/model string. Mirrors Claude Code's
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
function connectivityProbeTarget(
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
 * Actively probe the configured provider's reachability by issuing a real request
 * to its model-list endpoint and classifying the outcome. Respects the
 * essential-traffic-only privacy opt-out (a live probe is nonessential background
 * traffic → `status: 'skipped'`). A 2xx answer is `ok`; an endpoint that answers
 * with an auth/path error is `warn` + `reachable: true` (the network path is up,
 * the credentials or URL path are not); a timeout / DNS / connection-refused
 * failure is `error` + `reachable: false`. The single live-connectivity capability,
 * surfaced via `/doctor connectivity`.
 */
export async function probeProviderConnectivity(
  config: ProviderConfig,
  signal?: AbortSignal,
): Promise<ProviderConnectivityResult> {
  const provider = detectProvider(config);

  if (isEssentialTrafficOnly()) {
    return {
      provider,
      reachable: false,
      status: 'skipped',
      detail: `Connectivity probe skipped for ${provider}: nonessential network traffic is disabled. Unset SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC to enable live probing.`,
      errorKind: 'none',
    };
  }

  const target = connectivityProbeTarget(provider, config);
  if (!target) {
    return {
      provider,
      reachable: false,
      status: 'error',
      detail: `No base URL configured for ${provider}. Set AI_BASE_URL (or pass --base-url) to probe connectivity.`,
      errorKind: 'network',
    };
  }

  const started = Date.now();
  try {
    await httpGetJSON(target.url, target.headers, signal);
    const latencyMs = Date.now() - started;
    return {
      provider,
      reachable: true,
      status: 'ok',
      detail: `Reached ${provider} at ${target.url} (${latencyMs}ms).`,
      errorKind: 'none',
      latencyMs,
    };
  } catch (err) {
    const latencyMs = Date.now() - started;
    const diagnostic = diagnoseModelDiscovery(err, provider);
    const errorKind = classifyDiscoveryDiagnostic(diagnostic);
    // An auth/path (api) error proves the endpoint answered → the network path is
    // reachable; only a timeout / DNS / connection-refused is a true connectivity
    // failure. Mirrors Claude's connectivity-vs-config distinction.
    const reachable = errorKind === 'auth' || errorKind === 'api';
    return {
      provider,
      reachable,
      status: reachable ? 'warn' : 'error',
      detail: diagnostic,
      errorKind,
      latencyMs,
    };
  }
}

/**
 * User-declared trusted model ids (comma-separated SI_CUSTOM_MODEL) that bypass
 * provider listing entirely. Mirrors Claude's ANTHROPIC_CUSTOM_MODEL_OPTION: a
 * self-hosted / private model id that a live /models listing will never return
 * can still be force-accepted so validateModel does not reject it. Read at call
 * time so an override set after import takes effect.
 */
function customModelIds(): string[] {
  return (process.env.SI_CUSTOM_MODEL || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

/**
 * Deprecated / renamed model-id patterns mapped to a currently-recommended
 * successor. Advisory only: feeds `suggestion` when a requested id has NO
 * substring overlap with any live model (e.g. a fully removed id), so the user
 * still gets a concrete "try this instead" target. Mirrors Claude's
 * get3PFallbackSuggestion replacement chain.
 */
const DEPRECATED_REPLACEMENTS: ReadonlyArray<{ pattern: RegExp; replacement: string }> = [
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
  // deprecated→successor remap entirely (parity with Claude Code's
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
 * model remap (parity with Claude Code's useModelMigrationNotifications.tsx:L24-L29
 * opt-out). Exported so every consumer — the remap decision ({@link
 * isLegacyModelRemapDisabled}) AND the one-time startup-migration notice that
 * advertises how to disable the auto-remap — references ONE literal instead of
 * re-typing the variable name (no divergent copies).
 */
export const LEGACY_MODEL_REMAP_OPT_OUT_ENV = 'CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP';

/**
 * True when the legacy-model remap is opted out via
 * {@link LEGACY_MODEL_REMAP_OPT_OUT_ENV} (parity with Claude Code's env opt-out,
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
 * sonnet-4-5 → sonnet-4, haiku-4-5 → claude-3-5-haiku). Mirrors Claude Code's
 * get3PModelFallbackSuggestion (services/api/errors.ts:L936-L959): when a
 * third-party / self-hosted provider does not (yet) carry the newest generation,
 * the model-id error names the prior sibling it is likely to have — a concrete
 * "try this instead". This is DISTINCT from {@link suggestDeprecatedReplacement}
 * (which maps a RETIRED old id UP to its current successor); here the id is too new
 * for the endpoint, so we step DOWN. Returns undefined for an unrecognized id.
 */
const FAMILY_VERSION_FALLBACKS: ReadonlyArray<{ pattern: RegExp; fallback: string }> = [
  { pattern: /opus-?4[._-]?6/i, fallback: 'claude-opus-4-1' },
  { pattern: /sonnet-?4[._-]?6/i, fallback: 'claude-sonnet-4-5' },
  { pattern: /sonnet-?4[._-]?5/i, fallback: 'claude-sonnet-4' },
  { pattern: /opus-?4[._-]?5/i, fallback: 'claude-opus-4-1' },
  { pattern: /haiku-?4[._-]?5/i, fallback: 'claude-3-5-haiku' },
];
export function suggestFamilyVersionFallback(modelId: string, available: string[] = []): string | undefined {
  for (const { pattern, fallback } of FAMILY_VERSION_FALLBACKS) {
    if (pattern.test(modelId)) {
      // Prefer the prior version the provider actually lists; otherwise return the
      // canonical prior id so the user still has a concrete target.
      const live = available.find(id => id.toLowerCase().includes(fallback.toLowerCase()));
      return live ?? fallback;
    }
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// Model aliases — short, memorable family names accepted anywhere a model id is
// expected. Mirrors Claude Code's canonical MODEL_ALIASES set
// (utils/model/aliases.ts): a user can type a family name (`opus`) instead of a
// full provider id. The set, the membership guard, and the alias→canonical
// resolver live here so EVERY consumer (the subagent `model` override in
// tools/task.ts, validateModel below, and — via the public barrel — the
// CLI/slash paths) resolves the SAME short names to the SAME canonical ids.
// This is the single canonical alias table: tools/task.ts previously kept its
// own inline copy and now imports these instead (consolidation — no parallel
// alias maps).
// ---------------------------------------------------------------------------

/** Canonical set of accepted model-alias inputs (case-insensitive, trimmed). */
export const MODEL_ALIASES = [
  'sonnet', 'opus', 'haiku', 'best', 'sonnet[1m]', 'opus[1m]', 'opusplan',
] as const;
export type ModelAlias = (typeof MODEL_ALIASES)[number];

/**
 * Bare family aliases (sonnet/opus/haiku). These act as wildcards for the
 * subagent anti-downgrade check: a bare family request inherits the parent's
 * exact same-family model instead of the generic family default.
 */
export const MODEL_FAMILY_ALIASES = ['sonnet', 'opus', 'haiku'] as const;

/** Each accepted alias → the concrete family-default model id it resolves to. */
const MODEL_ALIAS_TARGETS: Record<ModelAlias, string> = {
  'sonnet': 'claude-sonnet-4',
  'opus': 'claude-opus-4',
  'haiku': 'claude-haiku-4-5',
  // "best" picks the strongest family; "opusplan" runs Opus for planning.
  'best': 'claude-opus-4',
  'opusplan': 'claude-opus-4',
  // The 1M-context suffix variants resolve to the same base model; the 1M window
  // itself is governed by model-capabilities, not the model id.
  'sonnet[1m]': 'claude-sonnet-4',
  'opus[1m]': 'claude-opus-4',
};

/**
 * Version-specific SHORT MODEL KEYS — stable, typeable selectors that name a
 * concrete model generation without the dated, provider-prefixed literal (e.g.
 * `opus45` → `claude-opus-4-5`). Mirrors Claude Code's ALL_MODEL_CONFIGS /
 * ModelKey set (utils/model/configs.ts:L86-L101) so a user can pin an exact
 * version with a memorable key anywhere a model id is accepted (`--model`,
 * `/model`, the subagent `model` override). This is DISTINCT from the bare
 * FAMILY aliases in {@link MODEL_ALIASES} (opus/sonnet/haiku), which resolve to
 * the family default and act as anti-downgrade wildcards; a short key always
 * pins one concrete generation. Every target id is a model AMI recognizes in
 * model-capabilities, so the context-window / thinking-budget lookup resolves.
 * (Functional change: new version-keyed short-key selector — parity gap
 * config-resolver/configs "stable short model keys as a user-facing selector".)
 */
export const MODEL_SHORT_KEYS: Record<string, string> = {
  haiku35: 'claude-3-5-haiku',
  haiku45: 'claude-haiku-4-5',
  sonnet35: 'claude-3-5-sonnet',
  sonnet37: 'claude-3-7-sonnet',
  sonnet40: 'claude-sonnet-4',
  sonnet45: 'claude-sonnet-4-5',
  sonnet46: 'claude-sonnet-4-6',
  opus40: 'claude-opus-4',
  opus41: 'claude-opus-4-1',
  opus45: 'claude-opus-4-5',
  opus46: 'claude-opus-4-6',
};

/**
 * Env-var knobs that steer model routing / legacy-id remap, surfaced in
 * `ami --introspect` (modelEnvToggles) so the feature manifest documents them —
 * the repo invariant that every new env-controlled capability is discoverable.
 * The SINGLE declaration of these toggles; the behavior lives in
 * {@link detectProvider} (routing) and {@link isLegacyModelRemapDisabled} /
 * {@link suggestDeprecatedReplacement} (remap opt-out).
 */
export const MODEL_ENV_TOGGLES = [
  {
    env: 'CLAUDE_CODE_USE_BEDROCK',
    affects: 'provider-routing',
    description: 'Route to the amazon-bedrock provider from the explicit toggle, independent of credential sniffing (parity with Claude Code getAPIProvider).',
  },
  {
    env: 'SUPERINFERENCE_USE_BEDROCK',
    affects: 'provider-routing',
    description: "AMI-convention alias of CLAUDE_CODE_USE_BEDROCK; either selects the amazon-bedrock provider.",
  },
  {
    env: 'CLAUDE_CODE_USE_FOUNDRY',
    affects: 'provider-routing',
    description: 'Route to the Azure/Microsoft Foundry endpoint (AMI azure-openai provider) from the explicit toggle, independent of credential sniffing (parity with Claude Code getAPIProvider).',
  },
  {
    env: 'SUPERINFERENCE_USE_FOUNDRY',
    affects: 'provider-routing',
    description: 'AMI-convention alias of CLAUDE_CODE_USE_FOUNDRY; either selects the azure-openai (Foundry) provider.',
  },
  {
    env: 'CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP',
    affects: 'model-remap',
    description: 'Opt out of the deprecated→successor model remap — suppresses both the startup settings migration and the /model "did you mean" suggestion (parity with Claude Code useModelMigrationNotifications).',
  },
] as const;

/** Normalize an input for alias matching: trimmed + lower-cased. */
function normalizeAlias(input: string): string {
  return typeof input === 'string' ? input.trim().toLowerCase() : '';
}

/** True when `input` (trimmed, case-insensitive) is a recognized model alias. */
export function isModelAlias(input: string): boolean {
  return (MODEL_ALIASES as readonly string[]).includes(normalizeAlias(input));
}

/**
 * True when `input` (trimmed, case-insensitive) is a recognized version-specific
 * short model key (e.g. `opus45`, `sonnet46`). Distinct from {@link isModelAlias},
 * which covers the bare family aliases.
 */
export function isModelShortKey(input: string): boolean {
  return Object.prototype.hasOwnProperty.call(MODEL_SHORT_KEYS, normalizeAlias(input));
}

/** True when `input` is a bare family alias (sonnet/opus/haiku). */
export function isModelFamilyAlias(input: string): boolean {
  return (MODEL_FAMILY_ALIASES as readonly string[]).includes(normalizeAlias(input));
}

/**
 * Resolve a model alias to its canonical family-default model id, or undefined
 * when `input` is not a recognized alias (a full/unknown model id passes through
 * for the caller to handle). Case-insensitive and whitespace-tolerant.
 */
export function resolveModelAlias(input: string): string | undefined {
  const key = normalizeAlias(input);
  if (isModelAlias(key)) return MODEL_ALIAS_TARGETS[key as ModelAlias];
  // Version-specific short keys (opus45/sonnet46/…) pin a concrete generation.
  if (isModelShortKey(key)) return MODEL_SHORT_KEYS[key];
  return undefined;
}

/**
 * Request shape for {@link getRuntimeMainLoopModel} (parity: Claude Code's
 * `getRuntimeMainLoopModel({ permissionMode, mainLoopModel, exceeds200kTokens })`,
 * utils/model/agent.ts:L80-88).
 */
export interface RuntimeMainLoopModelRequest {
  /** The active permission mode (`plan` switches a `opusplan` run to Opus). */
  permissionMode?: string;
  /** The configured main-loop model id/alias to re-resolve at runtime. */
  mainLoopModel: string | undefined;
}

/**
 * Re-resolve a main-loop / inherited model the same way the running main loop would
 * at spawn time, instead of copying the configured string verbatim (parity: Claude
 * Code's `getRuntimeMainLoopModel`, which the agent-inherit path runs the parent
 * model through — utils/model/agent.ts:L80-88). The behavioral difference AMI needs
 * is the DEFERRED `opusplan` alias: it resolves to Opus while the agent is in plan
 * mode (planning benefits from the strongest model) and to Sonnet during execution.
 * Every concrete model id — and every other alias — passes through unchanged, so a
 * subagent that INHERITS the parent model gets mode-conditional switching at spawn
 * without ever downgrading an explicitly-pinned model. (AMI governs the long-context
 * 1M window through model-capabilities, not a distinct model id, so unlike Claude
 * there is no separate context-size re-route here — the window is a capability of the
 * resolved model, not a different model.)
 */
export function getRuntimeMainLoopModel(req: RuntimeMainLoopModelRequest): string | undefined {
  const { permissionMode, mainLoopModel } = req;
  if (!mainLoopModel) return mainLoopModel;
  if (normalizeAlias(mainLoopModel) === 'opusplan') {
    return permissionMode === 'plan'
      ? MODEL_ALIAS_TARGETS['opus']
      : MODEL_ALIAS_TARGETS['sonnet'];
  }
  return mainLoopModel;
}

// ---------------------------------------------------------------------------
// Provider-aware default subagent model — the fast family default a brand-new
// subagent/teammate runs on when NO model is resolvable from any higher-priority
// source. Mirrors Claude Code's provider-aware getHardcodedTeammateModelFallback
// (utils/swarm/teammateModel.ts): a teammate the user never configured still gets
// a concrete, PROVIDER-CORRECT model id (Bedrock/Vertex/Foundry), never an empty
// string. AMI keeps ONE central send-path remap (provider.ts → resolveBedrockModelId)
// instead of Claude's N hardcoded per-provider config tables, so the per-provider
// mapping here REUSES that same canonical Bedrock resolver — no parallel id table.
// ---------------------------------------------------------------------------

/**
 * Canonical, provider-neutral fast model id a subagent falls back to when nothing
 * else selected a model. A read-only/delegated subagent does not need the main
 * model's full capability, so this is the fast/cheap family default (matches the
 * `haiku` alias target and the explore-mode fast default). {@link getDefaultSubagentModel}
 * maps it to the concrete id for the active provider.
 */
const DEFAULT_SUBAGENT_MODEL = 'claude-haiku-4-5';

/**
 * Map a canonical first-party model id to the concrete id to INVOKE for the
 * provider that `config` detects. For Amazon Bedrock the id is resolved against
 * the account's declared inference profiles / cross-region prefix via the SINGLE
 * canonical {@link resolveBedrockModelId} — the exact resolver the provider
 * send-path (provider.ts) already uses, so a selected id lands on a provisioned
 * region-scoped profile with no parallel per-provider id table. Vertex / Foundry /
 * first-party send the canonical id unchanged (AMI's one-central-remap design).
 */
function toProviderModelId(
  canonicalId: string,
  config: ProviderConfig,
  bedrockProfiles?: string[],
): string {
  if (toApiProviderClass(detectProvider(config)) === 'bedrock') {
    return resolveBedrockModelId(canonicalId, bedrockProfiles ?? getBedrockInferenceProfiles());
  }
  return canonicalId;
}

/**
 * Provider-aware default subagent model: the fast {@link DEFAULT_SUBAGENT_MODEL}
 * resolved to the concrete id for the provider `config` detects. Used as the FINAL
 * fallback when a subagent has no model configured anywhere (no per-call `model`,
 * no agent-definition model, no SUPERINFERENCE_SUBAGENT_MODEL override, and no
 * inheritable parent model), so a Bedrock run gets a provisioned region-scoped
 * profile id and a first-party run gets the canonical id — never an empty model
 * string. Mirrors Claude Code's provider-aware getHardcodedTeammateModelFallback.
 */
export function getDefaultSubagentModel(config: ProviderConfig): string {
  return toProviderModelId(DEFAULT_SUBAGENT_MODEL, config);
}

/**
 * One row of the per-provider model-string table: a logical short key, its
 * canonical first-party model id, and the concrete wire id to SEND on the active
 * provider.
 */
export interface ProviderModelString {
  /** Logical, typeable short key (e.g. `opus46`) — stable across providers. */
  key: string;
  /** The canonical first-party model id the key resolves to (`claude-opus-4-6`). */
  firstParty: string;
  /** The provider-specific wire id to invoke on the detected provider (e.g. a
   *  Bedrock region-scoped inference profile), or the canonical id unchanged for a
   *  first-party / Vertex / Foundry endpoint. */
  wireId: string;
}

/**
 * Per-provider canonical model-id table: for every logical short key in
 * {@link MODEL_SHORT_KEYS}, the canonical first-party id AND the concrete wire id
 * to invoke on the provider `config` detects. Mirrors Claude Code's
 * ALL_MODEL_CONFIGS / getBuiltinModelStrings(provider) — one logical ModelKey
 * mapped to a provider-specific id string — so the same logical model (`opus46`)
 * resolves to the right per-provider id without the user retyping it when they
 * switch providers.
 *
 * AMI realizes this through its SINGLE central send-path remap rather than N
 * hardcoded per-provider id tables: `wireId` is produced by the one canonical
 * {@link toProviderModelId} (which routes Bedrock ids through
 * {@link resolveBedrockModelId} against the account's inference profiles / region
 * prefix, and passes first-party / Vertex / Foundry ids through unchanged). No
 * parallel per-provider id table is introduced — this is the aggregated VIEW of the
 * existing resolution across every logical key, surfaced for selection UIs and
 * `ami --introspect`.
 */
/**
 * Build the per-provider model-string rows for `config` from the supplied Bedrock
 * inference-profile list (used verbatim when the detected provider is Bedrock; the
 * canonical first-party id passes through unchanged otherwise, via the single
 * {@link toProviderModelId} remap — no parallel per-provider id table). Factored so
 * the synchronous {@link getBuiltinModelStrings} view and the async
 * {@link ensureModelStringsInitialized} barrier produce byte-identical rows from the
 * SAME mapping — the barrier simply sources the profile list from an awaited (and
 * cacheable) resolver instead of a point-in-time env read.
 */
function buildModelStrings(config: ProviderConfig, bedrockProfiles: string[]): ProviderModelString[] {
  return Object.entries(MODEL_SHORT_KEYS).map(([key, firstParty]) => ({
    key,
    firstParty,
    wireId: toProviderModelId(firstParty, config, bedrockProfiles),
  }));
}

// ---------------------------------------------------------------------------
// Model-string initialization barrier — parity with Claude Code's
// ensureModelStringsInitialized / getModelStrings pair (utils/model/modelStrings.ts
// L136-L166). Building a model-option list for a Bedrock account should reflect the
// region-scoped inference-profile strings rather than interim defaults; Claude awaits
// its profile fetch before generating options. AMI sources Bedrock profiles through
// the single canonical resolver (bedrock.ts getBedrockInferenceProfiles) behind an
// injectable async seam, caches the resolved snapshot ONCE per provider+region+
// profile signature (sequential-guarded so a concurrent burst shares one resolution),
// and serves subsequent builds from that snapshot. getBuiltinModelStrings returns the
// cached snapshot when initialized and otherwise computes the interim default inline
// (mirrors Claude's getModelStrings falling through to built-in strings while the
// fetch is pending). The SINGLE model-string init path — no parallel state module.
// ---------------------------------------------------------------------------

/**
 * Injectable async source of the account's Bedrock inference-profile ids, awaited by
 * {@link ensureModelStringsInitialized} before building the Bedrock model-string
 * snapshot. Defaults to the synchronous env-backed {@link getBedrockInferenceProfiles}
 * (AMI's one-central-remap design keeps profiles in the environment), wrapped in a
 * resolved promise so the barrier's await is uniform. A test — or a future live
 * profile fetch — can replace it to make the Bedrock readiness await observable.
 */
export const modelStringsInitHooks: { resolveBedrockProfiles: () => Promise<string[]> } = {
  resolveBedrockProfiles: async () => getBedrockInferenceProfiles(),
};

/** Signature keying a cached model-string snapshot: provider + region + profiles. */
function modelStringsSignature(config: ProviderConfig): string {
  return [
    detectProvider(config),
    getBedrockRegionPrefix() ?? '',
    getBedrockInferenceProfiles().join(','),
  ].join(':::');
}

const modelStringsCache = new Map<string, ProviderModelString[]>();
const modelStringsInFlight = new Map<string, Promise<ProviderModelString[]>>();

/**
 * Await-until-ready barrier that resolves the Bedrock inference profiles (via the
 * {@link modelStringsInitHooks} seam) and caches the resulting per-provider
 * model-string snapshot before it is used to build a model-option list — so a Bedrock
 * selection UI shows region/profile-correct strings rather than interim defaults
 * (parity: Claude Code's ensureModelStringsInitialized). A snapshot already cached for
 * the current signature returns immediately WITHOUT re-resolving (the idempotent fast
 * path), and concurrent callers for the same signature share a single in-flight
 * resolution (sequential-guarded). For a non-Bedrock provider the resolver returns an
 * empty profile list and the snapshot is the canonical ids unchanged. Returns the
 * resolved snapshot. Awaited by {@link listModels} / {@link listModelsDiagnostic} on
 * the Bedrock static-list path.
 */
export async function ensureModelStringsInitialized(config: ProviderConfig): Promise<ProviderModelString[]> {
  const sig = modelStringsSignature(config);
  const cached = modelStringsCache.get(sig);
  if (cached) return cached;
  const existing = modelStringsInFlight.get(sig);
  if (existing) return existing;
  const pending = (async () => {
    const isBedrock = toApiProviderClass(detectProvider(config)) === 'bedrock';
    // Only the Bedrock path has a profile fetch to await; every other provider
    // resolves synchronously with an empty profile list (canonical ids unchanged).
    const profiles = isBedrock ? await modelStringsInitHooks.resolveBedrockProfiles() : [];
    const strings = buildModelStrings(config, profiles);
    modelStringsCache.set(sig, strings);
    return strings;
  })().finally(() => { modelStringsInFlight.delete(sig); });
  modelStringsInFlight.set(sig, pending);
  return pending;
}

export function getBuiltinModelStrings(config: ProviderConfig): ProviderModelString[] {
  // Serve the initialized snapshot when the barrier has run for this signature;
  // otherwise compute the interim default inline from the current env (parity with
  // Claude's getModelStrings falling through to built-in strings pre-init).
  const cached = modelStringsCache.get(modelStringsSignature(config));
  if (cached) return cached;
  return buildModelStrings(config, getBedrockInferenceProfiles());
}

/**
 * True when `modelName` begins with `prefix` at a SEGMENT BOUNDARY — the match
 * must reach the end of the name or a '-' separator. So "claude-opus-4-5"
 * matches "claude-opus-4-5-20251101" but NOT "claude-opus-4-50". Mirrors Claude
 * Code's prefixMatchesModel (utils/model/modelAllowlist.ts).
 */
function prefixMatchesModel(modelName: string, prefix: string): boolean {
  if (!modelName.startsWith(prefix)) return false;
  return modelName.length === prefix.length || modelName[prefix.length] === '-';
}

/**
 * True when `model` matches a version-prefix `entry` with segment-boundary
 * safety and 'claude-' shorthand: an entry like "opus-4-5" is auto-expanded to
 * "claude-opus-4-5" so it matches "claude-opus-4-5-20251101" while refusing
 * "claude-opus-4-50". Case-insensitive. Mirrors Claude Code's
 * modelMatchesVersionPrefix — the single boundary-aware matcher reused by the
 * validateModel suggestion path (and available to the engine allowlist check so
 * admins can pin a version family without accidentally matching a sibling).
 */
export function modelMatchesVersionPrefix(model: string, entry: string): boolean {
  const m = model.toLowerCase();
  const e = entry.toLowerCase();
  if (prefixMatchesModel(m, e)) return true;
  if (!e.startsWith('claude-') && prefixMatchesModel(m, `claude-${e}`)) return true;
  return false;
}

/**
 * Order candidate model ids by CLOSENESS to `target`, closest first. Closeness is
 * the absolute length delta (|id.length − target.length|) — the tightest
 * super/substring of the requested id — tie-broken by shorter id, then
 * case-insensitive lexicographic order for stable, deterministic output. Returns a
 * NEW array (never mutates the input). Used by {@link validateModel} to pick the
 * best "did you mean" suggestion from a match tier instead of the provider's
 * arbitrary list order.
 */
export function rankClosestMatches(candidates: string[], target: string): string[] {
  const t = target.length;
  return [...candidates].sort((a, b) => {
    const da = Math.abs(a.length - t);
    const db = Math.abs(b.length - t);
    if (da !== db) return da - db;
    if (a.length !== b.length) return a.length - b.length;
    return a.toLowerCase().localeCompare(b.toLowerCase());
  });
}

export async function validateModel(
  config: ProviderConfig,
  modelId: string,
  signal?: AbortSignal,
): Promise<ValidateModelResult> {
  // Normalize the argument up front so leading/trailing whitespace does not
  // silently defeat exact matching, and an empty/whitespace id gets a clear,
  // self-contained error rather than falling through to the substring branch
  // (where `id.includes('')` is always true and every model "matches").
  const normalized = (modelId ?? '').trim();
  if (normalized === '') {
    return { valid: false, available: [], reason: 'Model name cannot be empty', errorKind: 'none' };
  }

  // A recognized short alias (opus/sonnet/haiku/best/opusplan/[1m]) resolves to
  // its canonical family id, so the rest of this function matches/suggests as if
  // the user had typed that id. AMI resolves the alias to a concrete id rather
  // than blindly accepting the bare alias (Claude passes it through verbatim),
  // because AMI's providers require a concrete model id — resolving here avoids
  // validating-then-persisting an unusable bare alias like "opus".
  const effective = resolveModelAlias(normalized) ?? normalized;

  // A user-declared trusted id is accepted without any provider round-trip.
  if (customModelIds().some(c => c.toLowerCase() === effective.toLowerCase())) {
    return { valid: true, available: [], errorKind: 'none' };
  }

  const cacheKey = `${detectProvider(config)}:::${config.baseUrl ?? ''}:::${effective}`;
  if (validModelCache.has(cacheKey)) return { valid: true, available: [] };

  const { models, diagnostic } = await listModelsDiagnostic(config, signal);

  if (models.length === 0) {
    const errorKind = classifyDiscoveryDiagnostic(diagnostic);
    // Authentication failure is definitive — the model cannot be used with these
    // credentials, so do NOT fail open. A transient network/API error is soft:
    // keep valid=true so a blip doesn't block a switch, but surface the reason.
    if (errorKind === 'auth') {
      return { valid: false, available: [], reason: diagnostic ?? undefined, errorKind };
    }
    return { valid: true, available: [], reason: diagnostic ?? undefined, errorKind };
  }

  const ids = models.map(m => m.id);
  // Exact match first, then a case-insensitive exact match (providers differ in
  // id casing), before falling back to substring matching.
  const exact = ids.find(id => id === effective)
    ?? ids.find(id => id.toLowerCase() === effective.toLowerCase());
  if (exact) {
    validModelCache.add(cacheKey);
    return { valid: true, available: ids, errorKind: 'none' };
  }

  // Suggestion ranking: prefer a SEGMENT-BOUNDARY version-prefix match so a
  // precise request ("claude-opus-4-5") resolves to the dated build
  // ("claude-opus-4-5-20251101") and never a sibling ("claude-opus-4-50-preview").
  // Only when no boundary match exists do we fall back to the looser substring
  // "did you mean" (e.g. "gpt-4" → "gpt-4o"), then to a deprecated/renamed-id
  // replacement for ids with no overlap with any live model at all.
  const boundary = ids.filter(id =>
    modelMatchesVersionPrefix(id, effective) || modelMatchesVersionPrefix(effective, id),
  );
  const partial = boundary.length > 0
    ? boundary
    : ids.filter(id => id.includes(effective) || effective.includes(id));
  // FUNCTIONAL (additive): within the chosen match tier, rank the candidates so the
  // CLOSEST id wins instead of the provider's arbitrary list order (the old
  // `partial[0]`). "Closest" = smallest length delta from the requested id (the
  // tightest super/substring), tie-broken by shorter id then lexicographically for
  // determinism. So `gpt-4` against `[gpt-4o, gpt-4o-mini]` suggests `gpt-4o`, not
  // whichever the endpoint happened to return first.
  const ranked = rankClosestMatches(partial, effective);
  // Ranking of the "did you mean" suggestion when there is no match:
  //  1. a boundary/substring partial (handled above),
  //  2. a RETIRED-id successor (suggestDeprecatedReplacement maps old → new),
  //  3. a too-new family id stepped DOWN to the prior generation the endpoint is
  //     likelier to carry (suggestFamilyVersionFallback — parity with Claude's
  //     get3PModelFallbackSuggestion for third-party/self-hosted providers). This
  //     step-down is INTENTIONALLY third-party-only (mirrors get3PModelFallbackSuggestion,
  //     which self-gates on isFirstPartyProvider): on a first-party endpoint a
  //     too-new id has no "prior generation the gateway likely carries" rationale,
  //     so we leave the suggestion undefined rather than nudge a downgrade.
  const suggestion = ranked.length > 0
    ? ranked[0]
    : (suggestDeprecatedReplacement(effective, ids)
        ?? (isFirstPartyProvider() ? undefined : suggestFamilyVersionFallback(effective, ids)));

  return { valid: false, available: ids, suggestion, errorKind: 'none' };
}

// ---------------------------------------------------------------------------
// Human-readable model metadata — friendly display names + one-line
// descriptions for the /model picker and status line. Mirrors Claude Code's
// getPublicModelDisplayName (utils/model/model.ts:L344-L415) + the per-option
// label/description copy in utils/model/modelOptions.ts (L96-L209, L431-L459).
// AMI resolves these from the RAW provider model id with pattern matching (no
// provider-specific model-string table) so a dated snapshot
// ("claude-sonnet-4-5-20251022"), a gateway-prefixed id
// ("openrouter/anthropic/claude-sonnet-4.6"), the `[1m]` long-context suffix, or
// a bare family id all resolve to the SAME friendly label. Unknown ids fall back
// to the raw id itself — Claude's renderModelName default branch (a marketing
// label for a raw model id that is always a usable string).
// ---------------------------------------------------------------------------

/** Family tier → one-line picker description (Claude's modelOptions copy). */
const MODEL_FAMILY_DESCRIPTION = {
  opus: 'Most capable — best for complex, multi-step work',
  sonnet: 'Balanced — best for everyday coding tasks',
  haiku: 'Fastest and lowest cost — best for quick answers',
  gpt: 'OpenAI frontier model',
  gemini: 'Google Gemini model',
} as const;

type ModelFamily = keyof typeof MODEL_FAMILY_DESCRIPTION;

/**
 * Ordered display-name patterns, most-specific (versioned) first so a precise
 * version wins over the bare family. Each entry also carries the family tier its
 * description comes from.
 */
const MODEL_DISPLAY_PATTERNS: ReadonlyArray<{ pattern: RegExp; name: string; family: ModelFamily }> = [
  // Claude — Opus
  { pattern: /opus-?4[._-]?6/i, name: 'Opus 4.6', family: 'opus' },
  { pattern: /opus-?4[._-]?5/i, name: 'Opus 4.5', family: 'opus' },
  { pattern: /opus-?4[._-]?1/i, name: 'Opus 4.1', family: 'opus' },
  { pattern: /claude-?3[._-]?opus/i, name: 'Claude 3 Opus', family: 'opus' },
  { pattern: /opus-?4/i, name: 'Opus 4', family: 'opus' },
  { pattern: /\bopus\b/i, name: 'Opus', family: 'opus' },
  // Claude — Sonnet
  { pattern: /sonnet-?4[._-]?6/i, name: 'Sonnet 4.6', family: 'sonnet' },
  { pattern: /sonnet-?4[._-]?5/i, name: 'Sonnet 4.5', family: 'sonnet' },
  { pattern: /3[._-]?7[._-]?sonnet/i, name: 'Sonnet 3.7', family: 'sonnet' },
  { pattern: /3[._-]?5[._-]?sonnet/i, name: 'Sonnet 3.5', family: 'sonnet' },
  { pattern: /sonnet-?4/i, name: 'Sonnet 4', family: 'sonnet' },
  { pattern: /\bsonnet\b/i, name: 'Sonnet', family: 'sonnet' },
  // Claude — Haiku
  { pattern: /haiku-?4[._-]?5/i, name: 'Haiku 4.5', family: 'haiku' },
  { pattern: /3[._-]?5[._-]?haiku/i, name: 'Haiku 3.5', family: 'haiku' },
  { pattern: /\bhaiku\b/i, name: 'Haiku', family: 'haiku' },
  // OpenAI
  { pattern: /gpt-?6/i, name: 'GPT-6', family: 'gpt' },
  { pattern: /gpt-?5/i, name: 'GPT-5', family: 'gpt' },
  { pattern: /gpt-?4o-?mini/i, name: 'GPT-4o mini', family: 'gpt' },
  { pattern: /gpt-?4o/i, name: 'GPT-4o', family: 'gpt' },
  { pattern: /gpt-?4/i, name: 'GPT-4', family: 'gpt' },
  // Google Gemini
  { pattern: /gemini-?3/i, name: 'Gemini 3', family: 'gemini' },
  { pattern: /gemini-?2[._-]?5[._-]?pro/i, name: 'Gemini 2.5 Pro', family: 'gemini' },
  { pattern: /gemini-?2[._-]?5[._-]?flash/i, name: 'Gemini 2.5 Flash', family: 'gemini' },
];

/** Strip the optional `[1m]` long-context suffix; report whether it was present. */
function splitOneMillionSuffix(id: string): { base: string; is1m: boolean } {
  const is1m = /\[1m\]$/i.test(id);
  return { base: is1m ? id.replace(/\[1m\]$/i, '') : id, is1m };
}

function matchModelPattern(id: string): { name: string; family: ModelFamily } | undefined {
  return MODEL_DISPLAY_PATTERNS.find(e => e.pattern.test(id));
}

/**
 * Reverse of `applyModelOverride` (common/provider-config): given a
 * provider-specific override STRING that was swapped in from the settings
 * `modelOverrides` map (e.g. an opaque Bedrock inference-profile ARN) and that
 * same map, return the CANONICAL first-party model id it was an override for — so
 * the UI can display the clean "claude-opus-4-6" instead of the raw ARN the
 * engine actually sends. Scans the override VALUES (not keys) for an exact match
 * and returns the matching canonical key; returns the input unchanged when there
 * is no override map or no value matches. Mirrors Claude Code's
 * resolveOverriddenModel (utils/model/modelStrings.ts:L78-L100) — the single
 * inverse of the forward apply, so the display path and the send path stay in
 * lockstep (no parallel ARN→name table).
 */
export function resolveOverriddenModel(
  modelString: string,
  overrides: Record<string, string> | undefined,
): string {
  if (!overrides) return modelString;
  for (const [canonicalId, override] of Object.entries(overrides)) {
    if (override === modelString) return canonicalId;
  }
  return modelString;
}

/**
 * Friendly, human-readable display name for a model id ("Sonnet 4.6",
 * "GPT-4o"). Recognizes dated snapshots, gateway prefixes, and the `[1m]`
 * long-context suffix (rendered as " (1M context)"). Falls back to the raw id so
 * the return value is ALWAYS a non-empty, user-presentable string for any known
 * OR custom model — the marketing-label-for-a-raw-id affordance. When `overrides`
 * (the settings `modelOverrides` map) is supplied, a raw override string (e.g. a
 * Bedrock ARN) is first mapped back to its canonical id via
 * {@link resolveOverriddenModel} so the override's clean family name is shown.
 */
export function getModelDisplayName(modelId: string, overrides?: Record<string, string>): string {
  const id = resolveOverriddenModel((modelId ?? '').trim(), overrides);
  if (id === '') return '';
  const { base, is1m } = splitOneMillionSuffix(id);
  const hit = matchModelPattern(base);
  if (!hit) return id;
  return is1m ? `${hit.name} (1M context)` : hit.name;
}

/**
 * One-line, human-readable description for the /model picker, or undefined when
 * the model id is not recognized as a known family (an unknown/custom id has no
 * canned marketing copy). Mirrors Claude's per-option `description` field.
 */
export function getModelDescription(modelId: string): string | undefined {
  const { base } = splitOneMillionSuffix((modelId ?? '').trim());
  const hit = matchModelPattern(base);
  return hit ? MODEL_FAMILY_DESCRIPTION[hit.family] : undefined;
}

/**
 * The current (newest) concrete model id per Claude family. A pinned model whose
 * display name differs from its family's current model is an OLDER version, so the
 * picker can surface a "newer version available" upgrade affordance. Mirrors Claude
 * Code's getDefault{Opus,Sonnet,Haiku}Model used by getModelFamilyInfo
 * (utils/model/modelOptions.ts:L385-L459); kept in sync with the newest entries of
 * {@link MODEL_SHORT_KEYS}.
 */
const FAMILY_CURRENT_MODEL: Record<'opus' | 'sonnet' | 'haiku', string> = {
  opus: 'claude-opus-4-6',
  sonnet: 'claude-sonnet-4-6',
  haiku: 'claude-haiku-4-5',
};

/** Capitalized family alias to name in the upgrade hint ("Opus"/"Sonnet"/"Haiku"). */
const FAMILY_ALIAS_LABEL: Record<'opus' | 'sonnet' | 'haiku', string> = {
  opus: 'Opus', sonnet: 'Sonnet', haiku: 'Haiku',
};

/**
 * Upgrade affordance for a PINNED OLDER model: when `modelId` resolves to a known
 * Claude family (opus/sonnet/haiku) whose current version is a DIFFERENT (newer)
 * build than the pinned one, returns
 * `Newer version available · select <Alias> for <CurrentVersionName>` — otherwise
 * undefined (unknown id, non-Claude family, or already the current version).
 * Mirrors Claude Code's getKnownModelOption upgrade-hint description
 * (utils/model/modelOptions.ts:L431-L459). Surfaced by the /model picker via
 * {@link getModelMetadata}.
 */
export function getNewerVersionHint(modelId: string): string | undefined {
  const { base } = splitOneMillionSuffix((modelId ?? '').trim());
  const hit = matchModelPattern(base);
  if (!hit) return undefined;
  const family = hit.family;
  if (family !== 'opus' && family !== 'sonnet' && family !== 'haiku') return undefined;
  const current = FAMILY_CURRENT_MODEL[family];
  const currentName = getModelDisplayName(current);
  // Same build as the family's current version → nothing to upgrade to.
  if (hit.name === currentName) return undefined;
  return `Newer version available · select ${FAMILY_ALIAS_LABEL[family]} for ${currentName}`;
}

/**
 * Combined picker annotation for a model id: a friendly `displayName` (always a
 * usable string), an optional `description`, and an optional `upgrade` hint when a
 * newer version of the pinned model's family is available. Mirrors Claude's
 * ModelOption `{ label, description }` so a picker row can show a human label + a
 * sub-line (and an upgrade nudge) instead of a bare id.
 */
export function getModelMetadata(
  modelId: string,
  overrides?: Record<string, string>,
): { displayName: string; description?: string; upgrade?: string } {
  // Resolve a settings-override string (e.g. a Bedrock ARN) back to its canonical
  // id ONCE, so the description + upgrade hint also key off the clean id rather
  // than the opaque override the engine sends to the provider.
  const resolved = resolveOverriddenModel((modelId ?? '').trim(), overrides);
  return {
    displayName: getModelDisplayName(resolved),
    description: getModelDescription(resolved),
    upgrade: getNewerVersionHint(resolved),
  };
}

/** One selectable row in the agent/model-choice catalog. */
export interface AgentModelOption {
  /** The value a selection persists — a short family alias, or a custom full id. */
  value: string;
  /** Friendly display label ("Opus 4.6") for the row. */
  label: string;
  /** Optional one-line description shown under the label. */
  description?: string;
}

/**
 * Curated agent-model option catalog: one {value,label,description} row per
 * accepted family alias (the single {@link MODEL_ALIASES} set), with the human
 * copy resolved through {@link getModelDisplayName} / {@link getModelDescription}
 * (no parallel label table). When `currentModel` is a concrete id that is NOT one
 * of the alias values — a custom or pinned full model id — it is injected at the
 * HEAD of the list labeled "Current model (custom ID)", so an edit flow round-trips
 * that custom id instead of silently overwriting it with an alias. Mirrors Claude
 * Code's getAgentModelOptions + ModelSelector custom-id injection
 * (components/agents/ModelSelector.tsx:L21-L38). The SINGLE source of the agent
 * model-choice catalog — consumed by the config tool's model section so the model
 * sees the same alias set the picker offers (consolidation: replaces the inline
 * MODEL_ALIASES listing the config tool built by hand).
 */
export function getAgentModelOptions(currentModel?: string): AgentModelOption[] {
  const base: AgentModelOption[] = MODEL_ALIASES.map((alias) => {
    const canonical = resolveModelAlias(alias) ?? alias;
    const description = getModelDescription(canonical);
    const row: AgentModelOption = { value: alias, label: getModelDisplayName(canonical) };
    if (description) row.description = description;
    return row;
  });
  const current = (currentModel ?? '').trim();
  // A custom/pinned full id (not one of the alias values) rounds-trips as its own
  // head-of-list option so selecting it again preserves the exact id.
  if (current !== '' && !base.some(o => o.value === current)) {
    return [
      { value: current, label: getModelDisplayName(current), description: 'Current model (custom ID)' },
      ...base,
    ];
  }
  return base;
}

/**
 * Canonical display string for an agent / subagent model CHOICE, so every agent
 * surface (the `/agents` list + detail renderers, any model picker) formats a
 * stored `model` value the SAME way instead of printing the raw token. Mirrors
 * Claude Code's `getAgentModelDisplay` (utils/model/agent.ts:L124-L131): an absent
 * value is the inherit-by-default case ("Inherit from parent (default)"), the
 * explicit "inherit" sentinel is "Inherit from parent", and a concrete alias / id
 * is resolved through {@link resolveModelAlias} + {@link getModelDisplayName} to its
 * friendly name (falling back to the raw id when unrecognized). The SINGLE source
 * for rendering an agent's model choice — pairs with {@link getAgentModelOptions}
 * (the selectable catalog) so the picker and the display agree on wording.
 */
export function getAgentModelDisplay(model?: string | null): string {
  const raw = (model ?? '').trim();
  if (raw === '') return 'Inherit from parent (default)';
  if (raw.toLowerCase() === 'inherit') return 'Inherit from parent';
  const canonical = resolveModelAlias(raw) ?? raw;
  const display = getModelDisplayName(canonical);
  return display === '' ? raw : display;
}

/**
 * One row of the canonical {@link MODEL_OVERRIDES_AFFORDANCE} descriptor: the
 * settings key, the map shape, the config scopes that may declare it, whether the
 * override preserves capability resolution, and a human-readable description.
 */
export interface ModelOverridesAffordance {
  /** The settings.json key users set to declare overrides. */
  settingsKey: string;
  /** The value shape the key accepts (canonical model id → provider string). */
  valueShape: string;
  /** Config waterfall scopes that may declare the map (last-writer wins). */
  scopes: readonly string[];
  /** True: the CANONICAL id still drives every context-window / output-ceiling
   *  lookup — only the string SENT to the provider is overridden. */
  capabilityPreserving: boolean;
  /** One-paragraph human description of the affordance for the manifest. */
  description: string;
}

/**
 * Canonical introspection descriptor for the settings-driven `modelOverrides`
 * affordance (parity: Claude Code's applyModelOverrides, utils/model/
 * modelStrings.ts:L57-L76). The resolution/display path for `modelOverrides`
 * already exists end to end — the `modelOverrides` settings key
 * ({@link ProviderConfig}), its config-waterfall loader + forward apply
 * (common/provider-config: loadModelOverrides / applyModelOverride on the send
 * path), the inverse map ({@link resolveOverriddenModel} / provider.ts'
 * CANONICAL_ID_TO_KEY), and the `/model` picker's clean-name display. What was
 * MISSING is the repo-mandated INTROSPECTION surface describing the knob: this
 * single descriptor is surfaced in `ami --introspect` (modelOverrides) so the
 * affordance — its key, map shape, scopes, and capability-preserving semantics —
 * is discoverable, closing the "no /config or introspection surface describing it"
 * parity gap. The SINGLE human-facing description of the knob (no parallel copy).
 */
export const MODEL_OVERRIDES_AFFORDANCE: ModelOverridesAffordance = {
  settingsKey: 'modelOverrides',
  valueShape: 'Record<canonicalModelId, providerModelString>',
  scopes: ['managed', 'project', 'global', 'local'],
  capabilityPreserving: true,
  description:
    'Map a canonical first-party model id to an arbitrary provider-specific model string ' +
    '(typically a Bedrock inference-profile ARN, or any gateway-specific deployment id). ' +
    'The canonical id still drives every capability / context-window / output-ceiling ' +
    'lookup; only the string SENT to the provider is overridden. A model may be pinned by ' +
    'EITHER its canonical id or the override string and gets identical capability-correct routing.',
};

export function formatModelList(models: string[], provider: string): string {
  if (models.length === 0) return 'Could not retrieve model list from provider.';

  const lines = models.map(m => `  ${m}`);
  const header = `Available models (${provider}, ${models.length}):`;

  return `${header}\n${lines.join('\n')}`;
}
