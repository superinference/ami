import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import type { ProviderConfig } from './types';
import { MODEL_PREFERENCE } from './model-preference';
import {
  discoverFromModelList,
  modelCapabilitiesCacheDir,
  warmModelCapabilitiesFromCache,
  writeModelCapabilitiesCache,
  discoveryToCacheEntries,
  isFirstPartyProvider,
  toApiProviderClass,
} from './model-capabilities';
import { getBedrockInferenceProfiles } from './bedrock';
import './tools/tool-utils';
import { isEssentialTrafficOnly } from './analytics';
import { log } from './logger';
import './config';
import { parseRetryAfterHeader } from './rate-limit-parser';
import { MODEL_LIST_BASE_DELAY_MS, MODEL_LIST_MAX_ATTEMPTS, classifyDiscoveryDiagnostic, connectivityProbeTarget, customModelIds, detectProvider, diagnoseModelDiscovery, extractErrorDetail, isTransientModelListError, makeAbortError, modelListTimeoutMs, providerKeyHint, sanitizeHeaderValue, staticModelList, suggestDeprecatedReplacement } from './model-registry-detect-provider';
import type { ModelInfo, ProviderConnectivityResult, ValidateModelResult } from './model-registry-detect-provider';
import { buildModelStrings, modelMatchesVersionPrefix, modelStringsInitHooks, modelStringsSignature, rankClosestMatches, resolveModelAlias, suggestFamilyVersionFallback } from './model-registry-model-env-toggles';
import type { ProviderModelString } from './model-registry-model-env-toggles';
import type { ModelOverridesAffordance } from './model-registry-get-agent-model-options';
export { LEGACY_MODEL_REMAP_OPT_OUT_ENV, detectProvider, diagnoseModelDiscovery, extractErrorDetail, isLegacyModelRemapDisabled, listProviders, sanitizeHeaderValue, suggestDeprecatedReplacement } from './model-registry-detect-provider';
export type { ProviderConnectivityResult, ValidateModelResult } from './model-registry-detect-provider';
export { MODEL_ALIASES, MODEL_ENV_TOGGLES, MODEL_FAMILY_ALIASES, MODEL_SHORT_KEYS, getDefaultSubagentModel, getRuntimeMainLoopModel, isModelAlias, isModelFamilyAlias, isModelShortKey, modelMatchesVersionPrefix, modelStringsInitHooks, rankClosestMatches, resolveModelAlias, suggestFamilyVersionFallback } from './model-registry-model-env-toggles';
export type { ModelAlias, ProviderModelString, RuntimeMainLoopModelRequest } from './model-registry-model-env-toggles';
export { formatModelList, getAgentModelDisplay, getAgentModelOptions, getModelDescription, getModelDescriptionForModel, getModelDisplayName, getModelMetadata, getNewerVersionHint, resolveOverriddenModel } from './model-registry-get-agent-model-options';
export type { AgentModelOption, ModelOverridesAffordance } from './model-registry-get-agent-model-options';

const STATIC_MODEL_PROVIDERS = new Set([
  'anthropic', 'anthropic-vertex', 'google-vertex', 'azure-openai',
  'amazon-bedrock', 'groq', 'mistral', 'xai', 'deepseek',
  'togetherai', 'cohere', 'fireworks', 'perplexity', 'deepinfra',
  'cerebras', 'alibaba', 'luma', 'huggingface',
]);

/**
 * Low-level JSON GET for model discovery / connectivity probing.
 *
 * FUNCTIONAL (additive): accepts an optional {@link AbortSignal}. When supplied,
 * the in-flight request is torn down the instant the signal fires (and an already
 * -aborted signal rejects synchronously before any socket is opened) so a cancelled
 * `/model` listing / `/doctor connectivity` probe stops immediately instead of
 * running to the 10s timeout. Mirrors the reference's signal-threaded network waits
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
          // the reference's withRetry Retry-After handling). Header names are
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
    // interim defaults (parity: the reference's ensureModelStringsInitialized before
    // generating model options). A no-op await for every other static provider.
    if (toApiProviderClass(provider) === 'bedrock') await ensureModelStringsInitialized(config);
    return staticModelList(provider);
  }

  // Respect the essential-traffic-only privacy opt-out: a live model-list
  // discovery request is nonessential background traffic, so fall back to the
  // built-in static list instead of reaching the network. Mirrors the reference's
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
    // option list is built (parity: the reference's ensureModelStringsInitialized).
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
      // the reference's model-capability debug lines ('cache unchanged, skipping
      // write'), alongside 'cached N models' and 'fetch failed' below/above.
      const wrote = writeModelCapabilitiesCache(cacheDir, entries);
      if (!wrote) log('modelCapabilities', '[modelCapabilities] cache unchanged, skipping write');
    }
  }
  // Debug/introspection: report the outcome of capability discovery (how many
  // models the endpoint returned and how many advertised a context window), so
  // operators can see which models/windows were learned. Mirrors the reference's
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
// In-flight listing coalescing — mirrors the reference's `sequential()`-guarded,
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
// Mirrors the reference's module-level validModelCache.
const validModelCache = new Set<string>();

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
    // failure. Mirrors the reference's connectivity-vs-config distinction.
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

const modelStringsCache = new Map<string, ProviderModelString[]>();
const modelStringsInFlight = new Map<string, Promise<ProviderModelString[]>>();

/**
 * Await-until-ready barrier that resolves the Bedrock inference profiles (via the
 * {@link modelStringsInitHooks} seam) and caches the resulting per-provider
 * model-string snapshot before it is used to build a model-option list — so a Bedrock
 * selection UI shows region/profile-correct strings rather than interim defaults
 * (parity: the reference's ensureModelStringsInitialized). A snapshot already cached for
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
  // the reference's getModelStrings falling through to built-in strings pre-init).
  const cached = modelStringsCache.get(modelStringsSignature(config));
  if (cached) return cached;
  return buildModelStrings(config, getBedrockInferenceProfiles());
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
  // than blindly accepting the bare alias (the reference passes it through verbatim),
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
  //     likelier to carry (suggestFamilyVersionFallback — parity with the reference's
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

/**
 * Canonical introspection descriptor for the settings-driven `modelOverrides`
 * affordance (parity: the reference's applyModelOverrides, utils/model/
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
