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
} from './model-capabilities';
import { redactSecrets } from './tools/tool-utils';
import { isEssentialTrafficOnly } from './analytics';
import { log } from './logger';
import { validateBoundedIntEnvVar } from './config';

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

function httpGetJSON(
  url: string,
  headers: Record<string, string>,
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const requester = parsed.protocol === 'https:' ? https : http;

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
          const httpErr = new Error(`HTTP ${status}`) as Error & { statusCode: number; detail?: string };
          httpErr.statusCode = status;
          if (detail) httpErr.detail = detail;
          reject(httpErr);
          return;
        }
        try {
          resolve(JSON.parse(bodyText));
        } catch {
          reject(new Error('Invalid JSON response'));
        }
      });
      res.on('error', reject);
    });

    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', reject);
  });
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

export async function listModels(config: ProviderConfig): Promise<ModelInfo[]> {
  const provider = detectProvider(config);

  // Warm the in-memory context-window / output-cap tables from the persisted
  // capability cache (opt-in via SUPERINFERENCE_MODEL_CACHE_DIR) so even a cold
  // start — or a run whose live /models fetch is skipped/fails below — is sized
  // from the last-known advertised windows. A successful live discovery still wins.
  const cacheDir = modelCapabilitiesCacheDir();
  if (cacheDir) warmModelCapabilitiesFromCache(cacheDir);

  // Providers with known static model lists (no API listing endpoint)
  if (STATIC_MODEL_PROVIDERS.has(provider)) {
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
    if (provider === 'google') {
      return await listGeminiModels(config);
    }
    // OpenAI-compatible: OpenAI, Ollama, OpenRouter
    return await listOpenAIModels(config);
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
): Promise<{ models: ModelInfo[]; diagnostic: string | null }> {
  const provider = detectProvider(config);

  if (STATIC_MODEL_PROVIDERS.has(provider)) {
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
    const models = provider === 'google'
      ? await listGeminiModels(config)
      : await listOpenAIModels(config);
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

async function listOpenAIModels(config: ProviderConfig): Promise<ModelInfo[]> {
  if (!config.baseUrl) throw new Error('baseUrl is required for OpenAI-compatible model listing');
  const baseUrl = config.baseUrl.replace(/\/+$/, '');
  const url = `${baseUrl}/models`;
  const headers: Record<string, string> = {};
  if (config.apiKey) headers['Authorization'] = `Bearer ${config.apiKey}`;

  const data = await httpGetJSON(url, headers) as { data?: ModelInfo[] };
  const models = (data.data || []).map(m => ({
    id: m.id,
    name: m.id,
    owned_by: m.owned_by,
  }));
  const discovery = discoverFromModelList(models as unknown as Record<string, unknown>[]);
  // Persist the freshly-advertised windows/caps to the on-disk cache (opt-in via
  // SUPERINFERENCE_MODEL_CACHE_DIR) so the next cold start can warm them without a
  // round-trip. Skipped automatically when the model set is unchanged (no mtime churn).
  const cacheDir = modelCapabilitiesCacheDir();
  if (cacheDir) {
    const entries = discoveryToCacheEntries(discovery);
    if (entries.length > 0) writeModelCapabilitiesCache(cacheDir, entries);
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

async function listGeminiModels(config: ProviderConfig): Promise<ModelInfo[]> {
  // Default to the public Gemini endpoint; overridable via env (e.g. an offline
  // mirror, or a test server) without changing the default behavior.
  const url = process.env.SI_GEMINI_MODELS_URL || 'https://generativelanguage.googleapis.com/v1beta/models';
  const data = await httpGetJSON(url, { 'x-goog-api-key': config.apiKey }) as { models?: Array<{ name: string; displayName: string }> };
  return (data.models || []).map(m => ({
    id: m.name.replace('models/', ''),
    name: m.displayName,
  }));
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
 * True when the legacy-model remap is opted out via
 * CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP (parity with Claude Code's env opt-out,
 * useModelMigrationNotifications.tsx:L24-L29). Accepts the usual truthy spellings
 * (1/true/yes/on, case-insensitive, trimmed). The SINGLE source of the opt-out
 * decision, consumed by both the advisory {@link suggestDeprecatedReplacement}
 * suggestion and the persisted-settings startup auto-migration
 * (reference-journal's migrateDeprecatedModelAlias) so the knob governs every
 * legacy-id remap path identically.
 */
export function isLegacyModelRemapDisabled(): boolean {
  const v = (process.env.CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP ?? '').trim().toLowerCase();
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

export async function validateModel(
  config: ProviderConfig,
  modelId: string,
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

  const { models, diagnostic } = await listModelsDiagnostic(config);

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
  const suggestion = partial.length > 0
    ? partial[0]
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
 * Friendly, human-readable display name for a model id ("Sonnet 4.6",
 * "GPT-4o"). Recognizes dated snapshots, gateway prefixes, and the `[1m]`
 * long-context suffix (rendered as " (1M context)"). Falls back to the raw id so
 * the return value is ALWAYS a non-empty, user-presentable string for any known
 * OR custom model — the marketing-label-for-a-raw-id affordance.
 */
export function getModelDisplayName(modelId: string): string {
  const id = (modelId ?? '').trim();
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
export function getModelMetadata(modelId: string): { displayName: string; description?: string; upgrade?: string } {
  return {
    displayName: getModelDisplayName(modelId),
    description: getModelDescription(modelId),
    upgrade: getNewerVersionHint(modelId),
  };
}

export function formatModelList(models: string[], provider: string): string {
  if (models.length === 0) return 'Could not retrieve model list from provider.';

  const lines = models.map(m => `  ${m}`);
  const header = `Available models (${provider}, ${models.length}):`;

  return `${header}\n${lines.join('\n')}`;
}
