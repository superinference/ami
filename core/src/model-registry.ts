import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import type { ProviderConfig } from './types';
import { MODEL_PREFERENCE } from './provider';
import { discoverFromModelList } from './model-capabilities';
import { redactSecrets } from './tools/tool-utils';

// Default 10s; overridable per-call via env for tests and constrained
// environments (mirrors SUPERINFERENCE_API_TIMEOUT_MS / HF_ENDPOINT seams).
// Read at call time (not module load) so an override takes effect post-import.
function modelListTimeoutMs(): number {
  return Number(process.env.SI_MODEL_LIST_TIMEOUT_MS) || 10000;
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
        if (status < 200 || status >= 300) {
          const httpErr = new Error(`HTTP ${status}`) as Error & { statusCode: number };
          httpErr.statusCode = status;
          reject(httpErr);
          return;
        }
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString('utf-8')));
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

export async function listModels(config: ProviderConfig): Promise<ModelInfo[]> {
  const provider = detectProvider(config);

  // Providers with known static model lists (no API listing endpoint)
  if (STATIC_MODEL_PROVIDERS.has(provider)) {
    const models = MODEL_PREFERENCE[provider] || [];
    return models.map(id => ({ id, name: id }));
  }

  try {
    if (provider === 'google') {
      return await listGeminiModels(config);
    }
    // OpenAI-compatible: OpenAI, Ollama, OpenRouter
    return await listOpenAIModels(config);
  } catch {
    // Fallback to static list if API call fails
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

  if (status === 401 || status === 403) {
    return `Authentication failed (HTTP ${status}): ${provider} rejected the API key. Check ${providerKeyHint(provider)}.`;
  }
  if (status === 404) {
    return `Model list endpoint not found (HTTP 404) for ${provider}. Check the base URL includes the API path (e.g. ".../v1").`;
  }
  if (typeof status === 'number') {
    return `${provider} returned HTTP ${status} when listing models.`;
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
    const models = (MODEL_PREFERENCE[provider] || []).map(id => ({ id, name: id }));
    return {
      models,
      diagnostic: models.length > 0 ? null : `No built-in model list for ${provider}.`,
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
    return { models: [], diagnostic: diagnoseModelDiscovery(err, provider) };
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
  discoverFromModelList(models as unknown as Record<string, unknown>[]);
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

/** Normalize an input for alias matching: trimmed + lower-cased. */
function normalizeAlias(input: string): string {
  return typeof input === 'string' ? input.trim().toLowerCase() : '';
}

/** True when `input` (trimmed, case-insensitive) is a recognized model alias. */
export function isModelAlias(input: string): boolean {
  return (MODEL_ALIASES as readonly string[]).includes(normalizeAlias(input));
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
  return isModelAlias(key) ? MODEL_ALIAS_TARGETS[key as ModelAlias] : undefined;
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

  // Try substring match, then a deprecated/renamed-id replacement suggestion for
  // ids that have no overlap with any live model at all.
  const partial = ids.filter(id => id.includes(effective) || effective.includes(id));
  const suggestion = partial.length > 0
    ? partial[0]
    : suggestDeprecatedReplacement(effective, ids);

  return { valid: false, available: ids, suggestion, errorKind: 'none' };
}

export function formatModelList(models: string[], provider: string): string {
  if (models.length === 0) return 'Could not retrieve model list from provider.';

  const lines = models.map(m => `  ${m}`);
  const header = `Available models (${provider}, ${models.length}):`;

  return `${header}\n${lines.join('\n')}`;
}
