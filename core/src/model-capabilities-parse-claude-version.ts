// Split from model-capabilities.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { modelIdsMatch, positiveTokenCount, readAdvertisedContextWindow } from './model-capabilities-get-context-window';
import type { ModelCapabilities } from './model-capabilities-get-context-window';

/**
 * Choose a window from a models list. An exact id wins. A single-model
 * server (typical vLLM) is used when the ids differ. No length means the
 * caller should try a Hugging Face model card, then the table, then the default.
 */
export function pickAdvertisedContextWindow(records: Record<string, unknown>[], modelId: string): number | undefined {
  for (const record of records) {
    const id = String(record.id ?? record.name ?? '');
    if (!modelIdsMatch(modelId, id)) continue;
    const window = readAdvertisedContextWindow(record);
    if (window !== undefined) return window;
  }
  if (records.length === 1) return readAdvertisedContextWindow(records[0]);
  return undefined;
}

/** Tokenizer configs use this as "unset". It is not a context window. */
export const HUGGINGFACE_LENGTH_SENTINEL = 1_000_000_000;

export const HUGGINGFACE_CONTEXT_FIELDS = [
  'max_position_embeddings',
  'max_sequence_length',
  'max_sequence_len',
  'seq_length',
  'seq_len',
  'n_positions',
  'n_ctx',
  'model_max_length',
  'context_length',
  'max_input_tokens',
  'sliding_window',
] as const;

export const HUGGINGFACE_NESTED_CONFIGS = [
  'text_config',
  'llm_config',
  'language_config',
  'config',
  'cardData',
  'tokenizer_config',
] as const;

export function collectHuggingFaceLengths(value: unknown, depth: number, out: number[]): void {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return;
  const record = value as Record<string, unknown>;
  for (const key of HUGGINGFACE_CONTEXT_FIELDS) {
    const found = positiveTokenCount(record[key]);
    if (found !== undefined && found < HUGGINGFACE_LENGTH_SENTINEL) out.push(found);
  }
  // One nested config (text_config) plus one more (its own config). Deeper
  // wrappers are not a context length.
  if (depth >= 2) return;
  for (const key of HUGGINGFACE_NESTED_CONFIGS) {
    collectHuggingFaceLengths(record[key], depth + 1, out);
  }
}

/**
 * Largest context length published in a Hugging Face config or model card.
 * `max_position_embeddings` is often the base window while `sliding_window`
 * holds the extended one (Qwen2.5-7B is 32,768 and 131,072). The larger
 * value is the window the model can actually take. `original_max_position_embeddings`
 * is the pre-extension length and is ignored. Tokenizer sentinels at 1e30
 * are ignored.
 */
export function readHuggingFaceContextWindow(payload: unknown): number | undefined {
  const found: number[] = [];
  collectHuggingFaceLengths(payload, 0, found);
  if (found.length === 0) return undefined;
  return Math.max(...found);
}

/** `owner/name` repo id. Bare proprietary ids and gateway prefixes are skipped. */
export function huggingfaceRepoId(modelId: string): string | undefined {
  const id = modelId.trim().replace(/^models\//i, '');
  if (!id || /\s/.test(id) || id.includes('://') || id.includes('\\')) return undefined;
  const parts = id.split('/').filter(Boolean);
  if (parts.length !== 2) return undefined;
  if (parts.some(part => part === '.' || part === '..')) return undefined;
  return parts.join('/');
}

/**
 * Hub repos worth a model-card lookup. The requested id counts when it is
 * `owner/name`. A one-model server also counts, so a vLLM alias can still
 * resolve to the repo it is serving. A long catalog is not searched.
 */
export function huggingfaceRepoCandidates(modelId: string, records: Record<string, unknown>[]): string[] {
  const out: string[] = [];
  const add = (id: unknown) => {
    if (typeof id !== 'string') return;
    const repo = huggingfaceRepoId(id);
    if (!repo) return;
    if (out.some(existing => existing.toLowerCase() === repo.toLowerCase())) return;
    out.push(repo);
  };
  add(modelId);
  if (records.length === 1) {
    add(records[0].id);
    add(records[0].name);
  }
  return out;
}

/**
 * Parse a Claude model id into family/major/minor, ignoring any trailing date snapshot
 * (…-YYYYMMDD or …@YYYYMMDD). A flat regex cannot do this reliably because an 8-digit
 * date segment looks like a very large minor version — so we split the post-major segments
 * and treat only a 1-2 digit leading segment as the minor version.
 *
 * Examples:
 *   claude-sonnet-4-5            -> { sonnet, 4, 5 }
 *   claude-sonnet-4-5-20251022  -> { sonnet, 4, 5 }   (date ignored)
 *   claude-sonnet-4-20250514    -> { sonnet, 4, 0 }   (bare 4.0, dated — NOT minor 20250514)
 *   claude-opus-4               -> { opus, 4, 0 }
 *   claude-opus-5-5             -> { opus, 5, 5 }
 *   openrouter/anthropic/claude-sonnet-4-6 -> { sonnet, 4, 6 }  (unanchored)
 */
export function parseClaudeVersion(
  modelId: string,
): { family: string; major: number; minor: number } | null {
  if (typeof modelId !== 'string') return null;
  // Simple anchor (no nested quantifier → no ReDoS); the trailing version segments are parsed by
  // splitting on '-' rather than a `(?:-\d+)*` group.
  const m = /claude-(opus|sonnet|haiku|fable|mythos)-(\d+)/i.exec(modelId);
  if (!m) return null;
  const family = m[1].toLowerCase();
  const major = parseInt(m[2], 10);
  const after = modelId.slice(m.index + m[0].length); // e.g. '-5', '-5-20251022', '-20250514', ''
  const segs = after.split('-').filter(Boolean);
  // A 1-2 digit leading segment is the minor version; a longer run (a …-YYYYMMDD date) is not.
  const minor = segs.length > 0 && /^\d{1,2}$/.test(segs[0]) ? parseInt(segs[0], 10) : 0;
  return { family, major, minor };
}

export const REASONING_MODELS: Array<{
  pattern: RegExp;
  capabilities: ModelCapabilities;
}> = [
  // OpenAI reasoning models — reasoning_effort
  { pattern: /^o1($|-)/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },
  { pattern: /^o3($|-)/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },
  { pattern: /^o4-mini/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },

  // OpenAI GPT-5+ — reasoning models with reasoning_effort
  { pattern: /^gpt-(?:[5-9]|\d{2,})/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },

  // Google Gemini 2.5+ — thinkingConfig
  { pattern: /gemini-2\.5-pro/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 32768 } },
  { pattern: /gemini-2\.5-flash/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'low',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 32768 } },

  // DeepSeek reasoning — inline <think> tags
  { pattern: /deepseek-r1|deepseek-reasoner/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'high',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 0 } },
];

export const OPENAI_REASONING_PATTERN = /^o[1-9]($|-)/;

export function parseGptMajorVersion(modelId: string): number | null {
  if (!modelId.startsWith('gpt-')) return null;
  const afterPrefix = modelId.slice(4);
  const digitEnd = afterPrefix.search(/\D/);
  const majorStr = digitEnd === -1 ? afterPrefix : afterPrefix.slice(0, digitEnd);
  if (!majorStr) return null;
  const major = parseInt(majorStr, 10);
  return Number.isFinite(major) ? major : null;
}

export function classifyOpenAIModel(modelId: string): ModelCapabilities | null {
  if (OPENAI_REASONING_PATTERN.test(modelId)) {
    return {
      reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0,
    };
  }
  const major = parseGptMajorVersion(modelId);
  if (major !== null && major >= 5) {
    return {
      reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0,
    };
  }
  return null;
}

/**
 * Returns true if the model requires the /v1/responses endpoint for tool use.
 * gpt-5.6+ models do not support function tools with reasoning on /v1/chat/completions.
 */
export function requiresResponsesAPI(modelId: string): boolean {
  if (!modelId.startsWith('gpt-')) return false;
  const afterPrefix = modelId.slice(4);
  const versionMatch = afterPrefix.match(/^(\d+\.\d+|\d+)/);
  if (!versionMatch) return false;
  const version = parseFloat(versionMatch[1]);
  return version >= 5.6;
}

// ---------------------------------------------------------------------------
// Persistent on-disk cache of provider-advertised model capabilities
// ---------------------------------------------------------------------------
//
// A provider's live /models listing advertises each model's context window and
// completion cap. Re-fetching that on every cold start is wasteful and leaves the
// FIRST turn under-sized (compaction can fire too early) until discovery lands.
// Mirroring the reference's refreshModelCapabilities (utils/model/modelCapabilities.ts
// :L60-118), AMI persists the discovered windows/caps to a JSON file so the next
// run can WARM them before — or in lieu of — a network fetch. The file carries a
// timestamp, is written with a sorted, stable model list, is skipped when nothing
// changed (no spurious mtime churn), and is created 0600 (owner-only) because it
// records which models an operator uses.
//
// Opt-in by directory: the cache is active only when a directory is resolved via
// SUPERINFERENCE_MODEL_CACHE_DIR (read at call time). With it unset every function
// here is a no-op, so default behavior and existing discovery are unchanged.

/** One cached model's advertised windows (either field may be absent). */
export interface ModelCapabilityCacheEntry {
  id: string;
  contextWindow?: number;
  maxOutputTokens?: number;
}

/** On-disk cache payload: the sorted model list plus the write timestamp (ms). */
export interface ModelCapabilityCacheFile {
  models: ModelCapabilityCacheEntry[];
  timestamp: number;
}

/** Basename of the cache file inside the resolved cache directory. */
export const MODEL_CAPABILITIES_CACHE_FILE = 'model-capabilities.json';

/**
 * The directory the capabilities cache lives in, or `undefined` when caching is
 * not enabled. Resolved from `SUPERINFERENCE_MODEL_CACHE_DIR` at call time so a
 * test/process can toggle it. A blank value disables the cache (→ undefined).
 */
export function modelCapabilitiesCacheDir(env: NodeJS.ProcessEnv = process.env): string | undefined {
  const raw = env.SUPERINFERENCE_MODEL_CACHE_DIR;
  if (typeof raw !== 'string' || raw.trim() === '') return undefined;
  return raw.trim();
}

/** Absolute path to the cache file inside `dir`. */
export function modelCapabilitiesCachePath(dir: string): string {
  return path.join(dir, MODEL_CAPABILITIES_CACHE_FILE);
}

/** Parse/validate a raw JSON value into a cache payload, or null when malformed. */
export function parseCacheFile(raw: unknown): ModelCapabilityCacheFile | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const obj = raw as Record<string, unknown>;
  if (typeof obj.timestamp !== 'number' || !Array.isArray(obj.models)) return null;
  const models: ModelCapabilityCacheEntry[] = [];
  for (const item of obj.models) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    const rec = item as Record<string, unknown>;
    if (typeof rec.id !== 'string' || !rec.id) continue;
    const entry: ModelCapabilityCacheEntry = { id: rec.id };
    const cw = positiveTokenCount(rec.contextWindow);
    if (cw !== undefined) entry.contextWindow = cw;
    const mo = positiveTokenCount(rec.maxOutputTokens);
    if (mo !== undefined) entry.maxOutputTokens = mo;
    models.push(entry);
  }
  return { models, timestamp: obj.timestamp };
}

/** Stable-sort entries by id so the written file is deterministic (and the
 *  skip-when-unchanged compare below is order-independent). */
export function sortCacheEntries(models: ModelCapabilityCacheEntry[]): ModelCapabilityCacheEntry[] {
  return models.slice().sort((a, b) => a.id.localeCompare(b.id));
}

/** Compare two sorted model lists for equality, ignoring the timestamp. */
export function sameCacheModels(a: ModelCapabilityCacheEntry[], b: ModelCapabilityCacheEntry[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i].id !== b[i].id || a[i].contextWindow !== b[i].contextWindow || a[i].maxOutputTokens !== b[i].maxOutputTokens) {
      return false;
    }
  }
  return true;
}

/**
 * Convert a {@link discoverFromModelList} result into cache entries (merging the
 * context-window and output-cap maps by id) ready for {@link writeModelCapabilitiesCache}.
 */
export function discoveryToCacheEntries(discovery: {
  contextWindows: Record<string, number>;
  outputCaps: Record<string, number>;
}): ModelCapabilityCacheEntry[] {
  const byId = new Map<string, ModelCapabilityCacheEntry>();
  for (const [id, contextWindow] of Object.entries(discovery.contextWindows)) {
    byId.set(id, { id, contextWindow });
  }
  for (const [id, maxOutputTokens] of Object.entries(discovery.outputCaps)) {
    const entry = byId.get(id);
    if (entry) entry.maxOutputTokens = maxOutputTokens;
    else byId.set(id, { id, maxOutputTokens });
  }
  return [...byId.values()];
}

// ---------------------------------------------------------------------------
// User env-var capability override (escape hatch for pinned 3rd-party models)
// ---------------------------------------------------------------------------

/**
 * Capability dimensions a user can explicitly declare for a pinned third-party
 * model via a `*_SUPPORTED_CAPABILITIES` env var. Mirrors the reference's
 * `ModelCapabilityOverride` (utils/model/modelSupportOverrides.ts:L4-L9). AMI's
 * {@link ModelCapabilities} currently maps `effort`/`thinking` → `reasoning` and
 * `adaptive_thinking` → `supportsAdaptiveThinking`; `max_effort` /
 * `interleaved_thinking` are accepted by the generic predicate for forward parity.
 */
export const MODEL_CAPABILITY_OVERRIDES = ['effort', 'max_effort', 'thinking', 'adaptive_thinking', 'interleaved_thinking'] as const;

export type ModelCapabilityOverride = (typeof MODEL_CAPABILITY_OVERRIDES)[number];

/** Custom base-URL env vars that signal a gateway/proxy (non-first-party) endpoint. */
export const THIRD_PARTY_BASE_URL_ENV = [
  'ANTHROPIC_BASE_URL',
  'OPENAI_BASE_URL',
  'AI_BASE_URL',
  'PROVIDER_BASE_URL',
  'OLLAMA_BASE_URL',
] as const;

/**
 * True when no custom gateway/proxy endpoint is configured, i.e. requests go to a
 * provider's canonical first-party API where built-in capability detection is
 * reliable. The capability-override escape hatch below is INTENTIONALLY ignored on
 * a first-party endpoint: a user-declared list can only correct a misdetected
 * gateway model, never silently strip a known-correct first-party capability.
 * Mirrors the reference's `getAPIProvider() === 'firstParty'` short-circuit
 * (utils/model/modelSupportOverrides.ts:L32-L34); AMI detects it from the absence
 * of any custom `*_BASE_URL` env at call time.
 */
export function isFirstPartyProvider(env: NodeJS.ProcessEnv = process.env): boolean {
  return !THIRD_PARTY_BASE_URL_ENV.some(key => {
    const value = env[key];
    return typeof value === 'string' && value.trim() !== '';
  });
}

/** Per-tier env vars: a model pin plus its declared capability list. */
export const CAPABILITY_OVERRIDE_TIERS = [
  { modelEnvVar: 'ANTHROPIC_DEFAULT_OPUS_MODEL', capabilitiesEnvVar: 'ANTHROPIC_DEFAULT_OPUS_MODEL_SUPPORTED_CAPABILITIES' },
  { modelEnvVar: 'ANTHROPIC_DEFAULT_SONNET_MODEL', capabilitiesEnvVar: 'ANTHROPIC_DEFAULT_SONNET_MODEL_SUPPORTED_CAPABILITIES' },
  { modelEnvVar: 'ANTHROPIC_DEFAULT_HAIKU_MODEL', capabilitiesEnvVar: 'ANTHROPIC_DEFAULT_HAIKU_MODEL_SUPPORTED_CAPABILITIES' },
] as const;
