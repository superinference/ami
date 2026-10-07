export type ThinkingLevel = 'off' | 'low' | 'medium' | 'high' | 'max';

export interface ModelCapabilities {
  reasoning: boolean;
  defaultThinkingLevel: ThinkingLevel;
  supportsAdaptiveThinking: boolean;
  requiresTemperatureOne: boolean;
  temperatureMustBeUnset: boolean;
  maxThinkingBudget: number;
  maxContextTokens?: number;
  maxOutputTokens?: number;
  supportsVision?: boolean;
  supportsToolUse?: boolean;
}

/** Used when the server omits a length and the model id is not in the table. */
export const DEFAULT_CONTEXT_WINDOW = 128_000;

/**
 * Published context windows. Longer ids come first so `grok-4.20` is not
 * claimed by a shorter `grok-4` prefix. A live `/models` payload overrides
 * this table.
 *
 * Proprietary lengths checked 2026-10-06 against Anthropic's models overview
 * (Fable 5.1, Opus 5.5, Sonnet 5.5 at 1M; Haiku 4.5 stays on the 200k key),
 * OpenAI's models page (GPT-6 Astra, 6.1 Sol, and Luna at 1.05M), and xAI's
 * model pricing (Grok 4.5–4.7 at 500k, Grok 4.3 and 4.20 at 1M).
 */
const CONTEXT_WINDOWS: Record<string, number> = {
  'claude-fable-5': 1_000_000,
  'claude-opus-5': 1_000_000,
  'claude-sonnet-5': 1_000_000,
  'gpt-6-astra': 1_050_000,
  'gpt-6.1-sol': 1_050_000,
  'gpt-6-luna': 1_050_000,
  'gpt-6': 1_050_000,
  'grok-4.20': 1_000_000,
  'grok-4.7': 500_000,
  'grok-4.6': 500_000,
  'grok-4.5': 500_000,
  'grok-4.3': 1_000_000,
  'grok-build-0.1': 256_000,
  'claude-opus-4': 200000,
  'claude-sonnet-4': 200000,
  'claude-haiku-3.5': 200000,
  'claude-haiku-4': 200000,
  'claude-3-5-sonnet': 200000,
  'claude-3-opus': 200000,
  'gpt-4o-mini': 128000,
  'gpt-4o': 128000,
  'gpt-4-turbo': 128000,
  'gpt-4.1': 1048576,
  'o1-mini': 128000,
  'o1': 200000,
  'o3-mini': 200000,
  'o3': 200000,
  'o4-mini': 200000,
  'gemini-2.5-pro': 1048576,
  'gemini-2.5-flash': 1048576,
  'gemini-2.0-flash': 1048576,
  'gemini-3.6': 2097152,
  'gemini-3.1': 2097152,
  'gemini-3': 2097152,
  'gemini-1.5-pro': 1048576,
  'deepseek-chat': 64000,
  'deepseek-reasoner': 64000,
  // Qwen3.x — 256k native context
  'qwen3.8': 262144,
  'qwen3': 262144,
  // Qwen2.5 — size-specific context windows (most specific first)
  'qwen2.5-0.5b': 32768,
  'qwen2.5-1.5b': 32768,
  'qwen2.5-3b': 32768,
  'qwen2.5-7b': 131072,
  'qwen2.5-14b': 131072,
  'qwen2.5-32b': 131072,
  'qwen2.5-72b': 131072,
  'qwen2.5': 32768,
  'qwen2': 32768,
  // Small open-weight models
  'llama-3.2-1b': 131072,
  'llama-3.2-3b': 131072,
  'phi-3-mini': 4096,
  'phi-3.5-mini': 131072,
  'phi-4-mini': 131072,
};

export function getContextWindow(modelId: string): number {
  const lower = modelId.toLowerCase();
  for (const [key, tokens] of Object.entries(CONTEXT_WINDOWS)) {
    const keyLower = key.toLowerCase();
    const idx = lower.indexOf(keyLower);
    if (idx === -1) continue;
    if (keyLower.length <= 3 && idx > 0 && /[a-zA-Z0-9]/.test(lower[idx - 1])) continue;
    return tokens;
  }
  return DEFAULT_CONTEXT_WINDOW;
}

const ADVERTISED_CONTEXT_KEYS = [
  'max_input_tokens',
  'max_model_len',
  'context_length',
  'context_window',
  'max_context_length',
  'input_token_limit',
  'inputTokenLimit',
  'max_position_embeddings',
] as const;

function positiveTokenCount(value: unknown): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) return undefined;
  return Math.floor(value);
}

/**
 * Read a context length from one `/models` record.
 * Anthropic uses `max_input_tokens`, vLLM uses `max_model_len`, OpenRouter
 * uses `context_length`, and Gemini uses `inputTokenLimit`. `max_tokens` is
 * the completion cap and is ignored. Zero means the field was withheld.
 */
export function readAdvertisedContextWindow(record: Record<string, unknown>): number | undefined {
  const nested = record.model_info;
  const sources = [record, ...(nested && typeof nested === 'object' && !Array.isArray(nested) ? [nested as Record<string, unknown>] : [])];
  for (const source of sources) {
    for (const key of ADVERTISED_CONTEXT_KEYS) {
      const found = positiveTokenCount(source[key]);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

function modelIdsMatch(requested: string, advertised: string): boolean {
  const normalize = (id: string) => id.toLowerCase().replace(/^models\//, '');
  const want = normalize(requested);
  const got = normalize(advertised);
  if (!want || !got) return false;
  if (want === got) return true;
  const tail = (id: string) => {
    const parts = id.split('/');
    return parts[parts.length - 1];
  };
  return tail(want) === tail(got);
}

/** Pull model records out of OpenAI, Anthropic, and Gemini list payloads. */
export function collectModelRecords(payload: unknown): Record<string, unknown>[] {
  if (!payload || typeof payload !== 'object') return [];
  const body = payload as Record<string, unknown>;
  for (const key of ['data', 'models'] as const) {
    const list = body[key];
    if (Array.isArray(list)) {
      return list.filter((item): item is Record<string, unknown> => !!item && typeof item === 'object' && !Array.isArray(item));
    }
  }
  if (typeof body.id === 'string' || typeof body.name === 'string') return [body];
  return [];
}

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
const HUGGINGFACE_LENGTH_SENTINEL = 1_000_000_000;

const HUGGINGFACE_CONTEXT_FIELDS = [
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

const HUGGINGFACE_NESTED_CONFIGS = [
  'text_config',
  'llm_config',
  'language_config',
  'config',
  'cardData',
  'tokenizer_config',
] as const;

function collectHuggingFaceLengths(value: unknown, depth: number, out: number[]): void {
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

const REASONING_MODELS: Array<{
  pattern: RegExp;
  capabilities: ModelCapabilities;
}> = [
  // Anthropic Claude 4.x — adaptive thinking
  { pattern: /claude-opus-4/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: true, requiresTemperatureOne: true,
      temperatureMustBeUnset: false, maxThinkingBudget: 128000 } },
  { pattern: /claude-sonnet-4/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: true, requiresTemperatureOne: true,
      temperatureMustBeUnset: false, maxThinkingBudget: 128000 } },

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

// ---------------------------------------------------------------------------
// Dynamic model capability registry — populated at runtime from /v1/models
// ---------------------------------------------------------------------------

const dynamicCapabilities = new Map<string, ModelCapabilities | null>();

export function registerModelCapabilities(modelId: string, caps: ModelCapabilities | null): void {
  dynamicCapabilities.set(modelId.toLowerCase(), caps);
}

export function clearDynamicCapabilities(): void {
  dynamicCapabilities.clear();
}

const OPENAI_REASONING_PATTERN = /^o[1-9]($|-)/;

function classifyOpenAIModel(modelId: string): ModelCapabilities | null {
  if (OPENAI_REASONING_PATTERN.test(modelId)) {
    return {
      reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0,
    };
  }
  return null;
}

export function discoverFromModelList(records: Record<string, unknown>[]): {
  registered: string[];
  contextWindows: Record<string, number>;
} {
  const registered: string[] = [];
  const contextWindows: Record<string, number> = {};
  for (const record of records) {
    const id = String(record.id ?? record.name ?? '');
    if (!id) continue;
    const window = readAdvertisedContextWindow(record);
    if (window !== undefined) {
      CONTEXT_WINDOWS[id] = window;
      contextWindows[id] = window;
    }
    if (!dynamicCapabilities.has(id.toLowerCase())) {
      const caps = classifyOpenAIModel(id);
      dynamicCapabilities.set(id.toLowerCase(), caps);
      registered.push(id);
    }
  }
  return { registered, contextWindows };
}

export function getModelCapabilities(modelId: string): ModelCapabilities | null {
  const dynamic = dynamicCapabilities.get(modelId.toLowerCase());
  if (dynamic !== undefined) return dynamic;
  for (const entry of REASONING_MODELS) {
    if (entry.pattern.test(modelId)) {
      return entry.capabilities;
    }
  }
  return null;
}

export function isReasoningModel(modelId: string): boolean {
  return getModelCapabilities(modelId) !== null;
}

const BUDGET_MAP: Record<ThinkingLevel, number> = {
  off: 0,
  low: 4096,
  medium: 10240,
  high: 32768,
  max: 0,
};

/** Completion budget used when the caller omits maxTokens. */
export const DEFAULT_MAX_OUTPUT_TOKENS = 32_768;

/**
 * Tokens held back so the prompt and the completion do not land on the
 * same last token of the window.
 */
export const OUTPUT_FIT_SAFETY_TOKENS = 1_024;

/** Smallest completion we will still request when the window is nearly full. */
export const MIN_FITTED_OUTPUT_TOKENS = 256;

/** Slack subtracted before a reduced thinking budget is accepted. */
export const THINKING_HEADROOM_MARGIN = 4_096;

/** Smallest thinking budget worth sending. Below this, thinking is turned off. */
export const MIN_THINKING_BUDGET = 1_024;

/**
 * Largest completion budget that still fits beside the prompt.
 *
 * `requested` is the configured budget or an escalation step (64k / 128k /
 * 256k). Those steps are not clamped to the model, so a 32k window would
 * otherwise be asked for 256k output tokens.
 */
export function fitOutputTokens(
  requested: number,
  contextWindow: number,
  promptTokens: number,
): number {
  const ask = Number.isFinite(requested) && requested > 0
    ? Math.floor(requested)
    : DEFAULT_MAX_OUTPUT_TOKENS;
  if (!Number.isFinite(contextWindow) || contextWindow <= 0) return ask;
  const prompt = Number.isFinite(promptTokens) && promptTokens > 0 ? Math.floor(promptTokens) : 0;
  const cap = contextWindow - prompt - OUTPUT_FIT_SAFETY_TOKENS;
  if (cap < MIN_FITTED_OUTPUT_TOKENS) return MIN_FITTED_OUTPUT_TOKENS;
  return Math.min(ask, cap);
}

/**
 * Shrink a thinking budget so prompt + thinking + output stays under the window.
 * Returns the request unchanged when it already fits. Returns 0 when even
 * the 1024 floor would overflow — callers should disable thinking in that case.
 */
export function capThinkingBudget(
  requestedBudget: number,
  contextWindow: number,
  promptTokens: number,
  outputTokens: number,
): number {
  if (!Number.isFinite(requestedBudget) || requestedBudget <= 0) return 0;
  if (!Number.isFinite(contextWindow) || contextWindow <= 0) return Math.floor(requestedBudget);
  const prompt = Number.isFinite(promptTokens) ? Math.max(0, Math.floor(promptTokens)) : 0;
  const output = Number.isFinite(outputTokens) ? Math.max(0, Math.floor(outputTokens)) : 0;
  const headroom = contextWindow - prompt - output;
  // Keep the same safety margin fitOutputTokens reserved. Thinking must
  // not spend it, or prompt + output + thinking lands on the window edge.
  const usable = headroom - OUTPUT_FIT_SAFETY_TOKENS;
  if (usable >= requestedBudget) return Math.floor(requestedBudget);
  const withMargin = Math.floor(usable) - THINKING_HEADROOM_MARGIN;
  if (withMargin >= MIN_THINKING_BUDGET) return withMargin;
  if (usable >= MIN_THINKING_BUDGET) return MIN_THINKING_BUDGET;
  return 0;
}

/**
 * Character cap for one tool result.
 *
 * Message estimates use chars/4 and then a 4/3 pad, which is chars/3.
 * 85% of the token budget is the detached compact trigger, so a single
 * result that stays under this cap cannot fill the window by itself.
 */
export function maxToolOutputChars(tokenBudget: number): number {
  if (!Number.isFinite(tokenBudget) || tokenBudget <= 0) return 8_000;
  const tokenRoom = Math.floor(tokenBudget * 0.85);
  return Math.max(8_000, tokenRoom * 3);
}

export function resolveThinkingBudget(level: ThinkingLevel, modelId?: string): number {
  const budget = BUDGET_MAP[level];
  if (budget > 0) return budget;
  if (level === 'max') {
    const caps = getModelCapabilities(modelId ?? '');
    const maxBudget = caps?.maxThinkingBudget ?? 128000;
    return Math.max(1024, maxBudget - 8192);
  }
  return 0;
}

const PROVIDER_SAMPLING_DEFAULTS: Record<string, { temperature?: number; topP?: number; topK?: number }> = {
  'gemini': { temperature: 1.0, topK: 64 },
  'qwen': { temperature: 0.55, topP: 1 },
  'deepseek': { temperature: 0.6 },
};

export function getProviderSamplingDefaults(modelId: string): { temperature?: number; topP?: number; topK?: number } {
  for (const [prefix, defaults] of Object.entries(PROVIDER_SAMPLING_DEFAULTS)) {
    if (modelId.toLowerCase().includes(prefix)) return defaults;
  }
  return {};
}

export function resolveTemperature(
  modelId: string,
  configTemperature: number | undefined,
  thinking: { enabled: boolean } | undefined,
): number | undefined {
  if (!thinking?.enabled) {
    // If no explicit temperature configured, use provider-specific defaults
    if (configTemperature === undefined) {
      const defaults = getProviderSamplingDefaults(modelId);
      return defaults.temperature;
    }
    return configTemperature;
  }

  const caps = getModelCapabilities(modelId);
  if (!caps) {
    if (configTemperature === undefined) {
      const defaults = getProviderSamplingDefaults(modelId);
      return defaults.temperature;
    }
    return configTemperature;
  }

  // Claude: temperature must be omitted (SDK sets it to 1 internally)
  if (caps.requiresTemperatureOne) return undefined;

  // OpenAI o-series: temperature must not be sent at all
  if (caps.temperatureMustBeUnset) return undefined;

  return configTemperature;
}
