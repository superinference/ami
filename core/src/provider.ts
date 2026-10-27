import { streamText, jsonSchema } from 'ai';
import type { ModelMessage, ToolSet, ToolCallRepairFunction } from 'ai';
import { createOpenAI } from '@ai-sdk/openai';
import { createAnthropic } from '@ai-sdk/anthropic';
import { createGoogleGenerativeAI } from '@ai-sdk/google';
import { createVertexAnthropic } from '@ai-sdk/google-vertex/anthropic';
import { gzipSync } from 'node:zlib';
import { getModelCapabilities, resolveThinkingBudget, resolveTemperature, fitOutputTokens, DEFAULT_MAX_OUTPUT_TOKENS, getContextWindow, collectModelRecords, pickAdvertisedContextWindow, huggingfaceRepoCandidates, readHuggingFaceContextWindow, requiresResponsesAPI } from './model-capabilities';
import { sanitizeToolCallIds, buildConversationCachePoints, healOrphanedToolCalls, stripEmptyTextBlocks } from './provider-transform';
import { estimateTokens } from './utils/tokens';
import { parseRateLimitHeaders, shouldBackoff, suggestedWaitMs } from './rate-limit-parser';
import { isGatewayRejection } from './error-classifier';

/**
 * Retry-After from response headers, in SECONDS. Delegates to the shared
 * rate-limit parser, which handles BOTH a numeric delta-seconds value AND an
 * HTTP-date value (e.g. "Wed, 21 Oct 2026 07:28:00 GMT") — the HTTP-date form
 * was silently dropped by the previous inline `parseInt` sites.
 */
export function extractRetryAfterSeconds(
  headers: Record<string, string | string[] | undefined> | undefined,
): number | undefined {
  if (!headers) return undefined;
  const ms = parseRateLimitHeaders(headers).retryAfterMs;
  return ms != null ? Math.max(0, Math.round(ms / 1000)) : undefined;
}
import type {
  ProviderConfig,
  Message,
  ToolDefinition,
  ToolInputSchema,
  StreamChunk,
  ThinkingConfig,
} from './types';

// ---------------------------------------------------------------------------
// OpenAI tool format (kept for backward compatibility)
// ---------------------------------------------------------------------------

interface OpenAIToolFunction {
  type: 'function';
  function: {
    name: string;
    description: string;
    parameters: ToolInputSchema;
  };
}

export function toolDefinitionsToOpenAI(tools: ToolDefinition[]): OpenAIToolFunction[] {
  return tools.map((tool) => ({
    type: 'function' as const,
    function: {
      name: tool.name,
      description: tool.description,
      parameters: tool.inputSchema,
    },
  }));
}

// ---------------------------------------------------------------------------
// API key pattern detection — infer provider from key format
// ---------------------------------------------------------------------------

type ProviderName =
  | 'openai' | 'anthropic' | 'google'
  | 'groq' | 'mistral' | 'xai' | 'azure-openai'
  | 'amazon-bedrock' | 'togetherai' | 'cohere' | 'fireworks'
  | 'perplexity' | 'deepinfra' | 'deepseek' | 'cerebras'
  | 'openrouter' | 'ollama' | 'luma' | 'alibaba'
  | 'google-vertex' | 'anthropic-vertex' | 'ai-gateway' | 'huggingface';

interface InferredProvider {
  provider: ProviderName;
  defaultModel: string;
  defaultBaseUrl: string;
}

const MODEL_PREFERENCE: Record<string, string[]> = {
  google: [
    'gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-3.1-pro-preview',
    'gemini-3-pro-preview', 'gemini-3.5-flash', 'gemini-3.1-flash-lite',
  ],
  anthropic: [
    'claude-sonnet-4-6', 'claude-opus-4-6', 'claude-sonnet-4-5-20251022',
    'claude-haiku-4-5-20251001', 'claude-3-5-sonnet-20241022',
  ],
  openai: [
    'gpt-6-luna', 'gpt-6-sol', 'gpt-6-astra', 'gpt-6.1-sol',
    'o4-mini', 'gpt-4o', 'o3-mini', 'gpt-4o-mini', 'gpt-4-turbo',
  ],
  groq: ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768'],
  mistral: ['mistral-large-latest', 'mistral-medium-latest', 'mistral-small-latest', 'codestral-latest'],
  xai: ['grok-3', 'grok-3-mini', 'grok-2'],
  deepseek: ['deepseek-chat', 'deepseek-reasoner'],
  togetherai: ['meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo', 'Qwen/Qwen2.5-72B-Instruct-Turbo'],
  cohere: ['command-r-plus', 'command-r', 'command-light'],
  fireworks: ['accounts/fireworks/models/llama-v3p3-70b-instruct'],
  perplexity: ['sonar-pro', 'sonar', 'sonar-reasoning-pro'],
  deepinfra: ['meta-llama/Meta-Llama-3.1-70B-Instruct'],
  cerebras: ['zai-glm-4.7', 'gpt-oss-120b'],
  alibaba: ['qwen-max', 'qwen-plus', 'qwen-turbo'],
  'azure-openai': ['gpt-4o', 'gpt-4o-mini'],
  'google-vertex': ['gemini-2.5-pro', 'gemini-2.5-flash'],
  'anthropic-vertex': [
    'claude-opus-4-6', 'claude-sonnet-4-6', 'claude-sonnet-4-5-20251022',
    'claude-haiku-4-5-20251001',
  ],
  'amazon-bedrock': ['anthropic.claude-sonnet-4-6-20250514', 'anthropic.claude-haiku-4-5-20251001'],
  // Fast + cheap first: a tool-capable default that stays affordable per request
  // (claude-sonnet-4.6 kept only as a last-resort quality fallback).
  openrouter: [
    'openrouter/google/gemini-2.5-flash',
    'openrouter/google/gemini-2.5-flash-lite',
    'openrouter/openai/gpt-4o-mini',
    'openrouter/anthropic/claude-sonnet-4.6',
  ],
  huggingface: ['deepseek-ai/DeepSeek-V3-0324', 'Qwen/Qwen3-235B-A22B', 'Qwen/Qwen3-32B'],
  luma: ['luma-photon-latest'],
};

// API key prefix → provider detection
const KEY_PREFIXES: Array<{ prefix: string; provider: ProviderName }> = [
  { prefix: 'AIzaSy', provider: 'google' },
  { prefix: 'sk-ant-', provider: 'anthropic' },
  { prefix: 'sk-or-', provider: 'openrouter' },
  { prefix: 'gsk_', provider: 'groq' },
  { prefix: 'xai-', provider: 'xai' },
  { prefix: 'pplx-', provider: 'perplexity' },
  { prefix: 'hf_', provider: 'huggingface' },
  { prefix: 'sk-', provider: 'openai' },
];

// Base URL pattern → provider detection
const URL_PATTERNS: Array<{ pattern: string; provider: ProviderName }> = [
  { pattern: 'api.groq.com', provider: 'groq' },
  { pattern: 'api.mistral.ai', provider: 'mistral' },
  { pattern: 'api.x.ai', provider: 'xai' },
  { pattern: 'api.deepseek.com', provider: 'deepseek' },
  { pattern: 'api.together.xyz', provider: 'togetherai' },
  { pattern: 'api.cohere.ai', provider: 'cohere' },
  { pattern: 'api.cohere.com', provider: 'cohere' },
  { pattern: 'api.fireworks.ai', provider: 'fireworks' },
  { pattern: 'api.perplexity.ai', provider: 'perplexity' },
  { pattern: 'api.deepinfra.com', provider: 'deepinfra' },
  { pattern: 'api.cerebras.ai', provider: 'cerebras' },
  { pattern: 'dashscope.aliyuncs.com', provider: 'alibaba' },
  { pattern: 'openrouter.ai', provider: 'openrouter' },
  { pattern: 'generativelanguage.googleapis.com', provider: 'google' },
  { pattern: 'aiplatform.googleapis.com', provider: 'google-vertex' },
  { pattern: 'anthropic.com', provider: 'anthropic' },
  { pattern: 'openai.azure.com', provider: 'azure-openai' },
  { pattern: 'api.luma.ai', provider: 'luma' },
  { pattern: 'huggingface.co', provider: 'huggingface' },
  { pattern: 'gateway.ai.cloudflare.com', provider: 'ai-gateway' },
  { pattern: 'localhost', provider: 'ollama' },
  { pattern: '127.0.0.1', provider: 'ollama' },
];

// Env var → provider detection
const ENV_KEYS: Array<{ env: string; provider: ProviderName }> = [
  { env: 'GROQ_API_KEY', provider: 'groq' },
  { env: 'MISTRAL_API_KEY', provider: 'mistral' },
  { env: 'XAI_API_KEY', provider: 'xai' },
  { env: 'DEEPSEEK_API_KEY', provider: 'deepseek' },
  { env: 'TOGETHER_AI_API_KEY', provider: 'togetherai' },
  { env: 'COHERE_API_KEY', provider: 'cohere' },
  { env: 'FIREWORKS_API_KEY', provider: 'fireworks' },
  { env: 'PERPLEXITY_API_KEY', provider: 'perplexity' },
  { env: 'DEEPINFRA_API_KEY', provider: 'deepinfra' },
  { env: 'CEREBRAS_API_KEY', provider: 'cerebras' },
  { env: 'ALIBABA_API_KEY', provider: 'alibaba' },
  { env: 'DASHSCOPE_API_KEY', provider: 'alibaba' },
  { env: 'LUMA_API_KEY', provider: 'luma' },
  { env: 'OPENROUTER_API_KEY', provider: 'openrouter' },
  { env: 'HF_TOKEN', provider: 'huggingface' },
  { env: 'AZURE_OPENAI_API_KEY', provider: 'azure-openai' },
  { env: 'AWS_ACCESS_KEY_ID', provider: 'amazon-bedrock' },
  { env: 'ANTHROPIC_VERTEX_PROJECT_ID', provider: 'anthropic-vertex' },
  { env: 'GOOGLE_APPLICATION_CREDENTIALS', provider: 'google-vertex' },
];

// Default base URLs for providers that use OpenAI-compatible endpoints
const PROVIDER_BASE_URLS: Partial<Record<ProviderName, string>> = {
  groq: 'https://api.groq.com/openai/v1',
  mistral: 'https://api.mistral.ai/v1',
  xai: 'https://api.x.ai/v1',
  deepseek: 'https://api.deepseek.com',
  togetherai: 'https://api.together.xyz/v1',
  cohere: 'https://api.cohere.com/v2',
  fireworks: 'https://api.fireworks.ai/inference/v1',
  perplexity: 'https://api.perplexity.ai',
  deepinfra: 'https://api.deepinfra.com/v1/openai',
  huggingface: 'https://router.huggingface.co/v1',
  cerebras: 'https://api.cerebras.ai/v1',
  alibaba: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
  openrouter: 'https://openrouter.ai/api/v1',
  openai: 'https://api.openai.com/v1',
  luma: 'https://api.luma.ai',
};

/**
 * OpenRouter is addressed with an `openrouter/<vendor>/<model>` namespace so a
 * model id self-identifies its provider (and never collides with a native
 * vendor route). OpenRouter's own OpenAI-compatible API expects the bare
 * `<vendor>/<model>` slug, so the `openrouter/` segment is stripped before the
 * id reaches the wire.
 *
 * Exception: OpenRouter's OWN meta-routers live natively under the same prefix
 * as single tokens — `openrouter/auto` (best-model router) and `openrouter/free`
 * (random free-model router) — and MUST be sent verbatim (`openrouter/free`
 * stripped to `free` is rejected with "No endpoints available"). So we strip
 * only when the remainder is itself a `vendor/model` slug (contains a `/`); a
 * single-token remainder is a native OpenRouter id and is left untouched. The
 * `:free` suffix needs no handling — it rides along inside the model segment
 * (`openrouter/nvidia/nemotron:free` → `nvidia/nemotron:free`).
 */
export function stripOpenRouterPrefix(model: string): string {
  if (!model.startsWith('openrouter/')) return model;
  const rest = model.slice('openrouter/'.length);
  return rest.includes('/') ? rest : model;
}

function inferProviderFromApiKey(apiKey: string): InferredProvider | null {
  for (const { prefix, provider } of KEY_PREFIXES) {
    if (apiKey.startsWith(prefix)) {
      const models = MODEL_PREFERENCE[provider] || MODEL_PREFERENCE.openai;
      return {
        provider,
        defaultModel: models[0]!,
        defaultBaseUrl: PROVIDER_BASE_URLS[provider] || '',
      };
    }
  }
  return null;
}

function inferProviderFromBaseUrl(baseUrl: string): InferredProvider | null {
  for (const { pattern, provider } of URL_PATTERNS) {
    if (baseUrl.includes(pattern)) {
      const models = MODEL_PREFERENCE[provider] || MODEL_PREFERENCE.openai;
      return {
        provider,
        defaultModel: models[0]!,
        defaultBaseUrl: PROVIDER_BASE_URLS[provider] || baseUrl,
      };
    }
  }
  return null;
}

function inferProviderFromEnv(): InferredProvider | null {
  for (const { env, provider } of ENV_KEYS) {
    if (process.env[env]) {
      const models = MODEL_PREFERENCE[provider] || MODEL_PREFERENCE.openai;
      return {
        provider,
        defaultModel: models[0]!,
        defaultBaseUrl: PROVIDER_BASE_URLS[provider] || '',
      };
    }
  }
  return null;
}

async function resolveAvailableModel(
  provider: ProviderName | string,
  apiKey: string,
): Promise<string | null> {
  const preferences = MODEL_PREFERENCE[provider] || [];
  try {
    let availableModels: string[] = [];

    if (provider === 'google' || provider === 'google-vertex') {
      const res = await fetch(
        'https://generativelanguage.googleapis.com/v1beta/models',
        { headers: { 'x-goog-api-key': apiKey }, signal: AbortSignal.timeout(5000) },
      );
      if (res.ok) {
        const data = await res.json() as { models?: Array<{ name: string }> };
        availableModels = (data.models || []).map(m => m.name.replace('models/', ''));
      }
    } else if (provider === 'anthropic') {
      const res = await fetch('https://api.anthropic.com/v1/models', {
        headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
        signal: AbortSignal.timeout(5000),
      });
      if (res.ok) {
        const data = await res.json() as { data?: Array<{ id: string }> };
        availableModels = (data.data || []).map(m => m.id);
      }
    } else {
      // OpenAI-compatible providers all support GET /models
      const baseUrl = PROVIDER_BASE_URLS[provider as ProviderName] || 'https://api.openai.com/v1';
      const res = await fetch(`${baseUrl}/models`, {
        headers: { Authorization: `Bearer ${apiKey}` },
        signal: AbortSignal.timeout(5000),
      });
      if (res.ok) {
        const data = await res.json() as { data?: Array<{ id: string }> };
        availableModels = (data.data || []).map(m => m.id);
      }
    }

    if (availableModels.length === 0) return null;

    // OpenRouter's catalog is advertised with bare slugs, but we select and
    // display models in the `openrouter/` namespace. Match preferences by their
    // stripped slug, yet return the namespaced id so the API-boundary stripping
    // in resolveModel stays the single place the prefix is removed.
    const isOpenRouter = provider === 'openrouter';
    const available = new Set(availableModels);
    for (const pref of preferences) {
      if (available.has(isOpenRouter ? stripOpenRouterPrefix(pref) : pref)) return pref;
    }
    const first = availableModels[0] || null;
    return isOpenRouter && first ? `openrouter/${first}` : first;
  } catch {
    return null;
  }
}

export { resolveAvailableModel, MODEL_PREFERENCE, PROVIDER_BASE_URLS, inferProviderFromBaseUrl, inferProviderFromEnv };

const modelListCache = new Map<string, Promise<Record<string, unknown>[]>>();

function modelsEndpoint(config: ProviderConfig): { url: string; headers: Record<string, string> } | null {
  const provider = config.provider ?? '';
  const baseUrl = (config.baseUrl || '').replace(/\/+$/, '');
  const apiKey = config.apiKey || '';
  const officialAnthropic = (provider === 'anthropic' || provider === 'anthropic-vertex')
    && (!baseUrl || baseUrl.includes('anthropic.com'));
  const officialGoogle = (provider === 'google' || provider === 'google-vertex')
    && (!baseUrl || baseUrl.includes('googleapis.com'));

  if (officialAnthropic || baseUrl.includes('anthropic.com')) {
    const root = baseUrl.includes('anthropic.com') ? baseUrl : 'https://api.anthropic.com';
    const url = /\/v1$/.test(root) ? `${root}/models` : `${root}/v1/models`;
    return {
      url,
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
    };
  }
  if (officialGoogle || baseUrl.includes('generativelanguage.googleapis.com')) {
    const root = baseUrl.includes('generativelanguage.googleapis.com')
      ? baseUrl
      : 'https://generativelanguage.googleapis.com/v1beta';
    return {
      url: root.endsWith('/models') ? root : `${root}/models`,
      headers: { 'x-goog-api-key': apiKey },
    };
  }
  if (!baseUrl) return null;
  return {
    url: baseUrl.endsWith('/models') ? baseUrl : `${baseUrl}/models`,
    headers: apiKey ? { Authorization: `Bearer ${apiKey}` } : {},
  };
}

async function loadModelRecords(config: ProviderConfig): Promise<Record<string, unknown>[]> {
  const endpoint = modelsEndpoint(config);
  if (!endpoint) return [];
  const cacheKey = `${endpoint.url}|${endpoint.headers.Authorization ?? endpoint.headers['x-api-key'] ?? endpoint.headers['x-goog-api-key'] ?? ''}`;
  const cached = modelListCache.get(cacheKey);
  if (cached) return cached;
  const pending = (async () => {
    try {
      const res = await fetch(endpoint.url, {
        headers: endpoint.headers,
        signal: AbortSignal.timeout(1500),
      });
      if (!res.ok) {
        modelListCache.delete(cacheKey);
        return [];
      }
      return collectModelRecords(await res.json());
    } catch {
      modelListCache.delete(cacheKey);
      return [];
    }
  })();
  modelListCache.set(cacheKey, pending);
  return pending;
}

/**
 * Context window from the provider's model list.
 * Returns undefined when the server has no length, so the caller can try a
 * Hugging Face model card, then the proprietary table, then the default.
 */
export async function fetchAdvertisedContextWindow(config: ProviderConfig): Promise<number | undefined> {
  if (!config.model) return undefined;
  const records = await loadModelRecords(config);
  return pickAdvertisedContextWindow(records, config.model);
}

const huggingFaceCache = new Map<string, Promise<number | undefined>>();

function huggingFaceEndpoint(): string {
  return (process.env.HF_ENDPOINT || 'https://huggingface.co').replace(/\/+$/, '');
}

function huggingFaceToken(config: ProviderConfig): string | undefined {
  if (config.apiKey?.startsWith('hf_')) return config.apiKey;
  if (config.provider === 'huggingface' && config.apiKey && !config.apiKey.startsWith('sk-')) return config.apiKey;
  const env = process.env.HF_TOKEN;
  if (env && !env.startsWith('sk-')) return env;
  return undefined;
}

async function readHuggingFaceFile(url: string, headers: Record<string, string>): Promise<number | 'missing' | 'failed'> {
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(1000) });
    if (!res.ok) return 'missing';
    return readHuggingFaceContextWindow(await res.json()) ?? 'missing';
  } catch {
    return 'failed';
  }
}

/**
 * Context length from the Hub model card (`config.json` and
 * `tokenizer_config.json`). A network failure, a missing repo, or no
 * published length returns undefined so the caller can use the table.
 * `HF_ENDPOINT` selects the hub, including an offline mirror.
 */
export async function fetchHuggingFaceContextWindow(repo: string, config: ProviderConfig): Promise<number | undefined> {
  const endpoint = huggingFaceEndpoint();
  const token = huggingFaceToken(config);
  const cacheKey = `${endpoint}|${repo}|${token ?? ''}`;
  const cached = huggingFaceCache.get(cacheKey);
  if (cached) return cached;
  const pending = (async () => {
    const headers: Record<string, string> = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const root = repo.split('/').map(part => encodeURIComponent(part)).join('/');
    const lengths = await Promise.all([
      readHuggingFaceFile(`${endpoint}/${root}/resolve/main/config.json`, headers),
      readHuggingFaceFile(`${endpoint}/${root}/resolve/main/tokenizer_config.json`, headers),
    ]);
    if (lengths.some(length => length === 'failed')) huggingFaceCache.delete(cacheKey);
    const usable = lengths.filter((length): length is number => typeof length === 'number');
    if (usable.length === 0) return undefined;
    return Math.max(...usable);
  })();
  huggingFaceCache.set(cacheKey, pending);
  return pending;
}

/**
 * Served length, then a Hugging Face model card, then the model table,
 * then the 128k default. An explicit `contextWindow` on the config is kept:
 * provision already stored `max_model_len` there.
 */
export async function resolveContextWindow(config: ProviderConfig): Promise<number> {
  if (typeof config.contextWindow === 'number' && config.contextWindow > 0) {
    return config.contextWindow;
  }
  const records = config.model ? await loadModelRecords(config) : [];
  const fetched = config.model ? pickAdvertisedContextWindow(records, config.model) : undefined;
  if (fetched !== undefined) return fetched;
  for (const repo of huggingfaceRepoCandidates(config.model || '', records)) {
    const card = await fetchHuggingFaceContextWindow(repo, config);
    if (card !== undefined) return card;
  }
  return getContextWindow(config.model || '');
}

// ---------------------------------------------------------------------------
// Model resolution — pick the right AI SDK provider
// ---------------------------------------------------------------------------

// Providers that use native AI SDK packages (not OpenAI-compatible)
const NATIVE_PROVIDERS = new Set<ProviderName>(['anthropic', 'anthropic-vertex', 'google', 'google-vertex']);

function isVertexAIConfigured(): boolean {
  return !!(process.env.CLAUDE_CODE_USE_VERTEX === '1' || process.env.ANTHROPIC_VERTEX_PROJECT_ID);
}

// Providers that always route through OpenAI-compatible endpoint
const OPENAI_COMPAT_PROVIDERS = new Set<ProviderName>([
  'openai', 'groq', 'mistral', 'xai', 'deepseek', 'togetherai',
  'cohere', 'fireworks', 'perplexity', 'deepinfra', 'cerebras',
  'alibaba', 'openrouter', 'ollama', 'luma', 'ai-gateway',
]);

// ---------------------------------------------------------------------------
// Generic extra-body passthrough for OpenAI-compatible endpoints
// ---------------------------------------------------------------------------

/** True for a non-null, non-array plain object. */
export function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Deep-merge `overlay` into `base`, returning a new object. Nested plain
 * objects are merged recursively; every other value (scalars, arrays) from
 * `overlay` overrides the corresponding value in `base`.
 */
export function deepMergeRecords(
  base: Record<string, unknown>,
  overlay: Record<string, unknown>,
): Record<string, unknown> {
  const out: Record<string, unknown> = { ...base };
  for (const [key, value] of Object.entries(overlay)) {
    const current = out[key];
    if (isPlainRecord(current) && isPlainRecord(value)) {
      out[key] = deepMergeRecords(current, value);
    } else {
      out[key] = value;
    }
  }
  return out;
}

export function withExtraHeaders(
  extraHeaders: Record<string, string> | undefined,
  baseFetch: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch | undefined {
  if (!extraHeaders || Object.keys(extraHeaders).length === 0) return undefined;
  const wrapped: typeof globalThis.fetch = (input, init) => {
    const existingHeaders = init?.headers ?? {};
    const merged = existingHeaders instanceof Headers
      ? (() => { const h = new Headers(existingHeaders); for (const [k, v] of Object.entries(extraHeaders)) h.set(k, v); return h; })()
      : Array.isArray(existingHeaders)
        ? [...existingHeaders, ...Object.entries(extraHeaders)]
        : { ...existingHeaders, ...extraHeaders };
    return baseFetch(input, { ...init, headers: merged });
  };
  return wrapped;
}

/**
 * Build a `fetch` wrapper that deep-merges `extraBody` into the JSON request
 * body of every outgoing OpenAI-compatible request. Returns `undefined` when
 * there is nothing to merge, so callers can pass it straight to the AI SDK
 * (which then uses its default fetch). Non-JSON bodies are passed through
 * untouched.
 */
export function withExtraBody(
  extraBody: Record<string, unknown> | undefined,
  baseFetch: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch | undefined {
  if (!extraBody || Object.keys(extraBody).length === 0) return undefined;
  const wrapped: typeof globalThis.fetch = (input, init) => {
    if (init && typeof init.body === 'string') {
      try {
        const parsed: unknown = JSON.parse(init.body);
        if (isPlainRecord(parsed)) {
          init = { ...init, body: JSON.stringify(deepMergeRecords(parsed, extraBody)) };
        }
      } catch {
        // Non-JSON body — leave it untouched.
      }
    }
    return baseFetch(input, init);
  };
  return wrapped;
}

const GZIP_THRESHOLD_BYTES = 1024;

export function withGzipCompression(
  baseFetch: typeof globalThis.fetch = globalThis.fetch,
): typeof globalThis.fetch {
  return (input, init) => {
    if (init && typeof init.body === 'string' && init.body.length > GZIP_THRESHOLD_BYTES) {
      const compressed = gzipSync(Buffer.from(init.body, 'utf-8'));
      const existingHeaders = init.headers ?? {};
      const merged = existingHeaders instanceof Headers
        ? (() => { const h = new Headers(existingHeaders); h.set('content-encoding', 'gzip'); return h; })()
        : Array.isArray(existingHeaders)
          ? [...existingHeaders, ['content-encoding', 'gzip'] as [string, string]]
          : { ...existingHeaders, 'content-encoding': 'gzip' };
      return baseFetch(input, { ...init, body: compressed, headers: merged });
    }
    return baseFetch(input, init);
  };
}

/**
 * OpenAI routes a conversation to one cache when every turn sends the same
 * `prompt_cache_key`. vLLM and other compatible servers reject or ignore
 * unknown fields, so the key is attached only for official OpenAI hosts.
 * An explicit `extraBody.prompt_cache_key` is left unchanged.
 */
export function openAIPromptCacheBody(
  extraBody: Record<string, unknown> | undefined,
  cacheKey: string | undefined,
  provider: string | undefined,
  baseUrl: string | undefined,
): Record<string, unknown> | undefined {
  if (!cacheKey) return extraBody;
  const host = baseUrl ?? '';
  const official =
    host.includes('api.openai.com') ||
    host.includes('openai.azure.com') ||
    ((provider === 'openai' || provider === 'azure-openai') && host.length === 0);
  if (!official) return extraBody;
  if (extraBody && Object.prototype.hasOwnProperty.call(extraBody, 'prompt_cache_key')) {
    return extraBody;
  }
  return { ...(extraBody ?? {}), prompt_cache_key: cacheKey };
}

export function resolveModel(config: ProviderConfig) {
  const { apiKey } = config;
  const fromKey = inferProviderFromApiKey(apiKey);
  const fromUrl = config.baseUrl ? inferProviderFromBaseUrl(config.baseUrl) : null;
  // An explicit endpoint is the request. Ambient credentials (Vertex project,
  // Groq key, …) must not replace it, or a gemini / OpenAI-compatible model
  // is sent to the wrong SDK.
  const inferred = fromKey || fromUrl || (config.baseUrl ? null : inferProviderFromEnv());

  const rawModel = config.model || inferred?.defaultModel || 'gpt-4o';
  // An `openrouter/...` model id self-selects the OpenRouter provider and base
  // URL (even with no sk-or- key or openrouter.ai URL present), then the prefix
  // is stripped so the bare slug is what reaches OpenRouter's API.
  // An `openrouter/...` id is an explicit request, so it outranks ambient
  // inference (env keys, stray URLs) — but not an explicit config.provider /
  // config.baseUrl, which the caller set on purpose. Precedence:
  // explicit config > openrouter/ model prefix > inferred.
  const isOpenRouterModel = rawModel.startsWith('openrouter/');
  const provider = (config.provider as ProviderName) || (isOpenRouterModel ? 'openrouter' : inferred?.provider);
  const model = stripOpenRouterPrefix(rawModel);
  const baseUrl = config.baseUrl || (isOpenRouterModel ? PROVIDER_BASE_URLS.openrouter! : inferred?.defaultBaseUrl || '');
  const headerFetch = withExtraHeaders(config.extraHeaders);
  const bodyFetch = withExtraBody(openAIPromptCacheBody(config.extraBody, config.promptCacheKey, provider, baseUrl), headerFetch ?? globalThis.fetch);
  const fetch = bodyFetch ?? headerFetch;

  // Anthropic via Google Vertex AI
  if (provider === 'anthropic-vertex' || (model.startsWith('claude') && isVertexAIConfigured())) {
    const project = process.env.ANTHROPIC_VERTEX_PROJECT_ID || process.env.GOOGLE_VERTEX_PROJECT || '';
    const location = process.env.CLOUD_ML_REGION || process.env.GOOGLE_VERTEX_LOCATION || 'us-east5';
    return createVertexAnthropic({ project, location })(model);
  }

  // Native Anthropic SDK
  if (provider === 'anthropic' || model.startsWith('claude') || baseUrl.includes('anthropic.com')) {
    return createAnthropic({
      apiKey,
      baseURL: baseUrl || undefined,
      fetch: withGzipCompression(headerFetch ?? globalThis.fetch),
      ...(config.cacheTtl === '1h' ? { headers: { 'anthropic-beta': 'extended-cache-ttl-2025-04-11' } } : {}),
    })(model);
  }

  // Native Google Generative AI SDK
  if (provider === 'google' || provider === 'google-vertex' || model.startsWith('gemini') ||
      baseUrl.includes('generativelanguage.googleapis.com') || baseUrl.includes('aiplatform.googleapis.com')) {
    return createGoogleGenerativeAI({ apiKey, baseURL: baseUrl || undefined })(model);
  }

  // Azure OpenAI uses the OpenAI SDK with azure-specific baseURL
  if (provider === 'azure-openai' || baseUrl.includes('openai.azure.com')) {
    return createOpenAI({ apiKey, baseURL: baseUrl, fetch })(model);
  }

  // Amazon Bedrock — route through OpenAI-compatible if user provides a gateway URL
  if (provider === 'amazon-bedrock') {
    return createOpenAI({ apiKey, baseURL: baseUrl, fetch })(model);
  }

  // All OpenAI-compatible providers (groq, mistral, xai, deepseek, etc.)
  const effectiveBaseUrl = baseUrl || PROVIDER_BASE_URLS[provider as ProviderName] || 'https://api.openai.com/v1';
  const openai = createOpenAI({ baseURL: effectiveBaseUrl, apiKey, fetch });
  // gpt-6+ on OpenAI require /v1/responses for tool use with reasoning
  const isOpenAI = !baseUrl || effectiveBaseUrl.includes('api.openai.com');
  if (isOpenAI && requiresResponsesAPI(model)) {
    return openai.responses(model);
  }
  return openai.chat(model);
}

export { inferProviderFromApiKey };

// ---------------------------------------------------------------------------
// Convert our ToolDefinition[] to AI SDK ToolSet
// ---------------------------------------------------------------------------

export function convertToolsForSDK(tools: ToolDefinition[]): ToolSet {
  const sdkTools: Record<string, { description: string; inputSchema: ReturnType<typeof jsonSchema>; }> = {};
  for (const tool of tools) {
    sdkTools[tool.name] = {
      description: tool.description,
      inputSchema: jsonSchema(tool.inputSchema as any),
      // NO execute function — we handle execution in our engine loop
    };
  }
  return sdkTools;
}

// ---------------------------------------------------------------------------
// Convert our Message[] to AI SDK ModelMessage[]
// ---------------------------------------------------------------------------

export function convertMessages(messages: Message[]): ModelMessage[] {
  const toolNameById = new Map<string, string>();
  for (const msg of messages) {
    if (msg.role === 'assistant' && msg.tool_calls) {
      for (const tc of msg.tool_calls) toolNameById.set(tc.id, tc.function.name);
    }
  }

  return messages.map((msg, i): ModelMessage => {
    if (msg.role === 'tool') {
      const toolName = toolNameById.get(msg.tool_call_id) ?? '';
      return {
        role: 'tool',
        content: [
          {
            type: 'tool-result',
            toolCallId: msg.tool_call_id,
            toolName: toolName || 'unknown',
            output: { type: 'text', value: msg.content },
          },
        ],
      };
    }

    if (msg.role === 'assistant' && msg.tool_calls) {
      return {
        role: 'assistant',
        content: [
          ...(msg.content?.trim() ? [{ type: 'text' as const, text: msg.content }] : []),
          ...msg.tool_calls.map((tc) => ({
            type: 'tool-call' as const,
            toolCallId: tc.id,
            toolName: tc.function.name,
            input: (() => { try { return JSON.parse(tc.function.arguments); } catch { return {}; } })(),
          })),
        ],
      };
    }

    if (msg.role === 'assistant') {
      return { role: 'assistant', content: msg.content ?? '' };
    }

    if (msg.role === 'system') {
      return { role: 'system', content: msg.content };
    }

    // 'user' — handle both plain string and mixed content (text + image)
    if (Array.isArray(msg.content)) {
      const parts = msg.content.map((block) => {
        if (block.type === 'text') {
          return { type: 'text' as const, text: block.text };
        }
        // ImageContent -> AI SDK image part. The AI SDK v6 ImagePart field is
        // `mediaType` (not `mimeType`); using the wrong name drops the type.
        return {
          type: 'image' as const,
          image: block.source.data,
          mediaType: block.source.media_type,
        };
      });
      return { role: 'user' as const, content: parts } as ModelMessage;
    }
    return { role: 'user', content: msg.content };
  });
}

// ---------------------------------------------------------------------------
// Extract reasoning / thinking tokens from AI SDK usage objects
// ---------------------------------------------------------------------------

/**
 * Pull reasoning/thinking token counts out of the AI SDK usage object.
 * OpenAI-compatible vLLM (Qwen `reasoning_effort`) reports them as
 * `completion_tokens_details.reasoning_tokens`, which the SDK maps to
 * `usage.reasoningTokens` and/or `usage.outputTokenDetails.reasoningTokens`.
 */
export function extractReasoningTokens(usage: unknown): number {
  if (!usage || typeof usage !== 'object') return 0;
  const u = usage as Record<string, unknown>;

  const pick = (value: unknown): number | undefined => {
    if (typeof value === 'number' && Number.isFinite(value) && value > 0) return value;
    return undefined;
  };

  const fromTop = pick(u['reasoningTokens']);
  if (fromTop !== undefined) return fromTop;

  const details = u['outputTokenDetails'];
  if (details && typeof details === 'object') {
    const fromDetails = pick((details as Record<string, unknown>)['reasoningTokens']);
    if (fromDetails !== undefined) return fromDetails;
  }

  const raw = u['raw'];
  if (raw && typeof raw === 'object') {
    const rawObj = raw as Record<string, unknown>;
    const fromRaw = pick(rawObj['reasoning_tokens']);
    if (fromRaw !== undefined) return fromRaw;
    const ctd = rawObj['completion_tokens_details'];
    if (ctd && typeof ctd === 'object') {
      const fromCtd = pick((ctd as Record<string, unknown>)['reasoning_tokens']);
      if (fromCtd !== undefined) return fromCtd;
    }
  }
  return 0;
}

// ---------------------------------------------------------------------------
// Build provider-specific thinking / reasoning options
// ---------------------------------------------------------------------------

export function buildThinkingOptions(
  modelId: string,
  thinking: ThinkingConfig | undefined,
): Record<string, unknown> {
  if (!thinking?.enabled) return {};
  const caps = getModelCapabilities(modelId);

  // Anthropic Claude — thinking via providerOptions.anthropic.thinking
  if (caps?.supportsAdaptiveThinking) {
    const budgetTokens = thinking.budgetTokens ?? resolveThinkingBudget(thinking.level, modelId);
    return {
      providerOptions: {
        anthropic: {
          thinking: { type: 'enabled' as const, budgetTokens },
        },
      },
    };
  }

  // OpenAI o-series — reasoning_effort via providerOptions
  if (caps?.temperatureMustBeUnset) {
    const effortMap: Record<string, string> = { low: 'low', medium: 'medium', high: 'high', max: 'high' };
    return { providerOptions: { openai: { reasoningEffort: effortMap[thinking.level] ?? 'medium' } } };
  }

  // Google Gemini 2.5 — thinkingConfig via providerOptions
  if (modelId.startsWith('gemini-2.5') || modelId.startsWith('gemini-3')) {
    const budgetTokens = thinking.budgetTokens ?? resolveThinkingBudget(thinking.level, modelId);
    return { providerOptions: { google: { thinkingConfig: { thinkingBudget: budgetTokens, includeThoughts: true } } } };
  }

  // DeepSeek / other: no special options needed, thinking comes via inline tags
  return {};
}

// ---------------------------------------------------------------------------
// Prompt cache optimization for Anthropic
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// streamChatCompletion — main streaming function
// ---------------------------------------------------------------------------

function promptTokensForFit(
  systemPrompt: string | undefined,
  messages: Message[],
  tools: ToolDefinition[],
): number {
  let text = systemPrompt ?? '';
  for (const msg of messages) {
    text += typeof msg.content === 'string' ? msg.content : JSON.stringify(msg.content ?? '');
    if (msg.role === 'assistant' && msg.tool_calls) text += JSON.stringify(msg.tool_calls);
  }
  if (tools.length > 0) text += JSON.stringify(tools);
  return estimateTokens(text);
}

/**
 * Build the Anthropic ephemeral cache-control object, honoring an optional TTL.
 * '1h' requires Anthropic's extended-cache beta; default (undefined/'5m') uses
 * the standard 5-minute ephemeral cache.
 */
export function buildAnthropicCacheControl(cacheTtl?: '5m' | '1h'): { type: 'ephemeral'; ttl?: '1h' } {
  return cacheTtl === '1h' ? { type: 'ephemeral', ttl: '1h' } : { type: 'ephemeral' };
}

// ---------------------------------------------------------------------------
// repairToolCall — fix hallucinated tool names at the stream level
// ---------------------------------------------------------------------------

/**
 * Map of commonly-hallucinated tool names to the correct tool name.
 * Used by buildRepairToolCall to correct tool names before they reach
 * the engine's tool executor.
 */
const TOOL_NAME_CORRECTIONS: Record<string, string> = {
  read_file: 'file_read',
  edit_file: 'file_edit',
  write_file: 'file_edit',
  list_directory: 'list_dir',
  agentic_fetch: 'web_fetch',
  run_command: 'bash',
  execute: 'bash',
  search_files: 'grep',
  find_files: 'glob',
  search: 'grep',
  cat: 'file_read',
  ls: 'list_dir',
  find: 'glob',
  curl: 'web_fetch',
};

/**
 * Build a repairToolCall callback for the AI SDK's streamText().
 * When the model hallucinates a tool name (e.g. "read_file" instead of
 * "file_read"), this function maps it to the correct registered name.
 * Returns null if no correction is possible.
 */
export function buildRepairToolCall(tools: ToolDefinition[]): ToolCallRepairFunction<ToolSet> {
  const registeredNames = new Set(tools.map(t => t.name));
  return async ({ toolCall }) => {
    const hallucinated = toolCall.toolName;
    const corrected = TOOL_NAME_CORRECTIONS[hallucinated] ?? TOOL_NAME_CORRECTIONS[hallucinated.toLowerCase()];
    if (corrected && registeredNames.has(corrected)) {
      return { ...toolCall, toolName: corrected };
    }
    return null;
  };
}

export async function* streamChatCompletion(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[],
  abortSignal: AbortSignal,
  options?: { thinking?: ThinkingConfig; repairToolCall?: ToolCallRepairFunction<ToolSet>; streamTextImpl?: typeof streamText },
): AsyncGenerator<StreamChunk> {
  // Test seam: the AI SDK streamText entry point. Defaults to the real import so
  // production behavior is unchanged; tests inject a fake that drives the
  // stream-part switch (tool-call / finish-step / error / reasoning) and the
  // generator's catch (abort / MissingToolResults / API errors) deterministically.
  const streamTextFn = options?.streamTextImpl ?? streamText;
  const model = resolveModel(config);
  const inferred = inferProviderFromApiKey(config.apiKey)
    || (config.baseUrl ? inferProviderFromBaseUrl(config.baseUrl) : null)
    || inferProviderFromEnv();
  const modelId = config.model || inferred?.defaultModel || 'gpt-4o';
  const sdkTools = tools.length > 0 ? convertToolsForSDK(tools) : undefined;

  // Extract system message and pass via `system` parameter to avoid AI SDK warning
  let systemPrompt: string | undefined;
  const nonSystemMessages = messages.filter(m => {
    if (m.role === 'system') {
      systemPrompt = (systemPrompt ? systemPrompt + '\n\n' : '') + m.content;
      return false;
    }
    return true;
  });
  const isAnthropic = modelId.startsWith('claude') ||
    config.baseUrl?.includes('anthropic.com') ||
    config.provider === 'anthropic' ||
    config.provider === 'anthropic-vertex';

  const healed = healOrphanedToolCalls(nonSystemMessages);
  const sanitized = sanitizeToolCallIds(healed, isAnthropic ? 'anthropic' : '');
  const coreMessages = convertMessages(sanitized);

  if (isAnthropic) {
    stripEmptyTextBlocks(coreMessages as Array<Record<string, unknown>>);
  }

  // Apply per-message cache breakpoints for Anthropic prompt caching.
  // The AI SDK v4 expects cache_control on individual message content parts
  // via providerOptions, not as a top-level streamText option.
  const cacheControl = buildAnthropicCacheControl(config.cacheTtl);
  if (isAnthropic) {
    const breakpointIndices = new Set<number>();
    if (systemPrompt && coreMessages.length > 0) breakpointIndices.add(0);
    const convPoints = buildConversationCachePoints(coreMessages);
    for (const idx of Object.keys(convPoints)) breakpointIndices.add(Number(idx));
    for (const idx of breakpointIndices) {
      const msg = coreMessages[idx] as Record<string, unknown> | undefined;
      if (!msg) continue;
      const content = msg.content;
      if (Array.isArray(content)) {
        const hasTextPart = content.some((p: Record<string, unknown>) =>
          p.type === 'text' && typeof p.text === 'string' && p.text.trim() !== '');
        if (!hasTextPart) continue;
      } else if (typeof content === 'string' && content.trim() === '') {
        continue;
      }
      msg.providerOptions = {
        ...((msg.providerOptions as Record<string, unknown>) || {}),
        anthropic: {
          ...((msg.providerOptions as Record<string, Record<string, unknown>>) || {}).anthropic,
          cacheControl,
        },
      };
    }
  }

  try {
    const effectiveTemperature = resolveTemperature(modelId, config.temperature, options?.thinking);

    const result = streamTextFn({
      model,
      instructions: systemPrompt,
      messages: coreMessages,
      tools: sdkTools,
      maxOutputTokens: fitOutputTokens(
        config.maxTokens ?? DEFAULT_MAX_OUTPUT_TOKENS,
        config.contextWindow ?? getContextWindow(modelId),
        promptTokensForFit(systemPrompt, sanitized, tools),
      ),
      ...(effectiveTemperature !== undefined ? { temperature: effectiveTemperature } : {}),
      ...(options?.repairToolCall ? { repairToolCall: options.repairToolCall } : {}),
      abortSignal,
      maxRetries: 0,
      onError: ({ error }) => {
        const msg = error instanceof Error ? error.message : String(error);
        if (msg.includes('MissingToolResults')) return;
      },
      ...buildThinkingOptions(modelId, options?.thinking),
    });

    // Track tool call indices so engine.ts can accumulate them by index
    let toolCallIndex = 0;

    const stream = result.stream;
    for await (const part of stream) {
      switch (part.type) {
        case 'text-delta':
          yield { type: 'content_delta', text: part.text };
          break;

        case 'tool-call':
          // The AI SDK delivers a complete tool-call (not deltas).
          // We emit a single tool_call_delta with the full payload so
          // engine.ts's accumulator can build the final ToolCallContent.
          yield {
            type: 'tool_call_delta',
            toolCall: {
              index: toolCallIndex++,
              id: part.toolCallId,
              function: {
                name: part.toolName,
                arguments: JSON.stringify(part.input),
              },
            },
          };
          break;

        case 'finish-step':
          yield {
            type: 'done',
            finishReason: part.finishReason,
            usage: {
              promptTokens: part.usage.inputTokens ?? 0,
              completionTokens: part.usage.outputTokens ?? 0,
              totalTokens: part.usage.totalTokens ?? (
                (part.usage.inputTokens ?? 0) + (part.usage.outputTokens ?? 0)
              ),
              reasoningTokens: extractReasoningTokens(part.usage),
              cachedPromptTokens: (part.usage as { cachedInputTokens?: number }).cachedInputTokens ?? (part.usage as { inputTokenDetails?: { cacheReadTokens?: number } }).inputTokenDetails?.cacheReadTokens ?? 0,
            },
            responseHeaders: (part as { response?: { headers?: Record<string, string> } }).response?.headers,
          };
          return;

        case 'error': {
          const errObj = part.error as { statusCode?: number; responseHeaders?: Record<string, string>; message?: string; type?: string } | undefined;
          const errStatusCode = errObj?.statusCode;
          const errMsg = errStatusCode
            ? `HTTP ${errStatusCode}: ${errObj?.message ?? String(part.error)}`
            : errObj?.message ?? (typeof part.error === 'string' ? part.error : String(part.error));

          // Extract Retry-After (seconds) — handles numeric and HTTP-date forms.
          const errRetryAfter = extractRetryAfterSeconds(errObj?.responseHeaders);

          yield {
            type: 'error',
            error: errMsg,
            errorObject: part.error instanceof Error ? part.error : undefined,
            ...(errRetryAfter !== undefined && { retryAfter: errRetryAfter }),
            responseHeaders: errObj?.responseHeaders,
          };
          return;
        }

        case 'reasoning-delta':
          yield { type: 'thinking_delta', text: (part as { text?: string }).text ?? '' };
          break;

        default:
          // Ignore other part types (source, file, etc.)
          break;
      }
    }

    // If we reach here without a finish-step (shouldn't happen), emit done
    yield { type: 'done' };
  } catch (err: unknown) {
    if (abortSignal.aborted) {
      yield { type: 'error', error: 'Request aborted' };
    } else if (err instanceof Error && err.message.includes('MissingToolResults')) {
      yield { type: 'done', finishReason: 'tool-calls' };
    } else {
      const baseMsg = err instanceof Error ? err.message : String(err);

      // The AI SDK throws APICallError with statusCode and responseHeaders.
      // Extract these for the retry wrapper.
      const statusCode = (err as { statusCode?: number }).statusCode;
      const responseHeaders = (err as { responseHeaders?: Record<string, string> }).responseHeaders;

      // Format error with HTTP status prefix so extractStatusCode() can find it
      const errorMsg = statusCode ? `HTTP ${statusCode}: ${baseMsg}` : baseMsg;

      // Extract Retry-After (seconds) — handles numeric and HTTP-date forms.
      const retryAfter = extractRetryAfterSeconds(responseHeaders);

      yield {
        type: 'error',
        error: errorMsg,
        errorObject: err instanceof Error ? err : undefined,
        ...(retryAfter !== undefined && { retryAfter }),
        responseHeaders,
      };
    }
  }
}

// ---------------------------------------------------------------------------
// Retry logic with exponential backoff
// ---------------------------------------------------------------------------

const RETRYABLE_STATUS_CODES = new Set([429, 500, 502, 503, 529]);
const MAX_RETRIES = 5;
const BASE_DELAY_MS = 1000;

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException('Request aborted', 'AbortError'));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(new DOMException('Request aborted', 'AbortError'));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

function extractStatusCode(errorText: string): number | null {
  // Match "HTTP {status}" or "status code {status}" patterns commonly
  // found in AI SDK error messages and our own error formatting.
  const match = errorText.match(/(?:HTTP|status\s+code)\s+(\d+)/i);
  if (match) {
    return parseInt(match[1], 10);
  }
  return null;
}

const NETWORK_ERROR_CODES = new Set([
  'ECONNRESET', 'ECONNREFUSED', 'ECONNABORTED', 'ETIMEDOUT',
  'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH',
]);
export function isNetworkError(err: unknown): boolean {
  const code = (err as { code?: string })?.code;
  if (code && NETWORK_ERROR_CODES.has(code)) return true;
  const msg = err instanceof Error ? err.message : '';
  return /ECONNRESET|ECONNREFUSED|ECONNABORTED|ETIMEDOUT|socket hang up/i.test(msg);
}

// ---------------------------------------------------------------------------
// SSL/TLS certificate error detection — fail fast with actionable hints
// ---------------------------------------------------------------------------

const SSL_ERROR_CODES = new Set([
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'CERT_HAS_EXPIRED',
  'DEPTH_ZERO_SELF_SIGNED_CERT',
  'ERR_TLS_CERT_ALTNAME_INVALID',
  'SELF_SIGNED_CERT_IN_CHAIN',
]);

export function isSslError(err: unknown): boolean {
  const code = (err as { code?: string })?.code;
  if (code && SSL_ERROR_CODES.has(code)) return true;
  const msg = err instanceof Error ? err.message : '';
  return /self signed certificate|certificate has expired|unable to get local issuer certificate/i.test(msg);
}

export const DEFAULT_REQUEST_TIMEOUT_MS = 300_000; // 5 minutes

export async function* streamChatCompletionWithRetry(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[],
  abortSignal: AbortSignal,
  options?: { thinking?: ThinkingConfig; repairToolCall?: ToolCallRepairFunction<ToolSet>; requestTimeoutMs?: number; idleTimeoutMs?: number; maxRetries?: number; baseDelayMs?: number; maxDelayMs?: number; maxTotalWaitMs?: number; onRetry?: (attempt: number, maxRetries: number, delayMs: number, statusCode: number) => void; streamImpl?: typeof streamChatCompletion },
): AsyncGenerator<StreamChunk> {
  // Test seam: the underlying single-attempt stream. Defaults to the real
  // streamChatCompletion so production behavior is unchanged; tests inject a
  // generator that throws (abort / SSL / network / generic) to drive the catch
  // and network-retry branches, which real transports surface as error chunks
  // rather than thrown errors.
  const streamImpl = options?.streamImpl ?? streamChatCompletion;
  const envTimeout = Number(process.env.SUPERINFERENCE_API_TIMEOUT_MS);
  const timeoutMs = options?.requestTimeoutMs ?? (envTimeout > 0 ? envTimeout : DEFAULT_REQUEST_TIMEOUT_MS);
  const maxRetries = options?.maxRetries ?? MAX_RETRIES;
  const baseDelay = options?.baseDelayMs ?? BASE_DELAY_MS;
  const maxDelay = options?.maxDelayMs ?? 60_000;
  const maxTotalWait = options?.maxTotalWaitMs ?? 0;
  const retryStart = maxTotalWait > 0 ? Date.now() : 0;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (abortSignal.aborted) {
      yield { type: 'error', error: 'Request aborted' };
      return;
    }

    // Combine caller abort signal with per-request hard timeout
    const requestAbort = new AbortController();
    const timeoutId = setTimeout(() => requestAbort.abort(), timeoutMs);
    const onCallerAbort = () => requestAbort.abort();
    abortSignal.addEventListener('abort', onCallerAbort, { once: true });

    let contentStarted = false;
    let timedOut = false;
    let networkRetry = false;

    // Inactivity watchdog: kill a stream that goes quiet (connection open, no
    // bytes) far sooner than the total request timeout. Reset on every chunk.
    const idleMs = options?.idleTimeoutMs;
    let idleTimer: ReturnType<typeof setTimeout> | undefined;
    const resetIdle = () => {
      if (!idleMs) return;
      if (idleTimer) clearTimeout(idleTimer);
      idleTimer = setTimeout(() => requestAbort.abort(), idleMs);
    };

    try {
    resetIdle();
    for await (const chunk of streamImpl(config, messages, tools, requestAbort.signal, options)) {
      resetIdle();
      if (chunk.type === 'error' && !contentStarted) {
        const statusCode = extractStatusCode(chunk.error ?? '');

        if (statusCode === 401) {
          yield chunk;
          return;
        }

        // Gateway rejection: a proxy rejected the anthropic-beta header.
        // Strip cacheTtl (which controls the beta header) and retry immediately.
        if (statusCode === 400 && attempt < maxRetries && isGatewayRejection((chunk.error ?? '').toLowerCase())) {
          config = { ...config, cacheTtl: undefined };
          break; // retry without beta headers
        }

        // Honor the API's x-should-retry hint (the SDK retry layer is disabled
        // via maxRetries:0, so we honor it here): 'false' => never retry, even a
        // normally-retryable status (otherwise we hold the turn for minutes
        // re-sending a request the API already said is pointless); 'true' =>
        // retry even a normally-terminal status the API says is worth retrying;
        // absent => fall back to the retryable-status set.
        const shouldRetryHdr = chunk.responseHeaders?.['x-should-retry'];
        const retryAllowed = shouldRetryHdr === 'false'
          ? false
          : (shouldRetryHdr === 'true' || (statusCode !== null && RETRYABLE_STATUS_CODES.has(statusCode)));
        if (retryAllowed && attempt < maxRetries) {
          if (maxTotalWait > 0 && (Date.now() - retryStart) >= maxTotalWait) {
            yield chunk;
            return;
          }

          let delayMs: number;
          const retryAfterSeconds = chunk.retryAfter;
          const expBackoff = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
          if (statusCode === 429 && retryAfterSeconds != null) {
            delayMs = Math.max(retryAfterSeconds * 1000, baseDelay);
          } else if (statusCode === 429 && chunk.responseHeaders) {
            // No Retry-After header: when the server's rate-limit headers say we
            // are out of budget, honor the reset window (x-ratelimit-reset-*)
            // instead of a blind exponential backoff. Floored at baseDelay,
            // capped at maxDelay.
            const info = parseRateLimitHeaders(chunk.responseHeaders);
            delayMs = shouldBackoff(info)
              ? Math.min(Math.max(suggestedWaitMs(info, expBackoff), baseDelay), maxDelay)
              : expBackoff;
          } else {
            delayMs = expBackoff;
          }

          if (options?.onRetry) {
            // statusCode may be null when the retry was forced by x-should-retry:true
            // on an error with no parsed HTTP status; 0 is the established "unknown
            // status" sentinel other onRetry call sites use.
            options.onRetry(attempt + 1, maxRetries, delayMs, statusCode ?? 0);
          }

          try {
            await sleep(delayMs, abortSignal);
          } catch {
            yield { type: 'error', error: 'Request aborted' };
            return;
          }
          break; // break inner loop to retry
        }

        yield chunk;
        return;
      }

      // Once we yield any content, we're committed to this stream
      if (chunk.type === 'content_delta' || chunk.type === 'tool_call_delta' || chunk.type === 'thinking_delta') {
        contentStarted = true;
      }

      // Detect timeout: if we get a 'done' because the request was aborted by our timeout
      // (not the caller's abort), treat it as a timeout, not a normal completion
      if (chunk.type === 'done' && requestAbort.signal.aborted && !abortSignal.aborted && !chunk.finishReason) {
        timedOut = true;
        break;
      }

      yield chunk;

      if (chunk.type === 'done' || (chunk.type === 'error' && contentStarted)) {
        return;
      }
    }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if ((msg.includes('AbortError') || msg.includes('aborted') || msg.includes('abort')) && !abortSignal.aborted) {
        timedOut = true;
      } else if (requestAbort.signal.aborted && !abortSignal.aborted) {
        timedOut = true;
      } else if (isSslError(err)) {
        // SSL/TLS errors are not transient — fail immediately with actionable hints
        const original = err instanceof Error ? err.message : String(err);
        throw new Error(
          `SSL/TLS certificate error: ${original}. Possible fixes: ` +
          '(1) Set NODE_EXTRA_CA_CERTS=/path/to/ca-bundle.crt for corporate proxies ' +
          '(2) Check if a TLS-inspecting proxy needs its CA certificate added ' +
          "(3) Verify server certificates haven't expired",
        );
      } else if (!contentStarted && attempt < maxRetries && isNetworkError(err)) {
        networkRetry = true;
      } else {
        throw err;
      }
    } finally {
      clearTimeout(timeoutId);
      if (idleTimer) clearTimeout(idleTimer);
      abortSignal.removeEventListener('abort', onCallerAbort);
    }

    // Also detect timeout when the stream ended gracefully due to abort
    if (!timedOut && requestAbort.signal.aborted && !abortSignal.aborted) {
      timedOut = true;
    }

    if (timedOut) {
      if (contentStarted) {
        yield { type: 'error', error: `Request timed out after ${timeoutMs}ms (partial content received)` };
        return;
      }
      // No content yet — treat like a retryable error
      if (attempt < maxRetries) {
        if (maxTotalWait > 0 && (Date.now() - retryStart) >= maxTotalWait) {
          yield { type: 'error', error: `Retry time limit exceeded (${maxTotalWait}ms)` };
          return;
        }
        const timeoutDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
        if (options?.onRetry) {
          options.onRetry(attempt + 1, maxRetries, timeoutDelay, 0);
        }
        try {
          await sleep(timeoutDelay, abortSignal);
        } catch {
          yield { type: 'error', error: 'Request aborted' };
          return;
        }
        continue;
      }
      yield { type: 'error', error: `Request timed out after ${timeoutMs}ms` };
      return;
    }

    if (networkRetry) {
      if (maxTotalWait > 0 && (Date.now() - retryStart) >= maxTotalWait) {
        yield { type: 'error', error: 'Network error (retry time limit exceeded)' };
        return;
      }
      const netDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
      if (options?.onRetry) {
        options.onRetry(attempt + 1, maxRetries, netDelay, 0);
      }
      try {
        await sleep(netDelay, abortSignal);
      } catch {
        yield { type: 'error', error: 'Request aborted' };
        return;
      }
      continue;
    }

    // If we got here via 'break' (retry), the inner loop ended without return — continue outer loop
    if (contentStarted) {
      return; // Stream completed or errored mid-content
    }
  }

  yield { type: 'error', error: `Max retries (${maxRetries}) exceeded` };
}

// ---------------------------------------------------------------------------
// Credential sanitization — strip secrets from error messages
// ---------------------------------------------------------------------------

export const CREDENTIAL_PATTERNS: RegExp[] = [
  /sk-[a-zA-Z0-9_-]{10,}/g,
  /sk-ant-[a-zA-Z0-9_-]{10,}/g,
  /sk-or-[a-zA-Z0-9_-]{10,}/g,
  /key-[a-zA-Z0-9_-]{10,}/g,
  /Bearer\s+[a-zA-Z0-9_.\-/+=]{10,}/gi,
  /\b[a-zA-Z0-9._%+-]+:[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+/g,
  /(?:x-api-key|authorization|x-goog-api-key)[:\s]+[^\s,;]{10,}/gi,
  /AIzaSy[a-zA-Z0-9_-]{30,}/g,
  /gsk_[a-zA-Z0-9_-]{10,}/g,
  /xai-[a-zA-Z0-9_-]{10,}/g,
  /hf_[a-zA-Z0-9_-]{10,}/g,
];

export function sanitizeErrorMessage(error: Error | string): string {
  const msg = error instanceof Error ? error.message : error;
  let sanitized = msg;
  for (const pattern of CREDENTIAL_PATTERNS) {
    sanitized = sanitized.replace(new RegExp(pattern.source, pattern.flags), '[REDACTED]');
  }
  return sanitized;
}

// ---------------------------------------------------------------------------
// Retry-After header parsing
// ---------------------------------------------------------------------------

export const MAX_RETRY_AFTER_MS = 3_600_000;
const DEFAULT_RETRY_AFTER_MS = 60_000;
const MIN_RETRY_AFTER_MS = 1_000;

export function parseRetryAfterHeader(header: string | null | undefined): number {
  if (!header || !header.trim()) return DEFAULT_RETRY_AFTER_MS;
  const trimmed = header.trim();
  const numeric = Number(trimmed);
  if (!isNaN(numeric) && isFinite(numeric)) {
    const ms = numeric * 1000;
    return Math.max(MIN_RETRY_AFTER_MS, Math.min(ms, MAX_RETRY_AFTER_MS));
  }
  const date = Date.parse(trimmed);
  if (!isNaN(date)) {
    const delayMs = date - Date.now();
    if (delayMs <= 0) return MIN_RETRY_AFTER_MS;
    return Math.max(MIN_RETRY_AFTER_MS, Math.min(delayMs, MAX_RETRY_AFTER_MS));
  }
  return DEFAULT_RETRY_AFTER_MS;
}

// ---------------------------------------------------------------------------
// Prompt cache tracking — detect cache thrashing from mid-session tool loads
// ---------------------------------------------------------------------------

export const CACHE_MISS_WARN_THRESHOLD = 3;

export class PromptCacheTracker {
  private _hits = 0;
  private _misses = 0;
  private _consecutiveMisses = 0;

  recordCacheHit(promptTokens: number, cachedTokens: number): void {
    this._hits++;
    this._consecutiveMisses = 0;
  }

  recordCacheMiss(promptTokens: number, cachedTokens: number): void {
    this._misses++;
    this._consecutiveMisses++;
  }

  getHitRate(): number {
    const total = this._hits + this._misses;
    if (total === 0) return 0;
    return this._hits / total;
  }

  getTotalRequests(): number {
    return this._hits + this._misses;
  }

  getConsecutiveMisses(): number {
    return this._consecutiveMisses;
  }

  resetConsecutiveMisses(): void {
    this._consecutiveMisses = 0;
  }

  shouldWarnAboutMisses(): boolean {
    return this._consecutiveMisses >= CACHE_MISS_WARN_THRESHOLD;
  }
}
