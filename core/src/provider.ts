import { streamText, jsonSchema, stepCountIs } from 'ai';
import type { ModelMessage, ToolSet } from 'ai';
import { createOpenAI } from '@ai-sdk/openai';
import { createAnthropic } from '@ai-sdk/anthropic';
import { createGoogleGenerativeAI } from '@ai-sdk/google';
import { getModelCapabilities, resolveThinkingBudget, resolveTemperature } from './model-capabilities';
import { sanitizeToolCallIds, buildConversationCachePoints, healOrphanedToolCalls } from './provider-transform';
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
  | 'google-vertex' | 'ai-gateway' | 'huggingface';

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
  'amazon-bedrock': ['anthropic.claude-sonnet-4-6-20250514', 'anthropic.claude-haiku-4-5-20251001'],
  openrouter: ['anthropic/claude-sonnet-4', 'openai/gpt-4o'],
  huggingface: ['deepseek-ai/DeepSeek-V3-0324', 'Qwen/Qwen3-235B-A22B', 'Qwen/Qwen3-32B'],
  luma: ['luma-photon-latest'],
};

// API key prefix → provider detection
const KEY_PREFIXES: Array<{ prefix: string; provider: ProviderName; exclude?: string[] }> = [
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
  { env: 'HF_TOKEN', provider: 'huggingface' },
  { env: 'AZURE_OPENAI_API_KEY', provider: 'azure-openai' },
  { env: 'AWS_ACCESS_KEY_ID', provider: 'amazon-bedrock' },
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

function inferProviderFromApiKey(apiKey: string): InferredProvider | null {
  for (const { prefix, provider, exclude } of KEY_PREFIXES) {
    if (apiKey.startsWith(prefix)) {
      if (exclude?.some(ex => apiKey.startsWith(ex))) continue;
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

    const available = new Set(availableModels);
    for (const pref of preferences) {
      if (available.has(pref)) return pref;
    }
    return availableModels[0] || null;
  } catch {
    return null;
  }
}

export { resolveAvailableModel, MODEL_PREFERENCE, inferProviderFromBaseUrl, inferProviderFromEnv };

// ---------------------------------------------------------------------------
// Model resolution — pick the right AI SDK provider
// ---------------------------------------------------------------------------

// Providers that use native AI SDK packages (not OpenAI-compatible)
const NATIVE_PROVIDERS = new Set<ProviderName>(['anthropic', 'google', 'google-vertex']);

// Providers that always route through OpenAI-compatible endpoint
const OPENAI_COMPAT_PROVIDERS = new Set<ProviderName>([
  'openai', 'groq', 'mistral', 'xai', 'deepseek', 'togetherai',
  'cohere', 'fireworks', 'perplexity', 'deepinfra', 'cerebras',
  'alibaba', 'openrouter', 'ollama', 'luma', 'ai-gateway',
]);

export function resolveModel(config: ProviderConfig) {
  const { apiKey } = config;
  const inferred = inferProviderFromApiKey(apiKey)
    || (config.baseUrl ? inferProviderFromBaseUrl(config.baseUrl) : null)
    || inferProviderFromEnv();

  const provider = (config.provider as ProviderName) || inferred?.provider;
  const model = config.model || inferred?.defaultModel || 'gpt-4o';
  const baseUrl = config.baseUrl || inferred?.defaultBaseUrl || 'https://api.openai.com/v1';

  // Native Anthropic SDK
  if (provider === 'anthropic' || model.startsWith('claude') || baseUrl.includes('anthropic.com')) {
    return createAnthropic({ apiKey })(model);
  }

  // Native Google Generative AI SDK
  if (provider === 'google' || provider === 'google-vertex' || model.startsWith('gemini') ||
      baseUrl.includes('generativelanguage.googleapis.com') || baseUrl.includes('aiplatform.googleapis.com')) {
    return createGoogleGenerativeAI({ apiKey })(model);
  }

  // Azure OpenAI uses the OpenAI SDK with azure-specific baseURL
  if (provider === 'azure-openai' || baseUrl.includes('openai.azure.com')) {
    return createOpenAI({ apiKey, baseURL: baseUrl })(model);
  }

  // Amazon Bedrock — route through OpenAI-compatible if user provides a gateway URL
  if (provider === 'amazon-bedrock') {
    return createOpenAI({ apiKey, baseURL: baseUrl })(model);
  }

  // All OpenAI-compatible providers (groq, mistral, xai, deepseek, etc.)
  const effectiveBaseUrl = baseUrl || PROVIDER_BASE_URLS[provider as ProviderName] || 'https://api.openai.com/v1';
  const openai = createOpenAI({ baseURL: effectiveBaseUrl, apiKey });
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
      inputSchema: jsonSchema(tool.inputSchema),
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
          ...(msg.content ? [{ type: 'text' as const, text: msg.content }] : []),
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
        // ImageContent -> AI SDK image part
        return {
          type: 'image' as const,
          image: block.source.data,
          mimeType: block.source.media_type,
        };
      });
      return { role: 'user' as const, content: parts } as ModelMessage;
    }
    return { role: 'user', content: msg.content };
  });
}

// ---------------------------------------------------------------------------
// Build provider-specific thinking / reasoning options
// ---------------------------------------------------------------------------

function buildThinkingOptions(
  modelId: string,
  thinking: ThinkingConfig | undefined,
): Record<string, unknown> {
  if (!thinking?.enabled) return {};
  const caps = getModelCapabilities(modelId);

  // Anthropic Claude — adaptive thinking via experimental_thinking
  if (caps?.supportsAdaptiveThinking) {
    const budgetTokens = thinking.budgetTokens ?? resolveThinkingBudget(thinking.level);
    return { experimental_thinking: { enabled: true, budgetTokens } };
  }

  // OpenAI o-series — reasoning_effort via providerOptions
  if (caps?.temperatureMustBeUnset) {
    const effortMap: Record<string, string> = { low: 'low', medium: 'medium', high: 'high', max: 'high' };
    return { providerOptions: { openai: { reasoningEffort: effortMap[thinking.level] ?? 'medium' } } };
  }

  // Google Gemini 2.5 — thinkingConfig via providerOptions
  if (modelId.startsWith('gemini-2.5') || modelId.startsWith('gemini-3')) {
    const budgetTokens = thinking.budgetTokens ?? resolveThinkingBudget(thinking.level);
    return { providerOptions: { google: { thinkingConfig: { thinkingBudget: budgetTokens, includeThoughts: true } } } };
  }

  // DeepSeek / other: no special options needed, thinking comes via inline tags
  return {};
}

// ---------------------------------------------------------------------------
// Prompt cache optimization for Anthropic
// ---------------------------------------------------------------------------

function buildCacheBreakpoints(messages: ModelMessage[], hasSystemPrompt: boolean): Record<string, unknown> {
  const cachePoints: Record<number, { type: string }> = {};

  if (hasSystemPrompt && messages.length > 0) {
    cachePoints[0] = { type: 'ephemeral' };
  }

  const conversationPoints = buildConversationCachePoints(messages);
  Object.assign(cachePoints, conversationPoints);

  if (Object.keys(cachePoints).length === 0) return {};

  return {
    providerOptions: {
      anthropic: {
        cacheControl: cachePoints,
      },
    },
  };
}

// ---------------------------------------------------------------------------
// streamChatCompletion — main streaming function
// ---------------------------------------------------------------------------

export async function* streamChatCompletion(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[],
  abortSignal: AbortSignal,
  options?: { thinking?: ThinkingConfig },
): AsyncGenerator<StreamChunk> {
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
    config.provider === 'anthropic';

  const healed = healOrphanedToolCalls(nonSystemMessages);
  const sanitized = sanitizeToolCallIds(healed, isAnthropic ? 'anthropic' : '');
  const coreMessages = convertMessages(sanitized);

  const cacheBreakpoints = isAnthropic ? buildCacheBreakpoints(coreMessages, !!systemPrompt) : {};

  try {
    const effectiveTemperature = resolveTemperature(modelId, config.temperature ?? 0, options?.thinking);

    const result = streamText({
      model,
      system: systemPrompt,
      messages: coreMessages,
      tools: sdkTools,
      stopWhen: stepCountIs(1),
      maxOutputTokens: config.maxTokens ?? 8192,
      ...(effectiveTemperature !== undefined ? { temperature: effectiveTemperature } : {}),
      abortSignal,
      maxRetries: 0,
      onError: ({ error }) => {
        const msg = error instanceof Error ? error.message : String(error);
        if (msg.includes('MissingToolResults')) return;
      },
      ...buildThinkingOptions(modelId, options?.thinking),
      ...cacheBreakpoints,
    });

    // Track tool call indices so engine.ts can accumulate them by index
    let toolCallIndex = 0;

    const stream = result.fullStream;
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
              reasoningTokens: (part.usage as { reasoningTokens?: number }).reasoningTokens ?? 0,
            },
            responseHeaders: (part as { response?: { headers?: Record<string, string> } }).response?.headers,
          };
          return;

        case 'error': {
          const errObj = part.error as { statusCode?: number; responseHeaders?: Record<string, string>; message?: string } | undefined;
          const errStatusCode = errObj?.statusCode;
          const errMsg = errStatusCode
            ? `HTTP ${errStatusCode}: ${errObj?.message ?? String(part.error)}`
            : String(part.error);

          // Extract Retry-After from response headers if available
          let errRetryAfter: number | undefined;
          const rah = errObj?.responseHeaders?.['retry-after'];
          if (rah) {
            const parsed = parseInt(rah, 10);
            if (!isNaN(parsed)) errRetryAfter = parsed;
          }

          yield {
            type: 'error',
            error: errMsg,
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

      // Extract Retry-After from response headers if available
      let retryAfter: number | undefined;
      if (responseHeaders) {
        const retryAfterHeader = responseHeaders['retry-after'];
        if (retryAfterHeader) {
          const parsed = parseInt(retryAfterHeader, 10);
          if (!isNaN(parsed)) {
            retryAfter = parsed;
          }
        }
      }

      yield {
        type: 'error',
        error: errorMsg,
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

export async function* streamChatCompletionWithRetry(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[],
  abortSignal: AbortSignal,
  options?: { thinking?: ThinkingConfig; onRetry?: (attempt: number, maxRetries: number, delayMs: number, statusCode: number) => void },
): AsyncGenerator<StreamChunk> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (abortSignal.aborted) {
      yield { type: 'error', error: 'Request aborted' };
      return;
    }

    let contentStarted = false;

    for await (const chunk of streamChatCompletion(config, messages, tools, abortSignal, options)) {
      if (chunk.type === 'error' && !contentStarted) {
        const statusCode = extractStatusCode(chunk.error ?? '');

        if (statusCode === 401) {
          yield chunk;
          return;
        }

        if (statusCode !== null && RETRYABLE_STATUS_CODES.has(statusCode) && attempt < MAX_RETRIES) {
          let delayMs: number;
          const retryAfterSeconds = chunk.retryAfter;
          if (statusCode === 429 && retryAfterSeconds != null) {
            delayMs = retryAfterSeconds * 1000;
          } else {
            delayMs = BASE_DELAY_MS * Math.pow(2, attempt);
          }

          if (options?.onRetry) {
            options.onRetry(attempt + 1, MAX_RETRIES, delayMs, statusCode);
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

      yield chunk;

      if (chunk.type === 'done' || (chunk.type === 'error' && contentStarted)) {
        return;
      }
    }

    // If we got here via 'break' (retry), the inner loop ended without return — continue outer loop
    if (contentStarted) {
      return; // Stream completed or errored mid-content
    }
  }

  yield { type: 'error', error: `Max retries (${MAX_RETRIES}) exceeded` };
}
