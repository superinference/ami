// Split from provider.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { stripOpenRouterPrefix } from './openrouter-prefix';
import { MODEL_PREFERENCE } from './model-preference';
import { readHuggingFaceContextWindow } from './model-capabilities';
import { createCombinedAbortSignal } from './utils/combined-abort-signal';
import { parseRateLimitHeaders } from './rate-limit-parser';
import { isEnvTruthy } from './analytics';
import type { ProviderConfig, ToolInputSchema } from './types';

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

// ---------------------------------------------------------------------------
// OpenAI tool format (kept for backward compatibility)
// ---------------------------------------------------------------------------

export interface OpenAIToolFunction {
  type: 'function';
  function: {
    name: string;
    description: string;
    parameters: ToolInputSchema;
    /** Structured-result schema, emitted only for tools whose outputSchema has an
     *  object root (parity: the reference's MCP tool outputSchema exposure guard). */
    outputSchema?: ToolInputSchema;
    /** API-enforced structured outputs: when true the provider constrains generated
     *  arguments to exactly match `parameters`. Emitted only for a tool that opts in
     *  (`ToolDefinition.strict`) on a model that supports it. Parity: the reference's
     *  per-tool `strict` field (utils/api.ts). */
    strict?: boolean;
  };
}

// ---------------------------------------------------------------------------
// API key pattern detection — infer provider from key format
// ---------------------------------------------------------------------------

export type ProviderName =
  | 'openai' | 'anthropic' | 'google'
  | 'groq' | 'mistral' | 'xai' | 'azure-openai'
  | 'amazon-bedrock' | 'togetherai' | 'cohere' | 'fireworks'
  | 'perplexity' | 'deepinfra' | 'deepseek' | 'cerebras'
  | 'openrouter' | 'ollama' | 'luma' | 'alibaba' | 'nvidia'
  | 'google-vertex' | 'anthropic-vertex' | 'ai-gateway' | 'huggingface'
  | 'azure-foundry';

export interface InferredProvider {
  provider: ProviderName;
  defaultModel: string;
  defaultBaseUrl: string;
}

// API key prefix → provider detection
export const KEY_PREFIXES: Array<{ prefix: string; provider: ProviderName }> = [
  { prefix: 'AIzaSy', provider: 'google' },
  { prefix: 'sk-ant-', provider: 'anthropic' },
  { prefix: 'sk-or-', provider: 'openrouter' },
  { prefix: 'sk-di-', provider: 'deepinfra' },
  { prefix: 'gsk_', provider: 'groq' },
  { prefix: 'xai-', provider: 'xai' },
  { prefix: 'nvapi-', provider: 'nvidia' },
  { prefix: 'pplx-', provider: 'perplexity' },
  { prefix: 'hf_', provider: 'huggingface' },
  { prefix: 'cohere_', provider: 'cohere' },
  { prefix: 'sk-', provider: 'openai' },
];

// Base URL pattern → provider detection
export const URL_PATTERNS: Array<{ pattern: string; provider: ProviderName }> = [
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
  { pattern: 'services.ai.azure.com', provider: 'azure-foundry' },
  { pattern: 'openai.azure.com', provider: 'azure-openai' },
  { pattern: 'api.luma.ai', provider: 'luma' },
  { pattern: 'huggingface.co', provider: 'huggingface' },
  { pattern: 'integrate.api.nvidia.com', provider: 'nvidia' },
  { pattern: 'gateway.ai.cloudflare.com', provider: 'ai-gateway' },
  { pattern: 'localhost', provider: 'ollama' },
  { pattern: '127.0.0.1', provider: 'ollama' },
];

// Env var → provider detection
export const ENV_KEYS: Array<{ env: string; provider: ProviderName }> = [
  { env: 'OPENAI_API_KEY', provider: 'openai' },
  { env: 'ANTHROPIC_API_KEY', provider: 'anthropic' },
  { env: 'GOOGLE_API_KEY', provider: 'google' },
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
  { env: 'NVIDIA_API_KEY', provider: 'nvidia' },
  { env: 'AZURE_OPENAI_API_KEY', provider: 'azure-openai' },
  { env: 'AWS_ACCESS_KEY_ID', provider: 'amazon-bedrock' },
  { env: 'ANTHROPIC_VERTEX_PROJECT_ID', provider: 'anthropic-vertex' },
  { env: 'GOOGLE_APPLICATION_CREDENTIALS', provider: 'google-vertex' },
  { env: 'AZURE_AI_FOUNDRY_API_KEY', provider: 'azure-foundry' },
];

// Default base URLs for providers that use OpenAI-compatible endpoints
export const PROVIDER_BASE_URLS: Partial<Record<ProviderName, string>> = {
  groq: 'https://api.groq.com/openai/v1',
  mistral: 'https://api.mistral.ai/v1',
  xai: 'https://api.x.ai/v1',
  deepseek: 'https://api.deepseek.com',
  togetherai: 'https://api.together.xyz/v1',
  cohere: 'https://api.cohere.com/compatibility/v1',
  fireworks: 'https://api.fireworks.ai/inference/v1',
  perplexity: 'https://api.perplexity.ai',
  deepinfra: 'https://api.deepinfra.com/v1/openai',
  huggingface: 'https://router.huggingface.co/v1',
  cerebras: 'https://api.cerebras.ai/v1',
  alibaba: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
  openrouter: 'https://openrouter.ai/api/v1',
  openai: 'https://api.openai.com/v1',
  luma: 'https://api.luma.ai',
  nvidia: 'https://integrate.api.nvidia.com/v1',
};

export function inferProviderFromApiKey(apiKey: string): InferredProvider | null {
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

export function inferProviderFromBaseUrl(baseUrl: string): InferredProvider | null {
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

export function inferProviderFromEnv(): InferredProvider | null {
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

/**
 * `fetch` with a leak-free timeout. Combines an optional caller `init.signal` with
 * a `setTimeout`/`clearTimeout` deadline via the shared
 * {@link createCombinedAbortSignal} helper and frees the timer the instant the
 * request settles — instead of `AbortSignal.timeout(ms)`, whose timer is pinned
 * in native memory for the full duration even after the fetch has resolved. Every
 * short provider probe (model-list, HF metadata) routes through here so the single
 * canonical timeout discipline (no accumulating native timers) applies uniformly.
 */
export async function fetchWithTimeout(
  input: Parameters<typeof fetch>[0],
  init: RequestInit,
  timeoutMs: number,
): Promise<Response> {
  const { signal, cleanup } = createCombinedAbortSignal(init.signal ?? undefined, { timeoutMs });
  try {
    return await fetch(input, { ...init, signal });
  } finally {
    cleanup();
  }
}

export async function resolveAvailableModel(
  provider: ProviderName | string,
  apiKey: string,
): Promise<string | null> {
  const preferences = MODEL_PREFERENCE[provider] || [];
  try {
    let availableModels: string[] = [];

    if (provider === 'google' || provider === 'google-vertex') {
      const res = await fetchWithTimeout(
        'https://generativelanguage.googleapis.com/v1beta/models',
        { headers: { 'x-goog-api-key': apiKey } },
        5000,
      );
      if (res.ok) {
        const data = await res.json() as { models?: Array<{ name: string }> };
        availableModels = (data.models || []).map(m => m.name.replace('models/', ''));
      }
    } else if (provider === 'anthropic') {
      const res = await fetchWithTimeout('https://api.anthropic.com/v1/models', {
        headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
      }, 5000);
      if (res.ok) {
        const data = await res.json() as { data?: Array<{ id: string }> };
        availableModels = (data.data || []).map(m => m.id);
      }
    } else {
      // OpenAI-compatible providers all support GET /models
      const baseUrl = PROVIDER_BASE_URLS[provider as ProviderName] || 'https://api.openai.com/v1';
      const res = await fetchWithTimeout(`${baseUrl}/models`, {
        headers: { Authorization: `Bearer ${apiKey}` },
      }, 5000);
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

export interface ProviderDescriptor {
  id: string;
  envVars: string[];
  baseUrl: string;
  models: string[];
  nativeSdk: boolean;
  keyPrefixes: string[];
}

// ---------------------------------------------------------------------------
// Interactive cloud-auth lifecycle status (parity: the reference's AwsAuthStatus /
// AwsAuthStatusManager — components/AwsAuthStatusBox.tsx L4-L36,
// utils/awsAuthStatusManager.ts). AMI already surfaces the ACTIONABLE half of that
// manager (the "here is how to re-auth" affordance, common/event-processor.ts
// `onCloudAuthHint`) but had no way to represent the LIVE half: an auth command
// (e.g. `aws sso login`) actively running, the lines it streams, and whether it
// failed. {@link InteractiveAuthStatus} is that missing schema: a provider-agnostic
// snapshot of an in-flight provider authentication. It is produced by common's cloud
// auth-refresh flow as the command runs and read back through the `/auth-status`
// introspection surface. (Distinct from common's `AuthStatus` credential-resolution
// report, which is the STATIC "do I have a usable key" preflight; this is the LIVE
// "a login command is running right now" lifecycle.)
// ---------------------------------------------------------------------------

/**
 * A snapshot of an in-flight (or just-completed) interactive provider
 * authentication. Mirrors the reference's `AwsAuthStatus` three-field shape, but kept
 * provider-agnostic so any provider's login flow can report through it.
 */
export interface InteractiveAuthStatus {
  /** True while a login/auth command is actively running. */
  isAuthenticating: boolean;
  /** The failure message when the last auth attempt errored; undefined on success/in-progress. */
  error?: string;
  /** Lines the auth command streamed (stdout/stderr), oldest first. */
  output: string[];
}

/** The neutral initial {@link InteractiveAuthStatus}: not authenticating, no error, no output. */
export function idleInteractiveAuthStatus(): InteractiveAuthStatus {
  return { isAuthenticating: false, output: [] };
}

// ---------------------------------------------------------------------------
// Cross-provider canonical model-ID equivalence / translation table.
//
// the reference's configs.ts binds ONE logical model to its provider-specific id
// across every provider (firstParty / bedrock / vertex / foundry), with a
// `satisfies ModelConfig` compile-time guarantee that EVERY model resolves under
// EVERY provider, plus a CANONICAL_ID_TO_KEY reverse map. AMI's per-provider
// MODEL_PREFERENCE lists were independent, opaque strings with no equivalence, so
// a model chosen under one provider had no translation to the active provider and
// could surface as an invalid-model error after a provider switch. This table +
// {@link resolveModelForProvider} supply that single source of truth and the
// per-provider translation, consumed by {@link resolveModel}.
// ---------------------------------------------------------------------------

/**
 * The providers a canonical Claude model is addressable under. Every field is
 * REQUIRED (the comprehensiveness invariant — see the `provider-u020-w87` test),
 * mirroring the reference's `satisfies ModelConfig`.
 */
export interface CanonicalModelConfig {
  anthropic: string;
  'anthropic-vertex': string;
  'amazon-bedrock': string;
  'azure-foundry': string;
}

/** Canonical logical model key → its per-provider wire ids. */
export const CANONICAL_MODEL_EQUIVALENTS: Record<string, CanonicalModelConfig> = {
  'claude-opus-4-6': {
    anthropic: 'claude-opus-4-6',
    'anthropic-vertex': 'claude-opus-4-6',
    'amazon-bedrock': 'anthropic.claude-opus-4-6-20250514',
    'azure-foundry': 'claude-opus-4-6',
  },
  'claude-sonnet-4-6': {
    anthropic: 'claude-sonnet-4-6',
    'anthropic-vertex': 'claude-sonnet-4-6',
    'amazon-bedrock': 'anthropic.claude-sonnet-4-6-20250514',
    'azure-foundry': 'claude-sonnet-4-6',
  },
  'claude-haiku-4-5': {
    anthropic: 'claude-haiku-4-5-20251001',
    'anthropic-vertex': 'claude-haiku-4-5-20251001',
    'amazon-bedrock': 'anthropic.claude-haiku-4-5-20251001',
    'azure-foundry': 'claude-haiku-4-5',
  },
};

export function huggingFaceEndpoint(): string {
  return (process.env.HF_ENDPOINT || 'https://huggingface.co').replace(/\/+$/, '');
}

export function huggingFaceToken(config: ProviderConfig): string | undefined {
  if (config.apiKey?.startsWith('hf_')) return config.apiKey;
  if (config.provider === 'huggingface' && config.apiKey && !config.apiKey.startsWith('sk-')) return config.apiKey;
  const env = process.env.HF_TOKEN;
  if (env && !env.startsWith('sk-')) return env;
  return undefined;
}

export async function readHuggingFaceFile(url: string, headers: Record<string, string>): Promise<number | 'missing' | 'failed'> {
  try {
    const res = await fetchWithTimeout(url, { headers }, 1000);
    if (!res.ok) return 'missing';
    return readHuggingFaceContextWindow(await res.json()) ?? 'missing';
  } catch {
    return 'failed';
  }
}

export function isVertexAIConfigured(): boolean {
  return !!(isEnvTruthy(process.env.CLAUDE_CODE_USE_VERTEX) || process.env.ANTHROPIC_VERTEX_PROJECT_ID);
}

/**
 * AWS region-resolution waterfall: `AWS_REGION` → `AWS_DEFAULT_REGION` →
 * `'us-east-1'`. Mirrors the reference's `getAWSRegion()` (utils/envUtils.ts) and
 * the Anthropic Bedrock SDK's region behavior, so a Bedrock user relying on the
 * standard `AWS_DEFAULT_REGION` env (and the `us-east-1` default) resolves to the
 * right region instead of getting no region at all.
 */
export function getAWSRegion(env: NodeJS.ProcessEnv = process.env): string {
  return env.AWS_REGION || env.AWS_DEFAULT_REGION || 'us-east-1';
}
