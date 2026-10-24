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
    case 'openrouter': return 'AI_API_KEY (OpenRouter)';
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

export async function validateModel(
  config: ProviderConfig,
  modelId: string,
): Promise<{ valid: boolean; available: string[]; suggestion?: string }> {
  const models = await listModels(config);

  if (models.length === 0) {
    // Can't validate — provider doesn't support listing or network error
    return { valid: true, available: [] };
  }

  const ids = models.map(m => m.id);
  const exact = ids.find(id => id === modelId);
  if (exact) return { valid: true, available: ids };

  // Try substring match
  const partial = ids.filter(id => id.includes(modelId) || modelId.includes(id));
  const suggestion = partial.length > 0 ? partial[0] : undefined;

  return { valid: false, available: ids, suggestion };
}

export function formatModelList(models: string[], provider: string): string {
  if (models.length === 0) return 'Could not retrieve model list from provider.';

  const lines = models.map(m => `  ${m}`);
  const header = `Available models (${provider}, ${models.length}):`;

  return `${header}\n${lines.join('\n')}`;
}
