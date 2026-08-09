import * as https from 'https';
import * as http from 'http';
import { URL } from 'url';
import type { ProviderConfig } from './types';
import { MODEL_PREFERENCE } from './provider';

const TIMEOUT_MS = 10000;

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
      timeout: TIMEOUT_MS,
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => {
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

async function listOpenAIModels(config: ProviderConfig): Promise<ModelInfo[]> {
  if (!config.baseUrl) throw new Error('baseUrl is required for OpenAI-compatible model listing');
  const baseUrl = config.baseUrl.replace(/\/+$/, '');
  const url = `${baseUrl}/models`;
  const headers: Record<string, string> = {};
  if (config.apiKey) headers['Authorization'] = `Bearer ${config.apiKey}`;

  const data = await httpGetJSON(url, headers) as { data?: ModelInfo[] };
  return (data.data || []).map(m => ({
    id: m.id,
    name: m.id,
    owned_by: m.owned_by,
  }));
}

async function listGeminiModels(config: ProviderConfig): Promise<ModelInfo[]> {
  const url = 'https://generativelanguage.googleapis.com/v1beta/models';
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
