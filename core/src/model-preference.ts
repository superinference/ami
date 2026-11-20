// Per-provider preferred model id lists, extracted to a dependency-free leaf so
// model-registry.ts can consume MODEL_PREFERENCE WITHOUT a model-registry -> provider
// import cycle. Pure data; no other imports.

export const MODEL_PREFERENCE: Record<string, string[]> = {
  google: [
    'gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-3.1-pro-preview',
    'gemini-3-pro-preview', 'gemini-3.5-flash', 'gemini-3.1-flash-lite',
  ],
  anthropic: [
    'claude-sonnet-4-6', 'claude-opus-4-6', 'claude-sonnet-4-5-20251022',
    'claude-haiku-4-5-20251001', 'claude-3-5-sonnet-20241022',
  ],
  openai: [
    'gpt-5.6-luna', 'gpt-6-sol', 'gpt-6-astra', 'gpt-6.1-sol',
    'o4-mini', 'gpt-4o', 'o3-mini', 'gpt-4o-mini', 'gpt-4-turbo',
  ],
  groq: ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768'],
  mistral: ['mistral-large-latest', 'mistral-medium-latest', 'mistral-small-latest', 'codestral-latest'],
  xai: ['grok-3', 'grok-3-mini', 'grok-2'],
  deepseek: ['deepseek-chat', 'deepseek-reasoner'],
  togetherai: ['meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo', 'Qwen/Qwen2.5-72B-Instruct-Turbo'],
  cohere: ['command-a-03-2025', 'command-r7b-12-2024'],
  fireworks: ['accounts/fireworks/models/llama-v3p3-70b-instruct'],
  perplexity: ['sonar-pro', 'sonar', 'sonar-reasoning-pro'],
  deepinfra: ['meta-llama/Meta-Llama-3.1-70B-Instruct'],
  cerebras: ['zai-glm-4.7', 'gpt-oss-120b'],
  nvidia: ['nvidia/nemotron-3.5-bolt-e4b', 'nvidia/llama-3.1-nemotron-70b-instruct'],
  alibaba: ['qwen-max', 'qwen-plus', 'qwen-turbo'],
  'azure-openai': ['gpt-4o', 'gpt-4o-mini'],
  'google-vertex': ['gemini-2.5-pro', 'gemini-2.5-flash'],
  'anthropic-vertex': [
    'claude-opus-4-6', 'claude-sonnet-4-6', 'claude-sonnet-4-5-20251022',
    'claude-haiku-4-5-20251001',
  ],
  'amazon-bedrock': ['anthropic.claude-sonnet-4-6-20250514', 'anthropic.claude-haiku-4-5-20251001'],
  // Azure AI Foundry serves Claude models over an OpenAI-compatible endpoint
  // (`https://<resource>.services.ai.azure.com/models`). The ids are Foundry's
  // own the reference wire strings (the reference's per-ModelConfig `foundry` field).
  'azure-foundry': ['claude-opus-4-6', 'claude-sonnet-4-6', 'claude-haiku-4-5'],
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
