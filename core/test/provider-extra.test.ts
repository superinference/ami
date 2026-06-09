import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  toolDefinitionsToOpenAI,
  streamChatCompletion,
  streamChatCompletionWithRetry,
  convertMessages,
  convertToolsForSDK,
  inferProviderFromApiKey,
  inferProviderFromBaseUrl,
  inferProviderFromEnv,
  MODEL_PREFERENCE,
  resolveModel,
} from '../src/provider';
import type { ToolDefinition, ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeConfig(overrides?: Partial<ProviderConfig>): ProviderConfig {
  return {
    baseUrl: 'https://api.openai.com/v1',
    apiKey: 'test-key',
    model: 'gpt-4o',
    maxTokens: 100,
    temperature: 0,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// inferProviderFromApiKey (lines 162-175)
// ---------------------------------------------------------------------------
describe('inferProviderFromApiKey', () => {
  it('detects Anthropic from sk-ant- prefix', () => {
    const result = (inferProviderFromApiKey as any)('sk-ant-api03-test');
    assert.ok(result);
    assert.equal(result.provider, 'anthropic');
  });

  it('detects Google from AIzaSy prefix', () => {
    const result = (inferProviderFromApiKey as any)('AIzaSyTestKey123');
    assert.ok(result);
    assert.equal(result.provider, 'google');
  });

  it('detects Groq from gsk_ prefix', () => {
    const result = (inferProviderFromApiKey as any)('gsk_testkey');
    assert.ok(result);
    assert.equal(result.provider, 'groq');
  });

  it('detects xAI from xai- prefix', () => {
    const result = (inferProviderFromApiKey as any)('xai-testkey');
    assert.ok(result);
    assert.equal(result.provider, 'xai');
  });

  it('detects Perplexity from pplx- prefix', () => {
    const result = (inferProviderFromApiKey as any)('pplx-testkey');
    assert.ok(result);
    assert.equal(result.provider, 'perplexity');
  });

  it('detects OpenRouter from sk-or- prefix', () => {
    const result = (inferProviderFromApiKey as any)('sk-or-testkey');
    assert.ok(result);
    assert.equal(result.provider, 'openrouter');
  });

  it('detects OpenAI from sk- prefix (fallback)', () => {
    const result = (inferProviderFromApiKey as any)('sk-testkey');
    assert.ok(result);
    assert.equal(result.provider, 'openai');
  });

  it('returns null for unknown key format', () => {
    const result = (inferProviderFromApiKey as any)('unknown-key-format');
    assert.equal(result, null);
  });

  it('includes default model for detected provider', () => {
    const result = (inferProviderFromApiKey as any)('gsk_test');
    assert.ok(result);
    assert.ok(result.defaultModel, 'should have a default model');
  });
});

// ---------------------------------------------------------------------------
// inferProviderFromBaseUrl (lines 177-189)
// ---------------------------------------------------------------------------
describe('inferProviderFromBaseUrl', () => {
  it('detects Groq from api.groq.com', () => {
    const result = inferProviderFromBaseUrl('https://api.groq.com/openai/v1');
    assert.ok(result);
    assert.equal(result.provider, 'groq');
  });

  it('detects Mistral from api.mistral.ai', () => {
    const result = inferProviderFromBaseUrl('https://api.mistral.ai/v1');
    assert.ok(result);
    assert.equal(result.provider, 'mistral');
  });

  it('detects xAI from api.x.ai', () => {
    const result = inferProviderFromBaseUrl('https://api.x.ai/v1');
    assert.ok(result);
    assert.equal(result.provider, 'xai');
  });

  it('detects DeepSeek from api.deepseek.com', () => {
    const result = inferProviderFromBaseUrl('https://api.deepseek.com');
    assert.ok(result);
    assert.equal(result.provider, 'deepseek');
  });

  it('detects Together AI from api.together.xyz', () => {
    const result = inferProviderFromBaseUrl('https://api.together.xyz/v1');
    assert.ok(result);
    assert.equal(result.provider, 'togetherai');
  });

  it('detects Cohere from api.cohere.com', () => {
    const result = inferProviderFromBaseUrl('https://api.cohere.com/v2');
    assert.ok(result);
    assert.equal(result.provider, 'cohere');
  });

  it('detects Cohere from api.cohere.ai', () => {
    const result = inferProviderFromBaseUrl('https://api.cohere.ai/v1');
    assert.ok(result);
    assert.equal(result.provider, 'cohere');
  });

  it('detects Fireworks from api.fireworks.ai', () => {
    const result = inferProviderFromBaseUrl('https://api.fireworks.ai/inference/v1');
    assert.ok(result);
    assert.equal(result.provider, 'fireworks');
  });

  it('detects Cerebras from api.cerebras.ai', () => {
    const result = inferProviderFromBaseUrl('https://api.cerebras.ai/v1');
    assert.ok(result);
    assert.equal(result.provider, 'cerebras');
  });

  it('detects Alibaba from dashscope.aliyuncs.com', () => {
    const result = inferProviderFromBaseUrl('https://dashscope.aliyuncs.com/compatible-mode/v1');
    assert.ok(result);
    assert.equal(result.provider, 'alibaba');
  });

  it('detects Google Vertex from aiplatform.googleapis.com', () => {
    const result = inferProviderFromBaseUrl('https://aiplatform.googleapis.com/v1');
    assert.ok(result);
    assert.equal(result.provider, 'google-vertex');
  });

  it('detects Azure OpenAI from openai.azure.com', () => {
    const result = inferProviderFromBaseUrl('https://myinstance.openai.azure.com/v1');
    assert.ok(result);
    assert.equal(result.provider, 'azure-openai');
  });

  it('detects Luma from api.luma.ai', () => {
    const result = inferProviderFromBaseUrl('https://api.luma.ai');
    assert.ok(result);
    assert.equal(result.provider, 'luma');
  });

  it('detects AI Gateway from gateway.ai.cloudflare.com', () => {
    const result = inferProviderFromBaseUrl('https://gateway.ai.cloudflare.com/v1');
    assert.ok(result);
    assert.equal(result.provider, 'ai-gateway');
  });

  it('detects Ollama from 127.0.0.1', () => {
    const result = inferProviderFromBaseUrl('http://127.0.0.1:11434/v1');
    assert.ok(result);
    assert.equal(result.provider, 'ollama');
  });

  it('returns null for unknown URL', () => {
    const result = inferProviderFromBaseUrl('https://my-custom-api.com/v1');
    assert.equal(result, null);
  });
});

// ---------------------------------------------------------------------------
// inferProviderFromEnv (lines 191-203)
// ---------------------------------------------------------------------------
describe('inferProviderFromEnv', () => {
  it('returns null when no relevant env vars are set', () => {
    const saved: Record<string, string | undefined> = {};
    const envKeys = [
      'GROQ_API_KEY', 'MISTRAL_API_KEY', 'XAI_API_KEY', 'DEEPSEEK_API_KEY',
      'TOGETHER_AI_API_KEY', 'COHERE_API_KEY', 'FIREWORKS_API_KEY', 'PERPLEXITY_API_KEY',
      'DEEPINFRA_API_KEY', 'CEREBRAS_API_KEY', 'ALIBABA_API_KEY', 'DASHSCOPE_API_KEY',
      'LUMA_API_KEY', 'AZURE_OPENAI_API_KEY', 'AWS_ACCESS_KEY_ID', 'GOOGLE_APPLICATION_CREDENTIALS',
    ];
    for (const key of envKeys) {
      saved[key] = process.env[key];
      delete process.env[key];
    }

    try {
      const result = inferProviderFromEnv();
      assert.equal(result, null, 'Expected null when all provider env vars are cleared');
    } finally {
      for (const [key, val] of Object.entries(saved)) {
        if (val !== undefined) process.env[key] = val;
        else delete process.env[key];
      }
    }
  });
});

// ---------------------------------------------------------------------------
// resolveModel — provider-specific detection (lines 272-307)
// ---------------------------------------------------------------------------
describe('resolveModel — provider-specific detection', () => {
  it('detects Anthropic by anthropic.com in baseUrl', () => {
    const config = makeConfig({ baseUrl: 'https://api.anthropic.com/v1', model: 'claude-sonnet-4-6' });
    const result = resolveModel(config);
    assert.ok(result.provider.includes('anthropic'));
  });

  it('detects Google Gemini by model prefix', () => {
    const config = makeConfig({ model: 'gemini-2.5-pro', apiKey: 'AIzaSyTest' });
    const result = resolveModel(config);
    assert.ok(result.provider.includes('google'));
  });

  it('detects Azure OpenAI by URL', () => {
    const config = makeConfig({ baseUrl: 'https://myinstance.openai.azure.com/v1', model: 'gpt-4o' });
    const result = resolveModel(config);
    assert.ok(result.provider.includes('openai'));
  });

  it('detects Amazon Bedrock by provider field', () => {
    const config = makeConfig({ provider: 'amazon-bedrock', model: 'anthropic.claude-sonnet-4-6' });
    const result = resolveModel(config);
    assert.ok(result);
  });

  it('handles fallback to OpenAI for unknown providers', () => {
    const config = makeConfig({ baseUrl: 'https://unknown.api.com/v1', model: 'custom-model' });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('openai'));
  });
});

// ---------------------------------------------------------------------------
// convertMessages — edge cases (lines 331-398)
// ---------------------------------------------------------------------------
describe('convertMessages — edge cases', () => {
  it('converts tool message and finds tool name from preceding assistant', () => {
    const messages: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_1', type: 'function', function: { name: 'bash', arguments: '{"command":"ls"}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'call_1', content: 'file1.txt\nfile2.txt' },
    ];

    const result = convertMessages(messages);
    assert.equal(result.length, 2);
    assert.equal(result[1].role, 'tool');
  });

  it('handles tool message without matching tool_call', () => {
    const messages: Message[] = [
      { role: 'tool', tool_call_id: 'orphan_call', content: 'result' },
    ];

    const result = convertMessages(messages);
    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'tool');
  });

  it('converts assistant message with both content and tool_calls', () => {
    const messages: Message[] = [
      {
        role: 'assistant',
        content: 'Let me read that file.',
        tool_calls: [
          { id: 'call_1', type: 'function', function: { name: 'file_read', arguments: '{"file_path":"/tmp/a"}' } },
        ],
      },
    ];

    const result = convertMessages(messages);
    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'assistant');
    // Should have both text and tool-call content parts
    assert.ok(Array.isArray(result[0].content));
  });

  it('converts assistant message with invalid JSON arguments', () => {
    const messages: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_1', type: 'function', function: { name: 'bash', arguments: 'NOT JSON' } },
        ],
      },
    ];

    const result = convertMessages(messages);
    assert.equal(result.length, 1);
    // Should not throw — invalid JSON falls back to {}
  });

  it('converts user message with image content', () => {
    const messages: Message[] = [
      {
        role: 'user',
        content: [
          { type: 'text', text: 'What is this?' },
          {
            type: 'image',
            source: { type: 'base64', media_type: 'image/png', data: 'base64data' },
          },
        ],
      },
    ];

    const result = convertMessages(messages);
    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'user');
    assert.ok(Array.isArray(result[0].content));
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — retry logic (lines 683-752)
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry — retry on 429', () => {
  let server: http.Server;
  let port: number;
  let callCount = 0;

  before(async () => {
    callCount = 0;
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      callCount++;

      if (callCount <= 2) {
        // First 2 calls return 429
        res.writeHead(429, {
          'Content-Type': 'application/json',
          'Retry-After': '0',
        });
        res.end('{"error":{"message":"Rate limited","type":"rate_limit_error"}}');
        return;
      }

      // 3rd call succeeds
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}\n\n');
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Success"},"index":0}]}\n\n');
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });

    await new Promise<void>((resolve) => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('retries on 429 and eventually succeeds', async () => {
    const config = makeConfig({
      baseUrl: `http://127.0.0.1:${port}/v1`,
      model: 'test-model',
    });
    const messages: Message[] = [{ role: 'user', content: 'Hi' }];
    const ac = new AbortController();

    let retryCount = 0;
    const chunks: StreamChunk[] = [];
    for await (const chunk of streamChatCompletionWithRetry(
      config, messages, [], ac.signal,
      {
        onRetry: () => { retryCount++; },
      },
    )) {
      chunks.push(chunk);
    }

    assert.ok(retryCount >= 1, 'Should have retried at least once');
    const contentChunks = chunks.filter(c => c.type === 'content_delta');
    assert.ok(contentChunks.length > 0, 'Should have content after retry');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — non-retryable 401 (line 702-705)
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry — non-retryable errors', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end('{"error":{"message":"Unauthorized","type":"auth_error"}}');
    });

    await new Promise<void>((resolve) => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('does not retry 401 errors', async () => {
    const config = makeConfig({
      baseUrl: `http://127.0.0.1:${port}/v1`,
      model: 'test-model',
    });
    const messages: Message[] = [{ role: 'user', content: 'Hi' }];
    const ac = new AbortController();

    let retryCount = 0;
    const chunks: StreamChunk[] = [];
    for await (const chunk of streamChatCompletionWithRetry(
      config, messages, [], ac.signal,
      { onRetry: () => { retryCount++; } },
    )) {
      chunks.push(chunk);
    }

    assert.equal(retryCount, 0, 'Should not retry 401');
    assert.equal(chunks.length, 1);
    assert.equal(chunks[0].type, 'error');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — abort during retry (line 720-725)
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry — abort during wait', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(429, { 'Content-Type': 'application/json', 'Retry-After': '60' });
      res.end('{"error":{"message":"Rate limited","type":"rate_limit_error"}}');
    });

    await new Promise<void>((resolve) => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('yields abort error when signal is aborted during sleep', async () => {
    const config = makeConfig({
      baseUrl: `http://127.0.0.1:${port}/v1`,
      model: 'test-model',
    });
    const messages: Message[] = [{ role: 'user', content: 'Hi' }];
    const ac = new AbortController();

    // Abort after enough time for the 429 response to arrive and retry sleep to start
    setTimeout(() => ac.abort(), 200);

    const chunks: StreamChunk[] = [];
    for await (const chunk of streamChatCompletionWithRetry(
      config, messages, [], ac.signal,
    )) {
      chunks.push(chunk);
    }

    const errorChunk = chunks.find(c => c.type === 'error');
    assert.ok(errorChunk, 'Should have error chunk');
    assert.ok(errorChunk!.error!.includes('abort') || errorChunk!.error!.includes('Abort'),
      `Error should mention abort, got: ${errorChunk!.error}`);
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — already aborted (line 691-694)
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry — pre-aborted signal', () => {
  it('yields error immediately when signal is already aborted', async () => {
    const config = makeConfig({
      baseUrl: 'http://127.0.0.1:1/v1', // won't connect
      model: 'test-model',
    });
    const messages: Message[] = [{ role: 'user', content: 'Hi' }];
    const ac = new AbortController();
    ac.abort();

    const chunks: StreamChunk[] = [];
    for await (const chunk of streamChatCompletionWithRetry(
      config, messages, [], ac.signal,
    )) {
      chunks.push(chunk);
    }

    assert.equal(chunks.length, 1);
    assert.equal(chunks[0].type, 'error');
    assert.ok(chunks[0].error!.includes('abort') || chunks[0].error!.includes('Abort'));
  });
});

// ---------------------------------------------------------------------------
// MODEL_PREFERENCE exported data
// ---------------------------------------------------------------------------
describe('MODEL_PREFERENCE', () => {
  it('has entries for major providers', () => {
    assert.ok(MODEL_PREFERENCE.openai.length > 0);
    assert.ok(MODEL_PREFERENCE.anthropic.length > 0);
    assert.ok(MODEL_PREFERENCE.google.length > 0);
    assert.ok(MODEL_PREFERENCE.groq.length > 0);
    assert.ok(MODEL_PREFERENCE.deepseek.length > 0);
  });
});

// ---------------------------------------------------------------------------
// resolveModel — SDK creation paths (lines 284-306)
// ---------------------------------------------------------------------------
describe('resolveModel — SDK creation coverage', () => {
  it('creates Anthropic SDK for anthropic provider', () => {
    const config = makeConfig({ provider: 'anthropic', model: 'claude-sonnet-4-6' });
    const result = resolveModel(config);
    assert.ok(result, 'Should return a model');
    assert.ok(result.provider.includes('anthropic'));
  });

  it('creates Google SDK for google provider', () => {
    const config = makeConfig({ provider: 'google', model: 'gemini-2.5-pro', apiKey: 'AIzaSyTest' });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.includes('google'));
  });

  it('creates Google SDK for google-vertex provider', () => {
    const config = makeConfig({ provider: 'google-vertex', model: 'gemini-2.5-pro', apiKey: 'test' });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.includes('google'));
  });

  it('creates Google SDK for model starting with gemini', () => {
    const config = makeConfig({ model: 'gemini-3.1-pro-preview', apiKey: 'test' });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.includes('google'));
  });

  it('creates Google SDK for generativelanguage.googleapis.com baseUrl', () => {
    const config = makeConfig({
      baseUrl: 'https://generativelanguage.googleapis.com/v1',
      model: 'gemini-2.5-flash',
      apiKey: 'AIzaSyTest',
    });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.includes('google'));
  });

  it('creates Google SDK for aiplatform.googleapis.com baseUrl', () => {
    const config = makeConfig({
      baseUrl: 'https://aiplatform.googleapis.com/v1',
      model: 'gemini-2.5-pro',
      apiKey: 'test',
    });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.includes('google'));
  });

  it('creates OpenAI SDK for azure-openai provider', () => {
    const config = makeConfig({
      provider: 'azure-openai',
      baseUrl: 'https://myinstance.openai.azure.com/v1',
      model: 'gpt-4o',
    });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.includes('openai'));
  });

  it('creates OpenAI SDK for openai.azure.com baseUrl without provider', () => {
    const config = makeConfig({
      baseUrl: 'https://myinstance.openai.azure.com/v1',
      model: 'gpt-4o',
    });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.includes('openai'));
  });

  it('creates OpenAI SDK for amazon-bedrock provider', () => {
    const config = makeConfig({
      provider: 'amazon-bedrock',
      baseUrl: 'https://bedrock-gateway.example.com/v1',
      model: 'anthropic.claude-sonnet-4-6',
    });
    const result = resolveModel(config);
    assert.ok(result);
  });

  it('creates OpenAI-compatible SDK for groq provider', () => {
    const config = makeConfig({
      baseUrl: 'https://api.groq.com/openai/v1',
      apiKey: 'gsk_test',
      model: 'llama-3.3-70b-versatile',
    });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.startsWith('openai'));
  });

  it('creates OpenAI-compatible SDK for deepseek provider', () => {
    const config = makeConfig({
      baseUrl: 'https://api.deepseek.com',
      model: 'deepseek-chat',
    });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.startsWith('openai'));
  });

  it('creates OpenAI-compatible SDK for unknown provider with fallback', () => {
    const config = makeConfig({
      baseUrl: 'https://custom-api.example.com/v1',
      model: 'custom-model',
      apiKey: 'custom-key-no-prefix',
    });
    const result = resolveModel(config);
    assert.ok(result);
    assert.ok(result.provider.startsWith('openai'));
  });
});

// ---------------------------------------------------------------------------
// inferProviderFromEnv — with actual env vars set (lines 191-203)
// ---------------------------------------------------------------------------
describe('inferProviderFromEnv — with env vars', () => {
  it('detects groq when GROQ_API_KEY is set', () => {
    const saved = process.env.GROQ_API_KEY;
    process.env.GROQ_API_KEY = 'test-key';
    try {
      const result = inferProviderFromEnv();
      // May detect groq or another provider that was set earlier in ENV_KEYS
      assert.ok(result !== null);
      assert.ok(typeof result!.provider === 'string');
      assert.ok(typeof result!.defaultModel === 'string');
    } finally {
      if (saved !== undefined) process.env.GROQ_API_KEY = saved;
      else delete process.env.GROQ_API_KEY;
    }
  });
});

// ---------------------------------------------------------------------------
// resolveAvailableModel — HTTP calls (lines 205-254)
// ---------------------------------------------------------------------------
import { resolveAvailableModel } from '../src/provider';

describe('resolveAvailableModel', () => {
  it('returns null for provider with invalid API key', async () => {
    const result = await resolveAvailableModel('openai', 'invalid-key-for-testing');
    assert.equal(result, null);
  });

  it('returns null for unknown provider name', async () => {
    const result = await resolveAvailableModel('nonexistent-provider', 'fake-key');
    assert.equal(result, null);
  });
});

// ---------------------------------------------------------------------------
// sleep + extractStatusCode (lines 655-681)
// ---------------------------------------------------------------------------
describe('extractStatusCode + retry internals', () => {
  it('streamChatCompletionWithRetry uses exponential backoff for 500 errors', async () => {
    let callCount = 0;
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      callCount++;
      if (callCount <= 1) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end('{"error":{"message":"Internal server error"}}');
        return;
      }
      // Second call succeeds
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}\n\n');
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"content":"ok"},"index":0}]}\n\n');
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });

    await new Promise<void>(resolve => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    const p = (s.address() as { port: number }).port;

    try {
      const config = makeConfig({
        baseUrl: `http://127.0.0.1:${p}/v1`,
        model: 'test-model',
      });
      const messages: Message[] = [{ role: 'user', content: 'Hi' }];
      const ac = new AbortController();

      let retryCount = 0;
      const chunks: StreamChunk[] = [];
      for await (const chunk of streamChatCompletionWithRetry(
        config, messages, [], ac.signal,
        { onRetry: () => { retryCount++; } },
      )) {
        chunks.push(chunk);
      }

      assert.ok(retryCount >= 1, 'Should have retried on 500');
      const contentChunks = chunks.filter(c => c.type === 'content_delta');
      assert.ok(contentChunks.length > 0, 'Should have content after retry');
    } finally {
      s.close();
    }
  });

  it('yields max retries error when all attempts fail', async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end('{"error":{"message":"Service unavailable"}}');
    });

    await new Promise<void>(resolve => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    const p = (s.address() as { port: number }).port;

    try {
      const config = makeConfig({
        baseUrl: `http://127.0.0.1:${p}/v1`,
        model: 'test-model',
      });
      const messages: Message[] = [{ role: 'user', content: 'Hi' }];
      const ac = new AbortController();

      const chunks: StreamChunk[] = [];
      for await (const chunk of streamChatCompletionWithRetry(
        config, messages, [], ac.signal,
      )) {
        chunks.push(chunk);
      }

      const lastChunk = chunks[chunks.length - 1];
      assert.equal(lastChunk.type, 'error');
      // After max retries it should report the error or max retries exceeded
    } finally {
      s.close();
    }
  });
});
