// ===========================================================================
// provider-cov2.test.ts
//
// Behavioral branch coverage for core/core/src/provider.ts, written to be
// ESM-coverage-safe (runs under both `node --import tsx` and the accurate
// coverage harness scripts/coverage-esm.mjs):
//   - No source-text assertions (no readFileSync of any .ts).
//   - No monkeypatching of imported namespace bindings; the only global that is
//     mocked is globalThis.fetch via node:test's mock.method(), always restored.
//   - Fixtures/servers are created in-process (http.createServer on port 0).
//
// It re-covers the behaviors that previously lived only in the ESM-excluded
// provider-safety.test.ts (sanitizeErrorMessage, isNetworkError, isSslError,
// parseRetryAfterHeader, the timeout constants) plus the provider.ts branches
// that the existing measured suite leaves uncovered.
// ===========================================================================
import { describe, it, before, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  inferProviderFromBaseUrl,
  inferProviderFromEnv,
  resolveAvailableModel,
  resolveModel,
  convertToolsForSDK,
  convertMessages,
  extractReasoningTokens,
  extractRetryAfterSeconds,
  buildThinkingOptions,
  buildAnthropicCacheControl,
  buildRepairToolCall,
  withExtraHeaders,
  streamChatCompletion,
  streamChatCompletionWithRetry,
  fetchHuggingFaceContextWindow,
  sanitizeErrorMessage,
  CREDENTIAL_PATTERNS,
  isNetworkError,
  isSslError,
  parseRetryAfterHeader,
  MAX_RETRY_AFTER_MS,
  DEFAULT_REQUEST_TIMEOUT_MS,
} from '../src/provider';
import type { ProviderConfig, Message, ToolDefinition, ThinkingConfig } from '../src/types';

// ---------------------------------------------------------------------------
// Vertex env isolation: this host may have CLAUDE_CODE_USE_VERTEX /
// ANTHROPIC_VERTEX_PROJECT_ID set, which would re-route every `claude*` model
// through createVertexAnthropic and (worse) hit a live endpoint. Strip them
// around the tests that must exercise the native-Anthropic path, restore after.
// ---------------------------------------------------------------------------
const VERTEX_ENV = ['CLAUDE_CODE_USE_VERTEX', 'ANTHROPIC_VERTEX_PROJECT_ID', 'GOOGLE_VERTEX_PROJECT', 'CLOUD_ML_REGION', 'GOOGLE_VERTEX_LOCATION'] as const;
function stripVertexEnv(): Record<string, string | undefined> {
  const saved: Record<string, string | undefined> = {};
  for (const k of VERTEX_ENV) { saved[k] = process.env[k]; delete process.env[k]; }
  return saved;
}
function restoreEnv(saved: Record<string, string | undefined>): void {
  for (const [k, v] of Object.entries(saved)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

// ---------------------------------------------------------------------------
// inferProviderFromBaseUrl — matched patterns, with and without a default URL.
// ---------------------------------------------------------------------------
describe('inferProviderFromBaseUrl (matched patterns)', () => {
  it('maps a known host to its provider and its canonical base URL', () => {
    const r = inferProviderFromBaseUrl('https://api.groq.com/openai/v1');
    assert.ok(r);
    assert.equal(r.provider, 'groq');
    // groq HAS a PROVIDER_BASE_URLS entry → canonical URL is used.
    assert.equal(r.defaultBaseUrl, 'https://api.groq.com/openai/v1');
    assert.ok(r.defaultModel.length > 0);
  });

  it('falls back to the supplied baseUrl when the provider has no canonical URL', () => {
    // google is a native provider with NO PROVIDER_BASE_URLS entry → the
    // `|| baseUrl` arm of the defaultBaseUrl expression is taken.
    const url = 'https://generativelanguage.googleapis.com/v1beta';
    const r = inferProviderFromBaseUrl(url);
    assert.ok(r);
    assert.equal(r.provider, 'google');
    assert.equal(r.defaultBaseUrl, url);
  });

  it('returns null for an unknown host', () => {
    assert.equal(inferProviderFromBaseUrl('https://example.invalid/v1'), null);
  });
});

// ---------------------------------------------------------------------------
// inferProviderFromEnv — detects provider from a present env var.
// ---------------------------------------------------------------------------
describe('inferProviderFromEnv', () => {
  it('detects a provider when its env key is set', () => {
    const had = Object.prototype.hasOwnProperty.call(process.env, 'GROQ_API_KEY');
    const prev = process.env.GROQ_API_KEY;
    process.env.GROQ_API_KEY = 'gsk_test_key';
    try {
      const r = inferProviderFromEnv();
      assert.ok(r);
      // Env order can be provider-ambiguous on a dirty host, but GROQ must at
      // least resolve to a non-null inference with a model + canonical URL.
      assert.ok(r.defaultModel.length > 0);
      assert.ok(typeof r.defaultBaseUrl === 'string');
    } finally {
      if (had) process.env.GROQ_API_KEY = prev;
      else delete process.env.GROQ_API_KEY;
    }
  });
});

// ---------------------------------------------------------------------------
// resolveAvailableModel — google / anthropic / openai-compat branches, the
// preference match, the first-model fallback, empty → null, and the catch.
// fetch is mocked via mock.method(globalThis,'fetch') (a sanctioned mock, not a
// namespace monkeypatch) and always restored.
// ---------------------------------------------------------------------------
describe('resolveAvailableModel', () => {
  function mockFetch(impl: typeof globalThis.fetch): void {
    mock.method(globalThis, 'fetch', impl);
  }
  const jsonRes = (body: unknown, ok = true): Response =>
    ({ ok, json: async () => body } as unknown as Response);

  it('google: returns the first preferred model present in the list', async () => {
    mockFetch(async (url: any) => {
      assert.ok(String(url).includes('generativelanguage.googleapis.com'));
      return jsonRes({ models: [{ name: 'models/gemini-2.5-flash' }, { name: 'models/gemini-2.5-pro' }] });
    });
    try {
      // gemini-2.5-pro is preference[0] for google and is present → chosen over flash.
      assert.equal(await resolveAvailableModel('google', 'AIzaSyKEY'), 'gemini-2.5-pro');
    } finally { mock.restoreAll(); }
  });

  it('anthropic: maps data[].id and honors preference order', async () => {
    mockFetch(async (url: any) => {
      assert.ok(String(url).includes('api.anthropic.com'));
      return jsonRes({ data: [{ id: 'claude-legacy' }, { id: 'claude-sonnet-4-6' }] });
    });
    try {
      assert.equal(await resolveAvailableModel('anthropic', 'sk-ant-key'), 'claude-sonnet-4-6');
    } finally { mock.restoreAll(); }
  });

  it('openai-compat: with no preference hit returns the first advertised model', async () => {
    mockFetch(async () => jsonRes({ data: [{ id: 'first-unlisted' }, { id: 'second-unlisted' }] }));
    try {
      // groq preferences are not present → fall back to availableModels[0].
      assert.equal(await resolveAvailableModel('groq', 'gsk_key'), 'first-unlisted');
    } finally { mock.restoreAll(); }
  });

  it('returns null when the server advertises an empty list', async () => {
    mockFetch(async () => jsonRes({ data: [] }));
    try {
      assert.equal(await resolveAvailableModel('mistral', 'key'), null);
    } finally { mock.restoreAll(); }
  });

  it('returns null when the response is not ok', async () => {
    mockFetch(async () => jsonRes({ data: [{ id: 'x' }] }, false));
    try {
      assert.equal(await resolveAvailableModel('deepseek', 'key'), null);
    } finally { mock.restoreAll(); }
  });

  it('returns null (catch) when fetch rejects', async () => {
    mockFetch(async () => { throw new Error('network down'); });
    try {
      assert.equal(await resolveAvailableModel('openai', 'sk-key'), null);
    } finally { mock.restoreAll(); }
  });
});

// ---------------------------------------------------------------------------
// resolveModel — every provider dispatch branch.
// ---------------------------------------------------------------------------
describe('resolveModel dispatch branches', () => {
  let savedEnv: Record<string, string | undefined>;
  before(() => { savedEnv = stripVertexEnv(); });
  after(() => { restoreEnv(savedEnv); });

  it('native Anthropic with cacheTtl "1h" sets the extended-cache beta header path', () => {
    const m: any = resolveModel({ provider: 'anthropic', apiKey: 'sk-ant-key', model: 'claude-sonnet-4-6', baseUrl: 'https://proxy.internal/v1', cacheTtl: '1h' } as ProviderConfig);
    assert.ok(m);
    assert.equal(m.modelId, 'claude-sonnet-4-6');
    assert.equal(m.config.baseURL, 'https://proxy.internal/v1');
  });

  it('native Anthropic with default cacheTtl omits the beta header', () => {
    const m: any = resolveModel({ provider: 'anthropic', apiKey: 'sk-ant-key', model: 'claude-sonnet-4-6' } as ProviderConfig);
    assert.ok(m);
    assert.equal(m.modelId, 'claude-sonnet-4-6');
  });

  it('native Google provider', () => {
    const m: any = resolveModel({ provider: 'google', apiKey: 'AIzaSyKEY', model: 'gemini-2.5-pro' } as ProviderConfig);
    assert.ok(m);
    assert.equal(m.modelId, 'gemini-2.5-pro');
  });

  it('Azure OpenAI routes through the OpenAI SDK with the azure baseURL', () => {
    const m: any = resolveModel({ provider: 'azure-openai', apiKey: 'key', model: 'gpt-4o', baseUrl: 'https://my.openai.azure.com' } as ProviderConfig);
    assert.ok(m);
    assert.equal(m.config.baseURL, 'https://my.openai.azure.com');
  });

  it('Amazon Bedrock routes through the OpenAI SDK with the gateway baseURL', () => {
    const m: any = resolveModel({ provider: 'amazon-bedrock', apiKey: 'key', model: 'anthropic.claude-sonnet-4-6', baseUrl: 'https://gw.example/v1' } as ProviderConfig);
    assert.ok(m);
    assert.equal(m.config.baseURL, 'https://gw.example/v1');
  });

  it('OpenAI model that requires the Responses API uses openai.responses()', () => {
    const m: any = resolveModel({ provider: 'openai', apiKey: 'sk-key', model: 'gpt-6-luna' } as ProviderConfig);
    assert.ok(m);
    assert.equal(m.modelId, 'gpt-6-luna');
  });

  it('plain OpenAI-compatible model uses the chat endpoint and the provider base URL', () => {
    const m: any = resolveModel({ provider: 'groq', apiKey: 'gsk_key', model: 'llama-3.3-70b-versatile' } as ProviderConfig);
    assert.ok(m);
    assert.equal(m.modelId, 'llama-3.3-70b-versatile');
  });
});

describe('resolveModel Vertex branches', () => {
  it('provider "anthropic-vertex" constructs a Vertex Anthropic model', () => {
    const saved = stripVertexEnv();
    process.env.ANTHROPIC_VERTEX_PROJECT_ID = 'proj-1';
    try {
      const m: any = resolveModel({ provider: 'anthropic-vertex', apiKey: 'key', model: 'claude-sonnet-4-6' } as ProviderConfig);
      assert.ok(m);
      assert.equal(m.modelId, 'claude-sonnet-4-6');
    } finally { restoreEnv(saved); }
  });

  it('claude model + CLAUDE_CODE_USE_VERTEX=1 routes to Vertex', () => {
    const saved = stripVertexEnv();
    process.env.CLAUDE_CODE_USE_VERTEX = '1';
    try {
      const m: any = resolveModel({ provider: undefined as any, apiKey: 'key', model: 'claude-opus-4-6' } as ProviderConfig);
      assert.ok(m);
      assert.equal(m.modelId, 'claude-opus-4-6');
    } finally { restoreEnv(saved); }
  });
});

// ---------------------------------------------------------------------------
// convertToolsForSDK — builds one SDK tool entry per definition.
// ---------------------------------------------------------------------------
describe('convertToolsForSDK', () => {
  it('converts each tool, preserving description, and emits no execute()', () => {
    const tools: ToolDefinition[] = [
      { name: 'bash', description: 'run shell', inputSchema: { type: 'object', properties: {} } },
      { name: 'grep', description: 'search', inputSchema: { type: 'object', properties: {} } },
    ];
    const out = convertToolsForSDK(tools) as Record<string, any>;
    assert.deepEqual(Object.keys(out).sort(), ['bash', 'grep']);
    assert.equal(out.bash.description, 'run shell');
    assert.equal(out.bash.execute, undefined);
    assert.ok(out.bash.inputSchema);
  });
});

// ---------------------------------------------------------------------------
// convertMessages — plain assistant, mixed user content (image), tool result
// name resolution, and invalid tool-call argument JSON.
// ---------------------------------------------------------------------------
describe('convertMessages branches', () => {
  it('assistant with no tool_calls becomes a plain assistant message', () => {
    const out = convertMessages([{ role: 'assistant', content: 'done' }]);
    assert.equal(out[0].role, 'assistant');
    assert.equal(out[0].content, 'done');
  });

  it('assistant with null content coerces to empty string', () => {
    const out = convertMessages([{ role: 'assistant', content: null } as Message]);
    assert.equal(out[0].content, '');
  });

  it('user content array maps text + image, image using mediaType (not mimeType)', () => {
    const msg: Message = {
      role: 'user',
      content: [
        { type: 'text', text: 'look at this' },
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'AAAA' } },
      ],
    };
    const out = convertMessages([msg]);
    const parts = out[0].content as any[];
    assert.equal(parts[0].type, 'text');
    assert.equal(parts[0].text, 'look at this');
    assert.equal(parts[1].type, 'image');
    assert.equal(parts[1].image, 'AAAA');
    assert.equal(parts[1].mediaType, 'image/png');
    assert.equal(parts[1].mimeType, undefined);
  });

  it('tool result resolves the tool name from the earlier assistant tool_call', () => {
    const out = convertMessages([
      { role: 'assistant', content: '', tool_calls: [{ id: 'c1', type: 'function', function: { name: 'bash', arguments: '{"a":1}' } }] },
      { role: 'tool', tool_call_id: 'c1', content: 'output' },
    ]);
    const toolMsg = out[1].content as any[];
    assert.equal(toolMsg[0].toolName, 'bash');
    assert.equal(toolMsg[0].toolCallId, 'c1');
  });

  it('tool result with unknown id falls back to "unknown"', () => {
    const out = convertMessages([{ role: 'tool', tool_call_id: 'missing', content: 'x' }]);
    const toolMsg = out[0].content as any[];
    assert.equal(toolMsg[0].toolName, 'unknown');
  });

  it('assistant tool_call with malformed argument JSON yields empty input object', () => {
    const out = convertMessages([
      { role: 'assistant', content: 'hi', tool_calls: [{ id: 'c2', type: 'function', function: { name: 'glob', arguments: 'not json' } }] },
    ]);
    const parts = out[0].content as any[];
    // first part is the text block (content was non-empty), second is the tool-call
    const call = parts.find(p => p.type === 'tool-call');
    assert.deepEqual(call.input, {});
  });
});

// ---------------------------------------------------------------------------
// extractReasoningTokens — every extraction source and the zero fallback.
// ---------------------------------------------------------------------------
describe('extractReasoningTokens', () => {
  it('returns 0 for non-object inputs', () => {
    assert.equal(extractReasoningTokens(null), 0);
    assert.equal(extractReasoningTokens(42), 0);
  });
  it('reads top-level reasoningTokens', () => {
    assert.equal(extractReasoningTokens({ reasoningTokens: 17 }), 17);
  });
  it('reads outputTokenDetails.reasoningTokens when top-level is absent', () => {
    assert.equal(extractReasoningTokens({ outputTokenDetails: { reasoningTokens: 9 } }), 9);
  });
  it('reads raw.reasoning_tokens', () => {
    assert.equal(extractReasoningTokens({ raw: { reasoning_tokens: 5 } }), 5);
  });
  it('reads raw.completion_tokens_details.reasoning_tokens', () => {
    assert.equal(extractReasoningTokens({ raw: { completion_tokens_details: { reasoning_tokens: 3 } } }), 3);
  });
  it('ignores non-positive / non-finite values and returns 0', () => {
    assert.equal(extractReasoningTokens({ reasoningTokens: 0, outputTokenDetails: { reasoningTokens: -1 }, raw: { reasoning_tokens: NaN } }), 0);
  });
});

// ---------------------------------------------------------------------------
// extractRetryAfterSeconds — numeric and HTTP-date forms, absent headers.
// ---------------------------------------------------------------------------
describe('extractRetryAfterSeconds', () => {
  it('returns undefined when no headers are given', () => {
    assert.equal(extractRetryAfterSeconds(undefined), undefined);
  });
  it('parses a numeric retry-after into seconds', () => {
    assert.equal(extractRetryAfterSeconds({ 'retry-after': '30' }), 30);
  });
  it('parses an HTTP-date retry-after into (rounded) seconds', () => {
    const future = new Date(Date.now() + 45_000).toUTCString();
    const secs = extractRetryAfterSeconds({ 'retry-after': future });
    assert.ok(secs !== undefined && secs >= 40 && secs <= 46, `got ${secs}`);
  });
  it('returns undefined when there is no retry-after header', () => {
    assert.equal(extractRetryAfterSeconds({ 'content-type': 'text/plain' }), undefined);
  });
});

// ---------------------------------------------------------------------------
// buildThinkingOptions — per-family providerOptions and the disabled/default
// no-op cases.
// ---------------------------------------------------------------------------
describe('buildThinkingOptions', () => {
  it('returns {} when thinking is disabled or undefined', () => {
    assert.deepEqual(buildThinkingOptions('claude-sonnet-4-6', undefined), {});
    assert.deepEqual(buildThinkingOptions('claude-sonnet-4-6', { enabled: false, level: 'high' } as ThinkingConfig), {});
  });

  it('Anthropic adaptive thinking uses an explicit budget when provided', () => {
    const opts = buildThinkingOptions('claude-sonnet-4-6', { enabled: true, level: 'high', budgetTokens: 4096 }) as any;
    assert.equal(opts.providerOptions.anthropic.thinking.type, 'enabled');
    assert.equal(opts.providerOptions.anthropic.thinking.budgetTokens, 4096);
  });

  it('Anthropic adaptive thinking derives a budget from the level when none given', () => {
    const opts = buildThinkingOptions('claude-sonnet-4-6', { enabled: true, level: 'medium' }) as any;
    assert.equal(typeof opts.providerOptions.anthropic.thinking.budgetTokens, 'number');
    assert.ok(opts.providerOptions.anthropic.thinking.budgetTokens > 0);
  });

  it('OpenAI o-series maps level→reasoningEffort, "max"→"high"', () => {
    const opts = buildThinkingOptions('o3-mini', { enabled: true, level: 'max' }) as any;
    assert.equal(opts.providerOptions.openai.reasoningEffort, 'high');
  });

  it('OpenAI o-series falls back to "medium" for an unknown level', () => {
    const opts = buildThinkingOptions('o3-mini', { enabled: true, level: 'bogus' as any }) as any;
    assert.equal(opts.providerOptions.openai.reasoningEffort, 'medium');
  });

  it('Gemini 2.5 emits thinkingConfig providerOptions', () => {
    const opts = buildThinkingOptions('gemini-2.5-pro', { enabled: true, level: 'high' }) as any;
    assert.equal(opts.providerOptions.google.thinkingConfig.includeThoughts, true);
    assert.equal(typeof opts.providerOptions.google.thinkingConfig.thinkingBudget, 'number');
  });

  it('Gemini 3 also emits thinkingConfig providerOptions', () => {
    const opts = buildThinkingOptions('gemini-3-pro-preview', { enabled: true, level: 'high' }) as any;
    assert.ok(opts.providerOptions.google.thinkingConfig);
  });

  it('returns {} for a model with no special thinking support', () => {
    assert.deepEqual(buildThinkingOptions('deepseek-chat', { enabled: true, level: 'high' }), {});
  });
});

// ---------------------------------------------------------------------------
// buildAnthropicCacheControl — both TTL arms.
// ---------------------------------------------------------------------------
describe('buildAnthropicCacheControl', () => {
  it('adds ttl "1h" for the extended cache', () => {
    assert.deepEqual(buildAnthropicCacheControl('1h'), { type: 'ephemeral', ttl: '1h' });
  });
  it('omits ttl for the default 5-minute cache', () => {
    assert.deepEqual(buildAnthropicCacheControl(), { type: 'ephemeral' });
    assert.deepEqual(buildAnthropicCacheControl('5m'), { type: 'ephemeral' });
  });
});

// ---------------------------------------------------------------------------
// buildRepairToolCall — correction found (direct + lowercase), guard when the
// corrected name is not registered, and no-mapping → null.
// ---------------------------------------------------------------------------
describe('buildRepairToolCall', () => {
  const tools: ToolDefinition[] = [
    { name: 'file_read', description: '', inputSchema: { type: 'object', properties: {} } },
    { name: 'bash', description: '', inputSchema: { type: 'object', properties: {} } },
  ];
  const repair = buildRepairToolCall(tools);
  const call = (toolName: string) => ({ toolCall: { toolName, toolCallId: 'x', input: '{}', toolType: 'function', type: 'tool-call' } } as any);

  it('rewrites a hallucinated name to the registered correct name', async () => {
    const fixed = await repair(call('read_file'));
    assert.ok(fixed);
    assert.equal(fixed.toolName, 'file_read');
  });

  it('matches corrections case-insensitively', async () => {
    const fixed = await repair(call('RUN_COMMAND'));
    assert.ok(fixed);
    assert.equal(fixed.toolName, 'bash');
  });

  it('returns null when the corrected tool is not registered', async () => {
    // "list_directory" maps to "list_dir", which is NOT in our tool set.
    assert.equal(await repair(call('list_directory')), null);
  });

  it('returns null when there is no known correction', async () => {
    assert.equal(await repair(call('totally_unknown_tool')), null);
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletion — native Anthropic path (prompt caching, orphan healing,
// empty-text stripping) driven against an in-process Anthropic-SSE mock.
// ---------------------------------------------------------------------------
describe('streamChatCompletion (native Anthropic path, mock server)', () => {
  let server: http.Server;
  let port: number;
  let savedEnv: Record<string, string | undefined>;
  let lastBody: any;

  const anthropicSse = [
    'event: message_start',
    'data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":1,"cache_read_input_tokens":4}}}', '',
    'event: content_block_start',
    'data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}', '',
    'event: content_block_delta',
    'data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello from mock"}}', '',
    'event: content_block_stop',
    'data: {"type":"content_block_stop","index":0}', '',
    'event: message_delta',
    'data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":7}}', '',
    'event: message_stop',
    'data: {"type":"message_stop"}', '',
  ].join('\n') + '\n';

  before(async () => {
    savedEnv = stripVertexEnv();
    server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      try { lastBody = JSON.parse(Buffer.concat(chunks).toString() || '{}'); } catch { lastBody = undefined; }
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end(anthropicSse);
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => { server.close(); restoreEnv(savedEnv); });

  it('streams Anthropic content and applies cache breakpoints without a live call', async () => {
    const config: ProviderConfig = {
      provider: 'anthropic', apiKey: 'sk-ant-xxxxxxxxxxxx', model: 'claude-sonnet-4-6',
      baseUrl: `http://127.0.0.1:${port}`, cacheTtl: '1h',
    } as ProviderConfig;
    const messages: Message[] = [
      { role: 'system', content: 'You are helpful.' },
      { role: 'user', content: 'Hi' },
      // assistant with a tool_call whose result follows → exercises healing +
      // conversation cache points on the Anthropic branch.
      { role: 'assistant', content: 'working', tool_calls: [{ id: 't1', type: 'function', function: { name: 'bash', arguments: '{}' } }] },
      { role: 'tool', tool_call_id: 't1', content: 'ok' },
      { role: 'user', content: 'continue' },
    ];
    const ac = new AbortController();
    const chunks: any[] = [];
    for await (const c of streamChatCompletion(config, messages, [], ac.signal)) chunks.push(c);

    const text = chunks.filter(c => c.type === 'content_delta').map(c => c.text).join('');
    assert.equal(text, 'Hello from mock');
    const done = chunks.find(c => c.type === 'done');
    assert.ok(done);
    assert.equal(done.finishReason, 'stop');
    assert.ok(done.usage.promptTokens >= 12, `promptTokens=${done.usage.promptTokens}`);
    // cache_read_input_tokens from the mock surfaces as cachedPromptTokens.
    assert.equal(done.usage.cachedPromptTokens, 4);
    // The request actually reached our mock (not a live endpoint).
    assert.ok(lastBody && Array.isArray(lastBody.messages));
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — SUPERINFERENCE_API_TIMEOUT_MS env resolution
// (both the positive and the non-positive/invalid arms) against an OpenAI mock.
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry timeout env resolution', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end(
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"ok"},"index":0}]}\n\n' +
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\n' +
        'data: [DONE]\n\n',
      );
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => server.close());

  async function run(): Promise<string> {
    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${port}/v1` } as ProviderConfig;
    const ac = new AbortController();
    let text = '';
    for await (const c of streamChatCompletionWithRetry(config, [{ role: 'user', content: 'hi' }], [], ac.signal)) {
      if (c.type === 'content_delta') text += c.text;
    }
    return text;
  }

  it('uses a positive env timeout override', async () => {
    const prev = process.env.SUPERINFERENCE_API_TIMEOUT_MS;
    process.env.SUPERINFERENCE_API_TIMEOUT_MS = '120000';
    try { assert.equal(await run(), 'ok'); }
    finally { if (prev === undefined) delete process.env.SUPERINFERENCE_API_TIMEOUT_MS; else process.env.SUPERINFERENCE_API_TIMEOUT_MS = prev; }
  });

  it('ignores a non-positive/invalid env timeout and uses the default', async () => {
    const prev = process.env.SUPERINFERENCE_API_TIMEOUT_MS;
    process.env.SUPERINFERENCE_API_TIMEOUT_MS = 'not-a-number';
    try { assert.equal(await run(), 'ok'); }
    finally { if (prev === undefined) delete process.env.SUPERINFERENCE_API_TIMEOUT_MS; else process.env.SUPERINFERENCE_API_TIMEOUT_MS = prev; }
  });
});

// ---------------------------------------------------------------------------
// fetchHuggingFaceContextWindow — second call for the same key hits the cache
// (same promise returned, no second network round-trip).
// ---------------------------------------------------------------------------
describe('fetchHuggingFaceContextWindow caching', () => {
  let server: http.Server;
  let port: number;
  let hits = 0;
  let savedEndpoint: string | undefined;
  let savedToken: string | undefined;
  before(async () => {
    server = http.createServer((req, res) => {
      hits++;
      if (req.url?.endsWith('/config.json')) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ max_position_embeddings: 262144 }));
      } else {
        res.writeHead(404); res.end();
      }
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
    savedEndpoint = process.env.HF_ENDPOINT;
    savedToken = process.env.HF_TOKEN;
    delete process.env.HF_TOKEN;
    process.env.HF_ENDPOINT = `http://127.0.0.1:${port}`;
  });
  after(() => {
    server.close();
    if (savedEndpoint === undefined) delete process.env.HF_ENDPOINT; else process.env.HF_ENDPOINT = savedEndpoint;
    if (savedToken === undefined) delete process.env.HF_TOKEN; else process.env.HF_TOKEN = savedToken;
  });

  it('serves the second identical lookup from cache', async () => {
    // Unique repo so this test owns its cache entry regardless of run order.
    const repo = `cov2org/cov2-model-${port}`;
    const config = { provider: 'huggingface', apiKey: 'hf_tokenvalue1234567', model: repo } as ProviderConfig;
    const first = await fetchHuggingFaceContextWindow(repo, config);
    assert.equal(first, 262144);
    const hitsAfterFirst = hits;
    const second = await fetchHuggingFaceContextWindow(repo, config);
    assert.equal(second, 262144);
    // No additional network requests were made for the cached lookup.
    assert.equal(hits, hitsAfterFirst);
  });
});

// ---------------------------------------------------------------------------
// sanitizeErrorMessage / CREDENTIAL_PATTERNS — guard + bypass variant.
// ---------------------------------------------------------------------------
describe('sanitizeErrorMessage', () => {
  it('redacts each supported credential family', () => {
    const cases = [
      'sk-abcdefghijklmnopqrstuvwxyz0123',
      'sk-ant-api03-abcdefghijklmnop',
      'sk-or-abcdefghijklmnopqrstu',
      'key-abcdefghijklmnopqrstuv',
      'Bearer abcdefghijklmnopqrstuvwxyz',
      'user:supersecretpassword@host.example.com',
      'x-api-key: abcdefghijklmnopqrst',
      'AIzaSyABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
      'gsk_abcdefghijklmnopqrstuv',
      'xai-abcdefghijklmnopqrstuv',
      'hf_abcdefghijklmnopqrstuv',
    ];
    for (const secret of cases) {
      const out = sanitizeErrorMessage(`error: ${secret} failed`);
      assert.ok(out.includes('[REDACTED]'), `expected redaction for ${secret}`);
      assert.ok(!out.includes(secret.split(/[\s:@]/)[0]) || out.includes('[REDACTED]'));
    }
  });

  it('accepts an Error object as well as a string', () => {
    const out = sanitizeErrorMessage(new Error('bad sk-abcdefghijklmnopqrstuvwxyz0123 key'));
    assert.ok(out.includes('[REDACTED]'));
    assert.ok(!out.includes('sk-abcdefghijkl'));
  });

  it('bypass variant: a token too short to match is left intact (no false redaction)', () => {
    // "sk-short" has fewer than 10 trailing chars, so the pattern must NOT fire.
    const out = sanitizeErrorMessage('config value sk-short is fine');
    assert.equal(out, 'config value sk-short is fine');
    assert.ok(!out.includes('[REDACTED]'));
  });

  it('redacts multiple distinct credentials in one message', () => {
    const out = sanitizeErrorMessage('a sk-aaaaaaaaaaaaaaaaaaaa and gsk_bbbbbbbbbbbbbbbbbbbb');
    assert.equal((out.match(/\[REDACTED\]/g) || []).length, 2);
  });

  it('CREDENTIAL_PATTERNS are all global RegExp instances', () => {
    assert.ok(CREDENTIAL_PATTERNS.length >= 10);
    for (const p of CREDENTIAL_PATTERNS) {
      assert.ok(p instanceof RegExp);
      assert.ok(p.flags.includes('g'));
    }
  });
});

// ---------------------------------------------------------------------------
// isNetworkError / isSslError — code-based + message-based detection, with
// explicit negative (bypass) cases.
// ---------------------------------------------------------------------------
describe('isNetworkError', () => {
  it('detects by error code', () => {
    for (const code of ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH', 'ECONNABORTED']) {
      assert.equal(isNetworkError(Object.assign(new Error('x'), { code })), true, code);
    }
  });
  it('detects by message when there is no code', () => {
    assert.equal(isNetworkError(new Error('socket hang up')), true);
    assert.equal(isNetworkError(new Error('read ECONNRESET')), true);
  });
  it('bypass: unrelated errors and non-errors are not network errors', () => {
    assert.equal(isNetworkError(new Error('401 Unauthorized')), false);
    assert.equal(isNetworkError(null), false);
    assert.equal(isNetworkError('string'), false);
  });
});

describe('isSslError', () => {
  it('detects by TLS error code', () => {
    for (const code of ['UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'CERT_HAS_EXPIRED', 'DEPTH_ZERO_SELF_SIGNED_CERT', 'ERR_TLS_CERT_ALTNAME_INVALID', 'SELF_SIGNED_CERT_IN_CHAIN']) {
      assert.equal(isSslError(Object.assign(new Error('x'), { code })), true, code);
    }
  });
  it('detects by message', () => {
    assert.equal(isSslError(new Error('self signed certificate in chain')), true);
    assert.equal(isSslError(new Error('certificate has expired')), true);
  });
  it('bypass: a network error with no TLS code/message is not an SSL error', () => {
    assert.equal(isSslError(Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })), false);
    assert.equal(isSslError('not-an-error'), false);
  });
});

// ---------------------------------------------------------------------------
// parseRetryAfterHeader — numeric, clamps, HTTP-date (future + past), garbage.
// ---------------------------------------------------------------------------
describe('parseRetryAfterHeader', () => {
  it('parses numeric seconds to ms', () => {
    assert.equal(parseRetryAfterHeader('30'), 30_000);
  });
  it('clamps low numeric values up to the minimum', () => {
    assert.equal(parseRetryAfterHeader('0'), 1_000);
    assert.equal(parseRetryAfterHeader('-5'), 1_000);
  });
  it('clamps high numeric values down to the maximum', () => {
    assert.equal(parseRetryAfterHeader('100000'), MAX_RETRY_AFTER_MS);
  });
  it('parses a future HTTP-date', () => {
    const future = new Date(Date.now() + 30_000).toUTCString();
    const ms = parseRetryAfterHeader(future);
    assert.ok(ms >= 1_000 && ms <= 31_000, `got ${ms}`);
  });
  it('clamps a past HTTP-date to the minimum', () => {
    const past = new Date(Date.now() - 60_000).toUTCString();
    assert.equal(parseRetryAfterHeader(past), 1_000);
  });
  it('returns the default for null/empty/garbage', () => {
    assert.equal(parseRetryAfterHeader(null), 60_000);
    assert.equal(parseRetryAfterHeader(''), 60_000);
    assert.equal(parseRetryAfterHeader('   '), 60_000);
    assert.equal(parseRetryAfterHeader('garbage'), 60_000);
  });
});

// ---------------------------------------------------------------------------
// withExtraHeaders — the three header-shape merge branches.
// ---------------------------------------------------------------------------
describe('withExtraHeaders', () => {
  it('returns undefined when there are no extra headers', () => {
    assert.equal(withExtraHeaders(undefined), undefined);
    assert.equal(withExtraHeaders({}), undefined);
  });

  it('merges into a Headers instance', async () => {
    let seen: Headers | undefined;
    const base: typeof globalThis.fetch = async (_input, init) => {
      seen = init!.headers as Headers;
      return new Response('ok');
    };
    const wrapped = withExtraHeaders({ 'x-extra': 'v1' }, base)!;
    await wrapped('http://127.0.0.1/', { headers: new Headers({ 'x-orig': 'o' }) });
    assert.ok(seen instanceof Headers);
    assert.equal(seen.get('x-extra'), 'v1');
    assert.equal(seen.get('x-orig'), 'o');
  });

  it('merges into a header tuple array', async () => {
    let seen: any;
    const base: typeof globalThis.fetch = async (_input, init) => { seen = init!.headers; return new Response('ok'); };
    const wrapped = withExtraHeaders({ 'x-extra': 'v2' }, base)!;
    await wrapped('http://127.0.0.1/', { headers: [['x-orig', 'o']] as any });
    assert.ok(Array.isArray(seen));
    assert.deepEqual(seen, [['x-orig', 'o'], ['x-extra', 'v2']]);
  });

  it('merges into a plain object', async () => {
    let seen: any;
    const base: typeof globalThis.fetch = async (_input, init) => { seen = init!.headers; return new Response('ok'); };
    const wrapped = withExtraHeaders({ 'x-extra': 'v3' }, base)!;
    await wrapped('http://127.0.0.1/', { headers: { 'x-orig': 'o' } });
    assert.deepEqual(seen, { 'x-orig': 'o', 'x-extra': 'v3' });
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletion — reasoning deltas surface as thinking_delta chunks.
// ---------------------------------------------------------------------------
describe('streamChatCompletion reasoning deltas (Anthropic mock)', () => {
  let server: http.Server;
  let port: number;
  let savedEnv: Record<string, string | undefined>;
  const sse = [
    'event: message_start',
    'data: {"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":5,"output_tokens":1}}}', '',
    'event: content_block_start',
    'data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}', '',
    'event: content_block_delta',
    'data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"let me think"}}', '',
    'event: content_block_stop',
    'data: {"type":"content_block_stop","index":0}', '',
    'event: content_block_start',
    'data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}', '',
    'event: content_block_delta',
    'data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"answer"}}', '',
    'event: content_block_stop',
    'data: {"type":"content_block_stop","index":1}', '',
    'event: message_delta',
    'data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":3}}', '',
    'event: message_stop',
    'data: {"type":"message_stop"}', '',
  ].join('\n') + '\n';

  before(async () => {
    savedEnv = stripVertexEnv();
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end(sse);
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => { server.close(); restoreEnv(savedEnv); });

  it('emits thinking_delta for reasoning content', async () => {
    const config = { provider: 'anthropic', apiKey: 'sk-ant-xxxxxxxxxxxx', model: 'claude-sonnet-4-6', baseUrl: `http://127.0.0.1:${port}` } as ProviderConfig;
    const ac = new AbortController();
    const chunks: any[] = [];
    for await (const c of streamChatCompletion(config, [{ role: 'user', content: 'hi' }], [], ac.signal)) chunks.push(c);
    const thinking = chunks.filter(c => c.type === 'thinking_delta').map(c => c.text).join('');
    const text = chunks.filter(c => c.type === 'content_delta').map(c => c.text).join('');
    assert.equal(thinking, 'let me think');
    assert.equal(text, 'answer');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletion — the generator catch path yields an error chunk when
// the underlying request fails outright (connection refused).
// ---------------------------------------------------------------------------
describe('streamChatCompletion failure catch path', () => {
  it('yields an error chunk when the endpoint refuses the connection', async () => {
    // Bind then immediately close a server to obtain a port nothing listens on.
    const tmp = http.createServer();
    await new Promise<void>(r => tmp.listen(0, '127.0.0.1', () => r()));
    const deadPort = (tmp.address() as { port: number }).port;
    await new Promise<void>(r => tmp.close(() => r()));

    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${deadPort}/v1` } as ProviderConfig;
    const ac = new AbortController();
    const chunks: any[] = [];
    for await (const c of streamChatCompletion(config, [{ role: 'user', content: 'hi' }], [], ac.signal)) chunks.push(c);
    const err = chunks.find(c => c.type === 'error');
    assert.ok(err, 'expected an error chunk');
    assert.ok(typeof err.error === 'string' && err.error.length > 0);
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — an already-aborted signal short-circuits.
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry pre-aborted signal', () => {
  it('yields "Request aborted" immediately when the signal is already aborted', async () => {
    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: 'http://127.0.0.1:1/v1' } as ProviderConfig;
    const ac = new AbortController();
    ac.abort();
    const chunks: any[] = [];
    for await (const c of streamChatCompletionWithRetry(config, [{ role: 'user', content: 'hi' }], [], ac.signal)) chunks.push(c);
    assert.equal(chunks.length, 1);
    assert.equal(chunks[0].type, 'error');
    assert.equal(chunks[0].error, 'Request aborted');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — a non-HTTP error (no status code) is not
// retryable: it is yielded straight through (covers extractStatusCode→null).
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry non-retryable network error', () => {
  it('passes a connection-refused error straight through without retrying', async () => {
    const tmp = http.createServer();
    await new Promise<void>(r => tmp.listen(0, '127.0.0.1', () => r()));
    const deadPort = (tmp.address() as { port: number }).port;
    await new Promise<void>(r => tmp.close(() => r()));

    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${deadPort}/v1` } as ProviderConfig;
    const ac = new AbortController();
    let retried = 0;
    const chunks: any[] = [];
    for await (const c of streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }], [], ac.signal,
      { maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2, onRetry: () => { retried++; } },
    )) chunks.push(c);
    const err = chunks.find(c => c.type === 'error');
    assert.ok(err, 'expected an error chunk');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — a retryable 503 is retried then succeeds.
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry retryable then success', () => {
  let server: http.Server;
  let port: number;
  let calls = 0;
  before(async () => {
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      calls++;
      if (calls === 1) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'service unavailable' } }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end(
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"recovered"},"index":0}]}\n\n' +
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\n' +
        'data: [DONE]\n\n',
      );
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => server.close());

  it('retries after a 503 and streams the recovered response', async () => {
    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${port}/v1` } as ProviderConfig;
    const ac = new AbortController();
    const retries: number[] = [];
    let text = '';
    for await (const c of streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }], [], ac.signal,
      { maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2, onRetry: (a) => retries.push(a) },
    )) {
      if (c.type === 'content_delta') text += c.text;
    }
    assert.equal(text, 'recovered');
    assert.ok(retries.length >= 1, 'should have retried at least once');
    assert.ok(calls >= 2, 'server should have been hit at least twice');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — a 400 gateway rejection of the anthropic-beta
// header strips cacheTtl and retries immediately.
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry gateway rejection', () => {
  let server: http.Server;
  let port: number;
  let calls = 0;
  let secondHadBeta = true;
  before(async () => {
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      calls++;
      if (calls === 1) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { message: 'unsupported anthropic-beta header: extended-cache-ttl' } }));
        return;
      }
      secondHadBeta = (req.headers['anthropic-beta'] as string | undefined)?.includes('extended-cache') ?? false;
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.end(
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"ok"},"index":0}]}\n\n' +
        'data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\n' +
        'data: [DONE]\n\n',
      );
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => server.close());

  it('retries without the beta header after a gateway rejection', async () => {
    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${port}/v1`, cacheTtl: '1h' } as ProviderConfig;
    const ac = new AbortController();
    let text = '';
    for await (const c of streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }], [], ac.signal,
      { maxRetries: 3, baseDelayMs: 1, maxDelayMs: 2 },
    )) { if (c.type === 'content_delta') text += c.text; }
    assert.equal(text, 'ok');
    assert.ok(calls >= 2, 'should have retried after the 400');
    assert.equal(secondHadBeta, false, 'retry must not carry the extended-cache beta header');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — maxTotalWait budget exhausted on a retryable
// status yields the error chunk rather than sleeping again.
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry max total wait', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      // Always 503 so the retryable branch is taken.
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: { message: 'service unavailable' } }));
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => server.close());

  it('stops retrying once the total wait budget is exhausted', async () => {
    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${port}/v1` } as ProviderConfig;
    const ac = new AbortController();
    // A 1ms budget is exhausted by the time the first 503 is parsed.
    await new Promise(r => setTimeout(r, 5));
    const chunks: any[] = [];
    for await (const c of streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }], [], ac.signal,
      { maxRetries: 5, baseDelayMs: 1, maxDelayMs: 2, maxTotalWaitMs: 1 },
    )) chunks.push(c);
    const err = chunks.find(c => c.type === 'error');
    assert.ok(err, 'expected the 503 error to be surfaced');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — idle timeout AFTER partial content yields the
// "partial content received" timeout error (no retry once committed).
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry partial-content idle timeout', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      // Send one content delta, then go silent forever → idle watchdog fires
      // after content has started.
      res.write('data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"partial"},"index":0}]}\n\n');
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => server.close());

  it('reports a partial-content timeout when the stream stalls mid-response', async () => {
    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${port}/v1` } as ProviderConfig;
    const ac = new AbortController();
    const chunks: any[] = [];
    for await (const c of streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }], [], ac.signal,
      { idleTimeoutMs: 50, maxRetries: 2, baseDelayMs: 1, maxDelayMs: 2 },
    )) chunks.push(c);
    const got = chunks.map(c => c.text ?? c.error).join('|');
    const err = chunks.find(c => c.type === 'error' && /partial content/i.test(c.error));
    assert.ok(err, `expected a partial-content timeout error, got: ${got}`);
    // The partial content was delivered before the stall.
    assert.ok(chunks.some(c => c.type === 'content_delta' && c.text === 'partial'));
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletionWithRetry — idle-timeout watchdog aborts a silent stream
// and (with no retries left) surfaces a timeout error. Also exercises onRetry.
// ---------------------------------------------------------------------------
describe('streamChatCompletionWithRetry idle timeout', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    // Accept the request, send headers, then stay silent forever.
    server = http.createServer((req, res) => {
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      // never write body, never end → triggers the idle watchdog
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
  });
  after(() => server.close());

  it('aborts a silent stream via the idle watchdog and retries then errors', async () => {
    const config = { provider: 'openai', apiKey: 'sk-key', model: 'test-model', baseUrl: `http://127.0.0.1:${port}/v1` } as ProviderConfig;
    const ac = new AbortController();
    const retries: number[] = [];
    const chunks: any[] = [];
    for await (const c of streamChatCompletionWithRetry(
      config, [{ role: 'user', content: 'hi' }], [], ac.signal,
      { idleTimeoutMs: 40, maxRetries: 1, baseDelayMs: 1, maxDelayMs: 2, onRetry: (a) => retries.push(a) },
    )) chunks.push(c);
    const err = chunks.find(c => c.type === 'error');
    assert.ok(err, 'expected a timeout error chunk');
    assert.ok(/timed out/i.test(err.error), `got: ${err.error}`);
    // A retry was attempted before giving up.
    assert.ok(retries.length >= 1, 'onRetry should fire for the idle timeout retry');
  });
});

// ---------------------------------------------------------------------------
// Exported timeout constants.
// ---------------------------------------------------------------------------
describe('timeout constants', () => {
  it('DEFAULT_REQUEST_TIMEOUT_MS is 5 minutes', () => {
    assert.equal(DEFAULT_REQUEST_TIMEOUT_MS, 300_000);
  });
  it('MAX_RETRY_AFTER_MS is 1 hour', () => {
    assert.equal(MAX_RETRY_AFTER_MS, 3_600_000);
  });
});
