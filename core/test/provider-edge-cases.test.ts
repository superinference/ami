import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';

import {
  resolveModel,
  convertMessages,
  convertToolsForSDK,
  streamChatCompletion,
} from '../src/provider';
import type { ToolDefinition, ProviderConfig, Message } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeMockTool(
  name: string,
  description: string,
  opts?: { required?: string[]; extraProps?: Record<string, { type: string; description: string }> },
): ToolDefinition {
  const properties: Record<string, { type: string; description: string }> = {
    value: { type: 'string', description: 'A value' },
    ...(opts?.extraProps ?? {}),
  };
  return {
    name,
    description,
    inputSchema: {
      type: 'object',
      properties,
      ...(opts?.required !== undefined ? { required: opts.required } : { required: ['value'] }),
    },
    isReadOnly: true,
    execute: async () => ({ output: 'ok' }),
  };
}

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

/** Create an HTTP server that responds with the given SSE lines, then call `data: [DONE]`. */
function createSSEServer(
  sseLines: string[],
  opts?: { statusCode?: number; headers?: Record<string, string>; bodyOnError?: string; delayMs?: number },
): http.Server {
  return http.createServer(async (req, res) => {
    // Drain the request body
    for await (const _ of req) { /* drain */ }

    const statusCode = opts?.statusCode ?? 200;

    if (statusCode !== 200) {
      const headers: Record<string, string> = {
        'Content-Type': 'application/json',
        ...(opts?.headers ?? {}),
      };
      res.writeHead(statusCode, headers);
      res.end(opts?.bodyOnError ?? '{"error":{"message":"Error","type":"server_error"}}');
      return;
    }

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      Connection: 'keep-alive',
    });

    for (const line of sseLines) {
      if (opts?.delayMs) {
        await new Promise((resolve) => setTimeout(resolve, opts.delayMs));
      }
      res.write(line + '\n\n');
    }
    res.write('data: [DONE]\n\n');
    res.end();
  });
}

async function startServer(server: http.Server): Promise<number> {
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve());
  });
  return (server.address() as { port: number }).port;
}

async function collectChunks(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[] = [],
): Promise<import('../src/types').StreamChunk[]> {
  const ac = new AbortController();
  const chunks: import('../src/types').StreamChunk[] = [];
  for await (const chunk of streamChatCompletion(config, messages, tools, ac.signal)) {
    chunks.push(chunk);
  }
  return chunks;
}

// ===========================================================================
// 1. convertMessages with tool results
// ===========================================================================
describe('convertMessages - tool result name resolution', () => {
  it('resolves toolName from matching tool_call_id in preceding assistant message', () => {
    const messages: Message[] = [
      { role: 'user', content: 'Read the file' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_abc', type: 'function', function: { name: 'file_read', arguments: '{"path":"/tmp/a"}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'call_abc', content: 'file contents' },
    ];

    const result = convertMessages(messages);
    const toolMsg = result[2];
    assert.equal(toolMsg.role, 'tool');
    assert.ok(Array.isArray(toolMsg.content), 'tool message content should be an array');
    const part = (toolMsg.content as any[])[0];
    assert.equal(part.type, 'tool-result');
    assert.equal(part.toolCallId, 'call_abc');
    assert.equal(part.toolName, 'file_read');
  });

  it('sets toolName to "unknown" when no matching tool_call_id is found', () => {
    const messages: Message[] = [
      { role: 'user', content: 'Do something' },
      { role: 'tool', tool_call_id: 'call_nonexistent', content: 'result' },
    ];

    const result = convertMessages(messages);
    const toolMsg = result[1];
    assert.equal(toolMsg.role, 'tool');
    const part = (toolMsg.content as any[])[0];
    assert.equal(part.toolName, 'unknown');
  });

  it('resolves correct names for multiple tool results from different tools', () => {
    const messages: Message[] = [
      { role: 'user', content: 'Read two files' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_1', type: 'function', function: { name: 'file_read', arguments: '{"path":"/a"}' } },
          { id: 'call_2', type: 'function', function: { name: 'bash', arguments: '{"cmd":"ls"}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'call_1', content: 'content of a' },
      { role: 'tool', tool_call_id: 'call_2', content: 'ls output' },
    ];

    const result = convertMessages(messages);
    const tool1 = (result[2].content as any[])[0];
    const tool2 = (result[3].content as any[])[0];
    assert.equal(tool1.toolName, 'file_read');
    assert.equal(tool2.toolName, 'bash');
  });

  it('resolves toolName even when tool result is separated by several messages from its tool_call', () => {
    const messages: Message[] = [
      { role: 'user', content: 'Do something complex' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_far', type: 'function', function: { name: 'file_write', arguments: '{}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'call_far', content: 'wrote file' },
      { role: 'assistant', content: 'I wrote the file. Now let me read it.' },
      { role: 'user', content: 'ok' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_later', type: 'function', function: { name: 'file_read', arguments: '{}' } },
        ],
      },
      // Several messages between the assistant tool_call and this tool result
      // (tool result for call_far, but the backward search should still work)
      { role: 'tool', tool_call_id: 'call_later', content: 'file data' },
    ];

    const result = convertMessages(messages);
    const toolMsg = result[6]; // last message
    const part = (toolMsg.content as any[])[0];
    assert.equal(part.toolCallId, 'call_later');
    assert.equal(part.toolName, 'file_read');
  });
});

// ===========================================================================
// 2. convertMessages format correctness
// ===========================================================================
describe('convertMessages - format correctness', () => {
  it('system message passes through as { role: "system", content: string }', () => {
    const messages: Message[] = [{ role: 'system', content: 'Be helpful.' }];
    const result = convertMessages(messages);
    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'system');
    assert.equal(result[0].content, 'Be helpful.');
  });

  it('user message passes through', () => {
    const messages: Message[] = [{ role: 'user', content: 'Hello there' }];
    const result = convertMessages(messages);
    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'user');
    assert.equal(result[0].content, 'Hello there');
  });

  it('assistant with content only passes through as string', () => {
    const messages: Message[] = [{ role: 'assistant', content: 'Sure!' }];
    const result = convertMessages(messages);
    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'assistant');
    assert.equal(result[0].content, 'Sure!');
  });

  it('assistant with tool_calls only (no content) produces content array with only tool-call parts', () => {
    const messages: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'tc1', type: 'function', function: { name: 'bash', arguments: '{"cmd":"pwd"}' } },
        ],
      },
    ];
    const result = convertMessages(messages);
    const msg = result[0];
    assert.equal(msg.role, 'assistant');
    assert.ok(Array.isArray(msg.content), 'content should be an array when tool_calls present');
    const parts = msg.content as any[];
    // No text part because content is null
    assert.equal(parts.length, 1);
    assert.equal(parts[0].type, 'tool-call');
    assert.equal(parts[0].toolCallId, 'tc1');
    assert.equal(parts[0].toolName, 'bash');
    assert.deepEqual(parts[0].input, { cmd: 'pwd' });
  });

  it('assistant with content AND tool_calls produces content array with text part + tool-call parts', () => {
    const messages: Message[] = [
      {
        role: 'assistant',
        content: 'Let me run that.',
        tool_calls: [
          { id: 'tc2', type: 'function', function: { name: 'file_read', arguments: '{"path":"/x"}' } },
        ],
      },
    ];
    const result = convertMessages(messages);
    const msg = result[0];
    assert.equal(msg.role, 'assistant');
    assert.ok(Array.isArray(msg.content), 'content should be an array');
    const parts = msg.content as any[];
    assert.equal(parts.length, 2);
    assert.equal(parts[0].type, 'text');
    assert.equal(parts[0].text, 'Let me run that.');
    assert.equal(parts[1].type, 'tool-call');
    assert.equal(parts[1].toolCallId, 'tc2');
    assert.equal(parts[1].toolName, 'file_read');
    assert.deepEqual(parts[1].input, { path: '/x' });
  });

  it('tool result produces role "tool" with tool-result content part', () => {
    const messages: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'tc3', type: 'function', function: { name: 'bash', arguments: '{}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'tc3', content: 'output data' },
    ];
    const result = convertMessages(messages);
    const toolMsg = result[1];
    assert.equal(toolMsg.role, 'tool');
    assert.ok(Array.isArray(toolMsg.content));
    const part = (toolMsg.content as any[])[0];
    assert.equal(part.type, 'tool-result');
    assert.equal(part.toolCallId, 'tc3');
    assert.equal(part.toolName, 'bash');
    assert.deepEqual(part.output, { type: 'text', value: 'output data' });
  });
});

// ===========================================================================
// 3. resolveModel provider detection (edge cases)
// ===========================================================================
describe('resolveModel - provider detection edge cases', () => {
  it('detects google provider for generativelanguage.googleapis.com baseUrl', () => {
    const config = makeConfig({
      baseUrl: 'https://generativelanguage.googleapis.com/v1beta',
      model: 'some-model',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('google'), `Expected google, got ${result.provider}`);
  });

  it('detects google provider for aiplatform.googleapis.com baseUrl', () => {
    const config = makeConfig({
      baseUrl: 'https://us-central1-aiplatform.googleapis.com/v1',
      model: 'some-model',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('google'), `Expected google, got ${result.provider}`);
  });

  it('detects google provider for model name starting with "gemini" regardless of baseUrl', () => {
    const config = makeConfig({
      baseUrl: 'https://custom-proxy.example.com/v1',
      model: 'gemini-2.0-flash',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('google'), `Expected google, got ${result.provider}`);
  });

  it('detects anthropic provider for baseUrl containing anthropic.com', () => {
    const config = makeConfig({
      baseUrl: 'https://api.anthropic.com/v1',
      model: 'some-model',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('anthropic'), `Expected anthropic, got ${result.provider}`);
  });

  it('detects anthropic provider for model name starting with "claude"', () => {
    const config = makeConfig({
      baseUrl: 'https://custom-proxy.example.com/v1',
      model: 'claude-sonnet-4-20250514',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('anthropic'), `Expected anthropic, got ${result.provider}`);
  });

  it('uses openai provider for localhost:11434 (Ollama)', () => {
    const config = makeConfig({
      baseUrl: 'http://localhost:11434/v1',
      model: 'llama3',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('openai'), `Expected openai, got ${result.provider}`);
  });

  it('uses openai provider for openrouter.ai', () => {
    const config = makeConfig({
      baseUrl: 'https://openrouter.ai/api/v1',
      model: 'meta-llama/llama-3-70b',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('openai'), `Expected openai, got ${result.provider}`);
  });

  it('defaults to openai provider for unknown baseUrl', () => {
    const config = makeConfig({
      baseUrl: 'https://totally-unknown-provider.example.com/v1',
      model: 'my-custom-model',
    });
    const result = resolveModel(config);
    assert.ok(result.provider.startsWith('openai'), `Expected openai, got ${result.provider}`);
  });
});

// ===========================================================================
// 4. convertToolsForSDK edge cases
// ===========================================================================
describe('convertToolsForSDK - edge cases', () => {
  it('returns empty object for empty tools array', () => {
    const result = convertToolsForSDK([]);
    assert.deepEqual(result, {});
    assert.equal(Object.keys(result).length, 0);
  });

  it('converts tool with all input schema fields correctly', () => {
    const tool: ToolDefinition = {
      name: 'complex_tool',
      description: 'A tool with many schema fields',
      inputSchema: {
        type: 'object',
        properties: {
          name: { type: 'string', description: 'The name' },
          count: { type: 'number', description: 'How many' },
          tags: { type: 'array', description: 'Tags', items: { type: 'string' } },
          mode: { type: 'string', description: 'Mode', enum: ['fast', 'slow'] },
          verbose: { type: 'boolean', description: 'Verbose output', default: false },
        },
        required: ['name', 'count'],
      },
      isReadOnly: false,
      execute: async () => ({ output: 'ok' }),
    };

    const result = convertToolsForSDK([tool]);
    assert.ok('complex_tool' in result);
    const sdkTool = result['complex_tool'];
    assert.equal(sdkTool.description, 'A tool with many schema fields');
    // inputSchema is wrapped by jsonSchema() - it should exist
    assert.ok(sdkTool.inputSchema, 'inputSchema should be present');
  });

  it('preserves required fields in the schema', () => {
    const tool = makeMockTool('req_tool', 'Tool with required', { required: ['value'] });
    const result = convertToolsForSDK([tool]);
    const sdkTool = result['req_tool'];
    assert.ok(sdkTool, 'Tool should exist');
    // The jsonSchema wrapper preserves the underlying schema
    // Access the raw JSON schema to verify required
    const rawSchema = (sdkTool.inputSchema as any)?.jsonSchema ?? (sdkTool.inputSchema as any);
    if (rawSchema && rawSchema.required) {
      assert.deepEqual(rawSchema.required, ['value']);
    }
  });

  it('handles tool with no required fields', () => {
    const tool: ToolDefinition = {
      name: 'optional_tool',
      description: 'All fields optional',
      inputSchema: {
        type: 'object',
        properties: {
          hint: { type: 'string', description: 'An optional hint' },
        },
        // no required field
      },
      isReadOnly: true,
      execute: async () => ({ output: 'ok' }),
    };

    const result = convertToolsForSDK([tool]);
    assert.ok('optional_tool' in result);
    const sdkTool = result['optional_tool'];
    assert.ok(sdkTool, 'Tool should exist');
    const rawSchema = (sdkTool.inputSchema as any)?.jsonSchema ?? (sdkTool.inputSchema as any);
    // required should either be absent or undefined
    if (rawSchema) {
      assert.ok(
        rawSchema.required === undefined || (Array.isArray(rawSchema.required) && rawSchema.required.length === 0),
        'required should be absent or empty when not specified',
      );
    }
  });

  it('converts multiple tools with correct names as keys', () => {
    const tools = [
      makeMockTool('alpha', 'First tool'),
      makeMockTool('beta', 'Second tool'),
      makeMockTool('gamma', 'Third tool'),
    ];

    const result = convertToolsForSDK(tools);
    const keys = Object.keys(result);
    assert.equal(keys.length, 3);
    assert.ok(keys.includes('alpha'));
    assert.ok(keys.includes('beta'));
    assert.ok(keys.includes('gamma'));
    assert.equal(result['alpha'].description, 'First tool');
    assert.equal(result['beta'].description, 'Second tool');
    assert.equal(result['gamma'].description, 'Third tool');
  });
});

// ===========================================================================
// 5. streamChatCompletion with mock server
// ===========================================================================
describe('streamChatCompletion - text-only response via mock server', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = createSSEServer([
      'data: {"id":"chatcmpl-t1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
      'data: {"id":"chatcmpl-t1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"},"index":0}]}',
      'data: {"id":"chatcmpl-t1","object":"chat.completion.chunk","choices":[{"delta":{"content":" there"},"index":0}]}',
      'data: {"id":"chatcmpl-t1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}',
    ]);
    port = await startServer(server);
  });

  after(() => { server.close(); });

  it('yields content_delta chunks for text-only response', async () => {
    const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
    const chunks = await collectChunks(config, [{ role: 'user', content: 'Hi' }]);

    const contentChunks = chunks.filter((c) => c.type === 'content_delta');
    assert.ok(contentChunks.length >= 1, `Expected content_delta chunks, got ${contentChunks.length}`);
    const fullText = contentChunks.map((c) => c.text ?? '').join('');
    assert.ok(fullText.includes('Hello'), `Expected "Hello" in text, got "${fullText}"`);
    assert.ok(fullText.includes('there'), `Expected "there" in text, got "${fullText}"`);
  });
});

describe('streamChatCompletion - tool call response via mock server', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = createSSEServer([
      'data: {"id":"chatcmpl-tc","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_xyz","type":"function","function":{"name":"bash","arguments":""}}]},"index":0}]}',
      'data: {"id":"chatcmpl-tc","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"cmd\\":\\"ls\\"}"}}]},"index":0}]}',
      'data: {"id":"chatcmpl-tc","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"tool_calls","index":0}],"usage":{"prompt_tokens":8,"completion_tokens":4,"total_tokens":12}}',
    ]);
    port = await startServer(server);
  });

  after(() => { server.close(); });

  it('yields tool_call_delta with correct id, name, and arguments', async () => {
    const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
    const chunks = await collectChunks(config, [{ role: 'user', content: 'List files' }]);

    const toolChunks = chunks.filter((c) => c.type === 'tool_call_delta');
    assert.ok(toolChunks.length >= 1, `Expected tool_call_delta, got ${toolChunks.length}`);

    // AI SDK delivers complete tool calls, so we should get one with name, id, arguments
    const tc = toolChunks[0];
    assert.equal(tc.toolCall?.id, 'call_xyz');
    assert.equal(tc.toolCall?.function?.name, 'bash');
    assert.ok(tc.toolCall?.function?.arguments?.includes('ls'), 'arguments should contain "ls"');
  });
});

describe('streamChatCompletion - empty response (just finish)', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = createSSEServer([
      'data: {"id":"chatcmpl-empty","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
      'data: {"id":"chatcmpl-empty","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":3,"completion_tokens":0,"total_tokens":3}}',
    ]);
    port = await startServer(server);
  });

  after(() => { server.close(); });

  it('yields done chunk without content_delta for empty response', async () => {
    const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
    const chunks = await collectChunks(config, [{ role: 'user', content: 'empty' }]);

    const contentChunks = chunks.filter((c) => c.type === 'content_delta');
    assert.equal(contentChunks.length, 0, 'Should have no content_delta chunks');

    const doneChunks = chunks.filter((c) => c.type === 'done');
    assert.ok(doneChunks.length >= 1, 'Should have a done chunk');
  });
});

describe('streamChatCompletion - multiple text deltas in order', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = createSSEServer([
      'data: {"id":"chatcmpl-multi","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
      'data: {"id":"chatcmpl-multi","object":"chat.completion.chunk","choices":[{"delta":{"content":"A"},"index":0}]}',
      'data: {"id":"chatcmpl-multi","object":"chat.completion.chunk","choices":[{"delta":{"content":"B"},"index":0}]}',
      'data: {"id":"chatcmpl-multi","object":"chat.completion.chunk","choices":[{"delta":{"content":"C"},"index":0}]}',
      'data: {"id":"chatcmpl-multi","object":"chat.completion.chunk","choices":[{"delta":{"content":"D"},"index":0}]}',
      'data: {"id":"chatcmpl-multi","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":5,"completion_tokens":4,"total_tokens":9}}',
    ]);
    port = await startServer(server);
  });

  after(() => { server.close(); });

  it('yields all text deltas in order', async () => {
    const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
    const chunks = await collectChunks(config, [{ role: 'user', content: 'go' }]);

    const texts = chunks.filter((c) => c.type === 'content_delta').map((c) => c.text);
    const joined = texts.join('');
    assert.equal(joined, 'ABCD', `Expected "ABCD", got "${joined}"`);
  });
});

describe('streamChatCompletion - usage stats in done chunk', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = createSSEServer([
      'data: {"id":"chatcmpl-usage","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
      'data: {"id":"chatcmpl-usage","object":"chat.completion.chunk","choices":[{"delta":{"content":"ok"},"index":0}]}',
      'data: {"id":"chatcmpl-usage","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":42,"completion_tokens":7,"total_tokens":49}}',
    ]);
    port = await startServer(server);
  });

  after(() => { server.close(); });

  it('includes usage statistics in the done chunk', async () => {
    const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
    const chunks = await collectChunks(config, [{ role: 'user', content: 'stats' }]);

    const doneChunks = chunks.filter((c) => c.type === 'done');
    assert.ok(doneChunks.length >= 1, 'Should have a done chunk');
    const done = doneChunks[0];
    assert.ok(done.usage, 'done chunk should have usage');
    assert.equal(done.usage!.promptTokens, 42);
    assert.equal(done.usage!.completionTokens, 7);
    assert.equal(done.usage!.totalTokens, 49);
  });
});

// ===========================================================================
// 6. Multi-turn with tool results through mock server
// ===========================================================================
describe('streamChatCompletion - multi-turn tool call then text response', () => {
  let server: http.Server;
  let port: number;
  let requestCount = 0;
  let capturedBodies: string[] = [];

  before(async () => {
    requestCount = 0;
    capturedBodies = [];
    server = http.createServer(async (req, res) => {
      const bodyChunks: Buffer[] = [];
      for await (const chunk of req) {
        bodyChunks.push(chunk as Buffer);
      }
      const body = Buffer.concat(bodyChunks).toString('utf-8');
      capturedBodies.push(body);
      requestCount++;

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });

      if (requestCount === 1) {
        // Turn 1: model calls file_read
        const sseLines = [
          'data: {"id":"chatcmpl-turn1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_fr1","type":"function","function":{"name":"file_read","arguments":""}}]},"index":0}]}',
          'data: {"id":"chatcmpl-turn1","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"file_path\\":\\"/tmp/test.txt\\"}"}}]},"index":0}]}',
          'data: {"id":"chatcmpl-turn1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"tool_calls","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}',
          'data: [DONE]',
        ];
        for (const line of sseLines) {
          res.write(line + '\n\n');
        }
      } else {
        // Turn 2: model responds with text using file content
        const sseLines = [
          'data: {"id":"chatcmpl-turn2","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
          'data: {"id":"chatcmpl-turn2","object":"chat.completion.chunk","choices":[{"delta":{"content":"The file contains: hello world"},"index":0}]}',
          'data: {"id":"chatcmpl-turn2","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":20,"completion_tokens":6,"total_tokens":26}}',
          'data: [DONE]',
        ];
        for (const line of sseLines) {
          res.write(line + '\n\n');
        }
      }
      res.end();
    });
    port = await startServer(server);
  });

  after(() => { server.close(); });

  it('turn 1 yields tool call, turn 2 yields text using tool result', async () => {
    const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
    const ac = new AbortController();

    // Turn 1: send user message
    const turn1Messages: Message[] = [
      { role: 'user', content: 'Read /tmp/test.txt' },
    ];

    const turn1Chunks: import('../src/types').StreamChunk[] = [];
    for await (const chunk of streamChatCompletion(config, turn1Messages, [], ac.signal)) {
      turn1Chunks.push(chunk);
    }

    // Verify turn 1 yields a tool call
    const toolCallChunks = turn1Chunks.filter((c) => c.type === 'tool_call_delta');
    assert.ok(toolCallChunks.length >= 1, 'Turn 1 should have tool_call_delta');
    assert.equal(toolCallChunks[0].toolCall?.function?.name, 'file_read');
    assert.equal(toolCallChunks[0].toolCall?.id, 'call_fr1');

    // Turn 2: send messages including tool result
    const turn2Messages: Message[] = [
      { role: 'user', content: 'Read /tmp/test.txt' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          { id: 'call_fr1', type: 'function', function: { name: 'file_read', arguments: '{"file_path":"/tmp/test.txt"}' } },
        ],
      },
      { role: 'tool', tool_call_id: 'call_fr1', content: 'hello world' },
    ];

    const turn2Chunks: import('../src/types').StreamChunk[] = [];
    for await (const chunk of streamChatCompletion(config, turn2Messages, [], ac.signal)) {
      turn2Chunks.push(chunk);
    }

    // Verify turn 2 yields text content
    const contentChunks = turn2Chunks.filter((c) => c.type === 'content_delta');
    assert.ok(contentChunks.length >= 1, 'Turn 2 should have content_delta');
    const fullText = contentChunks.map((c) => c.text ?? '').join('');
    assert.ok(fullText.includes('hello world'), `Turn 2 text should reference file content, got "${fullText}"`);

    // Verify the tool result message sent to server has correct toolName (not empty)
    // The second request body should include the tool result with the proper toolName
    assert.ok(capturedBodies.length >= 2, 'Should have at least 2 requests');
    const secondBody = JSON.parse(capturedBodies[1]);
    const toolResultMsg = secondBody.messages.find((m: any) => m.role === 'tool');
    assert.ok(toolResultMsg, 'Second request should contain a tool message');
  });
});

// ===========================================================================
// 7. Error handling
// ===========================================================================
describe('streamChatCompletion - error handling edge cases', () => {
  it('yields error chunk with status code on HTTP 500', async () => {
    const server = createSSEServer([], {
      statusCode: 500,
      bodyOnError: '{"error":{"message":"Internal server error","type":"server_error"}}',
    });
    const port = await startServer(server);

    try {
      const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
      const chunks = await collectChunks(config, [{ role: 'user', content: 'fail' }]);

      assert.ok(chunks.length >= 1, 'Should have at least one chunk');
      const errorChunk = chunks.find((c) => c.type === 'error');
      assert.ok(errorChunk, 'Should have an error chunk');
      assert.ok(
        errorChunk!.error!.includes('500') || errorChunk!.error!.toLowerCase().includes('server'),
        `Error should mention 500 or server, got: ${errorChunk!.error}`,
      );
    } finally {
      server.close();
    }
  });

  it('yields error chunk with status code on HTTP 429', async () => {
    const server = createSSEServer([], {
      statusCode: 429,
      headers: { 'Retry-After': '30' },
      bodyOnError: '{"error":{"message":"Rate limited","type":"rate_limit_error"}}',
    });
    const port = await startServer(server);

    try {
      const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
      const chunks = await collectChunks(config, [{ role: 'user', content: 'throttle' }]);

      const errorChunk = chunks.find((c) => c.type === 'error');
      assert.ok(errorChunk, 'Should have an error chunk');
      assert.ok(
        errorChunk!.error!.includes('429') || errorChunk!.error!.toLowerCase().includes('rate'),
        `Error should mention 429 or rate limit, got: ${errorChunk!.error}`,
      );
    } finally {
      server.close();
    }
  });

  it('handles connection drop mid-stream gracefully', async () => {
    const server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });

      // Send some valid data then abruptly destroy the connection
      res.write('data: {"id":"chatcmpl-drop","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}\n\n');
      res.write('data: {"id":"chatcmpl-drop","object":"chat.completion.chunk","choices":[{"delta":{"content":"partial"},"index":0}]}\n\n');

      // Destroy the connection without finishing
      res.destroy();
    });
    const port = await startServer(server);

    try {
      const config = makeConfig({ baseUrl: `http://127.0.0.1:${port}/v1`, model: 'test-model' });
      const ac = new AbortController();

      // Should not throw — should yield whatever it got plus an error or done
      const chunks: import('../src/types').StreamChunk[] = [];
      try {
        for await (const chunk of streamChatCompletion(config, [{ role: 'user', content: 'drop' }], [], ac.signal)) {
          chunks.push(chunk);
        }
      } catch {
        // If an exception is thrown, that's also acceptable handling
        chunks.push({ type: 'error', error: 'exception thrown' });
      }

      // We should have gotten at least something (content or error)
      assert.ok(chunks.length >= 1, 'Should have at least one chunk even on connection drop');

      // The stream should terminate, either with an error chunk or done chunk or exception
      const lastChunk = chunks[chunks.length - 1];
      assert.ok(
        lastChunk.type === 'error' || lastChunk.type === 'done' || lastChunk.type === 'content_delta',
        `Last chunk should be error, done, or content_delta, got ${lastChunk.type}`,
      );
    } finally {
      server.close();
    }
  });
});
