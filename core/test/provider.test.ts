import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  toolDefinitionsToOpenAI,
  streamChatCompletion,
  resolveModel,
  convertMessages,
  convertToolsForSDK,
} from '../src/provider';
import type { ToolDefinition, ProviderConfig, Message } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeMockTool(name: string, description: string): ToolDefinition {
  return {
    name,
    description,
    inputSchema: {
      type: 'object',
      properties: {
        value: { type: 'string', description: 'A value' },
      },
      required: ['value'],
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

// ---------------------------------------------------------------------------
// toolDefinitionsToOpenAI
// ---------------------------------------------------------------------------
describe('toolDefinitionsToOpenAI', () => {
  it('converts tool definitions to OpenAI function format', () => {
    const tools = [
      makeMockTool('file_read', 'Read a file'),
      makeMockTool('bash', 'Run a command'),
    ];

    const result = toolDefinitionsToOpenAI(tools);
    assert.equal(result.length, 2);

    assert.equal(result[0].type, 'function');
    assert.equal(result[0].function.name, 'file_read');
    assert.equal(result[0].function.description, 'Read a file');
    assert.deepEqual(result[0].function.parameters.type, 'object');
    assert.ok(result[0].function.parameters.properties.value);

    assert.equal(result[1].function.name, 'bash');
  });

  it('returns empty array for empty tools', () => {
    const result = toolDefinitionsToOpenAI([]);
    assert.deepEqual(result, []);
  });

  it('preserves required fields', () => {
    const tools = [makeMockTool('test', 'Test tool')];
    const result = toolDefinitionsToOpenAI(tools);
    assert.deepEqual(result[0].function.parameters.required, ['value']);
  });
});

// ---------------------------------------------------------------------------
// resolveModel - provider detection
// ---------------------------------------------------------------------------
describe('resolveModel - provider detection', () => {
  it('detects OpenAI as default for standard OpenAI base URL', () => {
    const config = makeConfig({
      baseUrl: 'https://api.openai.com/v1',
      model: 'gpt-4o',
    });
    const result = resolveModel(config);

    assert.ok(result, 'resolveModel should return a result');
    // AI SDK provider IDs are namespaced (e.g. 'openai.chat', 'anthropic.messages')
    assert.ok(result.provider.startsWith('openai'), `Expected provider starting with 'openai', got '${result.provider}'`);
  });

  it('detects Anthropic by model name prefix', () => {
    const config = makeConfig({
      baseUrl: 'https://api.openai.com/v1', // URL doesn't matter for Anthropic detection
      model: 'claude-sonnet-4-20250514',
    });
    const result = resolveModel(config);

    assert.ok(result);
    assert.ok(result.provider.startsWith('anthropic'), `Expected provider starting with 'anthropic', got '${result.provider}'`);
  });

  it('detects Anthropic for claude-3.5 model names', () => {
    const config = makeConfig({
      model: 'claude-3-5-sonnet-20241022',
    });
    const result = resolveModel(config);

    assert.ok(result);
    assert.ok(result.provider.startsWith('anthropic'), `Expected provider starting with 'anthropic', got '${result.provider}'`);
  });

  it('detects Google/Gemini by URL', () => {
    const config = makeConfig({
      baseUrl: 'https://generativelanguage.googleapis.com/v1beta',
      model: 'gemini-2.0-flash',
    });
    const result = resolveModel(config);

    assert.ok(result);
    assert.ok(result.provider.startsWith('google'), `Expected provider starting with 'google', got '${result.provider}'`);
  });

  it('detects Ollama by localhost URL', () => {
    const config = makeConfig({
      baseUrl: 'http://localhost:11434/v1',
      model: 'llama3',
    });
    const result = resolveModel(config);

    assert.ok(result);
    // Ollama uses OpenAI-compatible format
    assert.ok(
      result.provider.startsWith('openai'),
      `Expected provider starting with 'openai', got '${result.provider}'`,
    );
  });

  // Note: removed 'uses explicit provider hint when set' test — ProviderConfig does not have a provider field.

  it('falls back to openai for unknown base URLs', () => {
    const config = makeConfig({
      baseUrl: 'https://custom-provider.example.com/v1',
      model: 'some-model',
    });
    const result = resolveModel(config);

    assert.ok(result);
    assert.ok(result.provider.startsWith('openai'), `Expected provider starting with 'openai', got '${result.provider}'`);
  });

  it('detects OpenRouter by URL', () => {
    const config = makeConfig({
      baseUrl: 'https://openrouter.ai/api/v1',
      model: 'anthropic/claude-sonnet-4',
    });
    const result = resolveModel(config);

    assert.ok(result);
    // OpenRouter uses OpenAI-compatible format
    assert.ok(
      result.provider.startsWith('openai'),
      `Expected provider starting with 'openai', got '${result.provider}'`,
    );
  });
});

// ---------------------------------------------------------------------------
// convertMessages - message format conversion
// ---------------------------------------------------------------------------
describe('convertMessages', () => {
  it('converts a simple user message', () => {
    const messages: Message[] = [{ role: 'user', content: 'Hello' }];
    const result = convertMessages(messages);

    assert.ok(Array.isArray(result));
    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'user');
    // Content might be string or structured depending on SDK format
    const content = typeof result[0].content === 'string'
      ? result[0].content
      : JSON.stringify(result[0].content);
    assert.ok(content.includes('Hello'));
  });

  it('converts a system message', () => {
    const messages: Message[] = [{ role: 'system', content: 'You are helpful.' }];
    const result = convertMessages(messages);

    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'system');
  });

  it('converts an assistant message with text content', () => {
    const messages: Message[] = [
      { role: 'assistant', content: 'I can help with that.' },
    ];
    const result = convertMessages(messages);

    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'assistant');
  });

  it('converts an assistant message with tool_calls', () => {
    const messages: Message[] = [
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          {
            id: 'call_1',
            type: 'function',
            function: {
              name: 'file_read',
              arguments: '{"file_path":"/tmp/test.txt"}',
            },
          },
        ],
      },
    ];
    const result = convertMessages(messages);

    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'assistant');
    // The converted message should preserve tool call information
    // The exact format depends on AI SDK, but the data should be present
  });

  it('converts a tool result message', () => {
    const messages: Message[] = [
      {
        role: 'tool',
        tool_call_id: 'call_1',
        content: 'file contents here',
      },
    ];
    const result = convertMessages(messages);

    assert.equal(result.length, 1);
    assert.equal(result[0].role, 'tool');
  });

  it('converts a multi-message conversation', () => {
    const messages: Message[] = [
      { role: 'system', content: 'You are a helper.' },
      { role: 'user', content: 'Hello' },
      { role: 'assistant', content: 'Hi there!' },
      { role: 'user', content: 'Read a file' },
      {
        role: 'assistant',
        content: null,
        tool_calls: [
          {
            id: 'call_1',
            type: 'function',
            function: { name: 'file_read', arguments: '{"file_path":"/tmp/x"}' },
          },
        ],
      },
      { role: 'tool', tool_call_id: 'call_1', content: 'file data' },
      { role: 'assistant', content: 'Here is the file content.' },
    ];
    const result = convertMessages(messages);

    assert.equal(result.length, 7);
    assert.equal(result[0].role, 'system');
    assert.equal(result[1].role, 'user');
    assert.equal(result[2].role, 'assistant');
    assert.equal(result[3].role, 'user');
    assert.equal(result[4].role, 'assistant');
    assert.equal(result[5].role, 'tool');
    assert.equal(result[6].role, 'assistant');
  });

  it('handles empty message list', () => {
    const result = convertMessages([]);
    assert.deepEqual(result, []);
  });
});

// ---------------------------------------------------------------------------
// convertToolsForSDK - tool definition conversion for AI SDK
// ---------------------------------------------------------------------------
describe('convertToolsForSDK', () => {
  it('converts tool definitions to AI SDK format', () => {
    const tools = [
      makeMockTool('file_read', 'Read a file'),
      makeMockTool('bash', 'Run a command'),
    ];

    const result = convertToolsForSDK(tools);

    assert.ok(result, 'Should return a result');
    assert.ok(typeof result === 'object', 'Should return an object');

    // AI SDK tools are keyed by name
    assert.ok('file_read' in result, 'Should have file_read tool');
    assert.ok('bash' in result, 'Should have bash tool');
  });

  it('returns empty object for no tools', () => {
    const result = convertToolsForSDK([]);
    assert.ok(typeof result === 'object');
    assert.equal(Object.keys(result).length, 0);
  });

  it('preserves tool descriptions', () => {
    const tools = [makeMockTool('test_tool', 'A test tool description')];
    const result = convertToolsForSDK(tools);

    assert.ok('test_tool' in result);
    // The tool should have the description preserved
    const tool = result['test_tool'];
    assert.ok(tool, 'Tool should exist');
    if (tool.description) {
      assert.equal(tool.description, 'A test tool description');
    }
  });

  it('preserves parameter schemas', () => {
    const tools = [makeMockTool('parameterized', 'Tool with params')];
    const result = convertToolsForSDK(tools);

    assert.ok('parameterized' in result);
    const tool = result['parameterized'];
    assert.ok(tool, 'Tool should exist');
    // The AI SDK wraps schemas via jsonSchema(), storing them as inputSchema
    assert.ok(tool.inputSchema || tool.parameters, 'Tool should have inputSchema or parameters');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletion - basic streaming via mock OpenAI-compatible server
// ---------------------------------------------------------------------------
describe('streamChatCompletion - streaming via AI SDK with mock server', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(chunk as Buffer);
      }

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });

      const sseLines = [
        'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
        'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"},"index":0}]}',
        'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"content":" world"},"index":0}]}',
        'data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}',
        'data: [DONE]',
      ];

      for (const line of sseLines) {
        res.write(line + '\n\n');
      }
      res.end();
    });

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve());
    });
    port = (server.address() as { port: number }).port;
  });

  after(() => {
    server.close();
  });

  it('streams text content deltas from mock server', async () => {
    const config = makeConfig({
      baseUrl: `http://127.0.0.1:${port}/v1`,
      model: 'test-model',
    });
    const messages: Message[] = [{ role: 'user', content: 'Hi' }];
    const ac = new AbortController();

    const chunks = [];
    for await (const chunk of streamChatCompletion(config, messages, [], ac.signal)) {
      chunks.push(chunk);
    }

    // Should have content deltas
    const contentChunks = chunks.filter(c => c.type === 'content_delta');
    assert.ok(contentChunks.length >= 1, `Expected at least 1 content_delta, got ${contentChunks.length}`);

    // Combined text should contain "Hello" and "world"
    const fullText = contentChunks.map(c => c.text ?? '').join('');
    assert.ok(fullText.includes('Hello'), `Expected text to include 'Hello', got '${fullText}'`);
    assert.ok(fullText.includes('world'), `Expected text to include 'world', got '${fullText}'`);

    // Should have a done event
    const doneChunks = chunks.filter(c => c.type === 'done');
    assert.ok(doneChunks.length >= 1, 'Should have at least one done event');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletion - tool calls via mock server
// ---------------------------------------------------------------------------
describe('streamChatCompletion - tool calls via AI SDK', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(chunk as Buffer);
      }

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });

      const sseLines = [
        'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"file_read","arguments":""}}]},"index":0}]}',
        'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\"file_path\\":"}}]},"index":0}]}',
        'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\\"/tmp/test.txt\\"}"}}]},"index":0}]}',
        'data: {"id":"chatcmpl-2","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"tool_calls","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}',
        'data: [DONE]',
      ];

      for (const line of sseLines) {
        res.write(line + '\n\n');
      }
      res.end();
    });

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve());
    });
    port = (server.address() as { port: number }).port;
  });

  after(() => {
    server.close();
  });

  it('accumulates tool call deltas from mock server', async () => {
    const config = makeConfig({
      baseUrl: `http://127.0.0.1:${port}/v1`,
      model: 'test-model',
    });
    const messages: Message[] = [{ role: 'user', content: 'Read file' }];
    const ac = new AbortController();

    const chunks = [];
    for await (const chunk of streamChatCompletion(config, messages, [], ac.signal)) {
      chunks.push(chunk);
    }

    const toolCallChunks = chunks.filter(c => c.type === 'tool_call_delta');
    assert.ok(toolCallChunks.length >= 1, `Expected at least 1 tool_call_delta, got ${toolCallChunks.length}`);

    // At least one chunk should have the tool name
    const hasName = toolCallChunks.some(c => c.toolCall?.function?.name === 'file_read');
    assert.ok(hasName, 'Should have a tool_call_delta with name file_read');

    // At least one chunk should have the tool call id
    const hasId = toolCallChunks.some(c => c.toolCall?.id === 'call_1');
    assert.ok(hasId, 'Should have a tool_call_delta with id call_1');
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletion - error handling
// ---------------------------------------------------------------------------
describe('streamChatCompletion - error handling via AI SDK', () => {
  it('handles non-200 responses', async () => {
    const server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end('{"error":{"message":"Internal server error","type":"server_error"}}');
    });

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve());
    });
    const port = (server.address() as { port: number }).port;

    try {
      const config = makeConfig({
        baseUrl: `http://127.0.0.1:${port}/v1`,
        model: 'test-model',
      });
      const messages: Message[] = [{ role: 'user', content: 'Hi' }];
      const ac = new AbortController();

      const chunks = [];
      for await (const chunk of streamChatCompletion(config, messages, [], ac.signal)) {
        chunks.push(chunk);
      }

      assert.equal(chunks.length, 1);
      assert.equal(chunks[0].type, 'error');
      assert.ok(chunks[0].error!.includes('500') || chunks[0].error!.includes('server'),
        `Error should mention 500 or server, got: ${chunks[0].error}`);
    } finally {
      server.close();
    }
  });

  it('handles 401 unauthorized', async () => {
    const server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end('{"error":{"message":"Unauthorized","type":"auth_error"}}');
    });

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve());
    });
    const port = (server.address() as { port: number }).port;

    try {
      const config = makeConfig({
        baseUrl: `http://127.0.0.1:${port}/v1`,
        model: 'test-model',
      });
      const messages: Message[] = [{ role: 'user', content: 'Hi' }];
      const ac = new AbortController();

      const chunks = [];
      for await (const chunk of streamChatCompletion(config, messages, [], ac.signal)) {
        chunks.push(chunk);
      }

      assert.equal(chunks[0].type, 'error');
      assert.ok(chunks[0].error!.includes('401') || chunks[0].error!.includes('auth') || chunks[0].error!.includes('Unauthorized'),
        `Error should mention 401 or auth, got: ${chunks[0].error}`);
    } finally {
      server.close();
    }
  });
});

// ---------------------------------------------------------------------------
// streamChatCompletion - abort signal
// ---------------------------------------------------------------------------
describe('streamChatCompletion - abort signal', () => {
  it('completes without streaming content when abort signal is already set', async () => {
    const server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: {"id":"chatcmpl-x","object":"chat.completion.chunk","choices":[{"delta":{"content":"x"},"index":0}]}\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });

    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve());
    });
    const port = (server.address() as { port: number }).port;

    try {
      const config = makeConfig({
        baseUrl: `http://127.0.0.1:${port}/v1`,
        model: 'test-model',
      });
      const messages: Message[] = [{ role: 'user', content: 'Hi' }];
      const ac = new AbortController();
      ac.abort();

      // With the AI SDK, a pre-aborted signal should either:
      // 1. Yield an error chunk with 'abort' in the message, or
      // 2. Complete immediately without streaming content, or
      // 3. Throw an abort error
      let gotAbortOrDone = false;
      let gotContent = false;
      try {
        for await (const chunk of streamChatCompletion(config, messages, [], ac.signal)) {
          if (chunk.type === 'error') {
            gotAbortOrDone = true;
          }
          if (chunk.type === 'done') {
            gotAbortOrDone = true;
          }
          if (chunk.type === 'content_delta') {
            gotContent = true;
          }
        }
      } catch (err) {
        gotAbortOrDone = true;
      }

      assert.ok(gotAbortOrDone, 'Should either get an error/done chunk or throw when signal is aborted');
      assert.ok(!gotContent, 'Should not receive content when signal is already aborted');
    } finally {
      server.close();
    }
  });
});
