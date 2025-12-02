import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import { generateTitle } from '../src/title-generator';
import type { ProviderConfig } from '../src/types';

// ---------------------------------------------------------------------------
// SSE mock server — returns OpenAI-compatible streaming responses
// ---------------------------------------------------------------------------

type SSEHandler = (body: Record<string, unknown>) => string[];

function createSSEServer(handler: SSEHandler): Promise<{ server: http.Server; port: number }> {
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(chunk as Buffer);
      }
      const body = JSON.parse(Buffer.concat(chunks).toString());
      const sseLines = handler(body);

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });

      for (const line of sseLines) {
        res.write(line + '\n\n');
      }
      res.end();
    });

    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, port: addr.port });
    });
  });
}

function textResponseSSE(text: string): string[] {
  const words = text.split(' ');
  const lines: string[] = [
    'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
  ];
  for (const word of words) {
    lines.push(`data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"content":"${word} "},"index":0}]}`);
  }
  lines.push(
    'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}',
    'data: [DONE]',
  );
  return lines;
}

function makeProvider(port: number, model = 'test-model'): ProviderConfig {
  return {
    baseUrl: `http://127.0.0.1:${port}/v1`,
    apiKey: 'test-key',
    model,
    maxTokens: 50,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
describe('generateTitle — basic streaming', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer(() => textResponseSSE('My Session Title'));
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('returns a title from streamed response', async () => {
    const title = await generateTitle('Hello how are you', makeProvider(port));
    // The SSE mock adds trailing space after each word, so result includes spaces
    assert.ok(title.includes('My'));
    assert.ok(title.includes('Session'));
    assert.ok(title.includes('Title'));
  });
});

describe('generateTitle — compaction model', () => {
  let server: http.Server;
  let port: number;
  let capturedModel: string | undefined;

  before(async () => {
    const mock = await createSSEServer((body) => {
      capturedModel = body.model as string;
      return textResponseSSE('Title');
    });
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('uses compactionModel when provided', async () => {
    await generateTitle('test', makeProvider(port, 'primary-model'), 'compact-model');
    assert.equal(capturedModel, 'compact-model');
  });
});

describe('generateTitle — fallback on error', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer(() => {
      // Return an error response that causes stream parsing to fail
      return ['data: {"error": "internal server error"}', 'data: [DONE]'];
    });
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('falls back to user message on error/empty response', async () => {
    const title = await generateTitle('My original message here', makeProvider(port));
    // Should fall back to user message (first 50 chars)
    assert.ok(title.length > 0);
    assert.ok(title.length <= 50);
  });
});

describe('generateTitle — network failure fallback', () => {
  it('falls back to user message on network error', async () => {
    // Use port 1 which should refuse connection
    const provider = makeProvider(1, 'gpt-4o');
    const title = await generateTitle('Fallback message here', provider);
    assert.equal(title, 'Fallback message here');
  });

  it('truncates long fallback message to 50 chars', async () => {
    const provider = makeProvider(1, 'gpt-4o');
    const longMsg = 'X'.repeat(200);
    const title = await generateTitle(longMsg, provider);
    assert.equal(title.length, 50);
    assert.equal(title, 'X'.repeat(50));
  });
});

describe('generateTitle — title cleanup logic', () => {
  // Test the cleanup regexes with a mock server returning specific content

  it('strips surrounding double quotes', async () => {
    const { server, port } = await createSSEServer(() =>
      textResponseSSE('"Quoted Title"'),
    );
    try {
      const title = await generateTitle('test', makeProvider(port));
      // The word-by-word SSE adds spaces; the title has no surrounding quotes
      assert.ok(!title.startsWith('"'));
      assert.ok(!title.endsWith('"'));
    } finally {
      server.close();
    }
  });

  it('strips thinking tags', async () => {
    // Send thinking tags as a single-word to avoid word splitting issues
    const { server, port } = await createSSEServer(() => {
      const lines: string[] = [
        'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
        `data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"content":"<think>reasoning</think>CleanTitle"},"index":0}]}`,
        'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}',
        'data: [DONE]',
      ];
      return lines;
    });
    try {
      const title = await generateTitle('test', makeProvider(port));
      assert.ok(!title.includes('<think>'));
      assert.ok(!title.includes('reasoning'));
      assert.ok(title.includes('CleanTitle'));
    } finally {
      server.close();
    }
  });

  it('truncates to 50 chars', async () => {
    const longText = 'A'.repeat(80);
    const { server, port } = await createSSEServer(() => {
      const lines: string[] = [
        'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
        `data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"content":"${longText}"},"index":0}]}`,
        'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}',
        'data: [DONE]',
      ];
      return lines;
    });
    try {
      const title = await generateTitle('test', makeProvider(port));
      assert.ok(title.length <= 50);
    } finally {
      server.close();
    }
  });
});
