import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

after(() => { setTimeout(() => process.exit(0), 200); });

import { streamChatCompletionWithRetry } from '../src/provider';
import { CostTracker } from '../src/cost-tracker';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeConfig(port: number): ProviderConfig {
  return {
    baseUrl: `http://127.0.0.1:${port}/v1`,
    apiKey: 'test-key',
    model: 'test-model',
    maxTokens: 100,
    temperature: 0,
  };
}

const defaultMessages: Message[] = [{ role: 'user', content: 'Hi' }];

/**
 * Creates an HTTP server that fails `failCount` times with `statusCode`,
 * then succeeds with a simple SSE response. Optionally adds Retry-After header.
 *
 * The SSE format includes "id", "object", and "choices" fields to be
 * compatible with what @ai-sdk/openai expects from an OpenAI-compatible endpoint.
 */
function createRetryServer(
  statusCode: number,
  failCount: number,
  options?: { retryAfterSeconds?: number },
): Promise<{ server: http.Server; port: number; requestCount: () => number }> {
  let requests = 0;
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(chunk as Buffer);
      }
      requests++;

      if (requests <= failCount) {
        const headers: Record<string, string> = {
          'Content-Type': 'application/json',
        };
        if (options?.retryAfterSeconds !== undefined) {
          headers['Retry-After'] = String(options.retryAfterSeconds);
        }
        res.writeHead(statusCode, headers);
        res.end(JSON.stringify({
          error: {
            message: `Mock error ${statusCode}`,
            type: 'server_error',
          },
        }));
        return;
      }

      // Success response - OpenAI-compatible SSE format
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      res.write(
        'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant","content":"ok"},"index":0}]}\n\n',
      );
      res.write(
        'data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}\n\n',
      );
      res.write('data: [DONE]\n\n');
      res.end();
    });

    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, port: addr.port, requestCount: () => requests });
    });
  });
}

/**
 * Creates an HTTP server that always fails with the given statusCode.
 */
function createAlwaysFailServer(
  statusCode: number,
): Promise<{ server: http.Server; port: number; requestCount: () => number }> {
  let requests = 0;
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      for await (const _ of req) {
        /* drain */
      }
      requests++;
      res.writeHead(statusCode, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        error: {
          message: `Always fail ${statusCode}`,
          type: 'server_error',
        },
      }));
    });

    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, port: addr.port, requestCount: () => requests });
    });
  });
}

async function collectChunks(
  gen: AsyncGenerator<StreamChunk>,
): Promise<StreamChunk[]> {
  const result: StreamChunk[] = [];
  for await (const chunk of gen) {
    result.push(chunk);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Retry logic tests
// ---------------------------------------------------------------------------

describe('streamChatCompletionWithRetry - retry on 429', () => {
  let server: http.Server;
  let port: number;
  let requestCount: () => number;

  before(async () => {
    // Fail once with 429, then succeed
    const mock = await createRetryServer(429, 1);
    server = mock.server;
    port = mock.port;
    requestCount = mock.requestCount;
  });

  after(() => {
    server.close();
  });

  it('retries on 429 and eventually succeeds', async () => {
    const config = makeConfig(port);
    const ac = new AbortController();

    const chunks = await collectChunks(
      streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
    );

    // Should have made 2 requests (1 fail + 1 success)
    assert.equal(requestCount(), 2);

    // Should have content from the successful response
    const contentChunks = chunks.filter((c) => c.type === 'content_delta');
    assert.ok(contentChunks.length >= 1);
  });
});

describe('streamChatCompletionWithRetry - retry on 429 with Retry-After', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    // Fail once with 429 + Retry-After: 1 second
    const mock = await createRetryServer(429, 1, { retryAfterSeconds: 1 });
    server = mock.server;
    port = mock.port;
  });

  after(() => {
    server.close();
  });

  it('respects Retry-After header on 429', async () => {
    const config = makeConfig(port);
    const ac = new AbortController();

    const start = Date.now();
    const chunks = await collectChunks(
      streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
    );
    const elapsed = Date.now() - start;

    // Should have waited at least ~1000ms for Retry-After
    assert.ok(elapsed >= 900, `Expected >= 900ms delay, got ${elapsed}ms`);

    // Should succeed
    const contentChunks = chunks.filter((c) => c.type === 'content_delta');
    assert.ok(contentChunks.length >= 1);
  });
});

describe('streamChatCompletionWithRetry - retry on 500/502/503', () => {
  it('retries on 500 and succeeds', async () => {
    const mock = await createRetryServer(500, 1);
    try {
      const config = makeConfig(mock.port);
      const ac = new AbortController();
      const chunks = await collectChunks(
        streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
      );
      assert.equal(mock.requestCount(), 2);
      const content = chunks.filter((c) => c.type === 'content_delta');
      assert.ok(content.length >= 1);
    } finally {
      mock.server.close();
    }
  });

  it('retries on 502 and succeeds', async () => {
    const mock = await createRetryServer(502, 1);
    try {
      const config = makeConfig(mock.port);
      const ac = new AbortController();
      const chunks = await collectChunks(
        streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
      );
      assert.equal(mock.requestCount(), 2);
      const content = chunks.filter((c) => c.type === 'content_delta');
      assert.ok(content.length >= 1);
    } finally {
      mock.server.close();
    }
  });

  it('retries on 503 and succeeds', async () => {
    const mock = await createRetryServer(503, 1);
    try {
      const config = makeConfig(mock.port);
      const ac = new AbortController();
      const chunks = await collectChunks(
        streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
      );
      assert.equal(mock.requestCount(), 2);
      const content = chunks.filter((c) => c.type === 'content_delta');
      assert.ok(content.length >= 1);
    } finally {
      mock.server.close();
    }
  });
});

describe('streamChatCompletionWithRetry - no retry on 401', () => {
  let server: http.Server;
  let port: number;
  let requestCount: () => number;

  before(async () => {
    const mock = await createAlwaysFailServer(401);
    server = mock.server;
    port = mock.port;
    requestCount = mock.requestCount;
  });

  after(() => {
    server.close();
  });

  it('does not retry on 401 auth error', async () => {
    const config = makeConfig(port);
    const ac = new AbortController();

    const chunks = await collectChunks(
      streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
    );

    // Should only make 1 request (no retries)
    assert.equal(requestCount(), 1);

    // Should yield an error
    assert.equal(chunks[0].type, 'error');
    assert.ok(
      chunks[0].error!.includes('401') || chunks[0].error!.includes('auth') || chunks[0].error!.includes('Unauthorized'),
      `Error should mention 401 or auth, got: ${chunks[0].error}`,
    );
  });
});

describe('streamChatCompletionWithRetry - no retry on abort', () => {
  it('does not retry when abort signal is already set', async () => {
    const mock = await createAlwaysFailServer(500);
    try {
      const config = makeConfig(mock.port);
      const ac = new AbortController();
      ac.abort();

      const chunks = await collectChunks(
        streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
      );

      // Should yield an error without making requests (or at most 1 that gets cut short)
      assert.equal(chunks[0].type, 'error');
      assert.ok(
        chunks[0].error!.includes('abort') || chunks[0].error!.includes('Abort'),
        `Error should mention abort, got: ${chunks[0].error}`,
      );
    } finally {
      mock.server.close();
    }
  });
});

describe('streamChatCompletionWithRetry - max retries exceeded', () => {
  let server: http.Server;
  let port: number;
  let requestCount: () => number;

  before(async () => {
    // Always fail with 500
    const mock = await createAlwaysFailServer(500);
    server = mock.server;
    port = mock.port;
    requestCount = mock.requestCount;
  });

  after(() => {
    server.close();
  });

  it('gives up after max retries and yields error', async () => {
    const config = makeConfig(port);
    const ac = new AbortController();

    const chunks = await collectChunks(
      streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal),
    );

    // Should have made 6 requests (1 initial + 5 retries)
    assert.equal(requestCount(), 6);

    // Should yield an error
    assert.equal(chunks[0].type, 'error');
    assert.ok(
      chunks[0].error!.includes('500') || chunks[0].error!.includes('server') || chunks[0].error!.includes('retries'),
      `Error should mention 500 or retries, got: ${chunks[0].error}`,
    );
  });
});

// ---------------------------------------------------------------------------
// Cost tracker tests
// ---------------------------------------------------------------------------

describe('CostTracker - cost estimation for different models', () => {
  it('estimates cost for gpt-4o', () => {
    const tracker = new CostTracker('gpt-4o');
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 1_000_000 });
    const stats = tracker.getStats();
    // 1M * $2.50/M + 1M * $10/M = $12.50
    assert.ok(
      Math.abs(stats.totalCost - 12.5) < 0.001,
      `Expected ~$12.50, got $${stats.totalCost}`,
    );
  });

  it('estimates cost for gpt-4o-mini', () => {
    const tracker = new CostTracker('gpt-4o-mini');
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 1_000_000 });
    const stats = tracker.getStats();
    // 1M * $0.15/M + 1M * $0.60/M = $0.75
    assert.ok(
      Math.abs(stats.totalCost - 0.75) < 0.001,
      `Expected ~$0.75, got $${stats.totalCost}`,
    );
  });

  it('estimates cost for claude-sonnet-4', () => {
    const tracker = new CostTracker('claude-sonnet-4');
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 1_000_000 });
    const stats = tracker.getStats();
    // 1M * $3/M + 1M * $15/M = $18
    assert.ok(
      Math.abs(stats.totalCost - 18) < 0.001,
      `Expected ~$18, got $${stats.totalCost}`,
    );
  });

  it('estimates cost for gemini-2.0-flash', () => {
    const tracker = new CostTracker('gemini-2.0-flash');
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 1_000_000 });
    const stats = tracker.getStats();
    // 1M * $0.10/M + 1M * $0.40/M = $0.50
    assert.ok(
      Math.abs(stats.totalCost - 0.5) < 0.001,
      `Expected ~$0.50, got $${stats.totalCost}`,
    );
  });

  it('uses default pricing for unknown models', () => {
    const tracker = new CostTracker('some-unknown-model');
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 1_000_000 });
    const stats = tracker.getStats();
    // 1M * $1/M + 1M * $3/M = $4
    assert.ok(
      Math.abs(stats.totalCost - 4) < 0.001,
      `Expected ~$4, got $${stats.totalCost}`,
    );
  });

  it('matches model by substring', () => {
    const tracker = new CostTracker('gpt-4o-2024-08-06');
    tracker.trackUsage({ promptTokens: 1_000_000, completionTokens: 1_000_000 });
    const stats = tracker.getStats();
    // Should match 'gpt-4o' pricing: $2.50 + $10 = $12.50
    assert.ok(
      Math.abs(stats.totalCost - 12.5) < 0.001,
      `Expected ~$12.50, got $${stats.totalCost}`,
    );
  });
});

describe('CostTracker - usage tracking accumulation', () => {
  it('accumulates usage across multiple calls', () => {
    const tracker = new CostTracker('gpt-4o-mini');

    tracker.trackUsage({ promptTokens: 100, completionTokens: 50 });
    tracker.trackUsage({ promptTokens: 200, completionTokens: 100 });
    tracker.trackUsage({ promptTokens: 300, completionTokens: 150 });

    const stats = tracker.getStats();
    assert.equal(stats.promptTokens, 600);
    assert.equal(stats.completionTokens, 300);
    assert.equal(stats.totalTokens, 900);
    assert.equal(stats.requestCount, 3);
  });

  it('tracks tool calls', () => {
    const tracker = new CostTracker('test-model');

    tracker.trackToolCall();
    tracker.trackToolCall();
    tracker.trackToolCall();

    const stats = tracker.getStats();
    assert.equal(stats.toolCallCount, 3);
  });

  it('tracks turns', () => {
    const tracker = new CostTracker('test-model');

    tracker.trackTurn();
    tracker.trackTurn();

    const stats = tracker.getStats();
    assert.equal(stats.turnCount, 2);
  });

  it('returns a copy from getStats', () => {
    const tracker = new CostTracker('test-model');
    tracker.trackUsage({ promptTokens: 100, completionTokens: 50 });

    const stats1 = tracker.getStats();
    tracker.trackUsage({ promptTokens: 100, completionTokens: 50 });
    const stats2 = tracker.getStats();

    // stats1 should not have been mutated
    assert.equal(stats1.promptTokens, 100);
    assert.equal(stats2.promptTokens, 200);
  });
});

describe('CostTracker - reset', () => {
  it('resets all stats to zero', () => {
    const tracker = new CostTracker('gpt-4o');

    tracker.trackUsage({ promptTokens: 1000, completionTokens: 500 });
    tracker.trackToolCall();
    tracker.trackToolCall();
    tracker.trackTurn();

    // Verify non-zero before reset
    let stats = tracker.getStats();
    assert.ok(stats.promptTokens > 0);
    assert.ok(stats.totalCost > 0);
    assert.equal(stats.toolCallCount, 2);
    assert.equal(stats.turnCount, 1);

    tracker.reset();

    stats = tracker.getStats();
    assert.equal(stats.promptTokens, 0);
    assert.equal(stats.completionTokens, 0);
    assert.equal(stats.totalTokens, 0);
    assert.equal(stats.totalCost, 0);
    assert.equal(stats.requestCount, 0);
    assert.equal(stats.toolCallCount, 0);
    assert.equal(stats.turnCount, 0);
  });
});
