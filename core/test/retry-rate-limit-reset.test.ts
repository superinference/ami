import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

// Streaming leaves keep-alive sockets open; force a clean exit after the file.
after(() => { setTimeout(() => process.exit(0), 200); });

import { streamChatCompletionWithRetry } from '../src/provider';
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
 * Server that fails the first request with a 429 carrying the given rate-limit
 * headers (and NO Retry-After), then succeeds with a minimal SSE stream.
 */
function createRateLimitServer(
  failHeaders: Record<string, string>,
): Promise<{ server: http.Server; port: number; requestCount: () => number }> {
  let requests = 0;
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain request body */ }
      requests++;

      if (requests <= 1) {
        res.writeHead(429, { 'Content-Type': 'application/json', ...failHeaders });
        res.end(JSON.stringify({
          error: { message: 'Mock 429 rate limit', type: 'rate_limit_error' },
        }));
        return;
      }

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

async function collectChunks(gen: AsyncGenerator<StreamChunk>): Promise<StreamChunk[]> {
  const result: StreamChunk[] = [];
  for await (const chunk of gen) result.push(chunk);
  return result;
}

// ---------------------------------------------------------------------------
// Tests: 429 backoff honors the server rate-limit reset window
// ---------------------------------------------------------------------------

describe('streamChatCompletionWithRetry - 429 honors rate-limit reset window', () => {
  it('waits for x-ratelimit-reset-requests when no Retry-After header is present', async () => {
    // Out of request budget, resets in ~1s, but NO Retry-After header. The
    // exponential path (baseDelay=40ms) would wait ~40ms; honoring the reset
    // window must wait ~1000ms instead.
    const mock = await createRateLimitServer({
      'x-ratelimit-remaining-requests': '0',
      'x-ratelimit-reset-requests': '1s',
    });
    try {
      const config = makeConfig(mock.port);
      const ac = new AbortController();
      let capturedDelay = -1;
      const chunks = await collectChunks(
        streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal, {
          baseDelayMs: 40,
          maxDelayMs: 60_000,
          onRetry: (_a, _m, delayMs) => { capturedDelay = delayMs; },
        }),
      );

      // RED (pre-fix): 429 w/o Retry-After falls to exponential backoff -> ~40ms.
      // GREEN (post-fix): suggestedWaitMs honors the ~1s reset window.
      assert.ok(
        capturedDelay >= 900 && capturedDelay <= 2000,
        `expected backoff to honor ~1s reset window, got ${capturedDelay}ms`,
      );
      assert.equal(mock.requestCount(), 2);
      const content = chunks.filter((c) => c.type === 'content_delta');
      assert.ok(content.length >= 1);
    } finally {
      mock.server.close();
    }
  });

  it('falls back to exponential backoff when rate-limit headers show remaining budget', async () => {
    // 429 with headers but budget remaining and no reset/Retry-After:
    // shouldBackoff() is false, so the wait stays on exponential backoff.
    const mock = await createRateLimitServer({
      'x-ratelimit-remaining-requests': '50',
    });
    try {
      const config = makeConfig(mock.port);
      const ac = new AbortController();
      let capturedDelay = -1;
      await collectChunks(
        streamChatCompletionWithRetry(config, defaultMessages, [], ac.signal, {
          baseDelayMs: 40,
          maxDelayMs: 60_000,
          onRetry: (_a, _m, delayMs) => { capturedDelay = delayMs; },
        }),
      );

      // attempt 0 exponential: min(40 * 2^0, 60000) = 40ms; must NOT inflate to
      // the reset-window branch.
      assert.ok(
        capturedDelay < 900,
        `expected exponential backoff (~40ms) when budget remains, got ${capturedDelay}ms`,
      );
    } finally {
      mock.server.close();
    }
  });
});
