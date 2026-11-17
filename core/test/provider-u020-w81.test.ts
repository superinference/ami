import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'node:http';

import { streamChatCompletion } from '../src/provider';
import { getInMemoryErrors, __resetLoggerForTest } from '../src/logger';
import type { ProviderConfig, Message, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// U020 / w81 — provider API errors are persisted to the error log enriched with
// the request URL + the server's own message body.
//
// Before this unit, streamChatCompletion yielded an `error` chunk on an API
// failure but NEVER logged it, so the self-explanatory `[url=…,status=…,body=…]`
// context (built by logger.httpErrorContext from the AI SDK APICallError) was
// never captured for a provider failure — the primary HTTP error source on AMI.
// ---------------------------------------------------------------------------

/** Mock server that answers every request with a non-200 + JSON error body. */
function createErrorServer(statusCode: number, body: string): http.Server {
  return http.createServer(async (req, res) => {
    for await (const _ of req) { /* drain */ }
    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.end(body);
  });
}

async function startServer(server: http.Server): Promise<number> {
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  return (server.address() as { port: number }).port;
}

function makeConfig(port: number): ProviderConfig {
  return {
    baseUrl: `http://127.0.0.1:${port}/v1`,
    apiKey: 'test-key',
    model: 'gpt-4o',
    maxTokens: 100,
    temperature: 0,
  };
}

async function collect(config: ProviderConfig, messages: Message[]): Promise<StreamChunk[]> {
  const ac = new AbortController();
  const chunks: StreamChunk[] = [];
  for await (const chunk of streamChatCompletion(config, messages, [], ac.signal)) {
    chunks.push(chunk);
  }
  return chunks;
}

describe('provider API error logging enrichment (U020/w81)', () => {
  beforeEach(() => {
    __resetLoggerForTest();
  });

  it('logs a provider HTTP failure with the request url and parsed server body', async () => {
    const server = createErrorServer(
      500,
      '{"error":{"message":"Internal boom","type":"server_error"}}',
    );
    const port = await startServer(server);
    try {
      const chunks = await collect(makeConfig(port), [{ role: 'user', content: 'fail' }]);

      // The surfaced error chunk still carries the HTTP status prefix (unchanged).
      const errorChunk = chunks.find((c) => c.type === 'error');
      assert.ok(errorChunk, 'should yield an error chunk');

      // The persisted error log entry is enriched with url + extracted server body.
      const logged = getInMemoryErrors().map((e) => e.error);
      const providerEntry = logged.find((e) => e.includes('[provider]') && e.includes('url='));
      assert.ok(
        providerEntry,
        `expected an enriched [provider] error entry, got: ${JSON.stringify(logged)}`,
      );
      assert.match(providerEntry!, /url=http:\/\/127\.0\.0\.1:\d+\/v1/);
      assert.match(providerEntry!, /status=500/);
      // The nested server message is extracted from the JSON body, not the blob.
      assert.match(providerEntry!, /body=Internal boom/);
    } finally {
      server.close();
    }
  });

  it('enriches a 429 rate-limit failure the same way', async () => {
    const server = createErrorServer(
      429,
      '{"error":{"message":"slow down","type":"rate_limit_error"}}',
    );
    const port = await startServer(server);
    try {
      await collect(makeConfig(port), [{ role: 'user', content: 'throttle' }]);
      const providerEntry = getInMemoryErrors()
        .map((e) => e.error)
        .find((e) => e.includes('[provider]') && e.includes('url='));
      assert.ok(providerEntry, 'expected an enriched [provider] error entry for the 429');
      assert.match(providerEntry!, /status=429/);
      assert.match(providerEntry!, /body=slow down/);
    } finally {
      server.close();
    }
  });
});
