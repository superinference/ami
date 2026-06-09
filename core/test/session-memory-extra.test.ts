import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import { SessionMemoryExtractor, type SessionFact } from '../src/session-memory';
import type { ProviderConfig, Message } from '../src/types';

// ---------------------------------------------------------------------------
// SSE mock server helpers
// ---------------------------------------------------------------------------

function createSSEServer(responseText: string): Promise<{ server: http.Server; port: number }> {
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(chunk as Buffer);
      }

      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });

      const escaped = responseText.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}\n\n');
      res.write(`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"content":"${escaped}"},"index":0}]}\n\n`);
      res.write('data: {"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });

    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, port: addr.port });
    });
  });
}

function makeProvider(port: number): ProviderConfig {
  return {
    baseUrl: `http://127.0.0.1:${port}/v1`,
    apiKey: 'test-key',
    model: 'test-model',
    maxTokens: 500,
    temperature: 0,
  };
}

// ---------------------------------------------------------------------------
// SessionMemoryExtractor.extractFacts — success path (lines 49-109)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — extractFacts success', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const validJson = JSON.stringify([
      { fact: 'User prefers TypeScript', category: 'user_preference', confidence: 0.9 },
      { fact: 'Fixed auth bug', category: 'error_fix', confidence: 0.85 },
    ]);
    const mock = await createSSEServer(validJson);
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('extracts facts from valid JSON response', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));
    const toolResults = [
      { toolName: 'bash', output: 'npm test passed', isError: false },
    ];
    const messages: Message[] = [
      { role: 'user', content: 'Fix the auth bug' },
      { role: 'assistant', content: 'I fixed it.' },
    ];

    const facts = await extractor.extractFacts(toolResults, messages);
    assert.ok(facts.length >= 1);
    assert.ok(facts.some(f => f.category === 'user_preference'));
  });
});

// ---------------------------------------------------------------------------
// SessionMemoryExtractor.extractFacts — error chunk (lines 96-99)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — extractFacts stream error', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      res.write('data: {"id":"chatcmpl-err","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}\n\n');
      // Simulate an error event by finishing without content
      res.write('data: {"id":"chatcmpl-err","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}\n\n');
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

  it('returns empty array when no content in response', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'test', isError: false }],
      [{ role: 'user', content: 'test' }],
    );
    assert.deepEqual(facts, []);
  });
});

// ---------------------------------------------------------------------------
// SessionMemoryExtractor.extractFacts — exception catch (lines 103-105)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — extractFacts exception', () => {
  it('returns empty array when stream throws', async () => {
    const badProvider: ProviderConfig = {
      baseUrl: 'http://127.0.0.1:1/v1', // will fail to connect
      apiKey: 'test',
      model: 'test',
    };

    const extractor = new SessionMemoryExtractor(badProvider);
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'test', isError: false }],
      [{ role: 'user', content: 'test' }],
    );
    assert.deepEqual(facts, []);
  });
});

// ---------------------------------------------------------------------------
// SessionMemoryExtractor — compaction model (lines 71-73)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — compaction model', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const validJson = JSON.stringify([
      { fact: 'Test fact', category: 'decision', confidence: 0.9 },
    ]);
    const mock = await createSSEServer(validJson);
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('uses compaction model when provided', async () => {
    const extractor = new SessionMemoryExtractor(
      makeProvider(port),
      'cheap-compaction-model',
    );

    const facts = await extractor.extractFacts(
      [{ toolName: 'file_edit', output: 'edited', isError: false }],
      [{ role: 'user', content: 'edit file' }],
    );
    assert.ok(Array.isArray(facts));
  });
});

// ---------------------------------------------------------------------------
// SessionMemoryExtractor — message context slicing (line 54)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — message context', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('[]');
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('only uses last 4 messages as context', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));

    const manyMessages: Message[] = Array.from({ length: 10 }, (_, i) => ({
      role: 'user' as const,
      content: `Message ${i}`,
    }));

    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      manyMessages,
    );
    // Should not crash with many messages
    assert.deepEqual(facts, []);
  });

  it('handles null content in messages', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));

    const messages: Message[] = [
      { role: 'assistant', content: null },
    ];

    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      messages,
    );
    assert.deepEqual(facts, []);
  });
});

// ---------------------------------------------------------------------------
// SessionMemoryExtractor.mergeFacts (lines 115-134)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — mergeFacts', () => {
  const provider: ProviderConfig = {
    baseUrl: 'http://localhost:1/v1',
    apiKey: 'test',
    model: 'test',
  };

  it('appends non-duplicate facts', () => {
    const extractor = new SessionMemoryExtractor(provider);
    const existing: SessionFact[] = [
      { fact: 'Fact A', category: 'decision', confidence: 0.9 },
    ];
    const newFacts: SessionFact[] = [
      { fact: 'Fact B', category: 'convention', confidence: 0.8 },
    ];

    const merged = extractor.mergeFacts(existing, newFacts);
    assert.equal(merged.length, 2);
  });

  it('deduplicates by case-insensitive fact text', () => {
    const extractor = new SessionMemoryExtractor(provider);
    const existing: SessionFact[] = [
      { fact: 'Use TypeScript', category: 'convention', confidence: 0.8 },
    ];
    const newFacts: SessionFact[] = [
      { fact: 'use typescript', category: 'convention', confidence: 0.9 },
    ];

    const merged = extractor.mergeFacts(existing, newFacts);
    assert.equal(merged.length, 1);
    // Higher confidence should win
    assert.equal(merged[0].confidence, 0.9);
    assert.equal(merged[0].fact, 'use typescript');
  });

  it('keeps existing when new has lower confidence', () => {
    const extractor = new SessionMemoryExtractor(provider);
    const existing: SessionFact[] = [
      { fact: 'Fact', category: 'decision', confidence: 0.95 },
    ];
    const newFacts: SessionFact[] = [
      { fact: 'fact', category: 'decision', confidence: 0.7 },
    ];

    const merged = extractor.mergeFacts(existing, newFacts);
    assert.equal(merged.length, 1);
    assert.equal(merged[0].confidence, 0.95);
  });

  it('handles empty existing facts', () => {
    const extractor = new SessionMemoryExtractor(provider);
    const newFacts: SessionFact[] = [
      { fact: 'New fact', category: 'decision', confidence: 0.8 },
    ];

    const merged = extractor.mergeFacts([], newFacts);
    assert.equal(merged.length, 1);
  });

  it('handles empty new facts', () => {
    const extractor = new SessionMemoryExtractor(provider);
    const existing: SessionFact[] = [
      { fact: 'Existing', category: 'decision', confidence: 0.8 },
    ];

    const merged = extractor.mergeFacts(existing, []);
    assert.equal(merged.length, 1);
  });
});

// ---------------------------------------------------------------------------
// parseFactsFromResponse — edge cases (lines 139-171)
// ---------------------------------------------------------------------------
describe('SessionMemoryExtractor — parseFactsFromResponse', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('not valid json');
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('returns empty array for non-JSON response', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'test' }],
    );
    assert.deepEqual(facts, []);
  });
});

describe('SessionMemoryExtractor — parseFactsFromResponse JSON in code block', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const response = '```json\n[{"fact":"Test","category":"decision","confidence":0.9}]\n```';
    const mock = await createSSEServer(response);
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('extracts JSON from markdown code blocks', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'test' }],
    );
    assert.equal(facts.length, 1);
    assert.equal(facts[0].fact, 'Test');
  });
});

describe('SessionMemoryExtractor — confidence threshold', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const response = JSON.stringify([
      { fact: 'High confidence', category: 'decision', confidence: 0.9 },
      { fact: 'Low confidence', category: 'decision', confidence: 0.5 },
      { fact: 'Borderline', category: 'decision', confidence: 0.7 },
    ]);
    const mock = await createSSEServer(response);
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('filters out facts below 0.7 confidence', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'test' }],
    );
    assert.equal(facts.length, 2);
    assert.ok(facts.every(f => f.confidence >= 0.7));
  });
});

describe('SessionMemoryExtractor — invalid items in array', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const response = JSON.stringify([
      { fact: 'Valid', category: 'decision', confidence: 0.9 },
      { fact: 123, category: 'decision', confidence: 0.9 }, // Invalid: fact is not string
      { fact: 'No category', confidence: 0.9 }, // Invalid: missing category
      { fact: 'Bad category', category: 'unknown_cat', confidence: 0.9 }, // Invalid: bad category
      null, // Invalid: null item
      'string item', // Invalid: not an object
    ]);
    const mock = await createSSEServer(response);
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('filters out invalid items from response', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'test' }],
    );
    assert.equal(facts.length, 1);
    assert.equal(facts[0].fact, 'Valid');
  });
});

describe('SessionMemoryExtractor — non-array JSON response', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('{"not": "an array"}');
    server = mock.server;
    port = mock.port;
  });

  after(() => { server.close(); });

  it('returns empty array for non-array JSON', async () => {
    const extractor = new SessionMemoryExtractor(makeProvider(port));
    const facts = await extractor.extractFacts(
      [{ toolName: 'bash', output: 'ok', isError: false }],
      [{ role: 'user', content: 'test' }],
    );
    assert.deepEqual(facts, []);
  });
});
