import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import { Critic } from '../src/superinference/critic';

// ---------------------------------------------------------------------------
// SSE mock server for Critic.evaluate()
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

      // Split text into word-level chunks for realistic SSE streaming
      const words = responseText.split(' ');
      res.write('data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}\n\n');
      for (const word of words) {
        const escaped = word.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
        res.write(`data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{"content":"${escaped} "},"index":0}]}\n\n`);
      }
      res.write('data: {"id":"chatcmpl-test","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });

    server.listen(0, '127.0.0.1', () => {
      const addr = server.address() as { port: number };
      resolve({ server, port: addr.port });
    });
  });
}

// ---------------------------------------------------------------------------
// Critic.evaluate() — success path (lines 41-53)
// ---------------------------------------------------------------------------
describe('Critic.evaluate() — success path', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('{"approved": true, "score": 0.95, "reason": "correct answer"}');
    server = mock.server;
    port = mock.port;
  });

  after(() => {
    server.close();
  });

  it('returns parsed decision when LLM responds with valid JSON', async () => {
    const critic = new Critic(0.05, 0.10);
    const provider = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'test-model',
      maxTokens: 200,
      temperature: 0,
    };
    const ac = new AbortController();

    const result = await critic.evaluate('What is 2+2?', 'The answer is 4.', provider, ac.signal);
    assert.equal(result.approved, true);
    assert.ok(result.score >= 0 && result.score <= 1);
    assert.equal(typeof result.reason, 'string');
  });
});

// ---------------------------------------------------------------------------
// Critic.evaluate() — rejected decision
// ---------------------------------------------------------------------------
describe('Critic.evaluate() — rejected decision', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('{"approved": false, "score": 0.2, "reason": "wrong answer"}');
    server = mock.server;
    port = mock.port;
  });

  after(() => {
    server.close();
  });

  it('returns rejected decision with false approval', async () => {
    const critic = new Critic(0.05, 0.10);
    const provider = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'test-model',
    };
    const ac = new AbortController();

    const result = await critic.evaluate('What is 2+2?', 'The answer is 5.', provider, ac.signal);
    assert.equal(result.approved, false);
    assert.ok(result.score <= 0.5);
  });
});

// ---------------------------------------------------------------------------
// Critic.evaluate() — no valid JSON in response (lines 46-53 null branch)
// ---------------------------------------------------------------------------
describe('Critic.evaluate() — unparseable response', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('I cannot evaluate this properly, sorry.');
    server = mock.server;
    port = mock.port;
  });

  after(() => {
    server.close();
  });

  it('returns fail-open default when response has no JSON', async () => {
    const critic = new Critic(0.05, 0.10);
    const provider = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'test-model',
    };
    const ac = new AbortController();

    const result = await critic.evaluate('query', 'result', provider, ac.signal);
    // Fail-open: approved=true, score=0.7
    assert.equal(result.approved, true);
    assert.equal(result.score, 0.7);
    assert.ok(result.reason.includes('failed'));
  });
});

// ---------------------------------------------------------------------------
// Critic.evaluate() — stream error / catch block (lines 54-56)
// ---------------------------------------------------------------------------
describe('Critic.evaluate() — stream error', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const s = http.createServer(async (req, res) => {
      for await (const _ of req) { /* drain */ }
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end('{"error":{"message":"Internal server error","type":"server_error"}}');
    });

    await new Promise<void>((resolve) => {
      s.listen(0, '127.0.0.1', () => resolve());
    });
    server = s;
    port = (s.address() as { port: number }).port;
  });

  after(() => {
    server.close();
  });

  it('returns fail-open default when stream throws', async () => {
    const critic = new Critic(0.05, 0.10);
    const provider = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'test-model',
    };
    const ac = new AbortController();

    const result = await critic.evaluate('query', 'result', provider, ac.signal);
    assert.equal(result.approved, true);
    assert.equal(result.score, 0.7);
    assert.ok(result.reason.includes('failed'));
  });
});

// ---------------------------------------------------------------------------
// Critic.evaluate() — score clamping (line 50)
// ---------------------------------------------------------------------------
describe('Critic.evaluate() — score clamping', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    // Score above 1 and negative score
    const mock = await createSSEServer('{"approved": true, "score": 5.0, "reason": "test"}');
    server = mock.server;
    port = mock.port;
  });

  after(() => {
    server.close();
  });

  it('clamps score to [0, 1] range', async () => {
    const critic = new Critic();
    const provider = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'test-model',
    };
    const ac = new AbortController();

    const result = await critic.evaluate('q', 'r', provider, ac.signal);
    assert.ok(result.score >= 0 && result.score <= 1, `Score should be clamped, got ${result.score}`);
  });
});

// ---------------------------------------------------------------------------
// Critic.evaluate() — missing fields in JSON (line 50-51)
// ---------------------------------------------------------------------------
describe('Critic.evaluate() — missing fields in JSON', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('{"approved": true}');
    server = mock.server;
    port = mock.port;
  });

  after(() => {
    server.close();
  });

  it('handles missing score and reason fields gracefully', async () => {
    const critic = new Critic();
    const provider = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'test-model',
    };
    const ac = new AbortController();

    const result = await critic.evaluate('q', 'r', provider, ac.signal);
    assert.equal(result.approved, true);
    assert.equal(result.score, 0); // Number(undefined) || 0
    assert.equal(result.reason, '');
  });
});

// ---------------------------------------------------------------------------
// Critic.evaluate() — long result truncation (line 24)
// ---------------------------------------------------------------------------
describe('Critic.evaluate() — result truncation', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    const mock = await createSSEServer('{"approved": true, "score": 0.8, "reason": "ok"}');
    server = mock.server;
    port = mock.port;
  });

  after(() => {
    server.close();
  });

  it('truncates result to 2000 chars in the prompt', async () => {
    const critic = new Critic();
    const provider = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'test-model',
    };
    const ac = new AbortController();

    const longResult = 'x'.repeat(5000);
    const result = await critic.evaluate('q', longResult, provider, ac.signal);
    // Should succeed — the function truncates internally
    assert.equal(result.approved, true);
  });
});

// ---------------------------------------------------------------------------
// Critic.extractJSON — invalid JSON that has balanced braces (line 80-81)
// ---------------------------------------------------------------------------
describe('Critic.extractJSON — invalid JSON content', () => {
  it('returns null for balanced braces with invalid JSON content', () => {
    const critic = new Critic();
    const result = critic.extractJSON('{not valid json content}');
    assert.equal(result, null);
  });

  it('handles deeply nested braces', () => {
    const critic = new Critic();
    const input = '{"outer": {"inner": {"deep": true}}}';
    const result = critic.extractJSON(input);
    assert.deepEqual(result, { outer: { inner: { deep: true } } });
  });

  it('handles JSON with trailing text', () => {
    const critic = new Critic();
    const input = '{"approved": true, "score": 0.9} extra text here';
    const result = critic.extractJSON(input);
    assert.ok(result !== null);
    assert.equal(result!.approved, true);
    assert.equal(result!.score, 0.9);
  });

  it('returns null when end <= start (unmatched braces)', () => {
    const critic = new Critic();
    // Only opening braces, no closing
    const result = critic.extractJSON('{{{{{');
    assert.equal(result, null);
  });
});

// ---------------------------------------------------------------------------
// Critic.ppv — edge case: denominator=0 (line 91)
// ---------------------------------------------------------------------------
describe('Critic.ppv — edge cases', () => {
  it('returns 0 when denominator is 0 (alpha=0, beta=1, p=0)', () => {
    const critic = new Critic(0, 1);
    // denominator = (1-1)*0 + 0*(1-0) = 0*0 + 0*1 = 0
    const ppv = critic.ppv(0);
    assert.equal(ppv, 0);
  });

  it('handles alpha=0 and beta=0 with p=0.5', () => {
    const critic = new Critic(0, 0);
    // PPV = (1-0)*0.5 / ((1-0)*0.5 + 0*(1-0.5)) = 0.5/0.5 = 1.0
    const ppv = critic.ppv(0.5);
    assert.equal(ppv, 1.0);
  });

  it('handles high alpha, low beta', () => {
    const critic = new Critic(0.5, 0.01);
    const ppv = critic.ppv(0.5);
    // PPV = 0.99*0.5 / (0.99*0.5 + 0.5*0.5) = 0.495 / (0.495 + 0.25) = 0.495/0.745
    assert.ok(ppv < 0.7, `Expected PPV < 0.7 with high alpha, got ${ppv}`);
  });
});
