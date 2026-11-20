import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import { generateTitle, parseTitleJson } from '../src/title-generator';
import type { ProviderConfig } from '../src/types';
import type { streamChatCompletion } from '../src/provider';

// ---------------------------------------------------------------------------
// U198 — title-generator parity with the reference's session-title path:
//   (a) parseTitleJson: structured JSON-schema title output instead of raw
//       free-form text + regex scrubbing (utils/sessionTitle.ts:90-120).
//   (b) few-shot examples + explicit format constraints in the naming prompt
//       (utils/sessionTitle.ts:56-68, commands/rename/generateSessionName.ts).
//   (c) deterministic side-query params: temperature override 0
//       (utils/hooks/apiQueryHookHelper.ts:88-108).
//   (d) external cancellation/abort propagation into title generation
//       (commands/rename/generateSessionName.ts:L10-13, rename.ts:L37-40).
// ---------------------------------------------------------------------------

const provider: ProviderConfig = {
  baseUrl: 'http://127.0.0.1:1/v1',
  apiKey: 'k',
  model: 'm',
  maxTokens: 50,
};

/** Build a test-seam streamer that emits the given text as one content_delta. */
function seam(text: string): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* () {
    yield { type: 'content_delta', text } as any;
    yield { type: 'done' } as any;
  }) as unknown as typeof streamChatCompletion;
}

/**
 * Seam that throws when the signal it is handed is already aborted — mirrors the
 * real streamer's abort behaviour so we can prove the EXTERNAL signal is linked
 * into the stream's internal AbortController.
 */
function abortAwareSeam(text: string): typeof streamChatCompletion {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return (async function* (_p: any, _m: any, _t: any, signal: AbortSignal) {
    if (signal?.aborted) throw new Error('aborted');
    yield { type: 'content_delta', text } as any;
    yield { type: 'done' } as any;
  }) as unknown as typeof streamChatCompletion;
}

// ---------------------------------------------------------------------------
// (a) parseTitleJson — structured JSON output
// ---------------------------------------------------------------------------
describe('parseTitleJson', () => {
  it('extracts the "title" field from a clean JSON object', () => {
    assert.equal(parseTitleJson('{"title": "Fix login button"}'), 'Fix login button');
  });
  it('extracts the "name" field as a fallback (kebab-case /rename path)', () => {
    assert.equal(parseTitleJson('{"name": "fix-login-bug"}'), 'fix-login-bug');
  });
  it('prefers "title" over "name" when both are present', () => {
    assert.equal(parseTitleJson('{"name": "a", "title": "b"}'), 'b');
  });
  it('extracts JSON embedded in surrounding prose / code fences', () => {
    assert.equal(
      parseTitleJson('Sure! Here it is:\n```json\n{"title": "Add OAuth auth"}\n```'),
      'Add OAuth auth',
    );
  });
  it('returns undefined for a bare JSON string (not an object)', () => {
    assert.equal(parseTitleJson('"just a string"'), undefined);
  });
  it('returns undefined for a JSON array', () => {
    assert.equal(parseTitleJson('["a","b"]'), undefined);
  });
  it('returns undefined for non-JSON text', () => {
    assert.equal(parseTitleJson('My Plain Title'), undefined);
  });
  it('returns undefined for empty / whitespace input', () => {
    assert.equal(parseTitleJson('   '), undefined);
  });
  it('returns undefined when the title field is empty / not a string', () => {
    assert.equal(parseTitleJson('{"title": "   "}'), undefined);
    assert.equal(parseTitleJson('{"title": 42}'), undefined);
  });
});

// ---------------------------------------------------------------------------
// (a) generateTitle parses structured JSON output from the model
// ---------------------------------------------------------------------------
describe('generateTitle — structured JSON output', () => {
  it('parses a {"title": ...} JSON object from the model', async () => {
    const title = await generateTitle('x', provider, undefined, seam('{"title": "Fix login button on mobile"}'));
    assert.equal(title, 'Fix login button on mobile');
  });
  it('parses a {"name": ...} kebab JSON object from the model', async () => {
    const title = await generateTitle('x', provider, undefined, seam('{"name": "debug-ci-tests"}'));
    assert.equal(title, 'debug-ci-tests');
  });
  it('still accepts plain (non-JSON) model output (regression)', async () => {
    const title = await generateTitle('x', provider, undefined, seam('My Plain Title'));
    assert.equal(title, 'My Plain Title');
  });
  it('sanitizes the JSON-extracted value (no markdown/control bytes leak)', async () => {
    const title = await generateTitle('x', provider, undefined, seam('{"title": "**Bold** title"}'));
    assert.equal(title, 'Bold title');
  });
});

// ---------------------------------------------------------------------------
// (d) external abort propagation
// ---------------------------------------------------------------------------
describe('generateTitle — external abort signal', () => {
  it('propagates an already-aborted external signal into the stream and falls back', async () => {
    const title = await generateTitle(
      'fallback message here',
      provider,
      undefined,
      abortAwareSeam('SHOULD NOT APPEAR'),
      AbortSignal.abort(),
    );
    assert.equal(title, 'fallback message here');
  });
  it('without an external signal, the stream runs normally', async () => {
    const title = await generateTitle(
      'ignored',
      provider,
      undefined,
      abortAwareSeam('{"title": "Ran fine"}'),
    );
    assert.equal(title, 'Ran fine');
  });
});

// ---------------------------------------------------------------------------
// (b)+(c) prompt wording (few-shot + format constraints + JSON) and
// deterministic temperature override, verified on the real request body.
// ---------------------------------------------------------------------------
describe('generateTitle — request shape (prompt + temperature)', () => {
  let server: http.Server;
  let port: number;
  let captured: Record<string, unknown> | undefined;

  before(async () => {
    server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const c of req) chunks.push(c as Buffer);
      captured = JSON.parse(Buffer.concat(chunks).toString());
      res.writeHead(200, { 'Content-Type': 'text/event-stream' });
      res.write('data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}\n\n');
      res.write('data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{"content":"{\\"title\\": \\"Ok\\"}"},"index":0}]}\n\n');
      res.write('data: {"id":"c","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}\n\n');
      res.write('data: [DONE]\n\n');
      res.end();
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    port = (server.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('sends temperature 0 and a few-shot / JSON-constrained prompt', async () => {
    const title = await generateTitle('Do a thing', {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'k',
      model: 'test-model',
      maxTokens: 50,
    });
    assert.equal(title, 'Ok');
    assert.ok(captured, 'request body captured');
    assert.equal((captured as { temperature?: number }).temperature, 0);
    const msgs = (captured as { messages: { content: string }[] }).messages;
    const prompt = msgs.map((m) => m.content).join('\n');
    // few-shot examples present
    assert.ok(/Fix login button/.test(prompt), 'has a positive few-shot example');
    // explicit JSON output instruction present
    assert.ok(/JSON/i.test(prompt) && /"title"/.test(prompt), 'instructs JSON {title} output');
    // explicit format constraint present
    assert.ok(/sentence case/i.test(prompt), 'has the sentence-case format constraint');
  });
});
