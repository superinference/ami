import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  isPlainRecord,
  deepMergeRecords,
  withExtraBody,
  resolveModel,
  streamChatCompletion,
} from '../src/provider';
import type { ProviderConfig, Message } from '../src/types';

// ---------------------------------------------------------------------------
// isPlainRecord
// ---------------------------------------------------------------------------
describe('isPlainRecord', () => {
  it('is true for a plain object', () => {
    assert.equal(isPlainRecord({ a: 1 }), true);
  });
  it('is false for null', () => {
    assert.equal(isPlainRecord(null), false);
  });
  it('is false for an array', () => {
    assert.equal(isPlainRecord([1, 2]), false);
  });
  it('is false for a scalar', () => {
    assert.equal(isPlainRecord(5), false);
    assert.equal(isPlainRecord('x'), false);
  });
});

// ---------------------------------------------------------------------------
// deepMergeRecords
// ---------------------------------------------------------------------------
describe('deepMergeRecords', () => {
  it('adds keys absent from the base', () => {
    assert.deepEqual(
      deepMergeRecords({ a: 1 }, { chat_template_kwargs: { enable_thinking: false } }),
      { a: 1, chat_template_kwargs: { enable_thinking: false } },
    );
  });

  it('overrides scalar values', () => {
    assert.deepEqual(deepMergeRecords({ temperature: 1 }, { temperature: 0.3 }), { temperature: 0.3 });
  });

  it('overrides arrays wholesale (does not concat)', () => {
    assert.deepEqual(deepMergeRecords({ stop: ['a'] }, { stop: ['b', 'c'] }), { stop: ['b', 'c'] });
  });

  it('merges nested plain objects recursively', () => {
    assert.deepEqual(
      deepMergeRecords(
        { chat_template_kwargs: { enable_thinking: true, keep: 1 } },
        { chat_template_kwargs: { enable_thinking: false } },
      ),
      { chat_template_kwargs: { enable_thinking: false, keep: 1 } },
    );
  });

  it('overlay object replaces a base scalar at the same key', () => {
    assert.deepEqual(deepMergeRecords({ a: 5 }, { a: { nested: true } }), { a: { nested: true } });
  });

  it('does not mutate the inputs', () => {
    const base = { a: { b: 1 } };
    deepMergeRecords(base, { a: { c: 2 } });
    assert.deepEqual(base, { a: { b: 1 } });
  });
});

// ---------------------------------------------------------------------------
// withExtraBody
// ---------------------------------------------------------------------------
describe('withExtraBody', () => {
  const okResponse = () => new Response('ok', { status: 200 });

  it('returns undefined when extraBody is undefined', () => {
    assert.equal(withExtraBody(undefined), undefined);
  });

  it('returns undefined when extraBody is an empty object', () => {
    assert.equal(withExtraBody({}), undefined);
  });

  it('merges extraBody into a JSON string body', async () => {
    let seen: string | undefined;
    const base = async (_input: any, init?: any) => { seen = init?.body; return okResponse(); };
    const f = withExtraBody({ chat_template_kwargs: { enable_thinking: false } }, base as typeof fetch)!;
    await f('http://x/v1/chat/completions', { method: 'POST', body: JSON.stringify({ model: 'q', temperature: 1 }) });
    assert.deepEqual(JSON.parse(seen!), {
      model: 'q',
      temperature: 1,
      chat_template_kwargs: { enable_thinking: false },
    });
  });

  it('passes through when there is no init', async () => {
    const args: any[] = [];
    const base = async (input: any, init?: any) => { args.push([input, init]); return okResponse(); };
    const f = withExtraBody({ a: 1 }, base as typeof fetch)!;
    await f('http://x/v1');
    assert.deepEqual(args[0], ['http://x/v1', undefined]);
  });

  it('passes through when the body is not a string', async () => {
    let seen: any;
    const base = async (_input: any, init?: any) => { seen = init?.body; return okResponse(); };
    const f = withExtraBody({ a: 1 }, base as typeof fetch)!;
    const buf = Buffer.from('binary');
    await f('http://x/v1', { method: 'POST', body: buf as any });
    assert.equal(seen, buf);
  });

  it('passes through a JSON body that is not a plain object', async () => {
    let seen: string | undefined;
    const base = async (_input: any, init?: any) => { seen = init?.body; return okResponse(); };
    const f = withExtraBody({ a: 1 }, base as typeof fetch)!;
    await f('http://x/v1', { method: 'POST', body: '[1,2,3]' });
    assert.equal(seen, '[1,2,3]');
  });

  it('passes through an invalid-JSON body untouched', async () => {
    let seen: string | undefined;
    const base = async (_input: any, init?: any) => { seen = init?.body; return okResponse(); };
    const f = withExtraBody({ a: 1 }, base as typeof fetch)!;
    await f('http://x/v1', { method: 'POST', body: '{not json' });
    assert.equal(seen, '{not json');
  });

  it('defaults to globalThis.fetch when no base fetch is given', async () => {
    const original = globalThis.fetch;
    let seen: string | undefined;
    globalThis.fetch = (async (_input: any, init?: any) => { seen = init?.body; return okResponse(); }) as typeof fetch;
    try {
      const f = withExtraBody({ a: 1 })!;
      await f('http://x/v1', { method: 'POST', body: '{"b":2}' });
      assert.deepEqual(JSON.parse(seen!), { b: 2, a: 1 });
    } finally {
      globalThis.fetch = original;
    }
  });
});

// ---------------------------------------------------------------------------
// resolveModel — extraBody wiring for the OpenAI-compatible branches
// ---------------------------------------------------------------------------
describe('resolveModel extraBody branches', () => {
  const extraBody = { chat_template_kwargs: { enable_thinking: false } };

  it('builds an azure-openai model with extraBody', () => {
    const model = resolveModel({
      provider: 'azure-openai', baseUrl: 'https://foo.openai.azure.com', apiKey: 'k', model: 'gpt-4o', extraBody,
    });
    assert.ok(model);
  });

  it('builds an amazon-bedrock model with extraBody', () => {
    const model = resolveModel({
      provider: 'amazon-bedrock', baseUrl: 'https://gw.example.com/v1', apiKey: 'k', model: 'gpt-4o', extraBody,
    });
    assert.ok(model);
  });

  it('builds a generic OpenAI-compatible model with extraBody', () => {
    // Explicit provider avoids ambient-env provider inference rerouting the branch.
    const model = resolveModel({
      provider: 'openai', baseUrl: 'http://vllm:8000/v1', apiKey: 'k', model: 'Qwen/Qwen3.8-27B', extraBody,
    });
    assert.ok(model);
  });
});

// ---------------------------------------------------------------------------
// Integration — extraBody reaches the wire through streamChatCompletion
// ---------------------------------------------------------------------------
describe('streamChatCompletion — extraBody on the wire', () => {
  let server: http.Server;
  let port: number;
  let lastBody: Record<string, unknown> | null = null;

  before(async () => {
    server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      try { lastBody = JSON.parse(Buffer.concat(chunks).toString('utf-8')); } catch { lastBody = null; }

      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
      const sseLines = [
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"},"index":0}]}',
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"hi"},"index":0}]}',
        'data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}',
        'data: [DONE]',
      ];
      for (const line of sseLines) res.write(line + '\n\n');
      res.end();
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });

  after(() => { server.close(); });

  it('merges chat_template_kwargs into the outgoing request body', async () => {
    const config: ProviderConfig = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'Qwen/Qwen3.8-27B',
      extraBody: { chat_template_kwargs: { enable_thinking: false, thinking_budget: 1024 } },
    };
    const messages: Message[] = [{ role: 'user', content: 'Hi' }];
    const ac = new AbortController();

    const chunks = [];
    for await (const chunk of streamChatCompletion(config, messages, [], ac.signal)) {
      chunks.push(chunk);
    }

    assert.ok(chunks.some(c => c.type === 'done'), 'stream should complete');
    assert.ok(lastBody, 'server should have received a JSON body');
    assert.deepEqual(lastBody!['chat_template_kwargs'], { enable_thinking: false, thinking_budget: 1024 });
    // The SDK-provided fields must still be present alongside the passthrough.
    assert.equal(lastBody!['model'], 'Qwen/Qwen3.8-27B');
  });

  it('sends no extra fields when extraBody is absent', async () => {
    const config: ProviderConfig = {
      baseUrl: `http://127.0.0.1:${port}/v1`,
      apiKey: 'test-key',
      model: 'Qwen/Qwen3.8-27B',
    };
    const messages: Message[] = [{ role: 'user', content: 'Hi' }];
    const ac = new AbortController();
    for await (const _chunk of streamChatCompletion(config, messages, [], ac.signal)) { /* drain */ }
    assert.ok(lastBody, 'server should have received a JSON body');
    assert.equal(lastBody!['chat_template_kwargs'], undefined);
  });
});
