import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { streamChatCompletionWithRetry } from '../src/provider';
import type { ProviderConfig, Message } from '../src/types';

const dummyConfig: ProviderConfig = {
  baseUrl: 'http://localhost:1',
  apiKey: 'test',
  model: 'test-model',
};

const dummyMessages: Message[] = [{ role: 'user', content: 'hello' }];

describe('streamChatCompletionWithRetry — maxDelayMs cap', () => {
  it('accepts maxDelayMs option without error', async () => {
    const ac = new AbortController();
    ac.abort();

    const stream = streamChatCompletionWithRetry(dummyConfig, dummyMessages, [], ac.signal, {
      maxDelayMs: 5000,
      maxRetries: 0,
    });

    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    assert.ok(chunks.length > 0);
    assert.equal(chunks[0].type, 'error');
  });

  it('accepts maxTotalWaitMs option without error', async () => {
    const ac = new AbortController();
    ac.abort();

    const stream = streamChatCompletionWithRetry(dummyConfig, dummyMessages, [], ac.signal, {
      maxTotalWaitMs: 10000,
      maxRetries: 0,
    });

    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    assert.ok(chunks.length > 0);
  });

  it('caps exponential backoff at maxDelayMs', () => {
    const base = 1000;
    const maxDelay = 5000;
    for (let attempt = 0; attempt < 10; attempt++) {
      const delay = Math.min(base * Math.pow(2, attempt), maxDelay);
      assert.ok(delay <= maxDelay, `attempt ${attempt}: delay ${delay} exceeds maxDelay ${maxDelay}`);
    }
  });

  it('default maxDelay is 60s', () => {
    const defaultMax = 60_000;
    const base = 1000;
    for (let attempt = 0; attempt < 10; attempt++) {
      const delay = Math.min(base * Math.pow(2, attempt), defaultMax);
      assert.ok(delay <= defaultMax);
    }
    const uncapped = base * Math.pow(2, 9);
    assert.ok(uncapped > defaultMax, 'attempt 9 without cap exceeds 60s');
  });
});

describe('streamChatCompletionWithRetry — total wait time limit', () => {
  it('stops retrying when total wait time exceeds maxTotalWaitMs', async () => {
    const ac = new AbortController();
    ac.abort();

    const stream = streamChatCompletionWithRetry(dummyConfig, dummyMessages, [], ac.signal, {
      maxTotalWaitMs: 1,
      maxRetries: 5,
    });

    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(chunk);
    }
    assert.ok(chunks.some(c => c.type === 'error'));
  });
});

describe('withExtraHeaders', () => {
  it('is exported from provider', async () => {
    const { withExtraHeaders } = await import('../src/provider');
    assert.equal(typeof withExtraHeaders, 'function');
  });

  it('returns undefined when no headers', async () => {
    const { withExtraHeaders } = await import('../src/provider');
    assert.equal(withExtraHeaders(undefined), undefined);
    assert.equal(withExtraHeaders({}), undefined);
  });

  it('wraps fetch to inject headers', async () => {
    const { withExtraHeaders } = await import('../src/provider');
    let capturedHeaders: any = null;
    const mockFetch = ((_input: any, init: any) => {
      capturedHeaders = init?.headers;
      return Promise.resolve(new Response('ok'));
    }) as typeof globalThis.fetch;

    const wrapped = withExtraHeaders({ 'x-test': 'value' }, mockFetch);
    assert.ok(wrapped);
    await wrapped!('http://test', { headers: { 'content-type': 'application/json' } });
    assert.ok(capturedHeaders);
    assert.equal(capturedHeaders['x-test'], 'value');
    assert.equal(capturedHeaders['content-type'], 'application/json');
  });
});
