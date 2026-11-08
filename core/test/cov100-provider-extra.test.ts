import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  withVertexAnthropicCache,
  injectAnthropicCacheControl,
  buildAnthropicCacheControl,
  SYSTEM_PROMPT_DYNAMIC_BOUNDARY,
} from '../src/provider';

// ---------------------------------------------------------------------------
// withVertexAnthropicCache — the ONLY path that makes prompt caching work on
// anthropic-on-vertex (the SDK drops providerOptions.cacheControl, so this
// wrapper injects cache_control into the RAW Anthropic Messages body at the
// HTTP layer and, for a 1h TTL, attaches the extended-cache beta header).
// ---------------------------------------------------------------------------
describe('withVertexAnthropicCache', () => {
  // A minimal capturing baseFetch that records the (input, init) it is called with.
  function capturing() {
    const calls: Array<{ input: unknown; init: any }> = [];
    const base = (async (input: any, init?: any) => {
      calls.push({ input, init });
      return new Response('ok', { status: 200 });
    }) as typeof globalThis.fetch;
    return { base, calls };
  }

  it('injects cache_control into a JSON Anthropic body and forwards it', async () => {
    const { base, calls } = capturing();
    const wrapped = withVertexAnthropicCache(base);

    const requestBody = JSON.stringify({
      model: 'claude-sonnet-4-6',
      system: 'You are a helpful agent.',
      tools: [{ name: 'bash', input_schema: {} }, { name: 'grep', input_schema: {} }],
      messages: [{ role: 'user', content: 'hi' }],
    });

    await wrapped('https://vertex.example/v1/messages', { method: 'POST', body: requestBody });

    assert.equal(calls.length, 1);
    const sent = JSON.parse(calls[0].init.body as string);

    // The default (5m) ephemeral cache_control marker is attached to the LAST tool.
    const expectedCC = buildAnthropicCacheControl(undefined);
    assert.deepEqual(sent.tools[sent.tools.length - 1].cache_control, expectedCC);
    // The first tool is NOT marked (only the last one is).
    assert.equal(sent.tools[0].cache_control, undefined);
    // A plain-string system becomes a cached text block (no dynamic boundary present).
    assert.deepEqual(sent.system, [
      { type: 'text', text: 'You are a helpful agent.', cache_control: expectedCC },
    ]);
    // The injected body must equal what injectAnthropicCacheControl produces directly.
    assert.equal(calls[0].init.body, injectAnthropicCacheControl(requestBody, expectedCC as any));
  });

  it('adds the extended-cache beta header only for a 1h TTL', async () => {
    const { base, calls } = capturing();
    const wrapped = withVertexAnthropicCache(base, '1h');

    await wrapped('https://vertex.example/v1/messages', {
      method: 'POST',
      body: JSON.stringify({ system: 'S', messages: [] }),
    });

    const headers = calls[0].init.headers as Headers;
    assert.ok(headers instanceof Headers);
    assert.equal(headers.get('anthropic-beta'), 'extended-cache-ttl-2025-04-11');

    // The 1h TTL also propagates into the cache_control marker (ttl: '1h').
    const sent = JSON.parse(calls[0].init.body as string);
    assert.deepEqual(sent.system[0].cache_control, { type: 'ephemeral', ttl: '1h' });
  });

  it('does not add the beta header for the default (5m) TTL', async () => {
    const { base, calls } = capturing();
    const wrapped = withVertexAnthropicCache(base); // no ttl -> 5m

    await wrapped('https://vertex.example/v1/messages', {
      method: 'POST',
      body: JSON.stringify({ system: 'S', messages: [] }),
    });

    const headers = calls[0].init.headers as Headers;
    assert.equal(headers.get('anthropic-beta'), null);
  });

  it('preserves a caller-supplied anthropic-beta header (does not overwrite)', async () => {
    const { base, calls } = capturing();
    const wrapped = withVertexAnthropicCache(base, '1h');

    await wrapped('https://vertex.example/v1/messages', {
      method: 'POST',
      body: JSON.stringify({ system: 'S', messages: [] }),
      headers: { 'anthropic-beta': 'my-custom-beta' },
    });

    const headers = calls[0].init.headers as Headers;
    assert.equal(headers.get('anthropic-beta'), 'my-custom-beta');
  });

  it('passes through untouched when init has no string body', async () => {
    const { base, calls } = capturing();
    const wrapped = withVertexAnthropicCache(base, '1h');

    // No init at all.
    await wrapped('https://vertex.example/ping');
    assert.deepEqual(calls[0], { input: 'https://vertex.example/ping', init: undefined });

    // init present but a non-string (binary) body.
    const buf = Buffer.from('binary');
    await wrapped('https://vertex.example/upload', { method: 'POST', body: buf as any });
    assert.equal(calls[1].init.body, buf);
    // No beta header injected on the pass-through path.
    assert.equal(calls[1].init.headers, undefined);
  });

  it('splits system at the dynamic boundary: static prefix cached, dynamic tail uncached', async () => {
    const { base, calls } = capturing();
    const wrapped = withVertexAnthropicCache(base);

    const system = `STATIC-RULES${SYSTEM_PROMPT_DYNAMIC_BOUNDARY}DYNAMIC-MEMORY`;
    await wrapped('https://vertex.example/v1/messages', {
      method: 'POST',
      body: JSON.stringify({ system, messages: [] }),
    });

    const sent = JSON.parse(calls[0].init.body as string);
    const expectedCC = buildAnthropicCacheControl(undefined);
    assert.deepEqual(sent.system, [
      { type: 'text', text: 'STATIC-RULES', cache_control: expectedCC },
      { type: 'text', text: 'DYNAMIC-MEMORY' },
    ]);
  });
});
