import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  withVertexAnthropicCache,
  injectAnthropicCacheControl,
  buildAnthropicCacheControl,
  SYSTEM_PROMPT_DYNAMIC_BOUNDARY,
  resolveProxyForUrl,
  withProxyDispatcher,
  mergeBetaTokens,
  withAnthropicBetaHeader,
  ONE_MILLION_CONTEXT_BETA,
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

// ---------------------------------------------------------------------------
// R2-M32 — proxy resolution (HTTPS_PROXY / HTTP_PROXY / NO_PROXY)
// ---------------------------------------------------------------------------
describe('resolveProxyForUrl', () => {
  it('returns HTTPS_PROXY for an https target', () => {
    const env = { HTTPS_PROXY: 'http://proxy.corp:8080' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.anthropic.com/v1/messages'), 'http://proxy.corp:8080');
  });

  it('falls back to HTTP_PROXY for an https target when HTTPS_PROXY is unset', () => {
    const env = { HTTP_PROXY: 'http://proxy.corp:3128' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.openai.com/v1'), 'http://proxy.corp:3128');
  });

  it('uses HTTP_PROXY (not HTTPS_PROXY) for an http target', () => {
    const env = { HTTP_PROXY: 'http://p:1', HTTPS_PROXY: 'http://p:2' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'http://internal.host/v1'), 'http://p:1');
  });

  it('returns undefined when the host matches NO_PROXY (exact, suffix, and wildcard)', () => {
    const exact = { HTTPS_PROXY: 'http://p', NO_PROXY: 'api.anthropic.com' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(exact, 'https://api.anthropic.com/v1'), undefined);
    const suffix = { HTTPS_PROXY: 'http://p', NO_PROXY: '.anthropic.com' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(suffix, 'https://api.anthropic.com/v1'), undefined);
    const bareSuffix = { HTTPS_PROXY: 'http://p', NO_PROXY: 'anthropic.com' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(bareSuffix, 'https://api.anthropic.com/v1'), undefined);
    const wildcard = { HTTPS_PROXY: 'http://p', NO_PROXY: '*' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(wildcard, 'https://anything.example/v1'), undefined);
  });

  it('still proxies a host that only partially resembles a NO_PROXY entry', () => {
    const env = { HTTPS_PROXY: 'http://p', NO_PROXY: 'anthropic.com' } as NodeJS.ProcessEnv;
    // notanthropic.com must NOT be suffix-matched by anthropic.com.
    assert.equal(resolveProxyForUrl(env, 'https://notanthropic.com/v1'), 'http://p');
  });

  it('returns undefined when no proxy env is set', () => {
    assert.equal(resolveProxyForUrl({} as NodeJS.ProcessEnv, 'https://api.anthropic.com/v1'), undefined);
  });

  it('returns undefined for a non-http(s) scheme or an unparseable url', () => {
    const env = { HTTPS_PROXY: 'http://p' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'ftp://host/x'), undefined);
    assert.equal(resolveProxyForUrl(env, 'not a url'), undefined);
  });

  it('ignores empty/whitespace NO_PROXY entries', () => {
    const env = { HTTPS_PROXY: 'http://p', NO_PROXY: ' , ,' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.anthropic.com/v1'), 'http://p');
  });
});

describe('withProxyDispatcher', () => {
  function capturing() {
    const calls: Array<{ input: unknown; init: any }> = [];
    const base = (async (input: any, init?: any) => {
      calls.push({ input, init });
      return new Response('ok', { status: 200 });
    }) as typeof globalThis.fetch;
    return { base, calls };
  }

  it('returns the base fetch UNCHANGED (same reference) when no proxy env is set', () => {
    const { base } = capturing();
    const wrapped = withProxyDispatcher(base, {} as NodeJS.ProcessEnv);
    assert.equal(wrapped, base);
  });

  it('returns undefined unchanged when base is undefined and no proxy env', () => {
    assert.equal(withProxyDispatcher(undefined, {} as NodeJS.ProcessEnv), undefined);
  });

  it('attaches a dispatcher for a proxied request', async () => {
    const { base, calls } = capturing();
    const env = { HTTPS_PROXY: 'http://proxy.corp:8080' } as NodeJS.ProcessEnv;
    const wrapped = withProxyDispatcher(base, env)!;
    await wrapped('https://api.anthropic.com/v1/messages', { method: 'POST' });
    assert.equal(calls.length, 1);
    assert.ok(calls[0].init.dispatcher, 'a dispatcher should be attached');
  });

  it('does not attach a dispatcher when the host is in NO_PROXY', async () => {
    const { base, calls } = capturing();
    const env = { HTTPS_PROXY: 'http://proxy.corp:8080', NO_PROXY: 'api.anthropic.com' } as NodeJS.ProcessEnv;
    const wrapped = withProxyDispatcher(base, env)!;
    await wrapped('https://api.anthropic.com/v1/messages', { method: 'POST' });
    assert.equal(calls[0].init.dispatcher, undefined);
  });

  it('respects a caller-supplied dispatcher (does not override)', async () => {
    const { base, calls } = capturing();
    const env = { HTTPS_PROXY: 'http://proxy.corp:8080' } as NodeJS.ProcessEnv;
    const wrapped = withProxyDispatcher(base, env)!;
    const sentinel = { sentinel: true };
    await wrapped('https://api.anthropic.com/v1', { dispatcher: sentinel } as any);
    assert.equal(calls[0].init.dispatcher, sentinel);
  });

  it('resolves the url from a URL instance and a Request-like input', async () => {
    const { base, calls } = capturing();
    const env = { HTTPS_PROXY: 'http://proxy.corp:8080' } as NodeJS.ProcessEnv;
    const wrapped = withProxyDispatcher(base, env)!;
    await wrapped(new URL('https://api.anthropic.com/v1'), { method: 'POST' });
    assert.ok(calls[0].init.dispatcher);
    await wrapped({ url: 'https://api.openai.com/v1' } as any, { method: 'POST' });
    assert.ok(calls[1].init.dispatcher);
  });

  it('passes through (no dispatcher) when the url cannot be resolved', async () => {
    const { base, calls } = capturing();
    const env = { HTTPS_PROXY: 'http://proxy.corp:8080' } as NodeJS.ProcessEnv;
    const wrapped = withProxyDispatcher(base, env)!;
    await wrapped({ noUrl: true } as any, { method: 'POST' });
    assert.equal(calls[0].init.dispatcher, undefined);
  });
});

// ---------------------------------------------------------------------------
// R2-M31 — Anthropic beta-header building (1M context)
// ---------------------------------------------------------------------------
describe('mergeBetaTokens', () => {
  it('adds the token to an empty/undefined list', () => {
    assert.equal(mergeBetaTokens(undefined, ONE_MILLION_CONTEXT_BETA), ONE_MILLION_CONTEXT_BETA);
    assert.equal(mergeBetaTokens('', ONE_MILLION_CONTEXT_BETA), ONE_MILLION_CONTEXT_BETA);
    assert.equal(mergeBetaTokens(null, ONE_MILLION_CONTEXT_BETA), ONE_MILLION_CONTEXT_BETA);
  });

  it('comma-joins a co-existing extended-cache beta without duplication', () => {
    assert.equal(
      mergeBetaTokens('extended-cache-ttl-2025-04-11', ONE_MILLION_CONTEXT_BETA),
      `extended-cache-ttl-2025-04-11,${ONE_MILLION_CONTEXT_BETA}`,
    );
  });

  it('never duplicates a token already present', () => {
    assert.equal(mergeBetaTokens(ONE_MILLION_CONTEXT_BETA, ONE_MILLION_CONTEXT_BETA), ONE_MILLION_CONTEXT_BETA);
    assert.equal(
      mergeBetaTokens(` ${ONE_MILLION_CONTEXT_BETA} , extended-cache-ttl-2025-04-11 `, ONE_MILLION_CONTEXT_BETA),
      `${ONE_MILLION_CONTEXT_BETA},extended-cache-ttl-2025-04-11`,
    );
  });
});

describe('withAnthropicBetaHeader', () => {
  function capturing() {
    const calls: Array<{ input: unknown; init: any }> = [];
    const base = (async (input: any, init?: any) => {
      calls.push({ input, init });
      return new Response('ok', { status: 200 });
    }) as typeof globalThis.fetch;
    return { base, calls };
  }

  it('sets the context-1m beta token when none is present', async () => {
    const { base, calls } = capturing();
    const wrapped = withAnthropicBetaHeader(base, ONE_MILLION_CONTEXT_BETA);
    await wrapped('https://vertex.example/v1/messages', { method: 'POST' });
    const headers = calls[0].init.headers as Headers;
    assert.equal(headers.get('anthropic-beta'), ONE_MILLION_CONTEXT_BETA);
  });

  it('comma-joins with an existing extended-cache beta (both present)', async () => {
    const { base, calls } = capturing();
    const wrapped = withAnthropicBetaHeader(base, ONE_MILLION_CONTEXT_BETA);
    await wrapped('https://vertex.example/v1/messages', {
      method: 'POST',
      headers: { 'anthropic-beta': 'extended-cache-ttl-2025-04-11' },
    });
    const headers = calls[0].init.headers as Headers;
    assert.equal(headers.get('anthropic-beta'), `extended-cache-ttl-2025-04-11,${ONE_MILLION_CONTEXT_BETA}`);
  });
});
