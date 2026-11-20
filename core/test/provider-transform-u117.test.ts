import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  buildConversationCachePoints,
  buildToolCacheBreakpoints,
  type CacheBreakpoint,
} from '../src/provider-transform';
import {
  injectAnthropicCacheControl,
  withVertexAnthropicCache,
  buildAnthropicCacheControl,
} from '../src/provider';
import type { ToolDefinition } from '../src/types';

// ---------------------------------------------------------------------------
// U117 — Prompt-cache TTL option (5m vs 1h) threaded into the emitted breakpoint
// shape of the cache-point index builders.
//
// Gap: `buildConversationCachePoints` / `buildToolCacheBreakpoints` hardcoded the
// breakpoint as `{ type: 'ephemeral' }` with no way to opt a breakpoint into the
// 1h extended-cache window (or a cross-session scope). They now accept the actual
// breakpoint object to emit, mirroring the reference's getCacheControl() return shape
// `{ type: 'ephemeral'; ttl?: '1h'; scope?: CacheScope }`, and that value is
// consumed on both the SDK (providerOptions) and the raw-vertex (injected body)
// cache paths.
// ---------------------------------------------------------------------------

const stubTool = (name: string): ToolDefinition => ({
  name,
  description: 'stub',
  inputSchema: { type: 'object', properties: {} },
  isReadOnly: true,
  execute: async () => ({ output: '' }),
});

describe('U117 buildConversationCachePoints — TTL/scope-aware breakpoint', () => {
  it('defaults to the 5-minute ephemeral breakpoint (behavior unchanged)', () => {
    const msgs = [{ role: 'user' }, { role: 'assistant' }];
    const points = buildConversationCachePoints(msgs);
    assert.deepEqual(points[1], { type: 'ephemeral' });
    assert.equal(points[1].ttl, undefined);
  });

  it('emits a 1h-TTL breakpoint when supplied, at every conversation point', () => {
    const bp: CacheBreakpoint = { type: 'ephemeral', ttl: '1h' };
    const msgs = [{ role: 'user' }, { role: 'assistant' }, { role: 'user' }];
    const points = buildConversationCachePoints(msgs, true, bp);
    // Both the last and second-last non-tool messages carry the 1h breakpoint.
    assert.deepEqual(points[1], { type: 'ephemeral', ttl: '1h' });
    assert.deepEqual(points[2], { type: 'ephemeral', ttl: '1h' });
  });

  it('threads an optional cross-session scope through the emitted breakpoint', () => {
    const bp: CacheBreakpoint = { type: 'ephemeral', ttl: '1h', scope: 'org' };
    const msgs = [{ role: 'user' }];
    const points = buildConversationCachePoints(msgs, true, bp);
    assert.deepEqual(points[0], { type: 'ephemeral', ttl: '1h', scope: 'org' });
  });

  it('still returns {} when caching is disabled, even with a 1h breakpoint', () => {
    const msgs = [{ role: 'user' }, { role: 'assistant' }];
    assert.deepEqual(
      buildConversationCachePoints(msgs, false, { type: 'ephemeral', ttl: '1h' }),
      {},
    );
  });
});

describe('U117 buildToolCacheBreakpoints — TTL/scope-aware breakpoint', () => {
  it('defaults to the 5-minute ephemeral breakpoint (behavior unchanged)', () => {
    const points = buildToolCacheBreakpoints([stubTool('a'), stubTool('b')]);
    assert.deepEqual(points[1], { type: 'ephemeral' });
    assert.equal(points[1].ttl, undefined);
  });

  it('emits a 1h-TTL breakpoint on the last tool when supplied', () => {
    const bp: CacheBreakpoint = { type: 'ephemeral', ttl: '1h' };
    const points = buildToolCacheBreakpoints([stubTool('a'), stubTool('b')], true, bp);
    assert.deepEqual(points[1], { type: 'ephemeral', ttl: '1h' });
    assert.equal(points[0], undefined);
  });

  it('returns {} when caching disabled, even with a 1h breakpoint', () => {
    assert.deepEqual(
      buildToolCacheBreakpoints([stubTool('a')], false, { type: 'ephemeral', ttl: '1h' }),
      {},
    );
  });
});

// ---------------------------------------------------------------------------
// Integration: the raw-vertex body path (injectAnthropicCacheControl) now routes
// its last-tool marking through buildToolCacheBreakpoints, so a 1h breakpoint is
// carried onto the wire.
// ---------------------------------------------------------------------------
describe('U117 injectAnthropicCacheControl consumes buildToolCacheBreakpoints', () => {
  it('marks ONLY the last tool (behavior preserved) with the given breakpoint', () => {
    const cc = { type: 'ephemeral' };
    const body = JSON.stringify({ tools: [{ name: 'a' }, { name: 'b' }, { name: 'c' }] });
    const out = JSON.parse(injectAnthropicCacheControl(body, cc));
    assert.deepEqual(out.tools[2].cache_control, cc, 'last tool marked');
    assert.equal(out.tools[0].cache_control, undefined);
    assert.equal(out.tools[1].cache_control, undefined);
  });

  it('carries a 1h TTL breakpoint onto the last tool', () => {
    const cc = { type: 'ephemeral', ttl: '1h' };
    const body = JSON.stringify({ tools: [{ name: 'a' }, { name: 'b' }] });
    const out = JSON.parse(injectAnthropicCacheControl(body, cc));
    assert.deepEqual(out.tools[1].cache_control, { type: 'ephemeral', ttl: '1h' });
  });

  it('is a no-op on an empty tools array', () => {
    const body = JSON.stringify({ tools: [] });
    const out = JSON.parse(injectAnthropicCacheControl(body, { type: 'ephemeral' }));
    assert.deepEqual(out.tools, []);
  });
});

// ---------------------------------------------------------------------------
// E2E: the full anthropic-on-vertex fetch wrapper routes a 1h TTL end to end —
// the outgoing request body's last tool carries the 1h breakpoint AND the
// extended-cache beta header is attached.
// ---------------------------------------------------------------------------
describe('U117 withVertexAnthropicCache 1h TTL end-to-end', () => {
  function capturing() {
    const calls: Array<{ init: { body?: unknown; headers?: unknown } }> = [];
    const base = (async (_input: unknown, init?: { body?: unknown; headers?: unknown }) => {
      calls.push({ init: init ?? {} });
      return new Response('ok', { status: 200 });
    }) as typeof globalThis.fetch;
    return { base, calls };
  }

  it('injects the 1h breakpoint on the last tool and adds the extended-cache beta', async () => {
    const { base, calls } = capturing();
    const wrapped = withVertexAnthropicCache(base, '1h');
    const requestBody = JSON.stringify({
      tools: [{ name: 'bash', input_schema: {} }, { name: 'grep', input_schema: {} }],
      messages: [{ role: 'user', content: 'hi' }],
    });
    await wrapped('https://vertex.example/v1/messages', { method: 'POST', body: requestBody });

    const sent = JSON.parse(calls[0].init.body as string);
    const expectedCC = buildAnthropicCacheControl('1h');
    assert.deepEqual(sent.tools[sent.tools.length - 1].cache_control, expectedCC);
    assert.equal(sent.tools[0].cache_control, undefined);
    assert.equal((sent.tools[sent.tools.length - 1].cache_control as { ttl?: string }).ttl, '1h');

    const headers = calls[0].init.headers as Headers;
    assert.equal(headers.get('anthropic-beta'), 'extended-cache-ttl-2025-04-11');
  });
});
