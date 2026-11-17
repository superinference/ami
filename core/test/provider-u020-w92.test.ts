// ===========================================================================
// provider-u020-w92.test.ts
//
// Parity unit U020 / w92 — cache-preserving tool denial for forked/derived
// requests (core/core/src/provider.ts :: withCacheSafeToolDeny).
//
// Gap (agentSummary.ts L8-11,L93-98 + btw/index.ts L3-11): to share the parent
// agent's prompt cache, a forked side-question/summary sends the IDENTICAL
// cache-key params — including the full `tools` array — and denies every call via
// a `canUseTool` callback returning behavior:'deny', NEVER `tools:[]` (which would
// change the cached prefix and bust the cache). AMI's provider stream has no
// per-call callback layer; the provider-level equivalent is `tool_choice:'none'`:
// the identical tools array is still advertised (cache prefix byte-matched) but the
// model is forbidden from invoking any of them.
//
// RED without the helper: `withCacheSafeToolDeny` does not exist (import + type
// errors); the assertions that it forces `toolChoice:'none'` while leaving every
// other field untouched, and that it does NOT mutate the input, have no backing.
//
// ESM-coverage-safe: no source-text assertions, no mocked globals.
// ===========================================================================
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { withCacheSafeToolDeny, normalizeToolChoice } from '../src/provider';
import type { ProviderConfig } from '../src/types';

const BASE: ProviderConfig = {
  provider: 'anthropic',
  model: 'claude-sonnet-4-6',
  apiKey: 'k',
  baseUrl: 'https://api.anthropic.com',
  maxTokens: 4096,
  thinking: { type: 'enabled', budgetTokens: 2048 },
} as unknown as ProviderConfig;

describe('withCacheSafeToolDeny', () => {
  it("forces toolChoice to 'none' so advertised tools are denied, not dropped", () => {
    const out = withCacheSafeToolDeny(BASE);
    assert.equal(out.toolChoice, 'none');
    // 'none' is a value the request layer honors (it round-trips normalizeToolChoice),
    // so the SDK keeps the tools block but forbids any call.
    assert.equal(normalizeToolChoice(out.toolChoice), 'none');
  });

  it('preserves every other cache-key field byte-for-byte', () => {
    const out = withCacheSafeToolDeny(BASE);
    assert.equal(out.provider, BASE.provider);
    assert.equal(out.model, BASE.model);
    assert.equal(out.apiKey, BASE.apiKey);
    assert.equal(out.baseUrl, BASE.baseUrl);
    assert.equal(out.maxTokens, BASE.maxTokens);
    // The thinking config must be the same reference so the cache key is unchanged.
    assert.equal(out.thinking, BASE.thinking);
  });

  it('does not mutate the input config (returns a shallow copy)', () => {
    const input = { ...BASE, toolChoice: 'auto' as const };
    const out = withCacheSafeToolDeny(input);
    assert.equal(input.toolChoice, 'auto', 'input must be untouched');
    assert.equal(out.toolChoice, 'none');
    assert.notEqual(out, input, 'a new object must be returned');
  });

  it("overrides a pre-set toolChoice with 'none'", () => {
    const forced = withCacheSafeToolDeny({ ...BASE, toolChoice: { type: 'tool', toolName: 'bash' } });
    assert.equal(forced.toolChoice, 'none');
  });
});
