// Unit U020 (wave 88) — provider.ts parity.
//
// Gap: "Accurate API token count with graceful degradation cascade ... bounded
// retries, and null-on-error" (references/claude/claude-code-main/src/services/
// tokenEstimation.ts:L140-201). Claude builds its count client with maxRetries:1
// so a single flaky 5xx / network blip on the best-effort token-count path
// self-heals before degrading to the heuristic. AMI's countTokensWithAPI
// previously returned null on the FIRST non-OK / transport failure with no retry.
//
// These tests pin the bounded-retry behavior via an injected fetch stub so we can
// count attempts deterministically (no real network / timing). They are RED
// against the pre-fix single-shot implementation:
//   - a transient 503-then-200 returned null (no retry) instead of the count.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { countTokensWithAPI } from '../src/provider.ts';

type Behavior = { status: number; json?: unknown } | { throw: Error };

/** Build an injectable fetch that plays a programmed script of behaviors and
 *  records how many times it was called. */
function scriptedFetch(script: Behavior[]): { fetchImpl: typeof globalThis.fetch; calls: () => number } {
  let i = 0;
  let calls = 0;
  const fetchImpl = (async () => {
    calls++;
    const b = script[Math.min(i, script.length - 1)];
    i++;
    if ('throw' in b) throw b.throw;
    return new Response(JSON.stringify(b.json ?? {}), {
      status: b.status,
      headers: { 'content-type': 'application/json' },
    });
  }) as unknown as typeof globalThis.fetch;
  return { fetchImpl, calls: () => calls };
}

const baseConfig = {
  apiKey: 'sk-ant-x',
  baseUrl: 'https://example.invalid',
  model: 'claude-opus-4-6',
  provider: 'anthropic',
} as never;

function netErr(code: string): Error {
  const e = new Error(`${code}: connection reset`);
  (e as unknown as { code: string }).code = code;
  return e;
}

describe('countTokensWithAPI — bounded transient retry (the reference maxRetries:1 parity)', () => {
  it('retries once on a transient 503 and returns the recovered count', async () => {
    const f = scriptedFetch([{ status: 503 }, { status: 200, json: { input_tokens: 77 } }]);
    const n = await countTokensWithAPI(baseConfig, { messages: [{ role: 'user', content: 'hi' }], fetchImpl: f.fetchImpl });
    assert.equal(n, 77);
    assert.equal(f.calls(), 2, 'should make exactly one retry after the 503');
  });

  it('retries once on a transport error (ECONNRESET) then succeeds', async () => {
    const f = scriptedFetch([{ throw: netErr('ECONNRESET') }, { status: 200, json: { input_tokens: 5 } }]);
    const n = await countTokensWithAPI(baseConfig, { messages: [], fetchImpl: f.fetchImpl });
    assert.equal(n, 5);
    assert.equal(f.calls(), 2);
  });

  it('does NOT retry a deterministic 400 — one attempt, null', async () => {
    const f = scriptedFetch([{ status: 400 }, { status: 200, json: { input_tokens: 9 } }]);
    const n = await countTokensWithAPI(baseConfig, { messages: [{ role: 'user', content: 'x' }], fetchImpl: f.fetchImpl });
    assert.equal(n, null);
    assert.equal(f.calls(), 1, 'a 4xx is deterministic — must not retry');
  });

  it('does NOT retry a non-network throw (e.g. JSON parse) — one attempt, null', async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls++;
      // 200 OK but a body that is not valid JSON → res.json() throws (not a network error).
      return new Response('<<not json>>', { status: 200, headers: { 'content-type': 'application/json' } });
    }) as unknown as typeof globalThis.fetch;
    const n = await countTokensWithAPI(baseConfig, { messages: [], fetchImpl });
    assert.equal(n, null);
    assert.equal(calls, 1, 'a parse error is not transient — must not retry');
  });

  it('maxRetries:0 disables the retry — a 503 returns null after one attempt', async () => {
    const f = scriptedFetch([{ status: 503 }, { status: 200, json: { input_tokens: 1 } }]);
    const n = await countTokensWithAPI(baseConfig, { messages: [], fetchImpl: f.fetchImpl, maxRetries: 0 });
    assert.equal(n, null);
    assert.equal(f.calls(), 1);
  });

  it('honors maxRetries:2 across repeated transient failures', async () => {
    const f = scriptedFetch([{ status: 500 }, { status: 502 }, { status: 200, json: { input_tokens: 3 } }]);
    const n = await countTokensWithAPI(baseConfig, { messages: [], fetchImpl: f.fetchImpl, maxRetries: 2 });
    assert.equal(n, 3);
    assert.equal(f.calls(), 3);
  });

  it('gives up after exhausting retries on persistent 529 overload', async () => {
    const f = scriptedFetch([{ status: 529 }, { status: 529 }, { status: 529 }]);
    const n = await countTokensWithAPI(baseConfig, { messages: [], fetchImpl: f.fetchImpl, maxRetries: 1 });
    assert.equal(n, null);
    assert.equal(f.calls(), 2, 'initial attempt + one bounded retry');
  });

  it('returns null without calling fetch when the signal is already aborted', async () => {
    const f = scriptedFetch([{ status: 200, json: { input_tokens: 10 } }]);
    const ac = new AbortController();
    ac.abort();
    const n = await countTokensWithAPI(baseConfig, { messages: [], fetchImpl: f.fetchImpl, signal: ac.signal });
    assert.equal(n, null);
    assert.equal(f.calls(), 0, 'an already-aborted request must not hit the wire');
  });

  it('still returns null on a 200 with a non-numeric input_tokens (no retry)', async () => {
    const f = scriptedFetch([{ status: 200, json: { input_tokens: 'nope' } }]);
    const n = await countTokensWithAPI(baseConfig, { messages: [], fetchImpl: f.fetchImpl });
    assert.equal(n, null);
    assert.equal(f.calls(), 1, 'a 200 answered — do not retry a bad body');
  });
});
