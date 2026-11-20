import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  SessionMemoryExtractor,
  DEFAULT_SESSION_MEMORY_CONFIG,
  MAX_SESSION_MEMORY_TOTAL_TOKENS,
  type SessionFact,
} from '../src/session-memory';
import { estimateTextTokens } from '../src/token-estimator';
import type { ProviderConfig } from '../src/types';

const provider: ProviderConfig = { model: 'test', apiKey: 'x', baseUrl: 'http://localhost' };

/** Total byte-aware token estimate across a set of facts (same count mergeFacts uses). */
function totalTokens(facts: SessionFact[]): number {
  return facts.reduce((sum, f) => sum + estimateTextTokens(f.fact), 0);
}

// A 36-ASCII-char fact estimates to 9 tokens (ceil(36/4)); two fit in a 20-token
// budget, three (27) do not.
function pad(label: string): string {
  const base = `${label} `;
  return (base + 'x'.repeat(36 - base.length)).slice(0, 36);
}

// ---------------------------------------------------------------------------
// Gaps U086: aggregate session-memory token budget (parity: MAX_TOTAL_SESSION_MEMORY_TOKENS)
// ---------------------------------------------------------------------------
describe('mergeFacts — aggregate token budget', () => {
  it('exposes a default 12000-token whole-document budget (the reference parity)', () => {
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.maxTotalTokens, 12_000);
    assert.equal(MAX_SESSION_MEMORY_TOTAL_TOKENS, 12_000);
  });

  it('evicts facts so the combined token total never exceeds maxTotalTokens', () => {
    // Entry count is well under maxEntries, so ONLY the token budget can trigger
    // eviction — this discriminates the new aggregate-budget path.
    const ex = new SessionMemoryExtractor(provider, undefined, {
      maxEntries: 100,
      maxTotalTokens: 20,
      recencyProtectedCount: 0,
    });
    const facts: SessionFact[] = [
      { fact: pad('alpha conv'), category: 'convention', confidence: 0.95 },
      { fact: pad('beta conv'), category: 'convention', confidence: 0.92 },
      { fact: pad('gamma conv'), category: 'convention', confidence: 0.9 },
      { fact: pad('delta conv'), category: 'convention', confidence: 0.88 },
      { fact: pad('epsilon conv'), category: 'convention', confidence: 0.86 },
    ];
    assert.ok(totalTokens(facts) > 20, 'precondition: input exceeds the budget');

    const merged = ex.mergeFacts([], facts);

    assert.ok(totalTokens(merged) <= 20, 'merged total must fit within the token budget');
    assert.ok(merged.length < facts.length, 'some facts must have been evicted');
  });

  it('preserves a critical fact over a higher-confidence non-critical one under the token budget', () => {
    const ex = new SessionMemoryExtractor(provider, undefined, {
      maxEntries: 100,
      maxTotalTokens: 20,
      recencyProtectedCount: 0,
    });
    const facts: SessionFact[] = [
      { fact: pad('aaa conv'), category: 'convention', confidence: 0.95 },
      { fact: pad('bbb conv'), category: 'convention', confidence: 0.92 },
      // lower confidence but critical category -> boosted above both conventions.
      { fact: pad('err fix'), category: 'error_fix', confidence: 0.75 },
    ];

    const merged = ex.mergeFacts([], facts);
    const kept = merged.map(f => f.fact);

    assert.ok(totalTokens(merged) <= 20);
    assert.ok(kept.includes(pad('err fix')), 'critical fact must survive the token-budget eviction');
    assert.ok(kept.includes(pad('aaa conv')), 'next-highest-scoring fact that still fits is kept');
    assert.ok(!kept.includes(pad('bbb conv')), 'lowest-scoring fact over budget is evicted');
  });

  it('protects recency-protected facts even when they alone exceed the budget', () => {
    const ex = new SessionMemoryExtractor(provider, undefined, {
      maxEntries: 100,
      maxTotalTokens: 10,
      recencyProtectedCount: 2,
    });
    const facts: SessionFact[] = [
      { fact: pad('old one'), category: 'convention', confidence: 0.99 },
      { fact: pad('recent a'), category: 'convention', confidence: 0.5 },
      { fact: pad('recent b'), category: 'convention', confidence: 0.5 },
    ];

    const merged = ex.mergeFacts([], facts);
    const kept = merged.map(f => f.fact);

    // The two newest facts are recency-protected and kept despite exceeding the
    // budget; the older high-confidence fact is evicted to honor the budget as
    // far as possible.
    assert.ok(kept.includes(pad('recent a')));
    assert.ok(kept.includes(pad('recent b')));
    assert.ok(!kept.includes(pad('old one')), 'non-protected fact is evicted under the budget');
  });

  it('does not evict when both entry-count and token budgets are satisfied', () => {
    // Regression lock: small facts well within both caps are retained verbatim.
    const ex = new SessionMemoryExtractor(provider, undefined, {
      maxEntries: 100,
      maxTotalTokens: 12_000,
      recencyProtectedCount: 20,
    });
    const facts: SessionFact[] = [
      { fact: 'a concise convention', category: 'convention', confidence: 0.9 },
      { fact: 'an error was fixed', category: 'error_fix', confidence: 0.8 },
    ];

    const merged = ex.mergeFacts([], facts);
    assert.equal(merged.length, 2);
    assert.ok(merged.some(f => f.fact === 'a concise convention'));
    assert.ok(merged.some(f => f.fact === 'an error was fixed'));
  });
});
