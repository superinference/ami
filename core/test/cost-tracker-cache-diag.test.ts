/**
 * cost-tracker-cache-diag — cost-tracker parity additions:
 *  - pricing-string affordances (formatPrice / formatModelPricing / getModelPricingString)
 *  - nonessential-traffic kill-switch on live pricing fetch
 *  - extended cache-break diagnostics: multi-cause explanation with magnitudes,
 *    extra cache-key dimensions, dual 5min/1h TTL buckets, and
 *    compaction/cache-deletion expected-drop suppression.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  formatPrice, formatModelPricing, getModelPricingString, diffStringLists,
  CacheMissDiagnostics, _resetPricingCache, _fetchPricing,
  type CacheDiagRequest,
} from '../src/cost-tracker';

const baseReq = (): CacheDiagRequest => ({
  toolDefHash: 't1', systemPromptHash: 's1', thinkingMode: 'off', model: 'claude-opus-4',
});

describe('pricing display helpers', () => {
  it('formatPrice: integers plain, fractions 2dp', () => {
    assert.equal(formatPrice(3), '$3');
    assert.equal(formatPrice(0.8), '$0.80');
    assert.equal(formatPrice(22.5), '$22.50');
    assert.equal(formatPrice(-1), '$?');
    assert.equal(formatPrice(NaN), '$?');
  });
  it('formatModelPricing joins input/output', () => {
    assert.equal(formatModelPricing(3, 15), '$3/$15 per Mtok');
  });
  it('getModelPricingString uses the pricing table for a known model', () => {
    assert.equal(getModelPricingString('claude-opus-4'), '$15/$75 per Mtok');
  });
  it('getModelPricingString returns empty for an unknown model (no guessed price)', () => {
    assert.equal(getModelPricingString('totally-unknown-model-xyz'), '');
  });
});

describe('nonessential-traffic kill-switch on live pricing', () => {
  const savedFetch = globalThis.fetch;
  afterEach(() => {
    globalThis.fetch = savedFetch;
    delete process.env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC;
    _resetPricingCache();
  });

  it('skips the gateway fetch when the knob is set', async () => {
    let called = false;
    globalThis.fetch = (async () => { called = true; return { ok: true, json: async () => ({}) } as Response; }) as unknown as typeof fetch;
    process.env.SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC = '1';
    _resetPricingCache();
    await _fetchPricing();
    assert.equal(called, false, 'no outbound pricing fetch when nonessential traffic is disabled');
  });

  it('performs the fetch when the knob is unset', async () => {
    let called = false;
    globalThis.fetch = (async () => { called = true; return { ok: false } as Response; }) as unknown as typeof fetch;
    _resetPricingCache();
    await _fetchPricing();
    assert.equal(called, true);
  });
});

describe('diffStringLists', () => {
  it('reports sorted added/removed', () => {
    assert.deepEqual(diffStringLists(['a', 'b'], ['b', 'c', 'a']), { added: ['c'], removed: [] });
    assert.deepEqual(diffStringLists(['x', 'y'], ['y']), { added: [], removed: ['x'] });
    assert.deepEqual(diffStringLists(undefined, undefined), { added: [], removed: [] });
  });
});

/** Drive one cache HIT then one break-triggering request, return the diagnostics. */
function primeHit(d: CacheMissDiagnostics): void {
  d.recordRequest(baseReq());
  d.recordResponse(1000, 0); // healthy cache read -> establishes baseline
}

describe('multi-cause cache-break explanation', () => {
  it('reports every concurrent cause with magnitudes', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000); // 1h TTL (idle not triggered)
    d.recordRequest({ ...baseReq(), systemPromptChars: 1000, betas: ['b1'], effortValue: 'default' });
    d.recordResponse(1000, 0);
    d.recordRequest({
      toolDefHash: 't2', systemPromptHash: 's2', systemPromptChars: 1500, thinkingMode: 'high',
      model: 'claude-sonnet-4', betas: ['b1', 'b2'], effortValue: 'high',
      perToolHashes: { Read: 'h2' },
    });
    d.recordResponse(0, 0); // full miss
    const causes = d.getLastCauses().map(c => c.cause);
    assert.ok(causes.includes('model_switched'));
    assert.ok(causes.includes('tool_definitions_changed'));
    assert.ok(causes.includes('system_prompt_changed'));
    assert.ok(causes.includes('thinking_mode_changed'));
    assert.ok(causes.includes('betas_changed'));
    assert.ok(causes.includes('effort_changed'));
    const explanation = d.getLastExplanation();
    assert.ok(explanation.includes('model changed'));
    assert.ok(explanation.includes('+500 chars'));
    assert.ok(explanation.includes('effort changed (default → high)'));
    // Top cause keeps model-switch priority for the single-cause tally.
    assert.equal(d.getLastMissCause(), 'model_switched');
  });

  it('classifies a same-tool-set schema change', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    d.recordRequest({ ...baseReq(), perToolHashes: { Read: 'h1', Bash: 'b1' } });
    d.recordResponse(1000, 0);
    d.recordRequest({ ...baseReq(), toolDefHash: 't2', perToolHashes: { Read: 'h2', Bash: 'b1' } });
    d.recordResponse(0, 0);
    assert.ok(d.getLastExplanation().includes('same tool set'));
    assert.ok(d.getLastExplanation().includes('Read'));
  });

  it('reports cache-control / extra-body / fast-mode / global-cache-strategy flips', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    d.recordRequest({ ...baseReq(), cacheControlHash: 'c1', extraBodyHash: 'e1', fastMode: false, globalCacheStrategy: 'none' });
    d.recordResponse(1000, 0);
    d.recordRequest({ ...baseReq(), cacheControlHash: 'c2', extraBodyHash: 'e2', fastMode: true, globalCacheStrategy: 'tool_based' });
    d.recordResponse(0, 0);
    const causes = d.getLastCauses().map(c => c.cause);
    assert.ok(causes.includes('cache_control_changed'));
    assert.ok(causes.includes('extra_body_changed'));
    assert.ok(causes.includes('fast_mode_changed'));
    assert.ok(causes.includes('global_cache_strategy_changed'));
  });

  it('falls back to unknown when nothing observable changed', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.recordRequest(baseReq()); // identical inputs
    d.recordResponse(0, 0);     // yet cache missed -> server-side
    assert.equal(d.getLastMissCause(), 'unknown');
  });
});

describe('dual 5min / 1h TTL buckets', () => {
  it('idle past the 1h TTL is its own bucket', () => {
    const d = new CacheMissDiagnostics(5 * 60 * 1000); // 5min TTL
    primeHit(d);
    d._setLastRequestTime(Date.now() - 2 * 60 * 60 * 1000); // 2h ago
    d.recordRequest(baseReq());
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'idle_past_1h_ttl');
    assert.ok(d.getLastExplanation().includes('1h TTL'));
  });
});

describe('expected-drop suppression (compaction / cache-deletion)', () => {
  it('notifyCompaction swallows the next drop instead of flagging a break', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.notifyCompaction();
    d.recordRequest(baseReq());
    d.recordResponse(0, 0); // legitimate post-compaction drop
    assert.equal(d.getTotalMisses(), 0);
    assert.equal(d.getExpectedDropCount(), 1);
    assert.equal(d.getLastCauses().length, 0);
    // Baseline advanced: a subsequent genuine miss is still detected.
    d.recordRequest({ ...baseReq(), model: 'claude-sonnet-4' });
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'model_switched');
  });

  it('notifyCacheDeletion also suppresses the next drop and reset() clears it', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.notifyCacheDeletion();
    d.reset();
    assert.equal(d.getExpectedDropCount(), 0);
  });
});
