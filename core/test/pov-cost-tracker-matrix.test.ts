/**
 * POV: core/core/src/cost-tracker.ts
 *
 * Behavioral regression matrix (round 3) for the cost-tracking surface:
 *   - estimateUsageCost component pricing (input / output / cache-read / cache-create)
 *   - isPricedModel true/false + substring resolution
 *   - CostTracker accumulation across calls (single source of truth agreement)
 *   - fingerprint stability + format
 *   - CacheMissDiagnostics.recordRequest/recordResponse + CACHE_MISS_CAUSES classification
 *   - unknown model -> DEFAULT_PRICING (documented) + zero/edge inputs
 *
 * All expected values are COMPUTED by calling the real code (or by the pricing
 * ratios documented in the source), never by reading the .ts text.
 */
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  CostTracker,
  estimateUsageCost,
  isPricedModel,
  fingerprint,
  CacheMissDiagnostics,
  CACHE_MISS_CAUSES,
  categorizeTool,
  _resetPricingCache,
  type CacheMissCause,
  type CacheDiagRequest,
} from '../src/cost-tracker';

// Keep every test on the deterministic hardcoded fallback table (no live fetch).
beforeEach(() => {
  _resetPricingCache();
});

const ZERO = { uncachedPromptTokens: 0, cachedPromptTokens: 0, cacheCreationTokens: 0, completionTokens: 0 };

// ---------------------------------------------------------------------------
// estimateUsageCost — per-component pricing
// ---------------------------------------------------------------------------

describe('estimateUsageCost — component additivity', () => {
  it('total cost equals the sum of each component priced alone (behavioral property)', () => {
    const models = ['gpt-4o', 'claude-opus-4', 'gemini-2.5-pro', 'o1', 'deepseek-r1'];
    for (const model of models) {
      const u = {
        uncachedPromptTokens: 123_456,
        cachedPromptTokens: 77_000,
        cacheCreationTokens: 41_000,
        completionTokens: 99_999,
      };
      const whole = estimateUsageCost(model, u);
      const parts =
        estimateUsageCost(model, { ...ZERO, uncachedPromptTokens: u.uncachedPromptTokens }) +
        estimateUsageCost(model, { ...ZERO, cachedPromptTokens: u.cachedPromptTokens }) +
        estimateUsageCost(model, { ...ZERO, cacheCreationTokens: u.cacheCreationTokens }) +
        estimateUsageCost(model, { ...ZERO, completionTokens: u.completionTokens });
      assert.ok(Math.abs(whole - parts) < 1e-9, `${model}: whole ${whole} != parts ${parts}`);
    }
  });

  it('prices exactly from the documented fallback table for claude-opus-4', () => {
    // claude-opus-4 fallback: [15 input, 75 output, null cached].
    // cached null -> input * 0.1 = 1.5 ; cache-create -> input * 1.25 = 18.75 per Mtok.
    const cost = estimateUsageCost('claude-opus-4', {
      uncachedPromptTokens: 1_000_000,
      cachedPromptTokens: 1_000_000,
      cacheCreationTokens: 1_000_000,
      completionTokens: 1_000_000,
    });
    const expected = 15 + 1.5 + 18.75 + 75;
    assert.ok(Math.abs(cost - expected) < 1e-6, `expected ${expected}, got ${cost}`);
  });

  it('cache-read is 10% of input, cache-create is 125% of input (ratio check, model-agnostic)', () => {
    const model = 'gpt-4o';
    const inputOnly = estimateUsageCost(model, { ...ZERO, uncachedPromptTokens: 1_000_000 });
    const cachedOnly = estimateUsageCost(model, { ...ZERO, cachedPromptTokens: 1_000_000 });
    const createOnly = estimateUsageCost(model, { ...ZERO, cacheCreationTokens: 1_000_000 });
    assert.ok(Math.abs(cachedOnly - inputOnly * 0.1) < 1e-9, 'cache-read = 0.1 * input');
    assert.ok(Math.abs(createOnly - inputOnly * 1.25) < 1e-9, 'cache-create = 1.25 * input');
  });

  it('is linear in token count (scaling the inputs scales the cost)', () => {
    const base = { uncachedPromptTokens: 10_000, cachedPromptTokens: 2_000, cacheCreationTokens: 500, completionTokens: 3_000 };
    const one = estimateUsageCost('gemini-1.5-pro', base);
    const ten = estimateUsageCost('gemini-1.5-pro', {
      uncachedPromptTokens: base.uncachedPromptTokens * 10,
      cachedPromptTokens: base.cachedPromptTokens * 10,
      cacheCreationTokens: base.cacheCreationTokens * 10,
      completionTokens: base.completionTokens * 10,
    });
    assert.ok(Math.abs(ten - one * 10) < 1e-6, `expected ${one * 10}, got ${ten}`);
  });

  it('all-zero usage costs exactly $0', () => {
    for (const model of ['gpt-4o', 'claude-opus-4', 'unknown-xyz']) {
      assert.equal(estimateUsageCost(model, ZERO), 0);
    }
  });

  it('floors negative components to 0 (never negative, partial negatives ignored)', () => {
    const cost = estimateUsageCost('gpt-4o', {
      uncachedPromptTokens: -100,
      cachedPromptTokens: -5,
      cacheCreationTokens: -5,
      completionTokens: 1_000_000, // only this counts: 10/Mtok
    });
    assert.ok(Math.abs(cost - 10) < 1e-6, `expected 10, got ${cost}`);
  });
});

// ---------------------------------------------------------------------------
// Unknown model -> DEFAULT_PRICING [1, 3, null]
// ---------------------------------------------------------------------------

describe('estimateUsageCost — unknown model falls back to DEFAULT_PRICING', () => {
  it('prices an unknown model at input 1/Mtok, output 3/Mtok, cached 0.1/Mtok', () => {
    const cost = estimateUsageCost('totally-made-up-model-9000', {
      uncachedPromptTokens: 1_000_000,
      cachedPromptTokens: 1_000_000,
      cacheCreationTokens: 0,
      completionTokens: 1_000_000,
    });
    // input 1 + cached (1*0.1) + output 3 = 4.1
    assert.ok(Math.abs(cost - 4.1) < 1e-6, `expected 4.1, got ${cost}`);
  });

  it('empty model string still resolves to DEFAULT_PRICING (non-negative, documented)', () => {
    const cost = estimateUsageCost('', { ...ZERO, uncachedPromptTokens: 1_000_000 });
    assert.ok(Math.abs(cost - 1) < 1e-6, `expected 1, got ${cost}`);
  });

  it('substring match beats default (claude-opus-4-20250514 prices as claude-opus-4)', () => {
    const viaSub = estimateUsageCost('claude-opus-4-20250514', { ...ZERO, uncachedPromptTokens: 1_000_000 });
    const viaExact = estimateUsageCost('claude-opus-4', { ...ZERO, uncachedPromptTokens: 1_000_000 });
    assert.equal(viaSub, viaExact);
    assert.ok(viaSub > 4, 'opus input (15) must exceed the default (1)');
  });
});

// ---------------------------------------------------------------------------
// isPricedModel
// ---------------------------------------------------------------------------

describe('isPricedModel', () => {
  it('true for every exact fallback-table key', () => {
    for (const m of ['gpt-4o', 'gpt-4o-mini', 'claude-sonnet-4', 'claude-opus-4', 'o1', 'o3-mini', 'gemini-3']) {
      assert.equal(isPricedModel(m), true, `${m} should be priced`);
    }
  });

  it('true for substring matches of a table key', () => {
    assert.equal(isPricedModel('anthropic/claude-opus-4-latest'), true);
    assert.equal(isPricedModel('openai/gpt-4o-2024-08-06'), true);
  });

  it('false for unknown model and empty/undefined-like inputs', () => {
    assert.equal(isPricedModel('nonexistent-model-zzz'), false);
    assert.equal(isPricedModel(''), false);
    assert.equal(isPricedModel(undefined as unknown as string), false);
  });

  it('agrees with estimateUsageCost: unpriced models get the generic default rate', () => {
    assert.equal(isPricedModel('some-random-model'), false);
    const defaultInput = estimateUsageCost('some-random-model', { ...ZERO, uncachedPromptTokens: 1_000_000 });
    assert.ok(Math.abs(defaultInput - 1) < 1e-9, 'unpriced -> default 1/Mtok');
  });
});

// ---------------------------------------------------------------------------
// CostTracker — accumulation
// ---------------------------------------------------------------------------

describe('CostTracker — accumulation across calls', () => {
  it('totalCost after N calls equals the sum of estimateUsageCost for each (single source of truth)', () => {
    const t = new CostTracker('gpt-4o');
    const calls = [
      { promptTokens: 1000, completionTokens: 500 },
      { promptTokens: 2000, completionTokens: 800, cachedPromptTokens: 500 },
      { promptTokens: 3000, completionTokens: 1200, cachedPromptTokens: 1000, cacheCreationTokens: 400 },
    ];
    let expectedCost = 0;
    for (const c of calls) {
      const cached = c.cachedPromptTokens ?? 0;
      const create = c.cacheCreationTokens ?? 0;
      expectedCost += estimateUsageCost('gpt-4o', {
        uncachedPromptTokens: c.promptTokens - cached,
        cachedPromptTokens: cached,
        cacheCreationTokens: create,
        completionTokens: c.completionTokens,
      });
      t.trackUsage(c);
    }
    const s = t.getStats();
    assert.ok(Math.abs(s.totalCost - expectedCost) < 1e-9, `cost ${s.totalCost} != ${expectedCost}`);
    assert.equal(s.promptTokens, 6000);
    assert.equal(s.completionTokens, 2500);
    assert.equal(s.totalTokens, 8500);
    assert.equal(s.requestCount, 3);
    assert.equal(s.cachedPromptTokens, 1500);
    assert.equal(s.cacheCreationTokens, 400);
    assert.equal(s.uncachedPromptTokens, 6000 - 1500);
  });

  it('cacheHitRate is cached / (cached + uncached) and 0 when no prompt tokens', () => {
    const empty = new CostTracker('gpt-4o');
    assert.equal(empty.getStats().cacheHitRate, 0);

    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 1000, completionTokens: 0, cachedPromptTokens: 250 });
    assert.ok(Math.abs(t.getStats().cacheHitRate - 0.25) < 1e-9);
  });

  it('cachedCostSavings is positive when cache is used and 0 otherwise', () => {
    const none = new CostTracker('gpt-4o');
    none.trackUsage({ promptTokens: 1000, completionTokens: 0 });
    assert.equal(none.getStats().cachedCostSavings, 0);

    const some = new CostTracker('gpt-4o');
    some.trackUsage({ promptTokens: 1_000_000, completionTokens: 0, cachedPromptTokens: 1_000_000 });
    // savings = full input (2.5) - cached (2.5*0.1=0.25) = 2.25
    assert.ok(Math.abs(some.getStats().cachedCostSavings - 2.25) < 1e-6, `got ${some.getStats().cachedCostSavings}`);
  });

  it('tool-call categorization accumulates byCategory', () => {
    const t = new CostTracker('gpt-4o');
    t.trackToolCall('bash');
    t.trackToolCall('skill');
    t.trackToolCall('mcp__server__tool');
    t.trackToolCall('task');
    t.trackToolCall(''); // no-op for category, still counts
    const s = t.getStats();
    assert.equal(s.toolCallCount, 5);
    assert.equal(s.byCategory?.builtin, 1);
    assert.equal(s.byCategory?.skill, 1);
    assert.equal(s.byCategory?.mcp, 1);
    assert.equal(s.byCategory?.subagent, 1);
  });

  it('trackTurn increments turnCount; reset() zeroes everything', () => {
    const t = new CostTracker('gpt-4o');
    t.trackUsage({ promptTokens: 1000, completionTokens: 500, cachedPromptTokens: 100 });
    t.trackToolCall('bash');
    t.trackTurn();
    t.trackTurn();
    assert.equal(t.getStats().turnCount, 2);
    t.reset();
    const s = t.getStats();
    assert.equal(s.totalCost, 0);
    assert.equal(s.promptTokens, 0);
    assert.equal(s.requestCount, 0);
    assert.equal(s.toolCallCount, 0);
    assert.equal(s.turnCount, 0);
    assert.equal(s.cachedPromptTokens, 0);
    assert.equal(s.cacheCreationTokens, 0);
    assert.equal(s.cachedCostSavings, 0);
    assert.deepEqual(s.byCategory, {});
  });

  it('getStats returns an independent byCategory snapshot (not a live reference)', () => {
    const t = new CostTracker('gpt-4o');
    t.trackToolCall('bash');
    const first = t.getStats().byCategory!;
    t.trackToolCall('bash');
    assert.equal(first.builtin, 1, 'earlier snapshot must not mutate');
    assert.equal(t.getStats().byCategory?.builtin, 2);
  });
});

describe('categorizeTool', () => {
  it('maps names to the four buckets', () => {
    assert.equal(categorizeTool('mcp__srv__x'), 'mcp');
    assert.equal(categorizeTool('server:tool'), 'mcp');
    assert.equal(categorizeTool('task'), 'subagent');
    assert.equal(categorizeTool('task_run'), 'subagent');
    assert.equal(categorizeTool('workflow'), 'subagent');
    assert.equal(categorizeTool('skill'), 'skill');
    assert.equal(categorizeTool('SkillThing'), 'skill');
    assert.equal(categorizeTool('bash'), 'builtin');
    assert.equal(categorizeTool('file_edit'), 'builtin');
  });
});

// ---------------------------------------------------------------------------
// fingerprint
// ---------------------------------------------------------------------------

describe('fingerprint — stability + format', () => {
  it('is deterministic for identical input', () => {
    const a = fingerprint('hello world this is a prompt');
    const b = fingerprint('hello world this is a prompt');
    assert.equal(a, b);
  });

  it('format is "<length>:<unsigned-hash>"', () => {
    const s = 'the quick brown fox';
    const fp = fingerprint(s);
    const [len, hash] = fp.split(':');
    assert.equal(Number(len), s.length);
    assert.ok(/^\d+$/.test(hash), `hash part should be unsigned integer, got ${hash}`);
    assert.ok(Number(hash) >= 0);
  });

  it('differs for different content of the same length', () => {
    assert.notEqual(fingerprint('aaaa'), fingerprint('bbbb'));
  });

  it('encodes length even when head+tail sample collides (long strings)', () => {
    const head = 'H'.repeat(100);
    const tail = 'T'.repeat(100);
    const short = head + tail;
    const long = head + 'X'.repeat(5000) + tail;
    // Same 100-char head and tail -> same sampled hash, but lengths differ -> fp differs.
    assert.notEqual(fingerprint(short), fingerprint(long));
    assert.equal(fingerprint(short).split(':')[1], fingerprint(long).split(':')[1]);
  });

  it('handles empty string', () => {
    assert.equal(fingerprint(''), '0:0');
  });
});

// ---------------------------------------------------------------------------
// CACHE_MISS_CAUSES constant
// ---------------------------------------------------------------------------

describe('CACHE_MISS_CAUSES', () => {
  it('contains the documented causes with no duplicates', () => {
    const expected: CacheMissCause[] = [
      'tool_definitions_changed',
      'system_prompt_changed',
      'idle_past_ttl',
      'thinking_mode_changed',
      'model_switched',
      'first_request',
      'unknown',
    ];
    assert.deepEqual([...CACHE_MISS_CAUSES], expected);
    assert.equal(new Set(CACHE_MISS_CAUSES).size, CACHE_MISS_CAUSES.length);
  });
});

// ---------------------------------------------------------------------------
// CacheMissDiagnostics — classification
// ---------------------------------------------------------------------------

const baseReq = (): CacheDiagRequest => ({
  toolDefHash: 'tools-v1',
  systemPromptHash: 'sys-v1',
  thinkingMode: 'off',
  model: 'gpt-4o',
});

/** Establish a cache HIT baseline so `prev` is set and isFirstRequest is false. */
function primeHit(d: CacheMissDiagnostics, req = baseReq()): void {
  d.recordRequest(req);
  d.recordResponse(5, 0); // cacheReadTokens > 0 -> hit, not a miss
}

describe('CacheMissDiagnostics — cause classification', () => {
  it('first miss with no history -> first_request', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(baseReq());
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'first_request');
    assert.equal(d.getTotalMisses(), 1);
  });

  it('cache hit records NO miss (lastCause stays null)', () => {
    const d = new CacheMissDiagnostics();
    d.recordRequest(baseReq());
    d.recordResponse(10, 0);
    assert.equal(d.getLastMissCause(), null);
    assert.equal(d.getTotalMisses(), 0);
  });

  it('model change dominates all other diffs -> model_switched', () => {
    const d = new CacheMissDiagnostics();
    primeHit(d);
    d.recordRequest({ toolDefHash: 'x', systemPromptHash: 'y', thinkingMode: 'high', model: 'claude-opus-4' });
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'model_switched');
  });

  it('idle past TTL (same model) -> idle_past_ttl, beating tool/prompt/thinking diffs', () => {
    const d = new CacheMissDiagnostics(1_000); // 1s TTL
    primeHit(d);
    d._setLastRequestTime(1); // pretend the prior request was long ago
    d.recordRequest({ ...baseReq(), toolDefHash: 'tools-v2' }); // pending time = now (huge elapsed)
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'idle_past_ttl');
  });

  it('tool definitions changed (same model, within TTL) -> tool_definitions_changed', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.recordRequest({ ...baseReq(), toolDefHash: 'tools-v2' });
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'tool_definitions_changed');
  });

  it('system prompt changed (tools same) -> system_prompt_changed', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.recordRequest({ ...baseReq(), systemPromptHash: 'sys-v2' });
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'system_prompt_changed');
  });

  it('thinking mode changed (tools + prompt same) -> thinking_mode_changed', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.recordRequest({ ...baseReq(), thinkingMode: 'high' });
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'thinking_mode_changed');
  });

  it('identical request but still a miss -> unknown', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.recordRequest(baseReq());
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'unknown');
  });

  it('recordResponse with no pending recordRequest is a no-op', () => {
    const d = new CacheMissDiagnostics();
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), null);
    assert.equal(d.getTotalMisses(), 0);
  });

  it('accumulates counts per cause and reports the histogram', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    // two tool-def misses
    d.recordRequest({ ...baseReq(), toolDefHash: 'a' });
    d.recordResponse(0, 0);
    d.recordRequest({ ...baseReq(), toolDefHash: 'b' });
    d.recordResponse(0, 0);
    // one unknown miss (identical to prev 'b')
    d.recordRequest({ ...baseReq(), toolDefHash: 'b' });
    d.recordResponse(0, 0);
    const hist = d.getCacheMissCauses();
    assert.equal(hist.get('tool_definitions_changed'), 2);
    assert.equal(hist.get('unknown'), 1);
    assert.equal(d.getTotalMisses(), 3);
    // returned map is a copy — mutating it must not affect the tracker
    hist.set('unknown', 99);
    assert.equal(d.getCacheMissCauses().get('unknown'), 1);
  });

  it('reset() clears history back to first-request behavior', () => {
    const d = new CacheMissDiagnostics();
    primeHit(d);
    d.recordRequest({ ...baseReq(), model: 'o1' });
    d.recordResponse(0, 0);
    assert.equal(d.getTotalMisses(), 1);
    d.reset();
    assert.equal(d.getTotalMisses(), 0);
    assert.equal(d.getLastMissCause(), null);
    // after reset, the next miss is treated as the first again
    d.recordRequest(baseReq());
    d.recordResponse(0, 0);
    assert.equal(d.getLastMissCause(), 'first_request');
  });

  it('every recorded cause is a member of CACHE_MISS_CAUSES', () => {
    const d = new CacheMissDiagnostics(60 * 60 * 1000);
    primeHit(d);
    d.recordRequest({ ...baseReq(), systemPromptHash: 'changed' });
    d.recordResponse(0, 0);
    const cause = d.getLastMissCause();
    assert.ok(cause && (CACHE_MISS_CAUSES as readonly string[]).includes(cause));
  });
});
