/**
 * profiler-bounded — profiler parity additions:
 *  - bounded-memory timing storage (reservoir) with EXACT count/sum/min/max
 *  - bounded per-turn retention (maxTurns eviction)
 *  - start/end phase-pair duration derivation (getPhaseSummary)
 *  - richer per-turn metrics (cache read/creation tokens, cache hit rate, input TPS)
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { Profiler } from '../src/profiler';

describe('bounded-memory timing storage', () => {
  it('caps retained samples while keeping count/sum/min/max exact', () => {
    const p = new Profiler({ maxSamples: 10, rand: () => 0 });
    for (let i = 1; i <= 100; i++) p.record('op', i);
    assert.ok(p.getRawDurations('op').length <= 10, 'raw samples bounded by maxSamples');
    const s = p.getStatistics('op')!;
    assert.equal(s.count, 100, 'count is exact (not sample size)');
    assert.equal(s.totalMs, 5050, 'sum is exact');
    assert.equal(s.minMs, 1, 'min is exact');
    assert.equal(s.maxMs, 100, 'max is exact');
    assert.ok(s.p50Ms > 0);
  });

  it('getSummary reports exact count/total under bounding', () => {
    const p = new Profiler({ maxSamples: 3, rand: () => 0 });
    for (let i = 0; i < 50; i++) p.record('x', 2);
    assert.deepEqual(p.getSummary().x, { count: 50, avgMs: 2, totalMs: 100 });
  });

  it('bounds TTFT samples too', () => {
    const p = new Profiler({ maxSamples: 4, rand: () => 0 });
    for (let i = 0; i < 20; i++) p.recordTtft('api', 100 + i);
    assert.ok((p.getRawTtft().get('api') ?? []).length <= 4);
    assert.equal(p.getTtftStatistics('api')!.count, 20);
  });
});

describe('bounded per-turn retention', () => {
  it('evicts the oldest turns past maxTurns', () => {
    const p = new Profiler({ maxTurns: 2 });
    const m = { apiLatency: 10, toolExecutionTime: 0, compactionTime: 0, tokensProduced: 1, tokensConsumed: 1, ttftMs: 1 };
    p.endTurn(0, m);
    p.endTurn(1, m);
    p.endTurn(2, m);
    const turns = p.getTurns().map(t => t.turnIndex);
    assert.deepEqual(turns, [1, 2]);
    assert.equal(p.getTurn(0), undefined);
  });
});

describe('getPhaseSummary', () => {
  it('pairs <phase>_start / <phase>_end into durations and omits unpaired', () => {
    const p = new Profiler();
    p.mark('setup_start');
    p.mark('setup_end');
    p.mark('network_start'); // no matching end
    const phases = p.getPhaseSummary();
    assert.ok('setup' in phases);
    assert.ok(phases.setup >= 0);
    assert.equal('network' in phases, false);
  });

  it('returns empty when there are no paired marks', () => {
    assert.deepEqual(new Profiler().getPhaseSummary(), {});
  });
});

describe('richer per-turn metrics', () => {
  it('derives cache hit rate and input TPS from cache token fields', () => {
    const p = new Profiler();
    p.endTurn(0, {
      apiLatency: 2000, toolExecutionTime: 0, compactionTime: 0,
      tokensProduced: 100, tokensConsumed: 300, ttftMs: 500,
      cacheReadTokens: 700, cacheCreationTokens: 50,
    });
    const t = p.getTurn(0)!;
    assert.equal(t.cacheReadTokens, 700);
    assert.equal(t.cacheCreationTokens, 50);
    // promptTokens = consumed(300) + cacheRead(700) = 1000 -> hit rate 0.7
    assert.equal(t.cacheHitRate, 0.7);
    // input TPS = 300 / (2000ms/1000) = 150
    assert.equal(t.inputTokensPerSec, 150);
  });

  it('defaults cache fields to 0 when omitted (cache hit rate 0)', () => {
    const p = new Profiler();
    p.endTurn(0, { apiLatency: 0, toolExecutionTime: 0, compactionTime: 0, tokensProduced: 0, tokensConsumed: 0, ttftMs: 0 });
    const t = p.getTurn(0)!;
    assert.equal(t.cacheReadTokens, 0);
    assert.equal(t.cacheHitRate, 0);
    assert.equal(t.inputTokensPerSec, 0);
  });
});
