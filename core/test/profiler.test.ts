import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { Profiler, computeStatistics, percentile } from '../src/profiler';

// ---------------------------------------------------------------------------
// start / stop
// ---------------------------------------------------------------------------
describe('Profiler – start/stop', () => {
  it('start(label) returns a stop function', () => {
    const p = new Profiler();
    const stop = p.start('test');
    assert.equal(typeof stop, 'function');
  });

  it('calling stop function records the duration', () => {
    const p = new Profiler();
    const stop = p.start('op');
    stop();
    const summary = p.getSummary();
    assert.ok('op' in summary);
    assert.equal(summary['op'].count, 1);
    assert.equal(typeof summary['op'].totalMs, 'number');
  });
});

// ---------------------------------------------------------------------------
// getSummary
// ---------------------------------------------------------------------------
describe('Profiler – getSummary', () => {
  it('shows count, avgMs, totalMs', () => {
    const p = new Profiler();
    p.record('op', 10);
    p.record('op', 20);
    p.record('op', 30);

    const summary = p.getSummary();
    assert.equal(summary['op'].count, 3);
    assert.equal(summary['op'].totalMs, 60);
    assert.equal(summary['op'].avgMs, 20);
  });

  it('multiple calls to same label accumulate', () => {
    const p = new Profiler();
    const s1 = p.start('x');
    s1();
    const s2 = p.start('x');
    s2();

    const summary = p.getSummary();
    assert.equal(summary['x'].count, 2);
  });

  it('different labels tracked separately', () => {
    const p = new Profiler();
    p.record('a', 100);
    p.record('b', 200);

    const summary = p.getSummary();
    assert.ok('a' in summary);
    assert.ok('b' in summary);
    assert.equal(summary['a'].totalMs, 100);
    assert.equal(summary['b'].totalMs, 200);
  });
});

// ---------------------------------------------------------------------------
// reset
// ---------------------------------------------------------------------------
describe('Profiler – reset', () => {
  it('clears all timings', () => {
    const p = new Profiler();
    p.record('a', 10);
    p.record('b', 20);
    p.reset();

    const summary = p.getSummary();
    assert.deepEqual(summary, {});
  });

  it('clears TTFT timings', () => {
    const p = new Profiler();
    p.recordTtft('api', 100);
    p.reset();
    assert.equal(p.getTtftStatistics('api'), null);
  });

  it('clears turn data', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, { apiLatency: 100, toolExecutionTime: 50, compactionTime: 0, tokensProduced: 100, tokensConsumed: 200, ttftMs: 20 });
    p.reset();
    assert.deepEqual(p.getTurns(), []);
  });
});

// ---------------------------------------------------------------------------
// record
// ---------------------------------------------------------------------------
describe('Profiler – record', () => {
  it('records pre-measured durations', () => {
    const p = new Profiler();
    p.record('manual', 42);
    const summary = p.getSummary();
    assert.equal(summary['manual'].count, 1);
    assert.equal(summary['manual'].totalMs, 42);
    assert.equal(summary['manual'].avgMs, 42);
  });
});

// ---------------------------------------------------------------------------
// getStatistics
// ---------------------------------------------------------------------------
describe('Profiler – getStatistics', () => {
  it('returns null for unknown label', () => {
    const p = new Profiler();
    assert.equal(p.getStatistics('nonexistent'), null);
  });

  it('returns full statistics for a label', () => {
    const p = new Profiler();
    for (let i = 1; i <= 100; i++) p.record('api', i);

    const stats = p.getStatistics('api');
    assert.ok(stats);
    assert.equal(stats.count, 100);
    assert.equal(stats.minMs, 1);
    assert.equal(stats.maxMs, 100);
    assert.equal(stats.totalMs, 5050);
    assert.equal(stats.p50Ms, 50);
    assert.equal(stats.p95Ms, 95);
    assert.equal(stats.p99Ms, 99);
    assert.ok(stats.stddevMs > 0);
  });

  it('handles single value', () => {
    const p = new Profiler();
    p.record('single', 42);
    const stats = p.getStatistics('single');
    assert.ok(stats);
    assert.equal(stats.count, 1);
    assert.equal(stats.minMs, 42);
    assert.equal(stats.maxMs, 42);
    assert.equal(stats.p50Ms, 42);
    assert.equal(stats.p95Ms, 42);
    assert.equal(stats.stddevMs, 0);
  });
});

// ---------------------------------------------------------------------------
// getAllStatistics
// ---------------------------------------------------------------------------
describe('Profiler – getAllStatistics', () => {
  it('returns statistics for all labels', () => {
    const p = new Profiler();
    p.record('a', 10);
    p.record('a', 20);
    p.record('b', 100);

    const all = p.getAllStatistics();
    assert.ok('a' in all);
    assert.ok('b' in all);
    assert.equal(all['a'].count, 2);
    assert.equal(all['b'].count, 1);
  });

  it('returns empty object for empty profiler', () => {
    const p = new Profiler();
    assert.deepEqual(p.getAllStatistics(), {});
  });
});

// ---------------------------------------------------------------------------
// getRawTimings / getRawDurations
// ---------------------------------------------------------------------------
describe('Profiler – raw timings access', () => {
  it('getRawTimings returns a copy of all timings', () => {
    const p = new Profiler();
    p.record('a', 10);
    p.record('a', 20);
    p.record('b', 30);

    const raw = p.getRawTimings();
    assert.deepEqual(raw.get('a'), [10, 20]);
    assert.deepEqual(raw.get('b'), [30]);

    // Mutating the returned map doesn't affect the profiler
    raw.get('a')!.push(999);
    assert.deepEqual(p.getRawDurations('a'), [10, 20]);
  });

  it('getRawDurations returns copy for single label', () => {
    const p = new Profiler();
    p.record('x', 5);
    p.record('x', 15);

    const raw = p.getRawDurations('x');
    assert.deepEqual(raw, [5, 15]);

    raw.push(999);
    assert.deepEqual(p.getRawDurations('x'), [5, 15]);
  });

  it('getRawDurations returns empty array for unknown label', () => {
    const p = new Profiler();
    assert.deepEqual(p.getRawDurations('nope'), []);
  });
});

// ---------------------------------------------------------------------------
// TTFT tracking
// ---------------------------------------------------------------------------
describe('Profiler – TTFT tracking', () => {
  it('recordTtft stores time-to-first-token values', () => {
    const p = new Profiler();
    p.recordTtft('api_call', 120);
    p.recordTtft('api_call', 150);
    p.recordTtft('api_call', 200);

    const stats = p.getTtftStatistics('api_call');
    assert.ok(stats);
    assert.equal(stats.count, 3);
    assert.equal(stats.minMs, 120);
    assert.equal(stats.maxMs, 200);
  });

  it('getTtftStatistics returns null for untracked label', () => {
    const p = new Profiler();
    assert.equal(p.getTtftStatistics('nothing'), null);
  });

  it('getRawTtft returns copy of all TTFT timings', () => {
    const p = new Profiler();
    p.recordTtft('a', 50);
    p.recordTtft('b', 100);

    const raw = p.getRawTtft();
    assert.deepEqual(raw.get('a'), [50]);
    assert.deepEqual(raw.get('b'), [100]);

    raw.get('a')!.push(999);
    const fresh = p.getRawTtft();
    assert.deepEqual(fresh.get('a'), [50]);
  });

  it('TTFT is independent from regular timings', () => {
    const p = new Profiler();
    p.record('api_call', 500);
    p.recordTtft('api_call', 120);

    const summary = p.getSummary();
    assert.equal(summary['api_call'].totalMs, 500);

    const ttft = p.getTtftStatistics('api_call');
    assert.ok(ttft);
    assert.equal(ttft.totalMs, 120);
  });
});

// ---------------------------------------------------------------------------
// Per-turn profiling
// ---------------------------------------------------------------------------
describe('Profiler – per-turn profiling', () => {
  it('beginTurn + endTurn records turn timing', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, {
      apiLatency: 500,
      toolExecutionTime: 200,
      compactionTime: 50,
      tokensProduced: 100,
      tokensConsumed: 500,
      ttftMs: 80,
    });

    const turns = p.getTurns();
    assert.equal(turns.length, 1);
    assert.equal(turns[0].turnIndex, 0);
    assert.equal(turns[0].apiLatency, 500);
    assert.equal(turns[0].toolExecutionTime, 200);
    assert.equal(turns[0].compactionTime, 50);
    assert.equal(turns[0].tokensProduced, 100);
    assert.equal(turns[0].tokensConsumed, 500);
    assert.equal(turns[0].ttftMs, 80);
    assert.ok(turns[0].duration >= 0);
    assert.ok(turns[0].startTime > 0);
    assert.ok(turns[0].endTime >= turns[0].startTime);
  });

  it('computes outputTokensPerSec from streaming duration', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, {
      apiLatency: 1000,
      toolExecutionTime: 0,
      compactionTime: 0,
      tokensProduced: 180,
      tokensConsumed: 500,
      ttftMs: 100,
    });

    const turn = p.getTurn(0);
    assert.ok(turn);
    // streamingDuration = 1000 - 100 = 900ms = 0.9s
    // outputTokensPerSec = 180 / 0.9 = 200
    assert.equal(turn.outputTokensPerSec, 200);
  });

  it('outputTokensPerSec is 0 when streaming duration is 0', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, {
      apiLatency: 100,
      toolExecutionTime: 0,
      compactionTime: 0,
      tokensProduced: 50,
      tokensConsumed: 200,
      ttftMs: 100,
    });

    const turn = p.getTurn(0);
    assert.ok(turn);
    assert.equal(turn.outputTokensPerSec, 0);
  });

  it('multiple turns are sorted by index', () => {
    const p = new Profiler();
    const metrics = { apiLatency: 100, toolExecutionTime: 50, compactionTime: 0, tokensProduced: 50, tokensConsumed: 200, ttftMs: 20 };

    p.beginTurn(2);
    p.endTurn(2, metrics);
    p.beginTurn(0);
    p.endTurn(0, metrics);
    p.beginTurn(1);
    p.endTurn(1, metrics);

    const turns = p.getTurns();
    assert.equal(turns.length, 3);
    assert.equal(turns[0].turnIndex, 0);
    assert.equal(turns[1].turnIndex, 1);
    assert.equal(turns[2].turnIndex, 2);
  });

  it('getTurn returns undefined for non-existent turn', () => {
    const p = new Profiler();
    assert.equal(p.getTurn(99), undefined);
  });
});

// ---------------------------------------------------------------------------
// computeStatistics standalone
// ---------------------------------------------------------------------------
describe('computeStatistics', () => {
  it('handles empty array', () => {
    const stats = computeStatistics([]);
    assert.equal(stats.count, 0);
    assert.equal(stats.avgMs, 0);
    assert.equal(stats.totalMs, 0);
    assert.equal(stats.stddevMs, 0);
  });

  it('computes correct values for uniform distribution', () => {
    const durations = Array.from({ length: 100 }, (_, i) => i + 1);
    const stats = computeStatistics(durations);
    assert.equal(stats.count, 100);
    assert.equal(stats.minMs, 1);
    assert.equal(stats.maxMs, 100);
    assert.equal(stats.p50Ms, 50);
    assert.equal(stats.p75Ms, 75);
    assert.equal(stats.p95Ms, 95);
    assert.equal(stats.p99Ms, 99);
    assert.ok(Math.abs(stats.avgMs - 50.5) < 0.1);
    assert.ok(stats.stddevMs > 28 && stats.stddevMs < 30);
  });

  it('computes correct stddev for identical values', () => {
    const stats = computeStatistics([5, 5, 5, 5, 5]);
    assert.equal(stats.stddevMs, 0);
    assert.equal(stats.minMs, 5);
    assert.equal(stats.maxMs, 5);
  });

  it('does not mutate input array', () => {
    const input = [30, 10, 20];
    computeStatistics(input);
    assert.deepEqual(input, [30, 10, 20]);
  });
});

// ---------------------------------------------------------------------------
// percentile standalone
// ---------------------------------------------------------------------------
describe('percentile', () => {
  it('returns 0 for empty array', () => {
    assert.equal(percentile([], 50), 0);
  });

  it('returns the single value for single-element array', () => {
    assert.equal(percentile([42], 50), 42);
    assert.equal(percentile([42], 99), 42);
  });

  it('computes P50 of sorted array', () => {
    const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    assert.equal(percentile(sorted, 50), 5);
  });

  it('computes P95 of sorted array', () => {
    const sorted = Array.from({ length: 20 }, (_, i) => i + 1);
    assert.equal(percentile(sorted, 95), 19);
  });

  it('P0 returns first element', () => {
    const sorted = [1, 2, 3, 4, 5];
    assert.equal(percentile(sorted, 0), 1);
  });

  it('P100 returns last element', () => {
    const sorted = [1, 2, 3, 4, 5];
    assert.equal(percentile(sorted, 100), 5);
  });
});

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------
describe('Profiler – edge cases', () => {
  it('very fast operations (< 1ms) are tracked', () => {
    const p = new Profiler();
    p.record('fast', 0);
    p.record('fast', 0.5);

    const summary = p.getSummary();
    assert.equal(summary['fast'].count, 2);
    assert.equal(summary['fast'].totalMs, 0.5);
  });

  it('empty profiler returns empty summary', () => {
    const p = new Profiler();
    const summary = p.getSummary();
    assert.deepEqual(summary, {});
  });

  it('concurrent timings do not interfere', () => {
    const p = new Profiler();
    const stopA = p.start('a');
    const stopB = p.start('b');

    p.record('a', 10);
    p.record('b', 20);

    stopA();
    stopB();

    const summary = p.getSummary();
    assert.equal(summary['a'].count, 2);
    assert.equal(summary['b'].count, 2);
    assert.ok(summary['a'].totalMs >= 10);
    assert.ok(summary['b'].totalMs >= 20);
  });

  it('statistics handle large datasets efficiently', () => {
    const p = new Profiler();
    for (let i = 0; i < 10000; i++) {
      p.record('load', Math.random() * 5000);
    }
    const stats = p.getStatistics('load');
    assert.ok(stats);
    assert.equal(stats.count, 10000);
    assert.ok(stats.p95Ms > stats.p50Ms);
    assert.ok(stats.p99Ms >= stats.p95Ms);
    assert.ok(stats.maxMs >= stats.p99Ms);
  });
});
