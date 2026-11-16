import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { Profiler, FpsTracker, slowWarning } from '../src/profiler';

describe('Profiler.timeAsync', () => {
  it('records duration and returns the result on success', async () => {
    const p = new Profiler();
    const out = await p.timeAsync('work', async () => 42);
    assert.equal(out, 42);
    assert.equal(p.getStatistics('work')!.count, 1);
  });

  it('records duration on failure, bumps a failed counter, and re-throws', async () => {
    const p = new Profiler();
    await assert.rejects(p.timeAsync('work', async () => { throw new Error('boom'); }), /boom/);
    assert.equal(p.getStatistics('work')!.count, 1);
    assert.equal(p.getCounter('work.failed'), 1);
  });
});

describe('Profiler slow-operation detection', () => {
  it('flags operations over the default threshold', () => {
    const p = new Profiler({ slowThresholdMs: 100 });
    p.record('fast', 50);
    p.record('slow', 500);
    const slow = p.getSlowOperations();
    assert.equal(slow.length, 1);
    assert.equal(slow[0].operation, 'slow');
    assert.equal(slow[0].durationMs, 500);
    assert.equal(slow[0].thresholdMs, 100);
  });

  it('honours per-label thresholds and bounds the retained list', () => {
    const p = new Profiler({ slowThresholdMs: 1000, maxSlowOps: 3 });
    p.setSlowThreshold(10, 'db');
    p.record('db', 50); // over per-label threshold
    assert.equal(p.getSlowOperations().length, 1);
    for (let i = 0; i < 10; i++) p.record('db', 50);
    assert.equal(p.getSlowOperations().length, 3); // capped
    assert.equal(p.getSlowOperations(1).length, 1);
  });
});

describe('Profiler.record poison-proofing', () => {
  it('rejects NaN durations entirely so aggregates are never poisoned', () => {
    const p = new Profiler();
    p.record('x', NaN);
    assert.equal(p.getStatistics('x'), null); // nothing was recorded
    p.record('x', 100);
    assert.equal(p.getStatistics('x')!.count, 1);
  });

  it('clamps negative durations to 0 so min/max never go below 0', () => {
    const p = new Profiler();
    p.record('x', -50);
    p.record('x', 100);
    const s = p.getStatistics('x')!;
    assert.equal(s.count, 2);
    assert.equal(s.minMs, 0);
    assert.ok(s.minMs >= 0);
  });
});

describe('Profiler monotonic clock', () => {
  it('start() cannot record a negative duration even if the wall clock steps backward', () => {
    const p = new Profiler();
    const origNow = Date.now;
    let calls = 0;
    (Date as unknown as { now: () => number }).now = () => (calls++ === 0 ? 10_000 : 1_000);
    try {
      const stop = p.start('op');
      stop();
    } finally {
      (Date as unknown as { now: () => number }).now = origNow;
    }
    const s = p.getStatistics('op')!;
    assert.ok(s.minMs >= 0, 'elapsed must never be negative');
  });
});

describe('Profiler endTurn boundary guard', () => {
  const metrics = { apiLatency: 100, toolExecutionTime: 0, compactionTime: 0, tokensProduced: 0, tokensConsumed: 0, ttftMs: 0 };

  it('flags a turn incomplete with zero duration when beginTurn was skipped', () => {
    const p = new Profiler();
    p.endTurn(5, metrics);
    const t = p.getTurn(5)!;
    assert.equal(t.incomplete, true);
    assert.equal(t.duration, 0);
  });

  it('marks a properly-bracketed turn complete', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, metrics);
    assert.equal(p.getTurn(0)!.incomplete, false);
  });

  it('clamps turn duration to 0 if the wall clock steps backward mid-turn', () => {
    const p = new Profiler();
    const origNow = Date.now;
    let calls = 0;
    // beginTurn sees a later time than endTurn (backward step).
    (Date as unknown as { now: () => number }).now = () => (calls++ === 0 ? 10_000 : 1_000);
    try {
      p.beginTurn(0);
      p.endTurn(0, metrics);
    } finally {
      (Date as unknown as { now: () => number }).now = origNow;
    }
    const t = p.getTurn(0)!;
    assert.equal(t.incomplete, false);
    assert.ok(t.duration >= 0);
    assert.equal(t.duration, 0);
  });
});

describe('Profiler memory-annotated timeline', () => {
  it('builds cumulative + delta + memory rows from checkpoints', () => {
    let t = 1000;
    let rss = 100;
    const p = new Profiler({ memSnapshot: () => ({ rss: (rss += 10) * 1_048_576, heapUsed: 5 * 1_048_576 }) });
    const origNow = Date.now;
    (Date as unknown as { now: () => number }).now = () => t;
    try {
      p.mark('start'); t += 100;
      p.mark('mid'); t += 250;
      p.mark('end');
    } finally {
      (Date as unknown as { now: () => number }).now = origNow;
    }
    const tl = p.getTimeline();
    assert.equal(tl.length, 3);
    assert.equal(tl[0].cumulativeMs, 0);
    assert.equal(tl[1].cumulativeMs, 100);
    assert.equal(tl[1].deltaMs, 100);
    assert.equal(tl[2].cumulativeMs, 350);
    assert.equal(tl[2].deltaMs, 250);
    assert.ok(tl[0].rssBytes > 0);
    assert.match(p.formatTimeline(), /start/);
  });

  it('empty timeline is handled', () => {
    const p = new Profiler();
    assert.deepEqual(p.getTimeline(), []);
    assert.equal(p.formatTimeline(), '(no checkpoints)');
  });
});

describe('Profiler counter/gauge/set metrics', () => {
  it('tracks counters, gauges and distinct sets', () => {
    const p = new Profiler();
    p.incrementCounter('hits');
    p.incrementCounter('hits', 4);
    p.setGauge('queue', 7);
    p.addToSet('users', 'a');
    p.addToSet('users', 'a');
    p.addToSet('users', 'b');
    assert.equal(p.getCounter('hits'), 5);
    assert.equal(p.getGauge('queue'), 7);
    assert.equal(p.getGauge('missing'), undefined);
    assert.equal(p.getSetSize('users'), 2);
    assert.deepEqual(p.getMetrics(), { counters: { hits: 5 }, gauges: { queue: 7 }, sets: { users: 2 } });
  });

  it('reset clears the new state', () => {
    const p = new Profiler({ slowThresholdMs: 1 });
    p.record('x', 100);
    p.mark('m');
    p.incrementCounter('c');
    p.reset();
    assert.deepEqual(p.getSlowOperations(), []);
    assert.deepEqual(p.getTimeline(), []);
    assert.equal(p.getCounter('c'), 0);
  });
});

describe('FpsTracker', () => {
  it('computes average and 1% low FPS from frame durations', () => {
    const f = new FpsTracker();
    // 99 frames at 16ms (≈60fps) + one slow 100ms frame
    for (let i = 0; i < 99; i++) f.record(16);
    f.record(100);
    const m = f.getMetrics();
    assert.ok(m.averageFps > 50 && m.averageFps < 62);
    assert.ok(m.low1PctFps < m.averageFps, '1% low should be below average');
    assert.ok(m.low1PctFps > 0);
  });

  it('ignores non-positive durations and handles empty', () => {
    const f = new FpsTracker();
    assert.deepEqual(f.getMetrics(), { averageFps: 0, low1PctFps: 0 });
    f.record(0); f.record(-5);
    assert.deepEqual(f.getMetrics(), { averageFps: 0, low1PctFps: 0 });
  });

  it('bounds retained samples and resets', () => {
    const f = new FpsTracker(10);
    for (let i = 0; i < 100; i++) f.record(16);
    assert.ok(f.getMetrics().averageFps > 0);
    f.reset();
    assert.deepEqual(f.getMetrics(), { averageFps: 0, low1PctFps: 0 });
  });
});

describe('slowWarning (bottleneck affordance hints — queryProfiler parity)', () => {
  it('classifies very-slow / slow / none by the default thresholds', () => {
    assert.deepEqual(slowWarning(1500, 'api_call'), { severity: 'very-slow', text: 'VERY SLOW' });
    assert.deepEqual(slowWarning(250, 'tool_bash'), { severity: 'slow', text: 'SLOW' });
    assert.deepEqual(slowWarning(10, 'fast'), { severity: 'none', text: '' });
    // Exactly at a boundary is NOT flagged (strict >, matching the reference).
    assert.equal(slowWarning(100, 'edge').severity, 'none');
    assert.equal(slowWarning(1000, 'edge').severity, 'slow');
  });

  it('never flags the first-checkpoint mark (process-start measurement)', () => {
    assert.deepEqual(slowWarning(99_999, 'query_user_input_received'), { severity: 'none', text: '' });
  });

  it('special-cases known bottlenecks below the generic slow threshold', () => {
    assert.deepEqual(slowWarning(60, 'git_status'), { severity: 'bottleneck', text: 'git status' });
    assert.deepEqual(slowWarning(60, 'tool_schema_build'), { severity: 'bottleneck', text: 'tool schemas' });
    // Under the bottleneck sensitivity → not flagged.
    assert.equal(slowWarning(40, 'git_status').severity, 'none');
  });

  it('honours caller-supplied thresholds', () => {
    assert.equal(slowWarning(30, 'x', { slowMs: 20 }).severity, 'slow');
    assert.equal(slowWarning(30, 'x', { slowMs: 20, verySlowMs: 25 }).severity, 'very-slow');
    assert.equal(slowWarning(55, 'git_status', { bottleneckMs: 60 }).severity, 'none');
  });
});

describe('Profiler.closeOpenTurns (finalize in-flight/aborted turns)', () => {
  it('records a begun-but-never-ended turn as incomplete with elapsed duration', () => {
    const p = new Profiler();
    p.beginTurn(0);
    const closed = p.closeOpenTurns();
    assert.equal(closed, 1);
    const t = p.getTurn(0)!;
    assert.equal(t.incomplete, true);
    assert.ok(t.duration >= 0);
    assert.equal(t.endTime >= t.startTime, true);
  });

  it('leaves properly-bracketed turns untouched and finalizes only the pending one', () => {
    const p = new Profiler();
    p.beginTurn(0);
    p.endTurn(0, { apiLatency: 10, toolExecutionTime: 0, compactionTime: 0, tokensProduced: 1, tokensConsumed: 1, ttftMs: 1 });
    p.beginTurn(1); // in-flight
    const closed = p.closeOpenTurns();
    assert.equal(closed, 1);
    assert.equal(p.getTurn(0)!.incomplete, false);
    assert.equal(p.getTurn(1)!.incomplete, true);
  });

  it('is idempotent and clears the pending-start map (no leak across queries)', () => {
    const p = new Profiler();
    p.beginTurn(5);
    assert.equal(p.closeOpenTurns(), 1);
    assert.equal(p.closeOpenTurns(), 0); // nothing pending the second time
  });
});
