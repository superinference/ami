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
  // A controllable clock so the WALL-CLOCK averageFps is deterministic in tests.
  function clock(start = 0) {
    let t = start;
    return { now: () => t, advance: (ms: number) => { t += ms; } };
  }

  it('computes WALL-CLOCK average FPS and descending-sort 1% low FPS', () => {
    const c = clock();
    const f = new FpsTracker({ now: c.now });
    // 99 frames at 16ms render time + one slow 100ms frame, each 16.667ms apart on the
    // wall clock (~60fps achieved). averageFps is the achieved rate over the window, NOT
    // the inverse of the mean render duration.
    for (let i = 0; i < 99; i++) { c.advance(16.667); f.record(16); }
    c.advance(16.667); f.record(100);
    const m = f.getMetrics();
    assert.ok(m, 'metrics present');
    // 100 frames over 99 intervals * 16.667ms ≈ 1.65s → ~60.6fps achieved (wall-clock).
    assert.ok(m!.averageFps > 59 && m!.averageFps < 62, `averageFps=${m!.averageFps}`);
    // p99 frame time: descending sort [100,16,...], p99Index = ceil(100*0.01)-1 = 0 → 100ms
    // → low1PctFps = 1000/100 = 10.
    assert.equal(m!.low1PctFps, 10);
    assert.ok(m!.low1PctFps < m!.averageFps, '1% low below average');
    // Rounded to two decimals.
    assert.equal(m!.averageFps, Math.round(m!.averageFps * 100) / 100);
  });

  it('p99Index uses ceil(len*0.01)-1 (nearest-rank on the slowest 1%)', () => {
    const c = clock();
    const f = new FpsTracker({ now: c.now });
    // 200 frames: the two slowest are 500ms; the rest are 10ms. Descending sort puts the
    // two 500ms first; p99Index = ceil(200*0.01)-1 = ceil(2)-1 = 1 → sorted[1] = 500ms.
    for (let i = 0; i < 198; i++) { c.advance(10); f.record(10); }
    c.advance(10); f.record(500);
    c.advance(10); f.record(500);
    const m = f.getMetrics();
    assert.ok(m);
    assert.equal(m!.low1PctFps, Math.round((1000 / 500) * 100) / 100); // 2
  });

  // Guard 1: empty sample window → undefined (not {0,0}).
  it('returns undefined when no frames were recorded', () => {
    const f = new FpsTracker({ now: clock().now });
    assert.equal(f.getMetrics(), undefined);
  });

  // Guard 2: non-positive elapsed wall-clock → undefined (never divide by zero/negative).
  it('returns undefined when elapsed wall-clock is non-positive', () => {
    const c = clock();
    const f = new FpsTracker({ now: c.now }); // clock never advances
    f.record(16);
    f.record(16);
    // firstRenderTime === lastRenderTime → totalTimeMs === 0 → undefined.
    assert.equal(f.getMetrics(), undefined);
  });

  // Guard 3: a zero p99 frame time yields low1PctFps 0, never 1000/0 = Infinity.
  it('guards against a zero p99 frame time (low1PctFps = 0)', () => {
    const c = clock();
    const f = new FpsTracker({ now: c.now });
    // All-zero frame durations: p99 frame time is 0 → low1PctFps must be 0, not Infinity.
    for (let i = 0; i < 100; i++) { c.advance(10); f.record(0); }
    const m = f.getMetrics();
    assert.ok(m);
    assert.equal(m!.low1PctFps, 0);
    assert.ok(Number.isFinite(m!.averageFps) && m!.averageFps > 0);
  });

  it('bounds retained samples (p99 buffer) but keeps the true frame count and resets', () => {
    const c = clock();
    const f = new FpsTracker({ maxSamples: 10, now: c.now });
    for (let i = 0; i < 100; i++) { c.advance(10); f.record(16); }
    const m = f.getMetrics();
    assert.ok(m);
    // 100 frames over 99*10ms = 990ms ≈ 101fps — the wall-clock average uses the TRUE
    // 100-frame count, not the bounded 10-sample buffer.
    assert.ok(m!.averageFps > 95 && m!.averageFps < 105, `averageFps=${m!.averageFps}`);
    f.reset();
    assert.equal(f.getMetrics(), undefined);
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
