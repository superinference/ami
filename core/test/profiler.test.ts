import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { Profiler } from '../src/profiler';

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
    // Start two timers at roughly the same time
    const stopA = p.start('a');
    const stopB = p.start('b');

    // Record pre-measured to have deterministic totals
    p.record('a', 10);
    p.record('b', 20);

    stopA();
    stopB();

    const summary = p.getSummary();
    // Each label should have 2 entries (one from record, one from start/stop)
    assert.equal(summary['a'].count, 2);
    assert.equal(summary['b'].count, 2);
    // The pre-measured values should be included in the totals
    assert.ok(summary['a'].totalMs >= 10);
    assert.ok(summary['b'].totalMs >= 20);
  });
});
