/**
 * U196 parity additions to the Profiler:
 *  - configurable timeline column alignment (totalPad/deltaPad) — profilerBase
 *    formatTimelineLine parity.
 *  - slow-operation TTL/recency window + exclude-prefix (DevBar recent-slow-ops
 *    window + model-prompt exclusion parity).
 *  - percentile() delegating to the single canonical linear-interpolation
 *    implementation (stats.tsx parity + no-duplication consolidation).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { Profiler, percentile } from '../src/profiler';
import { percentile as reservoirPercentile } from '../src/utils/reservoir';

describe('Profiler.formatTimeline column alignment', () => {
  // Deterministic memory snapshot so output is stable across runs.
  const mem = () => ({ rss: 10 * 1_048_576, heapUsed: 5 * 1_048_576 });

  it('defaults to 7/6 padding (unchanged for existing callers)', () => {
    const p = new Profiler({ memSnapshot: mem });
    p.mark('a');
    p.mark('b');
    const out = p.formatTimeline();
    // Cumulative column padded to width 7, delta to width 6; memory via the
    // shared formatFileSize humanizer (RSS + Heap, Claude profilerBase parity).
    assert.match(out, /a {2} {6}0ms {2}Δ {5}0ms {2}RSS: 10MB, Heap: 5MB/);
  });

  it('honours custom totalPad/deltaPad widths', () => {
    const p = new Profiler({ memSnapshot: mem });
    p.mark('a');
    const wide = p.formatTimeline({ totalPad: 10, deltaPad: 9 });
    const narrow = p.formatTimeline({ totalPad: 2, deltaPad: 1 });
    // Wider pad → more leading spaces before the "0ms".
    assert.ok(wide.indexOf('0ms') > narrow.indexOf('0ms'), 'wider totalPad pushes the cumulative column right');
    assert.match(narrow, /a {2} 0ms {2}Δ0ms {2}RSS: 10MB, Heap: 5MB/);
  });

  it('reports no checkpoints when empty regardless of pad opts', () => {
    const p = new Profiler({ memSnapshot: mem });
    assert.equal(p.formatTimeline({ totalPad: 20 }), '(no checkpoints)');
  });
});

describe('Profiler slow-operation TTL + exclusion', () => {
  it('filters slow operations older than maxAgeMs (TTL window)', () => {
    const p = new Profiler({ slowThresholdMs: 10 });
    p.record('old_op', 100);
    const slept = Date.now();
    // Busy-wait a few ms so the next record has a strictly later timestamp.
    while (Date.now() - slept < 5) { /* spin */ }
    p.record('new_op', 100);
    // A 1ms TTL keeps only the most recent op; a huge TTL keeps both.
    const recent = p.getSlowOperations(undefined, 1);
    assert.deepEqual(recent.map(o => o.operation), ['new_op']);
    assert.equal(p.getSlowOperations(undefined, 60_000).length, 2);
    // No TTL argument → all retained (back-compat with existing callers).
    assert.equal(p.getSlowOperations().length, 2);
  });

  it('never flags operations whose label matches an exclude prefix', () => {
    const p = new Profiler({ slowThresholdMs: 10, slowExcludePrefixes: ['claude_prompt'] });
    p.record('claude_prompt_turn', 5000); // excluded despite being very slow
    p.record('tool_bash', 5000);          // flagged
    assert.deepEqual(p.getSlowOperations().map(o => o.operation), ['tool_bash']);
  });

  it('treats a negative maxAgeMs the same as no window (defensive)', () => {
    const p = new Profiler({ slowThresholdMs: 10 });
    p.record('op', 100);
    assert.equal(p.getSlowOperations(undefined, -1).length, 1);
  });
});

describe('percentile consolidation (linear interpolation)', () => {
  it('matches the canonical utils/reservoir implementation exactly', () => {
    const samples = [5, 1, 9, 3, 7, 2, 8, 4, 6, 10];
    for (const q of [0, 10, 25, 50, 75, 95, 99, 100]) {
      assert.equal(percentile(samples, q), reservoirPercentile(samples, q), `p${q} must match the canonical impl`);
    }
  });
});
