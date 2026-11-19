import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  createProcessGrowthState,
  evaluateProcessGrowth,
  countDescendantsByRoot,
  DEFAULT_PROCESS_GROWTH_LIMITS,
  type ProcessGrowthLimits,
  type ProcessGrowthSample,
} from '../src/process-growth';

const LIMITS: ProcessGrowthLimits = {
  hostElevatedDescendants: 1000,
  growthPerSecond: 40,
  sustainMs: 3_000,
  minRisingSamples: 3,
};

function sample(atMs: number, host: number | null, scopes: Record<string, number> = {}): ProcessGrowthSample {
  return { atMs, hostDescendants: host, scopes: new Map(Object.entries(scopes)) };
}

test('evaluateProcessGrowth: first sample is ok (no baseline to compare)', () => {
  const s = createProcessGrowthState();
  const v = evaluateProcessGrowth(s, sample(0, 10), LIMITS);
  assert.equal(v.level, 'ok');
  assert.equal(v.rising, false);
  assert.deepEqual(v.trips, []);
});

test('evaluateProcessGrowth: high but FLAT load is elevated, never critical', () => {
  const s = createProcessGrowthState();
  evaluateProcessGrowth(s, sample(0, 2000), LIMITS);
  const v = evaluateProcessGrowth(s, sample(1000, 2000), LIMITS); // no growth, above elevated
  assert.equal(v.level, 'elevated');
  assert.deepEqual(v.trips, []);
  assert.equal(v.rising, false);
});

test('evaluateProcessGrowth: sustained fast growth trips critical after minRisingSamples + sustainMs', () => {
  const s = createProcessGrowthState();
  // +100/s each sample at 1s intervals (> 40/s). sustainMs=3000, minRisingSamples=3.
  evaluateProcessGrowth(s, sample(0, 0), LIMITS);
  let v = evaluateProcessGrowth(s, sample(1000, 100), LIMITS); // rising sample #1
  assert.equal(v.level, 'elevated');
  assert.equal(v.rising, true);
  v = evaluateProcessGrowth(s, sample(2000, 200), LIMITS); // #2
  assert.equal(v.level, 'elevated');
  v = evaluateProcessGrowth(s, sample(3000, 300), LIMITS); // #3, spans 3000ms → trips
  assert.equal(v.level, 'critical');
  assert.equal(v.trips.length, 1);
  assert.equal(v.trips[0].scopeId, null); // host
  assert.equal(v.trips[0].descendants, 300);
  assert.equal(v.trips[0].growthPerSecond, 100);
});

test('evaluateProcessGrowth: growth below the per-second threshold clears the streak', () => {
  const s = createProcessGrowthState();
  evaluateProcessGrowth(s, sample(0, 0), LIMITS);
  evaluateProcessGrowth(s, sample(1000, 100), LIMITS); // fast
  evaluateProcessGrowth(s, sample(2000, 200), LIMITS); // fast
  const slow = evaluateProcessGrowth(s, sample(3000, 210), LIMITS); // +10/s → under threshold, resets streak
  assert.equal(slow.level, 'ok');
  assert.equal(slow.rising, false);
  // Must rebuild the streak from scratch — a single later fast sample does not re-trip.
  const next = evaluateProcessGrowth(s, sample(4000, 310), LIMITS);
  assert.equal(next.level, 'elevated');
  assert.deepEqual(next.trips, []);
});

test('evaluateProcessGrowth: per-scope growth trips only the offending scope', () => {
  const s = createProcessGrowthState();
  evaluateProcessGrowth(s, sample(0, 0, { a: 0, b: 0 }), LIMITS);
  evaluateProcessGrowth(s, sample(1000, 0, { a: 100, b: 1 }), LIMITS);
  evaluateProcessGrowth(s, sample(2000, 0, { a: 200, b: 2 }), LIMITS);
  const v = evaluateProcessGrowth(s, sample(3000, 0, { a: 300, b: 3 }), LIMITS);
  assert.equal(v.level, 'critical');
  assert.equal(v.trips.length, 1);
  assert.equal(v.trips[0].scopeId, 'a');
});

test('evaluateProcessGrowth: a disappeared scope drops its streak', () => {
  const s = createProcessGrowthState();
  evaluateProcessGrowth(s, sample(0, 0, { a: 0 }), LIMITS);
  evaluateProcessGrowth(s, sample(1000, 0, { a: 100 }), LIMITS);
  assert.equal(s.streaks.has('a'), true);
  evaluateProcessGrowth(s, sample(2000, 0, {}), LIMITS); // scope a gone
  assert.equal(s.streaks.has('a'), false);
});

test('evaluateProcessGrowth: unknown sample neither trips nor clears an open streak', () => {
  const s = createProcessGrowthState();
  evaluateProcessGrowth(s, sample(0, 0), LIMITS);
  evaluateProcessGrowth(s, sample(1000, 100), LIMITS); // streak open
  const unknown = evaluateProcessGrowth(s, sample(2000, null), LIMITS);
  assert.equal(unknown.level, 'unknown');
  assert.equal(unknown.rising, true); // streak still open
  assert.deepEqual(unknown.trips, []);
});

test('evaluateProcessGrowth: a single long interval cannot fabricate a trip', () => {
  const s = createProcessGrowthState();
  evaluateProcessGrowth(s, sample(0, 0), LIMITS);
  // One sample 10s later with huge growth: only 1 rising sample → not minRisingSamples.
  const v = evaluateProcessGrowth(s, sample(10_000, 10_000), LIMITS);
  assert.equal(v.level, 'elevated');
  assert.deepEqual(v.trips, []);
});

test('DEFAULT_PROCESS_GROWTH_LIMITS: documented defaults', () => {
  assert.equal(DEFAULT_PROCESS_GROWTH_LIMITS.growthPerSecond, 40);
  assert.equal(DEFAULT_PROCESS_GROWTH_LIMITS.sustainMs, 10_000);
  assert.equal(DEFAULT_PROCESS_GROWTH_LIMITS.minRisingSamples, 3);
  assert.equal(DEFAULT_PROCESS_GROWTH_LIMITS.hostElevatedDescendants, 1500);
});

test('countDescendantsByRoot: counts whole subtrees per root', () => {
  const rows = [
    { pid: 1, ppid: 0 },
    { pid: 10, ppid: 1 },
    { pid: 11, ppid: 10 },
    { pid: 12, ppid: 10 },
    { pid: 20, ppid: 1 },
  ];
  const counts = countDescendantsByRoot(rows, [1, 10, 20]);
  assert.equal(counts.get(1), 4); // 10,11,12,20
  assert.equal(counts.get(10), 2); // 11,12
  assert.equal(counts.get(20), 0);
});

test('countDescendantsByRoot: ignores self-parented rows and prunes skipped subtrees', () => {
  const rows = [
    { pid: 5, ppid: 5 }, // self-parented → ignored
    { pid: 10, ppid: 1 },
    { pid: 11, ppid: 10 },
    { pid: 12, ppid: 10 },
  ];
  const skip = new Set([10]);
  const counts = countDescendantsByRoot(rows, [1], skip);
  assert.equal(counts.get(1), 0); // 10 skipped → 11,12 unreachable
  const noSkip = countDescendantsByRoot(rows, [1]);
  assert.equal(noSkip.get(1), 3);
});
