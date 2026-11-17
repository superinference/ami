import { test } from 'node:test';
import assert from 'node:assert/strict';
import { HeapDumpGuard, HEAP_DUMP_RSS_THRESHOLD_BYTES } from '../src/heap-dump';

test('default threshold is 1.5 GB (parity: heapDumpService auto-1.5GB)', () => {
  assert.equal(HEAP_DUMP_RSS_THRESHOLD_BYTES, 1_500 * 1024 * 1024);
  const g = new HeapDumpGuard();
  assert.equal(g.threshold, HEAP_DUMP_RSS_THRESHOLD_BYTES);
});

test('does not fire below the threshold', () => {
  const written: string[] = [];
  const g = new HeapDumpGuard(1000, {
    rss: () => 999,
    writeSnapshot: (f) => { written.push(f); return f; },
    dir: () => '/tmp',
    now: () => 1,
  });
  assert.equal(g.check(), null);
  assert.equal(g.hasFired, false);
  assert.deepEqual(written, []);
});

test('fires exactly once at/over the threshold and returns the written path', () => {
  const written: string[] = [];
  const g = new HeapDumpGuard(1000, {
    rss: () => 1000,
    writeSnapshot: (f) => { written.push(f); return f; },
    dir: () => '/tmp/dumps',
    now: () => 42,
  });
  const first = g.check();
  assert.equal(first, '/tmp/dumps/ami-heap-42.heapsnapshot');
  assert.equal(g.hasFired, true);
  // A second check — even still over threshold — is a no-op (one-shot).
  assert.equal(g.check(), null);
  assert.equal(written.length, 1);
});

test('a throwing rss reader is swallowed and does not fire', () => {
  const g = new HeapDumpGuard(1000, {
    rss: () => { throw new Error('boom'); },
    writeSnapshot: (f) => f,
  });
  assert.equal(g.check(), null);
  assert.equal(g.hasFired, false);
});

test('a throwing writer still marks fired (no retry storm) and returns null', () => {
  let calls = 0;
  const g = new HeapDumpGuard(1000, {
    rss: () => 2000,
    writeSnapshot: () => { calls++; throw new Error('disk full'); },
    now: () => 7,
  });
  assert.equal(g.check(), null);
  assert.equal(g.hasFired, true);
  // Does not retry the write on the next turn.
  assert.equal(g.check(), null);
  assert.equal(calls, 1);
});

test('writeSnapshot may return a path different from the requested one', () => {
  const g = new HeapDumpGuard(1, {
    rss: () => 10,
    writeSnapshot: () => '/var/dumps/actual.heapsnapshot',
  });
  assert.equal(g.check(), '/var/dumps/actual.heapsnapshot');
});

test('non-positive / non-finite threshold falls back to the default', () => {
  assert.equal(new HeapDumpGuard(0).threshold, HEAP_DUMP_RSS_THRESHOLD_BYTES);
  assert.equal(new HeapDumpGuard(-5).threshold, HEAP_DUMP_RSS_THRESHOLD_BYTES);
  assert.equal(new HeapDumpGuard(Number.NaN).threshold, HEAP_DUMP_RSS_THRESHOLD_BYTES);
  assert.equal(new HeapDumpGuard(undefined).threshold, HEAP_DUMP_RSS_THRESHOLD_BYTES);
});
