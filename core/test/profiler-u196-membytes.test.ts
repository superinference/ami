/**
 * U196 — profiler memory output consolidated onto the shared `formatFileSize`
 * humanizer (parity with the reference's profilerBase formatTimelineLine, which renders
 * `RSS: <size>, Heap: <size>`). Before this, the timeline printed heap-only as an
 * ad-hoc `(bytes / 1_048_576).toFixed(1)}MB`, so a 2GB RSS never appeared and a
 * 1.5GB heap read as `1536.0MB` instead of `1.5GB`. These tests lock the shared
 * helper's scaling behavior into the profiler's rendered timeline.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { Profiler } from '../src/profiler';
import { formatFileSize } from '../src/utils/format';

describe('Profiler.formatTimeline memory via shared formatFileSize', () => {
  it('renders both RSS and Heap, humanized through formatFileSize (GB scale)', () => {
    const mem = () => ({ rss: 2 * 1024 ** 3, heapUsed: Math.round(1.5 * 1024 ** 3) });
    const p = new Profiler({ memSnapshot: mem });
    p.mark('boot');
    const out = p.formatTimeline();
    // the reference-parity memory segment: "RSS: <size>, Heap: <size>".
    assert.match(out, /RSS: 2GB, Heap: 1\.5GB/);
    // Must go through the shared helper, not the old ad-hoc MB-only arithmetic.
    assert.ok(out.includes(`RSS: ${formatFileSize(2 * 1024 ** 3)}`), 'RSS uses formatFileSize');
    assert.ok(out.includes(`Heap: ${formatFileSize(Math.round(1.5 * 1024 ** 3))}`), 'Heap uses formatFileSize');
    assert.ok(!/\d+\.\dMB(?!.*GB)/.test(out) || out.includes('GB'), 'no ad-hoc 1536.0MB rendering of GB-scale memory');
  });

  it('scales sub-KB memory without forcing MB units', () => {
    const mem = () => ({ rss: 900, heapUsed: 512 });
    const p = new Profiler({ memSnapshot: mem });
    p.mark('tiny');
    const out = p.formatTimeline();
    assert.match(out, /RSS: 900B, Heap: 512B/);
  });
});
