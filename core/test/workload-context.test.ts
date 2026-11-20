/**
 * U513 — workload-context.ts: turn-scoped workload (execution-origin) tag via
 * AsyncLocalStorage (the reference workloadContext parity).
 *
 * Covers the canonical ALS primitive (getWorkload / hasWorkloadBoundary /
 * runWithWorkload / WORKLOAD_CRON) and that AnalyticsTracker consumes exactly
 * this one ALS for invocation-source resolution + auto-attribution — no parallel
 * context mechanism.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  getWorkload,
  hasWorkloadBoundary,
  runWithWorkload,
  WORKLOAD_CRON,
} from '../src/workload-context';
import { AnalyticsTracker } from '../src/analytics';

describe('workload-context — ALS primitive', () => {
  it('WORKLOAD_CRON is the lowercase `cron` tag', () => {
    assert.equal(WORKLOAD_CRON, 'cron');
  });

  it('getWorkload() is undefined and hasWorkloadBoundary() is false outside any boundary', () => {
    assert.equal(getWorkload(), undefined);
    assert.equal(hasWorkloadBoundary(), false);
  });

  it('resolves getWorkload() for the whole async chain, across awaits', async () => {
    const seen = await runWithWorkload(WORKLOAD_CRON, async () => {
      await new Promise(r => setImmediate(r));
      return getWorkload();
    });
    assert.equal(seen, 'cron');
    // No leak after the boundary resolves.
    assert.equal(getWorkload(), undefined);
    assert.equal(hasWorkloadBoundary(), false);
  });

  it('isolates concurrent/interleaved chains (a global slot would cross-contaminate)', async () => {
    const a = runWithWorkload('srcA', async () => {
      await new Promise(r => setImmediate(r));
      await new Promise(r => setImmediate(r));
      return getWorkload();
    });
    const b = runWithWorkload('srcB', async () => {
      await new Promise(r => setImmediate(r));
      return getWorkload();
    });
    assert.deepEqual(await Promise.all([a, b]), ['srcA', 'srcB']);
  });

  it('ALWAYS establishes a boundary even for undefined (shadows a leaked outer context)', () => {
    // Outer leaked cron context; an inner runWithWorkload(undefined) must NOT see it.
    const result = runWithWorkload(WORKLOAD_CRON, () => {
      assert.equal(getWorkload(), 'cron');
      assert.equal(hasWorkloadBoundary(), true);
      return runWithWorkload(undefined, () => {
        // A pass-through (`return fn()`) would leak 'cron' here; `.run()` must shadow it.
        return { workload: getWorkload(), boundary: hasWorkloadBoundary() };
      });
    });
    assert.equal(result.workload, undefined, 'inner boundary shadows the outer cron tag');
    assert.equal(result.boundary, true, 'a boundary exists even when its workload is undefined');
  });

  it('returns fn()`s value (sync) and the boundary is gone synchronously after', () => {
    const v = runWithWorkload(WORKLOAD_CRON, () => 42);
    assert.equal(v, 42);
    assert.equal(getWorkload(), undefined);
  });
});

describe('workload-context — AnalyticsTracker consumes the SAME ALS (no parallel mechanism)', () => {
  it('currentSource() resolves from the shared workload boundary (set via runWithWorkload)', async () => {
    const t = new AnalyticsTracker();
    const seen = await runWithWorkload(WORKLOAD_CRON, async () => {
      await new Promise(r => setImmediate(r));
      return t.currentSource();
    });
    assert.equal(seen, 'cron');
    assert.equal(t.currentSource(), undefined);
  });

  it('runWithSource is backed by the same ALS getWorkload() reads', async () => {
    const t = new AnalyticsTracker();
    const seen = await t.runWithSource('agent', async () => {
      await new Promise(r => setImmediate(r));
      return getWorkload();
    });
    assert.equal(seen, 'agent');
  });

  it('auto-attributes `source` onto events logged inside a workload boundary', () => {
    const t = new AnalyticsTracker();
    runWithWorkload(WORKLOAD_CRON, () => {
      t.log('cron_fired', { jobId: 'j1' });
    });
    const ev = t.getEventsByType('cron_fired');
    assert.equal(ev.length, 1);
    assert.equal(ev[0].data.source, 'cron', 'event is attributed to the cron workload');
    assert.equal(ev[0].data.jobId, 'j1');
  });

  it('an explicit `source` wins over the boundary (no overwrite)', () => {
    const t = new AnalyticsTracker();
    runWithWorkload(WORKLOAD_CRON, () => {
      t.log('evt', { source: 'explicit' });
    });
    assert.equal(t.getEventsByType('evt')[0].data.source, 'explicit');
  });

  it('does NOT auto-attribute from the synchronous push/pop stack (only ALS boundaries)', () => {
    const t = new AnalyticsTracker();
    t.pushSource('stack-only');
    t.log('evt', { info: 'x' });
    t.popSource();
    // Stack-based event shape is unchanged — no `source` stamped.
    assert.deepEqual(t.getEventsByType('evt')[0].data, { info: 'x' });
  });

  it('a boundary carrying undefined does not stamp a source', () => {
    const t = new AnalyticsTracker();
    runWithWorkload(undefined, () => {
      t.log('evt', { info: 'y' });
    });
    assert.deepEqual(t.getEventsByType('evt')[0].data, { info: 'y' });
  });
});
