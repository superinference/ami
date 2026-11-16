/**
 * U033 — core/core/src/cron.ts parity improvements.
 *
 * Gaps implemented (see plannings/parity/gaps.jsonl):
 *  - Full-range (semantic) detection for dom/dow OR-semantics
 *    (claudeRef: src/utils/cron.ts:L129-158 — domWild/dowWild via expanded-set length).
 *  - Recurring jitter proportional to the ACTUAL gap between the next two fires
 *    (claudeRef: src/utils/cronTasks.ts:L367-398 — jitteredNextCronRunMs uses t2-t1).
 *  - Grammatically correct singular/plural phrasing in the human-readable schedule
 *    (claudeRef: src/utils/cron.ts:L231-254 — "Every minute"/"Every hour" singular).
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  CronScheduler,
  fieldCoversFullRange,
  resetScheduler,
  DEFAULT_CRON_JITTER,
  type CronJitterConfig,
} from '../src/cron';

let sched: CronScheduler;
let tmpDir: string;

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-u033-'));
  sched = new CronScheduler(tmpDir);
});

// ---------------------------------------------------------------------------
// Full-range OR-semantics detection
// ---------------------------------------------------------------------------

describe('fieldCoversFullRange', () => {
  it('treats a literal wildcard as full-range', () => {
    assert.equal(fieldCoversFullRange('*', 1, 31), true);
    assert.equal(fieldCoversFullRange('*', 0, 6), true);
  });

  it('treats an explicit full span as full-range', () => {
    assert.equal(fieldCoversFullRange('1-31', 1, 31), true);
    assert.equal(fieldCoversFullRange('0-6', 0, 6), true);
    assert.equal(fieldCoversFullRange('0-7', 0, 6), true); // DOW Sunday-via-7 span
    assert.equal(fieldCoversFullRange('*/1', 0, 6), true);
  });

  it('rejects a partial field', () => {
    assert.equal(fieldCoversFullRange('15', 1, 31), false);
    assert.equal(fieldCoversFullRange('1-5', 0, 6), false);
    assert.equal(fieldCoversFullRange('*/2', 0, 6), false);
  });
});

describe('computeNextRun — full-range dom/dow is semantically wildcard (OR-semantics)', () => {
  it('"0 9 1-31 * 1" fires ONLY on Mondays, not every day', () => {
    // dom "1-31" matches every day → it is a semantic wildcard, so the OR between
    // dom and dow must NOT apply; only the restricted dow (Monday) gates the day.
    const base = Date.parse('2026-11-16T00:00:00'); // Monday 2026-11-16
    // From a Tuesday, the next fire must skip to the following Monday — NOT the next day.
    const fromTue = Date.parse('2026-11-17T10:00:00');
    const next = sched.computeNextRun('0 9 1-31 * 1', fromTue)!;
    assert.notEqual(next, null);
    const d = new Date(next);
    assert.equal(d.getDay(), 1, 'next fire must be a Monday');
    assert.equal(d.getHours(), 9);
    assert.equal(d.getMinutes(), 0);
    // Sanity: that Monday is 2026-11-23, not 2026-11-18.
    assert.equal(d.getDate(), 23);
    assert.ok(base < next);
  });

  it('"0 9 15 * 0-6" (full-range dow) fires only on the 15th', () => {
    const from = Date.parse('2026-11-16T00:00:00');
    const next = sched.computeNextRun('0 9 15 * 0-6', from)!;
    const d = new Date(next);
    assert.equal(d.getDate(), 15, 'must fire on the 15th regardless of weekday');
  });
});

// ---------------------------------------------------------------------------
// Recurring jitter proportional to the ACTUAL inter-fire gap
// ---------------------------------------------------------------------------

describe('getDueJobs — recurring jitter uses the real gap between the next two fires', () => {
  // Replicates the scheduler's deterministic per-id hash so the test can reason
  // about the exact jitter window without depending on a specific job id.
  function normHash(id: string): number {
    const h = id.split('').reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0);
    return Math.abs(h) / 2147483647;
  }

  it('"0,30 * * * *" (real gap 30min) jitters by the 30-min gap, not a 60-min estimate', () => {
    // recurringFrac=1 + an effectively-uncapped cap makes maxJitter == the basis
    // period, so the basis (real 30-min gap vs the old crude 60-min estimate) is
    // directly observable through whether a job is "due".
    const cfg: CronJitterConfig = {
      ...DEFAULT_CRON_JITTER,
      recurringFrac: 1,
      recurringCapMs: 1e12,
    };
    const s = new CronScheduler(tmpDir, 'u033', cfg);
    const job = s.create({ cron: '0,30 * * * *', prompt: 'p' });
    const nh = normHash(job.id);
    // Place the fire time X in the past, where  nh*30min < X < nh*60min.
    // New (gap-based, max nh*30min): due.  Old (estimate-based, nh*60min): not due.
    const X = Math.floor(nh * 45 * 60 * 1000);
    assert.ok(X > 0, 'id must have a non-degenerate hash');
    job.nextRun = Date.now() - X;
    assert.equal(s.getDueJobs().length, 1, 'job must be due under the real-gap jitter basis');
    s.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// Grammatically correct singular/plural phrasing
// ---------------------------------------------------------------------------

describe('formatSchedule — singular/plural grammar', () => {
  it('renders "every minute" (not "every 1 minutes") for */1 minutes', () => {
    assert.equal(sched.formatSchedule('*/1 * * * *'), 'every minute');
  });

  it('renders "every hour" (not "every 1 hours") for */1 hours', () => {
    const out = sched.formatSchedule('0 */1 * * *');
    assert.ok(out.includes('every hour'), `got: ${out}`);
    assert.ok(!out.includes('every 1 hours'), `got: ${out}`);
  });

  it('keeps correct plurals for N>1', () => {
    assert.ok(sched.formatSchedule('*/15 * * * *').includes('every 15 minutes'));
    assert.ok(sched.formatSchedule('0 */2 * * *').includes('every 2 hours'));
  });
});
