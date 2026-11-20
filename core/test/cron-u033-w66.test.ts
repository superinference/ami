/**
 * U033 (wave 66) — core/core/src/cron.ts additive parity improvements.
 *
 * Gaps implemented here (see plannings/parity/gaps.jsonl, target core/core/src/cron.ts):
 *  - Expose `maxAgeDays` through create() opts so a caller can set a per-job expiry
 *    (or 0 = unlimited) at creation, not only by mutating the job object afterwards.
 *    The permanent flag + maxAgeDays===0-unlimited handling in expireOld already
 *    existed; this wires the knob into the public create() API + durable round-trip.
 *    (upstreamRef: src/utils/cronTasks.ts recurringMaxAgeMs + src/tools/ScheduleCronTool.)
 *  - Human-readable schedule parity (upstreamRef: src/utils/cron.ts cronToHuman
 *    L257-305): "every N hours at :MM" when the hour is a step and the minute is a
 *    fixed value, and a "Weekdays" label for the 1-5 day-of-week range.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CronScheduler, type CronJob } from '../src/cron';

let dir: string;
const mk = () => fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u033-w66-'));
const tick = (s: CronScheduler, cb: (j: CronJob) => void) =>
  (s as unknown as { tick: (f: (j: CronJob) => void) => void }).tick(cb);

beforeEach(() => { dir = mk(); });
afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

describe('create() — maxAgeDays opt', () => {
  it('sets a per-job maxAgeDays from create() opts', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '* * * * *', prompt: 'p', maxAgeDays: 3 });
    assert.equal(j.maxAgeDays, 3);
  });

  it('maxAgeDays=0 from create() opts means unlimited (survives expiry)', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '* * * * *', prompt: 'p', maxAgeDays: 0 });
    assert.equal(j.maxAgeDays, 0);
    j.createdAt = Date.now() - 999 * 86400000;
    tick(s, () => {});
    assert.ok(s.get(j.id), 'maxAgeDays=0 job must never age out');
  });

  it('a create()-set maxAgeDays drives expiry (expires after the window)', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '* * * * *', prompt: 'p', maxAgeDays: 1 });
    j.createdAt = Date.now() - 2 * 86400000; // 2 days old, window is 1 day
    tick(s, () => {});
    assert.equal(s.get(j.id), undefined, 'maxAgeDays=1 job must expire after 1 day');
  });

  it('rejects a non-positive / fractional maxAgeDays that is not 0', () => {
    const s = new CronScheduler(dir, 's');
    assert.throws(() => s.create({ cron: '* * * * *', prompt: 'p', maxAgeDays: -1 }), /maxAgeDays/);
    assert.throws(() => s.create({ cron: '* * * * *', prompt: 'p', maxAgeDays: 1.5 }), /maxAgeDays/);
  });

  it('durably round-trips maxAgeDays across a reload', () => {
    const a = new CronScheduler(dir, 's');
    const j = a.create({ cron: '0 9 * * *', prompt: 'p', durable: true, maxAgeDays: 5 });
    a.stopTicker();
    const b = new CronScheduler(dir, 's2');
    assert.equal(b.get(j.id)?.maxAgeDays, 5);
    b.stopTicker();
  });
});

describe('formatSchedule — every-N-hours-at-:MM + Weekdays', () => {
  let s: CronScheduler;
  beforeEach(() => { s = new CronScheduler(dir, 's'); });

  it('renders "every N hours" with no suffix when the minute is :00', () => {
    const out = s.formatSchedule('0 */2 * * *');
    assert.ok(out.includes('every 2 hours'), out);
    assert.ok(!out.includes('at minute 0'), `must not show the raw minute piece: ${out}`);
  });

  it('renders "every N hours at :MM" when the minute is a fixed non-zero value', () => {
    assert.equal(s.formatSchedule('30 */2 * * *'), 'every 2 hours at :30');
  });

  it('renders singular "every hour" for */1 hour step', () => {
    const out = s.formatSchedule('0 */1 * * *');
    assert.ok(out.includes('every hour'), out);
    assert.ok(!out.includes('every 1 hours'), out);
  });

  it('labels the 1-5 day-of-week range as Weekdays', () => {
    const out = s.formatSchedule('0 9 * * 1-5');
    assert.ok(out.includes('Weekdays'), `expected a Weekdays label: ${out}`);
    assert.ok(out.includes('at 9:00 am'), out);
  });

  it('keeps the Mon to Fri expansion alongside the Weekdays label (superset)', () => {
    const out = s.formatSchedule('0 0 * * 1-5');
    assert.ok(out.includes('Weekdays'), out);
    assert.ok(out.includes('Mon to Fri'), out);
  });

  it('leaves other day-of-week ranges unchanged', () => {
    assert.ok(s.formatSchedule('0 0 * * 1-6').includes('Mon to Sat'));
  });
});
