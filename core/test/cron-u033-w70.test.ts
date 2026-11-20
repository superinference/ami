/**
 * U033 (wave 70) — core/core/src/cron.ts additive parity improvements.
 *
 * Gaps implemented here (see plannings/parity/gaps.jsonl, target core/core/src/cron.ts):
 *
 *  - Durable-teammate orphan guard (upstreamRef:
 *    src/tools/ScheduleCronTool/CronCreateTool.ts:L105-114). Claude rejects a
 *    durable cron created inside a teammate/sub-agent context (errorCode 4) because
 *    the teammate does not persist across sessions, so the durable job's stored
 *    agentId would orphan on restart. AMI now rejects create({ durable:true, agentId })
 *    the same way — session-only jobs for a teammate are still allowed.
 *
 *  - Natural-language cron rendering parity (upstreamRef: src/utils/cron.ts cronToHuman
 *    L183-308, src/utils/cronScheduler.ts:L552). formatSchedule() now reads a fixed
 *    daily time as a single fluent phrase leading with the day context:
 *    "every day at 9:00 am", "every Mon at 9:00 am", "Weekdays at 9:00 am (Mon to Fri)"
 *    instead of the mechanical "at 9:00 am" / "at 9:00 am, on Mon". AMI keeps its
 *    lowercase am/pm style and the Mon-to-Fri expansion (superset divergence).
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CronScheduler } from '../src/cron';

let dir: string;
const mk = () => fs.mkdtempSync(path.join(os.tmpdir(), 'cron-u033-w70-'));

beforeEach(() => { dir = mk(); });
afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

describe('create() — durable-teammate orphan guard', () => {
  it('rejects a durable cron owned by a teammate/sub-agent (would orphan on restart)', () => {
    const s = new CronScheduler(dir, 's');
    assert.throws(
      () => s.create({ cron: '0 9 * * *', prompt: 'p', durable: true, agentId: 'agent-7' }),
      /durable cron cannot be created by a teammate/i,
    );
    // Nothing persisted — the rejected job must not land on disk.
    assert.equal(s.list().length, 0);
    s.stopTicker();
  });

  it('allows a session-only (non-durable) cron for a teammate', () => {
    const s = new CronScheduler(dir, 's');
    const job = s.create({ cron: '0 9 * * *', prompt: 'p', durable: false, agentId: 'agent-7' });
    assert.equal(job.agentId, 'agent-7');
    assert.equal(job.durable, false);
    s.stopTicker();
  });

  it('allows a durable cron for the team lead (no agentId)', () => {
    const s = new CronScheduler(dir, 's');
    const job = s.create({ cron: '0 9 * * *', prompt: 'p', durable: true });
    assert.equal(job.durable, true);
    assert.equal(job.agentId, undefined);
    s.stopTicker();
  });
});

describe('formatSchedule() — fluent day+time phrasing', () => {
  let s: CronScheduler;
  beforeEach(() => { s = new CronScheduler(dir, 's'); });
  afterEach(() => { s.stopTicker(); });

  it('renders an everyday fixed time as "every day at <time>"', () => {
    assert.equal(s.formatSchedule('0 9 * * *'), 'every day at 9:00 am');
    assert.equal(s.formatSchedule('30 14 * * *'), 'every day at 2:30 pm');
  });

  it('renders a single weekday fixed time as "every <Weekday> at <time>"', () => {
    assert.equal(s.formatSchedule('0 9 * * 1'), 'every Mon at 9:00 am');
    assert.equal(s.formatSchedule('0 9 * * 0'), 'every Sun at 9:00 am');
    assert.equal(s.formatSchedule('0 9 * * 6'), 'every Sat at 9:00 am');
  });

  it('renders the 1-5 range + fixed time as "Weekdays at <time> (Mon to Fri)"', () => {
    assert.equal(s.formatSchedule('0 9 * * 1-5'), 'Weekdays at 9:00 am (Mon to Fri)');
    assert.ok(s.formatSchedule('0 0 * * 1-5').includes('Weekdays'));
    assert.ok(s.formatSchedule('0 0 * * 1-5').includes('Mon to Fri'));
  });

  it('does NOT collapse to a day phrase when day-of-month/month restrict the schedule', () => {
    const out = s.formatSchedule('30 9 5 6 1');
    assert.ok(out.includes('at 9:30 am'), out);
    assert.ok(out.includes('on day 5'), out);
    assert.ok(out.includes('in month 6'), out);
    assert.ok(!out.startsWith('every '), `restricted schedule must not read as a plain day phrase: ${out}`);
  });

  it('leaves multi-day-of-week + fixed time on the mechanical path', () => {
    const out = s.formatSchedule('0 9 * * 1,3,5');
    assert.ok(out.includes('at 9:00 am'), out);
    assert.ok(out.includes('Mon'), out);
    assert.ok(out.includes('Wed'), out);
    assert.ok(out.includes('Fri'), out);
  });

  it('keeps the existing minute/hour-step phrasing untouched', () => {
    assert.equal(s.formatSchedule('30 */2 * * *'), 'every 2 hours at :30');
    assert.ok(s.formatSchedule('*/5 * * * *').includes('every 5 minutes'));
    assert.equal(s.formatSchedule('* * * * *'), 'every minute');
  });
});
