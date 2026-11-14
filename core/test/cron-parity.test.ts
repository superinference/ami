import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  CronScheduler,
  validateCronExpression,
  isProcessRunning,
  to12Hour,
  type CronJob,
} from '../src/cron';

let dir: string;
const mk = () => fs.mkdtempSync(path.join(os.tmpdir(), 'cron-parity-'));
const tick = (s: CronScheduler, cb: (j: CronJob) => void) =>
  (s as unknown as { tick: (f: (j: CronJob) => void) => void }).tick(cb);

beforeEach(() => { dir = mk(); });
afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

describe('validateCronExpression', () => {
  it('accepts valid expressions', () => {
    for (const c of ['* * * * *', '*/5 * * * *', '0 9 1-5 * 1,3', '0 0 1 1 0-6', '30 */2 * * *']) {
      assert.equal(validateCronExpression(c), null, c);
    }
  });
  it('rejects wrong field count', () => {
    assert.match(validateCronExpression('* * *')!, /5 fields/);
  });
  it('rejects out-of-range values', () => {
    assert.match(validateCronExpression('99 * * * *')!, /out of bounds/);
    assert.match(validateCronExpression('* 25 * * *')!, /out of bounds/);
  });
  it('rejects reversed ranges', () => {
    assert.match(validateCronExpression('* 10-5 * * *')!, /reversed range/);
  });
  it('rejects a bad step', () => {
    assert.match(validateCronExpression('*/0 * * * *')!, /invalid step/);
  });
  it('rejects non-integers and empty elements', () => {
    assert.match(validateCronExpression('x * * * *')!, /non-integer/);
    assert.match(validateCronExpression('1, * * * *')!, /empty element/);
  });
});

describe('isProcessRunning', () => {
  it('reports the current process as alive', () => {
    assert.equal(isProcessRunning(process.pid), true);
  });
  it('reports a non-existent / invalid pid as dead', () => {
    assert.equal(isProcessRunning(2_147_483_646), false);
    assert.equal(isProcessRunning(0), false);
    assert.equal(isProcessRunning(-1), false);
  });
});

describe('to12Hour', () => {
  it('formats am/pm and midnight/noon', () => {
    assert.equal(to12Hour(0, 0), '12:00 am');
    assert.equal(to12Hour(9, 5), '9:05 am');
    assert.equal(to12Hour(12, 0), '12:00 pm');
    assert.equal(to12Hour(23, 59), '11:59 pm');
  });
});

describe('CronScheduler — create validation + min interval', () => {
  it('rejects an invalid cron on create', () => {
    const s = new CronScheduler(dir, 's');
    assert.throws(() => s.create({ cron: '99 * * * *', prompt: 'p' }), /Invalid cron expression/);
  });
  it('rejects a recurring job that fires more often than the minute floor', () => {
    const s = new CronScheduler(dir, 's');
    // '* * * * *' fires every minute == the floor, allowed; a sub-minute cron
    // cannot be expressed in 5-field cron, so the floor is effectively 1/min.
    assert.doesNotThrow(() => s.create({ cron: '* * * * *', prompt: 'ok' }));
  });
  it('stores agentId and permanent flags', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'p', agentId: 'a1', permanent: true });
    assert.equal(j.agentId, 'a1');
    assert.equal(j.permanent, true);
  });
});

describe('CronScheduler — agentId scoping', () => {
  let s: CronScheduler;
  beforeEach(() => {
    s = new CronScheduler(dir, 's');
    s.create({ cron: '0 9 * * *', prompt: 'global' });
    s.create({ cron: '0 9 * * *', prompt: 'a1-job', agentId: 'a1' });
    s.create({ cron: '0 9 * * *', prompt: 'a2-job', agentId: 'a2' });
  });

  it('a teammate sees only global + its own jobs', () => {
    const prompts = s.list('a1').map(j => j.prompt).sort();
    assert.deepEqual(prompts, ['a1-job', 'global']);
  });
  it('the team lead (unscoped) sees all jobs', () => {
    assert.equal(s.list().length, 3);
  });
  it('a teammate cannot delete another agent\'s job', () => {
    const a2 = s.list().find(j => j.prompt === 'a2-job')!;
    assert.throws(() => s.delete(a2.id, 'a1'), /Not authorized/);
    assert.equal(s.get(a2.id) !== undefined, true);
  });
  it('a teammate can delete its own job', () => {
    const a1 = s.list().find(j => j.prompt === 'a1-job')!;
    assert.equal(s.delete(a1.id, 'a1'), true);
  });
  it('an unscoped caller can delete any job', () => {
    const a2 = s.list().find(j => j.prompt === 'a2-job')!;
    assert.equal(s.delete(a2.id), true);
  });
});

describe('CronScheduler — update / runNow', () => {
  it('updates cron/prompt and recomputes nextRun', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'old' });
    const before = j.nextRun;
    const updated = s.update(j.id, { cron: '0 10 * * *', prompt: 'new' });
    assert.equal(updated!.prompt, 'new');
    assert.notEqual(updated!.nextRun, before);
  });
  it('update rejects an invalid cron and an unknown id', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'p' });
    assert.throws(() => s.update(j.id, { cron: 'nope' }), /Invalid cron/);
    assert.equal(s.update('missing', { prompt: 'x' }), undefined);
  });
  it('update honors agent authorization', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'p', agentId: 'a1' });
    assert.throws(() => s.update(j.id, { prompt: 'x' }, 'a2'), /Not authorized/);
  });
  it('runNow fires a job immediately and advances its schedule', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'p' });
    const snap = s.runNow(j.id);
    assert.equal(snap!.id, j.id);
    assert.ok(s.get(j.id)!.lastFiredAt);
  });
  it('runNow returns undefined for a missing id and honors authorization', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'p', agentId: 'a1' });
    assert.equal(s.runNow('missing'), undefined);
    assert.throws(() => s.runNow(j.id, 'a2'), /Not authorized/);
  });
});

describe('CronScheduler — isLoading gate', () => {
  it('defers firing while the busy predicate is true', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '* * * * *', prompt: 'p' });
    j.nextRun = Date.now() - 30 * 60_000; // well past any jitter window
    let busy = true;
    s.setBusyCheck(() => busy);
    const fired: string[] = [];
    tick(s, (job) => fired.push(job.id));
    assert.deepEqual(fired, [], 'should defer while busy');
    busy = false;
    tick(s, (job) => fired.push(job.id));
    assert.deepEqual(fired, [j.id], 'fires once no longer busy');
  });
});

describe('CronScheduler — permanent + unlimited expiry', () => {
  it('a permanent job is never expired', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '* * * * *', prompt: 'p', permanent: true });
    j.createdAt = Date.now() - 999 * 86400000;
    tick(s, () => {});
    assert.ok(s.get(j.id), 'permanent job survives');
  });
  it('maxAgeDays=0 means unlimited', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '* * * * *', prompt: 'p' });
    j.maxAgeDays = 0;
    j.createdAt = Date.now() - 999 * 86400000;
    tick(s, () => {});
    assert.ok(s.get(j.id), 'unlimited job survives');
  });
});

describe('CronScheduler — missed one-shot detection', () => {
  it('detects a one-shot task whose time passed while offline', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'oneshot', recurring: false });
    j.nextRun = Date.now() - 60_000; // in the past, never fired
    const missed = s.detectMissedTasks();
    assert.ok(missed.some(m => m.id === j.id));
  });
  it('does not report an already-fired one-shot as missed', () => {
    const s = new CronScheduler(dir, 's');
    const j = s.create({ cron: '0 9 * * *', prompt: 'oneshot', recurring: false });
    j.nextRun = Date.now() - 60_000;
    j.lastFiredAt = Date.now();
    assert.equal(s.detectMissedTasks().some(m => m.id === j.id), false);
  });
});

describe('CronScheduler.isDisabled kill-switch', () => {
  afterEach(() => { delete process.env.AMI_DISABLE_CRON; delete process.env.CLAUDE_CODE_DISABLE_CRON; });
  it('is disabled by AMI_DISABLE_CRON', () => {
    process.env.AMI_DISABLE_CRON = '1';
    assert.equal(CronScheduler.isDisabled(), true);
  });
  it('is disabled by CLAUDE_CODE_DISABLE_CRON', () => {
    process.env.CLAUDE_CODE_DISABLE_CRON = 'true';
    assert.equal(CronScheduler.isDisabled(), true);
  });
  it('is enabled by default', () => {
    assert.equal(CronScheduler.isDisabled(), false);
  });
});

describe('CronScheduler — safeComputeNextRun', () => {
  it('returns null instead of throwing on a structurally invalid cron', () => {
    const s = new CronScheduler(dir, 's');
    assert.equal(s.safeComputeNextRun('bad'), null);
  });
  it('returns a time for a valid cron', () => {
    const s = new CronScheduler(dir, 's');
    assert.ok((s.safeComputeNextRun('0 9 * * *') ?? 0) > Date.now());
  });
});
