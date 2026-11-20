/**
 * Parity unit U033 (w97) — CronScheduler runtime gating options:
 *
 *  1. Injectable programmatic kill-switch (setKillSwitch): the CALLBACK form of
 *     the env AMI_DISABLE_CRON gate, polled once at tick entry so flipping it mid
 *     session stops an already-running scheduler (not just new ones). Surfaced via
 *     isKilled(). Mirrors the reference's isKilled() scheduler option.
 *
 *  2. Per-task filter (setTaskFilter): a predicate applied before ANY side effect.
 *     A job returning false is invisible to this scheduler instance — never fired,
 *     stamped, deleted/expired, surfaced as missed, or counted in getNextFireTime().
 *     Mirrors the reference's per-task filter scheduler option.
 *
 *  3. Orphaned-cron cleanup (setAgentAliveCheck): a due recurring cron whose owning
 *     teammate (agentId) is gone is pruned, with a diagnostic log, instead of firing
 *     into nowhere every tick. Mirrors the reference's onFireTask orphaned-cron removal.
 *
 * References: src/utils/cronScheduler.ts (isKilled/filter) and
 *             src/hooks/useScheduledTasks.ts (orphaned-cron cleanup).
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CronScheduler, resetScheduler } from '../src/cron';

type Ticker = { tick(cb: (j: unknown) => void): void };
function tickOnce(sched: CronScheduler, cb: (j: unknown) => void = () => {}): void {
  (sched as unknown as Ticker).tick(cb);
}

let sched: CronScheduler;
let tmpDir: string;
const savedEnv: Record<string, string | undefined> = {};

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-ksf-'));
  sched = new CronScheduler(tmpDir);
  savedEnv.AMI_DISABLE_CRON = process.env.AMI_DISABLE_CRON;
  savedEnv.CLAUDE_CODE_DISABLE_CRON = process.env.CLAUDE_CODE_DISABLE_CRON;
  delete process.env.AMI_DISABLE_CRON;
  delete process.env.CLAUDE_CODE_DISABLE_CRON;
});

afterEach(() => {
  for (const k of Object.keys(savedEnv)) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
  try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
});

// Create a job whose nextRun is forced into the past so it is unconditionally due.
function dueJob(opts: { cron?: string; agentId?: string; recurring?: boolean } = {}) {
  const job = sched.create({
    cron: opts.cron ?? '* * * * *',
    prompt: 'do the thing',
    recurring: opts.recurring ?? true,
    durable: false,
    ...(opts.agentId ? { agentId: opts.agentId } : {}),
  });
  job.nextRun = Date.now() - 600_000; // well past any jitter window
  return job;
}

// ---------------------------------------------------------------------------
// 1. Injectable kill-switch
// ---------------------------------------------------------------------------
describe('CronScheduler.setKillSwitch (injectable kill-switch)', () => {
  it('fires normally when no kill-switch is set', () => {
    dueJob();
    let fired = 0;
    tickOnce(sched, () => { fired++; });
    assert.equal(fired, 1);
  });

  it('stops an already-running scheduler the moment the callback flips to true', () => {
    const job = dueJob();
    let killed = false;
    sched.setKillSwitch(() => killed);

    let fired = 0;
    tickOnce(sched, () => { fired++; });
    assert.equal(fired, 1, 'fires while the switch is off');

    killed = true;
    job.nextRun = Date.now() - 600_000; // still due, but the kill-switch must gate it
    tickOnce(sched, () => { fired++; });
    assert.equal(fired, 1, 'bails without firing while the switch is on');

    killed = false;
    job.nextRun = Date.now() - 600_000;
    tickOnce(sched, () => { fired++; });
    assert.equal(fired, 2, 'resumes firing when the switch flips back off');
  });

  it('isKilled() reflects the env gate, the callback, and clearing with null', () => {
    assert.equal(sched.isKilled(), false);
    sched.setKillSwitch(() => true);
    assert.equal(sched.isKilled(), true, 'callback kill-switch reported');
    sched.setKillSwitch(null);
    assert.equal(sched.isKilled(), false, 'cleared callback');
    process.env.AMI_DISABLE_CRON = '1';
    assert.equal(sched.isKilled(), true, 'env gate reported by isKilled()');
  });
});

// ---------------------------------------------------------------------------
// 2. Per-task filter
// ---------------------------------------------------------------------------
describe('CronScheduler.setTaskFilter (per-task visibility)', () => {
  it('a filtered-out job is never due and never fires', () => {
    const keep = dueJob({ agentId: 'keep@team' });
    dueJob({ agentId: 'drop@team' });
    sched.setTaskFilter(j => j.agentId === 'keep@team');

    const due = sched.getDueJobs();
    assert.deepEqual(due.map(j => j.id), [keep.id]);

    const firedIds: string[] = [];
    tickOnce(sched, (j) => { firedIds.push((j as { id: string }).id); });
    assert.deepEqual(firedIds, [keep.id], 'only the visible job fires');
  });

  it('getNextFireTime() ignores filtered-out jobs', () => {
    const soon = sched.create({ cron: '* * * * *', prompt: 'soon', recurring: true, durable: false, agentId: 'a@t' });
    const later = sched.create({ cron: '* * * * *', prompt: 'later', recurring: true, durable: false, agentId: 'b@t' });
    soon.nextRun = 1000;
    later.nextRun = 5000;

    sched.setTaskFilter(j => j.agentId === 'b@t'); // hide the sooner one
    assert.equal(sched.getNextFireTime(), 5000);

    sched.setTaskFilter(null);
    assert.equal(sched.getNextFireTime(), 1000);
  });

  it('detectMissedTasks() excludes filtered-out jobs', () => {
    const a = sched.create({ cron: '* * * * *', prompt: 'a', recurring: false, durable: false, agentId: 'a@t' });
    const b = sched.create({ cron: '* * * * *', prompt: 'b', recurring: false, durable: false, agentId: 'b@t' });
    a.nextRun = Date.now() - 600_000;
    b.nextRun = Date.now() - 600_000;

    sched.setTaskFilter(j => j.agentId === 'a@t');
    const missed = sched.detectMissedTasks().map(j => j.id);
    assert.deepEqual(missed, [a.id], 'only the visible missed job is surfaced');
  });

  it('expireOld() never expires a filtered-out aged job', () => {
    const aged = sched.create({ cron: '* * * * *', prompt: 'old', recurring: true, durable: false, agentId: 'hidden@t' });
    aged.createdAt = Date.now() - 999 * 86400000; // far beyond MAX_AGE_DAYS
    aged.nextRun = Date.now() - 1000;

    sched.setTaskFilter(() => false); // hide everything
    tickOnce(sched);
    assert.ok(sched.get(aged.id), 'a hidden job is not expired by this instance');

    sched.setTaskFilter(null);
    tickOnce(sched);
    assert.equal(sched.get(aged.id), undefined, 'visible again → expired');
  });
});

// ---------------------------------------------------------------------------
// 3. Orphaned-cron cleanup
// ---------------------------------------------------------------------------
describe('CronScheduler.setAgentAliveCheck (orphaned-cron cleanup)', () => {
  it('prunes a due recurring cron whose owner is gone, logging a diagnostic', () => {
    const orphan = dueJob({ agentId: 'ghost@team' });
    sched.setAgentAliveCheck(() => false); // owner is always gone

    let fired = 0;
    tickOnce(sched, () => { fired++; });
    assert.equal(fired, 0, 'an orphaned cron fires into nowhere — it must NOT fire');
    assert.equal(sched.get(orphan.id), undefined, 'orphaned cron is pruned');
  });

  it('fires normally while the owner is still alive', () => {
    const live = dueJob({ agentId: 'alive@team' });
    sched.setAgentAliveCheck(id => id === 'alive@team');

    let fired = 0;
    tickOnce(sched, () => { fired++; });
    assert.equal(fired, 1);
    assert.ok(sched.get(live.id), 'a live-owner recurring cron stays scheduled');
  });

  it('never prunes a job without an agentId (global job)', () => {
    const global = dueJob(); // no agentId
    sched.setAgentAliveCheck(() => false);
    let fired = 0;
    tickOnce(sched, () => { fired++; });
    assert.equal(fired, 1, 'a global job has no owner to vanish');
    assert.ok(sched.get(global.id), 'global recurring job survives');
  });
});
