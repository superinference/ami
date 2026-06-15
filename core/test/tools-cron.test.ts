import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { CronScheduler } from '../src/cron';
import { cronCreateTool, getCronScheduler, resetCronScheduler } from '../src/tools/cron-create';
import { cronDeleteTool } from '../src/tools/cron-delete';
import { cronListTool } from '../src/tools/cron-list';
import { scheduleWakeupTool } from '../src/tools/schedule-wakeup';
import type { ToolContext } from '../src/types';

function ctx(cwd: string): ToolContext {
  return { cwd, abortSignal: new AbortController().signal };
}

// ---------------------------------------------------------------------------
// CronScheduler core
// ---------------------------------------------------------------------------

describe('CronScheduler – core', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    sched = new CronScheduler(tmpDir);
  });

  it('creates a job with correct defaults', () => {
    const job = sched.create({ cron: '*/5 * * * *', prompt: 'test' });
    assert.ok(job.id.startsWith('cron_'));
    assert.equal(job.cron, '*/5 * * * *');
    assert.equal(job.prompt, 'test');
    assert.equal(job.recurring, true);
    assert.equal(job.durable, false);
    assert.ok(job.nextRun);
  });

  it('creates a one-shot job', () => {
    const job = sched.create({ cron: '0 9 * * *', prompt: 'once', recurring: false });
    assert.equal(job.recurring, false);
  });

  it('deletes a job', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'del' });
    assert.equal(sched.delete(job.id), true);
    assert.equal(sched.get(job.id), undefined);
  });

  it('returns false for deleting non-existent job', () => {
    assert.equal(sched.delete('nonexistent'), false);
  });

  it('lists all jobs', () => {
    sched.create({ cron: '* * * * *', prompt: 'a' });
    sched.create({ cron: '0 * * * *', prompt: 'b' });
    assert.equal(sched.list().length, 2);
  });

  it('gets a job by id', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'g' });
    const got = sched.get(job.id);
    assert.equal(got?.prompt, 'g');
  });

  it('getDueJobs finds due jobs', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'due' });
    (job as any).nextRun = Date.now() - 20 * 60 * 1000;
    const due = sched.getDueJobs();
    assert.ok(due.length >= 1);
  });

  it('markRun deletes one-shot jobs', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'once', recurring: false });
    sched.markRun(job.id);
    assert.equal(sched.get(job.id), undefined);
  });

  it('markRun reschedules recurring jobs', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'rec', recurring: true });
    const oldNext = job.nextRun;
    sched.markRun(job.id);
    const updated = sched.get(job.id);
    assert.ok(updated);
    assert.ok(updated.lastRun);
  });
});

// ---------------------------------------------------------------------------
// matchField via computeNextRun
// ---------------------------------------------------------------------------

describe('CronScheduler – cron parsing', () => {
  let sched: CronScheduler;

  beforeEach(() => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    sched = new CronScheduler(tmpDir);
  });

  it('computes next run for every-minute cron', () => {
    const next = sched.computeNextRun('* * * * *');
    assert.ok(next > Date.now() - 1000);
    assert.ok(next < Date.now() + 120000);
  });

  it('computes next run for step expressions', () => {
    const next = sched.computeNextRun('*/10 * * * *');
    assert.ok(next > Date.now() - 1000);
  });

  it('handles range expressions', () => {
    const next = sched.computeNextRun('0-30 * * * *');
    assert.ok(next > 0);
  });

  it('handles list expressions', () => {
    const next = sched.computeNextRun('0,15,30,45 * * * *');
    assert.ok(next > 0);
  });

  it('throws for invalid cron', () => {
    assert.throws(() => sched.computeNextRun('bad'), /Invalid cron expression/);
  });

  it('formatSchedule produces human-readable text', () => {
    assert.ok(sched.formatSchedule('*/5 * * * *').includes('every 5 minutes'));
    assert.ok(sched.formatSchedule('0 9 * * *').includes('minute 0'));
    assert.ok(sched.formatSchedule('0 9 * * 1,5').includes('Mon'));
    assert.equal(sched.formatSchedule('* * * * *'), 'every minute');
  });
});

// ---------------------------------------------------------------------------
// Durable persistence
// ---------------------------------------------------------------------------

describe('CronScheduler – durable persistence', () => {
  it('saves and loads durable jobs', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const sched1 = new CronScheduler(tmpDir);
    sched1.create({ cron: '0 9 * * *', prompt: 'durable test', durable: true });
    sched1.create({ cron: '0 10 * * *', prompt: 'session only', durable: false });

    const sched2 = new CronScheduler(tmpDir);
    const jobs = sched2.list();
    assert.ok(jobs.some(j => j.prompt === 'durable test'));
    assert.ok(!jobs.some(j => j.prompt === 'session only'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('removes durable job from disk on delete', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const sched = new CronScheduler(tmpDir);
    const job = sched.create({ cron: '* * * * *', prompt: 'd', durable: true });
    sched.delete(job.id);

    const sched2 = new CronScheduler(tmpDir);
    assert.equal(sched2.list().length, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// cron_create tool
// ---------------------------------------------------------------------------

describe('cronCreateTool', () => {
  beforeEach(() => resetCronScheduler());

  it('has correct name', () => {
    assert.equal(cronCreateTool.name, 'cron_create');
  });

  it('requires cron and prompt', () => {
    assert.ok(cronCreateTool.inputSchema.required?.includes('cron'));
    assert.ok(cronCreateTool.inputSchema.required?.includes('prompt'));
  });

  it('creates a job and returns id', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const result = await cronCreateTool.execute(
      { cron: '*/5 * * * *', prompt: 'hello' },
      ctx(tmpDir),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Scheduled job'));
    assert.ok(result.output.includes('every 5 minutes'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('rejects invalid cron', async () => {
    const result = await cronCreateTool.execute(
      { cron: 'bad', prompt: 'test' },
      ctx(os.tmpdir()),
    );
    assert.ok(result.isError);
  });

  it('rejects empty prompt', async () => {
    const result = await cronCreateTool.execute(
      { cron: '* * * * *', prompt: '' },
      ctx(os.tmpdir()),
    );
    assert.ok(result.isError);
  });
});

// ---------------------------------------------------------------------------
// cron_delete tool
// ---------------------------------------------------------------------------

describe('cronDeleteTool', () => {
  beforeEach(() => resetCronScheduler());

  it('has correct name', () => {
    assert.equal(cronDeleteTool.name, 'cron_delete');
  });

  it('deletes existing job', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const sched = getCronScheduler(tmpDir);
    const job = sched.create({ cron: '* * * * *', prompt: 'del' });

    const result = await cronDeleteTool.execute({ id: job.id }, ctx(tmpDir));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Deleted'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns error for non-existent id', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    resetCronScheduler();
    const result = await cronDeleteTool.execute({ id: 'fake' }, ctx(tmpDir));
    assert.ok(result.isError);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('rejects missing id', async () => {
    const result = await cronDeleteTool.execute({}, ctx(os.tmpdir()));
    assert.ok(result.isError);
  });
});

// ---------------------------------------------------------------------------
// cron_list tool
// ---------------------------------------------------------------------------

describe('cronListTool', () => {
  beforeEach(() => resetCronScheduler());

  it('has correct name', () => {
    assert.equal(cronListTool.name, 'cron_list');
  });

  it('shows "No scheduled jobs" when empty', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const result = await cronListTool.execute({}, ctx(tmpDir));
    assert.ok(result.output.includes('No scheduled jobs'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('lists jobs with details', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const sched = getCronScheduler(tmpDir);
    sched.create({ cron: '*/5 * * * *', prompt: 'list test', recurring: true, durable: false });

    const result = await cronListTool.execute({}, ctx(tmpDir));
    assert.ok(result.output.includes('1 scheduled'));
    assert.ok(result.output.includes('recurring'));
    assert.ok(result.output.includes('session-only'));
    assert.ok(result.output.includes('list test'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// schedule_wakeup tool
// ---------------------------------------------------------------------------

describe('scheduleWakeupTool', () => {
  beforeEach(() => resetCronScheduler());

  it('has correct name', () => {
    assert.equal(scheduleWakeupTool.name, 'schedule_wakeup');
  });

  it('requires delaySeconds, prompt, reason', () => {
    assert.ok(scheduleWakeupTool.inputSchema.required?.includes('delaySeconds'));
    assert.ok(scheduleWakeupTool.inputSchema.required?.includes('prompt'));
    assert.ok(scheduleWakeupTool.inputSchema.required?.includes('reason'));
  });

  it('creates a wakeup and returns details', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const result = await scheduleWakeupTool.execute(
      { delaySeconds: 120, prompt: 'check status', reason: 'waiting for CI' },
      ctx(tmpDir),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Wakeup scheduled'));
    assert.ok(result.output.includes('120s'));
    assert.ok(result.output.includes('waiting for CI'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('clamps delay to 60 minimum', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const result = await scheduleWakeupTool.execute(
      { delaySeconds: 10, prompt: 'fast', reason: 'test' },
      ctx(tmpDir),
    );
    assert.ok(result.output.includes('60s'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('clamps delay to 3600 maximum', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-'));
    const result = await scheduleWakeupTool.execute(
      { delaySeconds: 9999, prompt: 'slow', reason: 'test' },
      ctx(tmpDir),
    );
    assert.ok(result.output.includes('3600s'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('rejects empty prompt', async () => {
    const result = await scheduleWakeupTool.execute(
      { delaySeconds: 60, prompt: '', reason: 'test' },
      ctx(os.tmpdir()),
    );
    assert.ok(result.isError);
  });
});
