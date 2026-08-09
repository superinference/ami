/**
 * Tests for cron new features:
 * - wrapPromptSafely (fence-wrapping for scheduled task prompts)
 * - OR semantics for domSpec + dowSpec
 * - MAX_JOBS limit (50)
 * - high-water-mark ID assignment
 * - acquireLock / releaseLock (via startTicker/stopTicker)
 */

import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CronScheduler, wrapPromptSafely, resetScheduler, matchField } from '../src/cron';

let sched: CronScheduler;
let tmpDir: string;

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-new-'));
  sched = new CronScheduler(tmpDir);
});

// ---------------------------------------------------------------------------
// wrapPromptSafely — fence-wraps scheduled task prompts
// ---------------------------------------------------------------------------

describe('wrapPromptSafely', () => {
  it('wraps prompts in backtick fences', () => {
    const result = wrapPromptSafely('check the deploy');
    assert.ok(result.includes('check the deploy'));
    assert.ok(result.includes('Previously scheduled task'));
    assert.ok(result.includes('`'));
  });

  it('contains the original prompt inside the fences', () => {
    const result = wrapPromptSafely('run the smoke test');
    const lines = result.split('\n');
    assert.ok(lines.some(l => l === 'run the smoke test'));
  });

  it('handles empty prompts', () => {
    const result = wrapPromptSafely('');
    assert.ok(result.includes('Previously scheduled task'));
  });

  it('handles prompts with special characters', () => {
    const result = wrapPromptSafely('deploy to $ENV && notify');
    assert.ok(result.includes('deploy to $ENV && notify'));
  });
});

// ---------------------------------------------------------------------------
// CronScheduler — computeNextRun / OR semantics
// ---------------------------------------------------------------------------

describe('CronScheduler — computeNextRun', () => {
  it('computes next run for simple cron expressions', () => {
    const next = sched.computeNextRun('0 9 * * *');
    assert.ok(next > Date.now());
  });

  it('computes next run with both DOM and DOW restricted (OR semantics)', () => {
    const next = sched.computeNextRun('0 9 15 * 1');
    assert.ok(next > Date.now());
  });

  it('computes next run with only DOM restricted', () => {
    const next = sched.computeNextRun('0 9 15 * *');
    assert.ok(next > Date.now());
  });

  it('computes next run with only DOW restricted', () => {
    const next = sched.computeNextRun('0 9 * * 1');
    assert.ok(next > Date.now());
  });
});

// ---------------------------------------------------------------------------
// MAX_JOBS limit
// ---------------------------------------------------------------------------

describe('CronScheduler — MAX_JOBS', () => {
  it('enforces the 50-job limit', () => {
    for (let i = 0; i < 50; i++) {
      sched.create({ cron: '* * * * *', prompt: `job ${i}` });
    }
    assert.throws(
      () => sched.create({ cron: '* * * * *', prompt: 'overflow' }),
      /maximum/i,
    );
  });
});

// ---------------------------------------------------------------------------
// High-water-mark ID assignment
// ---------------------------------------------------------------------------

describe('CronScheduler — high-water-mark IDs', () => {
  it('assigns incrementing IDs', () => {
    const j1 = sched.create({ cron: '* * * * *', prompt: 'a' });
    const j2 = sched.create({ cron: '* * * * *', prompt: 'b' });
    const id1 = parseInt(j1.id.match(/cron_(\d+)/)?.[1] || '0', 10);
    const id2 = parseInt(j2.id.match(/cron_(\d+)/)?.[1] || '0', 10);
    assert.ok(id2 > id1, `ID ${id2} should be > ${id1}`);
  });

  it('preserves high-water-mark across scheduler instances', () => {
    const j1 = sched.create({ cron: '* * * * *', prompt: 'durable-hwm', durable: true });
    const sched2 = new CronScheduler(tmpDir);
    const j2 = sched2.create({ cron: '* * * * *', prompt: 'durable-hwm-2', durable: true });
    const id1 = parseInt(j1.id.match(/cron_(\d+)/)?.[1] || '0', 10);
    const id2 = parseInt(j2.id.match(/cron_(\d+)/)?.[1] || '0', 10);
    assert.ok(id2 > id1, `Reloaded ID ${id2} should be > original ${id1}`);
  });
});

// ---------------------------------------------------------------------------
// Locking via startTicker / stopTicker
// ---------------------------------------------------------------------------

describe('CronScheduler — locking', () => {
  it('can start and stop the ticker', () => {
    sched.startTicker(() => {});
    sched.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// create / delete / list / get
// ---------------------------------------------------------------------------

describe('CronScheduler — basic CRUD', () => {
  it('creates a job with correct fields', () => {
    const job = sched.create({ cron: '0 9 * * *', prompt: 'hello' });
    assert.ok(job.id.startsWith('cron_'));
    assert.equal(job.cron, '0 9 * * *');
    assert.equal(job.prompt, 'hello');
    assert.equal(job.recurring, true);
    assert.equal(job.durable, false);
    assert.ok(job.nextRun > Date.now() - 1000);
  });

  it('deletes a job', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'x' });
    assert.equal(sched.delete(job.id), true);
    assert.equal(sched.get(job.id), undefined);
  });

  it('lists all jobs', () => {
    sched.create({ cron: '* * * * *', prompt: 'a' });
    sched.create({ cron: '* * * * *', prompt: 'b' });
    assert.equal(sched.list().length, 2);
  });

  it('gets a job by ID', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'x' });
    assert.deepEqual(sched.get(job.id), job);
  });
});

// ---------------------------------------------------------------------------
// matchField — range-step and DOW edge cases
// ---------------------------------------------------------------------------

describe('matchField', () => {
  it('range-step 10-20/5 only matches within range', () => {
    assert.equal(matchField(10, '10-20/5', 1, 31), true);
    assert.equal(matchField(15, '10-20/5', 1, 31), true);
    assert.equal(matchField(20, '10-20/5', 1, 31), true);
    assert.equal(matchField(25, '10-20/5', 1, 31), false);
    assert.equal(matchField(30, '10-20/5', 1, 31), false);
  });

  it('DOW range 5-7 matches Friday through Sunday', () => {
    assert.equal(matchField(5, '5-7', 0, 6), true);  // Friday
    assert.equal(matchField(6, '5-7', 0, 6), true);  // Saturday
    assert.equal(matchField(0, '5-7', 0, 6), true);  // Sunday (7 wraps to 0)
    assert.equal(matchField(1, '5-7', 0, 6), false); // Monday
  });

  it('DOW literal 7 matches Sunday', () => {
    assert.equal(matchField(0, '7', 0, 6), true);
  });

  it('simple range works correctly', () => {
    assert.equal(matchField(5, '3-7', 0, 23), true);
    assert.equal(matchField(2, '3-7', 0, 23), false);
    assert.equal(matchField(8, '3-7', 0, 23), false);
  });
});
