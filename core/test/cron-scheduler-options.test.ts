/**
 * Parity unit U033 — CronScheduler runtime options:
 *  - getNextFireTime(): soonest scheduled fire across all jobs (null if none).
 *  - mid-session kill-switch: tick() stops firing already-running schedulers
 *    the moment AMI_DISABLE_CRON / CLAUDE_CODE_DISABLE_CRON flips on, without
 *    tearing the scheduler down.
 *  - batched fire-stamp persistence: N durable fires in one tick coalesce into
 *    a single file write instead of one write per fired job.
 *
 * Mirrors Claude's createCronScheduler() options (getNextFireTime / isKilled /
 * batched markCronTasksFired) in references/claude/.../utils/cronScheduler.ts.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CronScheduler, resetScheduler } from '../src/cron';

let sched: CronScheduler;
let tmpDir: string;
const savedEnv: Record<string, string | undefined> = {};

beforeEach(() => {
  resetScheduler();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cron-opts-'));
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
});

// ---------------------------------------------------------------------------
// getNextFireTime()
// ---------------------------------------------------------------------------

describe('CronScheduler.getNextFireTime', () => {
  it('returns null when there are no jobs', () => {
    assert.equal(sched.getNextFireTime(), null);
  });

  it('returns the soonest nextRun across all jobs', () => {
    const a = sched.create({ cron: '*/5 * * * *', prompt: 'a', recurring: true });
    const b = sched.create({ cron: '*/10 * * * *', prompt: 'b', recurring: true });
    const soon = Date.now() + 60_000;
    const later = Date.now() + 600_000;
    sched.get(a.id)!.nextRun = later;
    sched.get(b.id)!.nextRun = soon;
    assert.equal(sched.getNextFireTime(), soon);
  });

  it('ignores jobs whose nextRun is undefined', () => {
    const a = sched.create({ cron: '*/5 * * * *', prompt: 'a', recurring: true });
    const pending = Date.now() + 120_000;
    sched.get(a.id)!.nextRun = pending;
    const b = sched.create({ cron: '*/10 * * * *', prompt: 'b', recurring: true });
    sched.get(b.id)!.nextRun = undefined;
    assert.equal(sched.getNextFireTime(), pending);
  });

  it('returns null when every job has an undefined nextRun', () => {
    const a = sched.create({ cron: '*/5 * * * *', prompt: 'a', recurring: true });
    sched.get(a.id)!.nextRun = undefined;
    assert.equal(sched.getNextFireTime(), null);
  });
});

// ---------------------------------------------------------------------------
// Mid-session kill-switch — tick() bails while the env gate is on.
// ---------------------------------------------------------------------------

describe('CronScheduler mid-session kill-switch', () => {
  function makeDueJob(durable = false): string {
    const job = sched.create({ cron: '*/5 * * * *', prompt: 'ping', recurring: true, durable });
    sched.get(job.id)!.nextRun = Date.now() - 3_600_000; // overdue
    return job.id;
  }

  it('fires a due job when the kill-switch is off', () => {
    makeDueJob();
    let fired = 0;
    (sched as unknown as { tick(cb: (j: unknown) => void): void }).tick(() => { fired++; });
    assert.equal(fired, 1);
  });

  it('does NOT fire when AMI_DISABLE_CRON flips on mid-session', () => {
    makeDueJob();
    process.env.AMI_DISABLE_CRON = '1';
    let fired = 0;
    (sched as unknown as { tick(cb: (j: unknown) => void): void }).tick(() => { fired++; });
    assert.equal(fired, 0, 'kill-switch must stop an already-running scheduler');
  });

  it('does NOT fire when CLAUDE_CODE_DISABLE_CRON flips on mid-session', () => {
    makeDueJob();
    process.env.CLAUDE_CODE_DISABLE_CRON = 'true';
    let fired = 0;
    (sched as unknown as { tick(cb: (j: unknown) => void): void }).tick(() => { fired++; });
    assert.equal(fired, 0);
  });

  it('resumes firing after the kill-switch is cleared (no teardown)', () => {
    const id = makeDueJob();
    process.env.AMI_DISABLE_CRON = '1';
    let fired = 0;
    const tick = (sched as unknown as { tick(cb: (j: unknown) => void): void }).tick.bind(sched);
    tick(() => { fired++; });
    assert.equal(fired, 0);
    // job still present + still due — scheduler was not torn down
    assert.ok(sched.get(id));
    delete process.env.AMI_DISABLE_CRON;
    tick(() => { fired++; });
    assert.equal(fired, 1);
  });
});

// ---------------------------------------------------------------------------
// Batched fire-stamp persistence — N durable fires = one write per tick.
// ---------------------------------------------------------------------------

describe('CronScheduler batched fire-stamp persistence', () => {
  it('coalesces N durable fires in one tick into a single file write', () => {
    const ids: string[] = [];
    for (let i = 0; i < 3; i++) {
      const job = sched.create({ cron: '*/5 * * * *', prompt: `p${i}`, recurring: true, durable: true });
      sched.get(job.id)!.nextRun = Date.now() - 3_600_000;
      ids.push(job.id);
    }
    // Spy on the low-level write seam, counting only writes to the task file.
    let writes = 0;
    const persistPath = (sched as unknown as { persistPath: string }).persistPath;
    (sched as unknown as { rawWriteFile(f: string, d: string): void }).rawWriteFile = (f: string) => {
      if (path.basename(f).startsWith(path.basename(persistPath))) writes++;
      fs.writeFileSync(f, 'ok', 'utf-8'); // keep the rename step happy
    };
    (sched as unknown as { tick(cb: (j: unknown) => void): void }).tick(() => {});
    assert.equal(writes, 1, `expected 1 batched write, got ${writes}`);
  });
});
