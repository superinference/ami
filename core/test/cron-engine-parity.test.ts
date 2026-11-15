import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  CronScheduler,
  matchField,
  type CronJob,
} from '../src/cron';

// Additive-superset hardening tests for the cron scheduler ENGINE (cron.ts only).
// All timing is driven by fixed timestamps / explicit markRun() calls — no real
// waiting. Each test documents the RED (pre-fix) behavior it discriminates.

let dir: string;
const mk = () => fs.mkdtempSync(path.join(os.tmpdir(), 'cron-engine-parity-'));
beforeEach(() => { dir = mk(); });
afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

describe('leap-cycle scan horizon', () => {
  it('resolves a Feb-29 schedule whose next fire is >1 year out', () => {
    // RED (527040-minute / ~366-day horizon): returns null — a leap-day reminder
    // created in a non-leap year silently never fires.
    const s = new CronScheduler(dir);
    const from = new Date('2025-03-01T00:00:00').getTime();
    const next = s.computeNextRun('0 0 29 2 *', from);
    assert.notEqual(next, null, 'Feb 29 schedule must resolve within the leap-cycle horizon');
    const d = new Date(next as number);
    assert.equal(d.getFullYear(), 2028);
    assert.equal(d.getMonth(), 1);   // February (0-based)
    assert.equal(d.getDate(), 29);
  });

  it('still returns null for a truly impossible calendar date', () => {
    const s = new CronScheduler(dir);
    assert.equal(s.computeNextRun('0 0 30 2 *'), null, 'Feb 30 never exists');
    assert.equal(s.computeNextRun('0 0 31 4 *'), null, 'April 31 never exists');
  });
});

describe('unsatisfiable-schedule rejection in create()', () => {
  it('throws when a structurally-valid cron never matches a real date', () => {
    // RED: create() returned a job with nextRun undefined — a dead job the caller
    // believed was scheduled; only the cron tool (not the engine) guarded this.
    const s = new CronScheduler(dir);
    assert.throws(() => s.create({ cron: '0 0 30 2 *', prompt: 'p' }), /unsatisfiable/);
  });

  it('still creates a satisfiable schedule with a concrete nextRun', () => {
    const s = new CronScheduler(dir);
    const job = s.create({ cron: '0 9 * * *', prompt: 'p' });
    assert.equal(typeof job.nextRun, 'number');
  });
});

describe('per-job max-runs budget', () => {
  it('validates maxRuns is a positive integer', () => {
    // RED: maxRuns was ignored entirely — a bogus budget created a job that either
    // never retired or (negative) is nonsensical.
    const s = new CronScheduler(dir);
    assert.throws(() => s.create({ cron: '*/5 * * * *', prompt: 'p', maxRuns: 0 }), /positive integer/);
    assert.throws(() => s.create({ cron: '*/5 * * * *', prompt: 'p', maxRuns: -1 }), /positive integer/);
    assert.throws(() => s.create({ cron: '*/5 * * * *', prompt: 'p', maxRuns: 2.5 }), /positive integer/);
  });

  it('retires a recurring job after it exhausts its run budget', () => {
    // RED: markRun always recomputed nextRun for a recurring job — maxRuns had no
    // effect and the job fired forever.
    const s = new CronScheduler(dir);
    const job = s.create({ cron: '*/5 * * * *', prompt: 'p', maxRuns: 2 });
    assert.equal(job.runCount, 0);
    assert.equal(job.maxRuns, 2);

    s.markRun(job.id);
    const afterOne = s.get(job.id);
    assert.ok(afterOne, 'job survives its first fire');
    assert.equal(afterOne!.runCount, 1);
    assert.equal(typeof afterOne!.nextRun, 'number', 'still scheduled after 1/2 runs');

    s.markRun(job.id);
    assert.equal(s.get(job.id), undefined, 'job retired once the budget is exhausted');
  });

  it('leaves budget-less recurring jobs unlimited', () => {
    const s = new CronScheduler(dir);
    const job = s.create({ cron: '*/5 * * * *', prompt: 'p' });
    for (let i = 0; i < 5; i++) s.markRun(job.id);
    assert.ok(s.get(job.id), 'no budget → job keeps firing');
    assert.equal(s.get(job.id)!.runCount, undefined);
  });
});

describe('matchField day-of-week ranges ending at 7 (Sunday)', () => {
  it('treats 0-7 as every day (not Sunday-only)', () => {
    // RED: raw %7 collapsed "0-7" to 0-0, matching ONLY Sunday and silently
    // dropping Mon–Sat.
    for (let d = 0; d <= 6; d++) {
      assert.equal(matchField(d, '0-7', 0, 6), true, `day ${d} should match 0-7`);
    }
  });

  it('treats 5-7 as Fri/Sat/Sun', () => {
    assert.deepEqual(
      [0, 1, 2, 3, 4, 5, 6].map(d => matchField(d, '5-7', 0, 6)),
      [true, false, false, false, false, true, true],
    );
  });

  it('keeps 7-7 as Sunday-only (regression guard)', () => {
    assert.equal(matchField(0, '7-7', 0, 6), true);
    assert.equal(matchField(3, '7-7', 0, 6), false);
  });
});

/** Subclass that simulates a crash mid-write via the rawWriteFile seam. */
class CrashingScheduler extends CronScheduler {
  crash = false;
  protected rawWriteFile(filePath: string, data: string): void {
    if (this.crash) {
      // Simulate a torn write: flush a truncated prefix, then die.
      fs.writeFileSync(filePath, data.slice(0, Math.floor(data.length / 2)), 'utf-8');
      throw new Error('simulated crash mid-write');
    }
    super['rawWriteFile'](filePath, data);
  }
}

describe('atomic durable persistence', () => {
  it('preserves existing durable state when a save crashes mid-write', () => {
    // RED (direct writeFileSync to the target): a torn write corrupts
    // scheduled_tasks.json; on reload loadDurable JSON.parse fails, deletes the
    // file, and EVERY durable job is lost. Here the second create()'s save crashes
    // and must leave the first job's persisted state intact + uncorrupted.
    const s = new CrashingScheduler(dir);
    const a = s.create({ cron: '0 9 * * *', prompt: 'job-A', durable: true });

    s.crash = true;
    // Save of job B crashes; the atomic temp+rename path must NOT touch the file.
    s.create({ cron: '0 10 * * *', prompt: 'job-B', durable: true });
    s.crash = false;

    const reloaded = new CronScheduler(dir);
    const jobs: CronJob[] = reloaded.list();
    assert.equal(jobs.length, 1, 'only the committed job survives the crash');
    assert.equal(jobs[0].id, a.id);
    assert.equal(jobs[0].prompt, 'job-A');
  });

  it('round-trips durable jobs and leaves no temp file behind', () => {
    const s = new CronScheduler(dir);
    const a = s.create({ cron: '0 9 * * *', prompt: 'keep', durable: true, maxRuns: 3 });
    const reloaded = new CronScheduler(dir);
    const got = reloaded.get(a.id);
    assert.ok(got);
    assert.equal(got!.maxRuns, 3);
    assert.equal(got!.runCount, 0);
    // No *.tmp artifact should linger anywhere under the persist dir.
    const persistDir = path.dirname((s as unknown as { persistPath: string }).persistPath);
    const leftovers = fs.readdirSync(persistDir).filter(f => f.endsWith('.tmp'));
    assert.deepEqual(leftovers, []);
  });
});
