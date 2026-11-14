import { describe, it, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { CronScheduler } from '../src/cron';
import { projectStateDir } from '../src/reference-journal';

// Exercises the private scheduler tick() loop (expireOld -> getDueJobs -> run
// each due job once), including the durable-job lock gate. tick() normally only
// fires from an unref'd 5s setInterval, so we invoke it directly (deterministic)
// to cover the tick body and its branches.

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-cov-'));
  tmpDirs.push(d);
  return d;
}

function invokeTick(sched: CronScheduler, onDue: (job: unknown) => void): void {
  // tick is TS-private; call it directly to exercise the loop deterministically.
  (sched as unknown as { tick: (cb: (job: unknown) => void) => void }).tick(onDue);
}

describe('cron tick() loop (cov100)', () => {
  after(() => {
    for (const d of tmpDirs) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  let cwd: string;
  beforeEach(() => { cwd = mkTmp(); });

  it('runs a non-durable due job once: fires onDue and marks it run', () => {
    const sched = new CronScheduler(cwd, 'sess-A');
    const job = sched.create({ cron: '* * * * *', prompt: 'hello', recurring: true, durable: false });
    // Force the job to be well past due (beyond any jitter window).
    job.nextRun = Date.now() - 10_000_000;

    const fired: unknown[] = [];
    invokeTick(sched, (j) => fired.push(j));

    assert.equal(fired.length, 1, 'due job should fire exactly once');
    assert.equal((fired[0] as { id: string }).id, job.id);
    // markRun was called: recurring job gets lastRun/lastFiredAt set and a new nextRun.
    const after = sched.get(job.id);
    assert.ok(after, 'recurring job should still exist after run');
    assert.ok(after!.lastRun && after!.lastFiredAt, 'markRun should set run timestamps');
    assert.ok(after!.nextRun! > Date.now(), 'nextRun should be rescheduled into the future');
  });

  it('skips nothing when there are no due jobs', () => {
    const sched = new CronScheduler(cwd, 'sess-B');
    const job = sched.create({ cron: '* * * * *', prompt: 'later' });
    job.nextRun = Date.now() + 10_000_000; // far in the future -> not due

    const fired: unknown[] = [];
    invokeTick(sched, (j) => fired.push(j));

    assert.equal(fired.length, 0, 'no job is due, so onDue must not fire');
  });

  it('runs a durable due job when the lock is free', () => {
    const sched = new CronScheduler(cwd, 'sess-C');
    const job = sched.create({ cron: '* * * * *', prompt: 'durable work', durable: true });
    job.nextRun = Date.now() - 10_000_000;

    const fired: unknown[] = [];
    invokeTick(sched, (j) => fired.push(j));

    assert.equal(fired.length, 1, 'durable due job should run when lock is acquirable');
    assert.equal((fired[0] as { id: string }).id, job.id);
    // Acquiring the lock wrote a lock file owned by this session.
    const lockPath = path.join(projectStateDir(cwd), 'cron.lock');
    assert.ok(fs.existsSync(lockPath), 'acquireLock should have created the lock file');
    const lock = JSON.parse(fs.readFileSync(lockPath, 'utf-8'));
    assert.equal(lock.sessionId, 'sess-C');
  });

  it('skips a durable due job when another live session holds the lock', () => {
    const sched = new CronScheduler(cwd, 'sess-D');
    const job = sched.create({ cron: '* * * * *', prompt: 'contended', durable: true });
    job.nextRun = Date.now() - 10_000_000;

    // A different session whose owning process is ALIVE owns the lock, so
    // acquireLock() must defer (PID-liveness never steals a live owner's lease).
    const lockDir = projectStateDir(cwd);
    fs.mkdirSync(lockDir, { recursive: true });
    fs.writeFileSync(
      path.join(lockDir, 'cron.lock'),
      JSON.stringify({ sessionId: 'other-session', pid: process.pid, timestamp: Date.now() }),
    );

    const fired: unknown[] = [];
    invokeTick(sched, (j) => fired.push(j));

    assert.equal(fired.length, 0, 'durable job must be skipped when another session holds the lock');
    // It was NOT marked run: no run timestamps and nextRun unchanged (still in the past).
    const after = sched.get(job.id);
    assert.ok(after, 'skipped job should still exist');
    assert.equal(after!.lastRun, undefined, 'skipped job must not be marked run');
    assert.ok(after!.nextRun! < Date.now(), 'skipped job nextRun should be unchanged');
  });

  it('expires a too-old recurring job during the tick, firing it one final time', () => {
    const sched = new CronScheduler(cwd, 'sess-E');
    const job = sched.create({ cron: '* * * * *', prompt: 'ancient', recurring: true });
    // Older than its max age so expireOld() removes it at the top of tick().
    job.createdAt = Date.now() - (job.maxAgeDays ?? 7) * 86400000 - 86400000;
    job.nextRun = Date.now() - 10_000_000; // due → fires once before deletion

    const fired: unknown[] = [];
    invokeTick(sched, (j) => fired.push(j));

    // An aged-but-due recurring job fires exactly once, then is removed.
    assert.equal(fired.length, 1, 'aged recurring job should fire one final time');
    assert.equal(sched.get(job.id), undefined, 'expired job should be removed from the scheduler');
  });

  it('expires a too-old recurring job WITHOUT a final fire when it is not due', () => {
    const sched = new CronScheduler(cwd, 'sess-E2');
    const job = sched.create({ cron: '0 0 1 1 *', prompt: 'ancient-not-due', recurring: true });
    job.createdAt = Date.now() - (job.maxAgeDays ?? 7) * 86400000 - 86400000;
    job.nextRun = Date.now() + 10_000_000; // not due → no final fire

    const fired: unknown[] = [];
    invokeTick(sched, (j) => fired.push(j));

    assert.equal(fired.length, 0, 'not-due expired job should not fire');
    assert.equal(sched.get(job.id), undefined, 'expired job should be removed');
  });
});
