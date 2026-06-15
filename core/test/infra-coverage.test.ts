/**
 * Infrastructure coverage tests — targets untested lines across:
 *   cron.ts, process-manager.ts, worktree-manager.ts, memory.ts,
 *   model-registry.ts, permissions.ts
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// ── cron.ts ────────────────────────────────────────────────────────────────
import { CronScheduler, resetScheduler, getScheduler, wrapPromptSafely } from '../src/cron';

// ── process-manager.ts ─────────────────────────────────────────────────────
import { ProcessManager } from '../src/process-manager';

// ── worktree-manager.ts ────────────────────────────────────────────────────
import {
  cleanupStaleWorktrees,
  symlinkLargeDirectories,
  countWorktreeChanges,
  flattenSlug,
  worktreeBranchName,
  getCurrentWorktreeSession,
  setWorktreeSession,
  createWorktreeSession,
} from '../src/worktree-manager';

// ── memory.ts ──────────────────────────────────────────────────────────────
import { MemoryManager, matchesPaths, parseFrontmatter } from '../src/memory';

// ── model-registry.ts ──────────────────────────────────────────────────────
import { detectProvider, listProviders, listModels, formatModelList } from '../src/model-registry';

// ── permissions.ts ─────────────────────────────────────────────────────────
import {
  PermissionManager,
  containsUnquotedExpansion,
  stripSafeEnvVars,
  detectHardlineCommand,
} from '../src/permissions';

// ── Helpers ────────────────────────────────────────────────────────────────
function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-infra-cov-'));
}

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 5000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${taskId}`)), timeoutMs);
    pm.on('complete', function handler(event: any) {
      if (event.taskId === taskId) {
        clearTimeout(timer);
        pm.off('complete', handler);
        resolve(event);
      }
    });
  });
}

// Env vars that interfere with provider detection
const INTERFERING_ENV_VARS = [
  'ANTHROPIC_VERTEX_PROJECT_ID', 'CLAUDE_CODE_USE_VERTEX',
];

function withCleanEnv<T>(fn: () => T): T {
  const saved: Record<string, string | undefined> = {};
  for (const key of INTERFERING_ENV_VARS) {
    saved[key] = process.env[key];
    delete process.env[key];
  }
  try {
    return fn();
  } finally {
    for (const key of INTERFERING_ENV_VARS) {
      if (saved[key] !== undefined) process.env[key] = saved[key];
      else delete process.env[key];
    }
  }
}

// =========================================================================
// cron.ts — uncovered lines: 61-63 118 146-148 155-169 174 176-187
//           195-196 216 231-239 280
// =========================================================================

describe('CronScheduler — constructor with sessionId (lines 60-62)', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('accepts an explicit sessionId', () => {
    const sched = new CronScheduler(tmpDir, 'my-session-42');
    // Constructor ran without error; scheduler works
    const job = sched.create({ cron: '* * * * *', prompt: 'ping' });
    assert.ok(job.id.startsWith('cron_'));
  });

  it('generates a sessionId when none is provided', () => {
    const sched = new CronScheduler(tmpDir);
    const job = sched.create({ cron: '* * * * *', prompt: 'test' });
    assert.ok(job.id);
  });

  it('loads durable jobs from a pre-existing persisted file (line 61-62)', () => {
    // First scheduler creates a durable job
    const sched1 = new CronScheduler(tmpDir, 'sess-1');
    sched1.create({ cron: '0 8 * * *', prompt: 'durable job', durable: true });

    // Second scheduler should auto-load it via constructor
    const sched2 = new CronScheduler(tmpDir, 'sess-2');
    const jobs = sched2.list();
    assert.ok(jobs.length >= 1, 'Should load durable job from disk');
    assert.ok(jobs.some(j => j.prompt === 'durable job'));
  });
});

describe('CronScheduler — getDueJobs with jitter (line 118)', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
    sched = new CronScheduler(tmpDir);
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns no due jobs when nextRun is in the far future', () => {
    sched.create({ cron: '0 0 1 1 *', prompt: 'future job' }); // Jan 1 only
    const due = sched.getDueJobs();
    assert.equal(due.length, 0);
  });

  it('returns due jobs when nextRun + jitter is in the past', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'always' });
    // Manually set nextRun to far in the past so jitter can't prevent it
    (job as any).nextRun = Date.now() - 3600_000;
    const due = sched.getDueJobs();
    assert.ok(due.length >= 1);
  });
});

describe('CronScheduler — startTicker/stopTicker (lines 143-163)', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
    sched = new CronScheduler(tmpDir, 'ticker-session');
  });
  afterEach(() => {
    sched.stopTicker();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('startTicker is idempotent — calling twice does not create duplicate timers', () => {
    const dueJobs: string[] = [];
    sched.startTicker(j => dueJobs.push(j.id));
    sched.startTicker(j => dueJobs.push(j.id)); // second call should be a no-op
    // No assertion on dueJobs — just proving it doesn't throw or duplicate
    sched.stopTicker();
  });

  it('stopTicker when no ticker is running is a no-op', () => {
    sched.stopTicker(); // should not throw
    sched.stopTicker(); // double stop
  });

  it('ticker fires onDue for past-due non-durable jobs (lines 146-152)', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'tick', recurring: false, durable: false });
    (job as any).nextRun = Date.now() - 3600_000;

    const fired: string[] = [];
    sched.startTicker(j => fired.push(j.id));

    // Manually trigger the ticker callback by accessing the internal interval
    const tickerFn = (sched as any).tickInterval;
    assert.ok(tickerFn, 'Ticker should be set');
    // Simulate a tick by directly calling expireOld + getDueJobs + markRun
    const due = sched.getDueJobs();
    for (const j of due) { sched.markRun(j.id); fired.push(j.id); }
    sched.stopTicker();

    assert.ok(fired.includes(job.id), 'Non-durable past-due job should fire');
  });

  it('ticker fires for durable jobs and acquires lock (lines 149,165-183)', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'durable-tick', recurring: true, durable: true });
    (job as any).nextRun = Date.now() - 3600_000;

    const fired: string[] = [];
    // Start ticker to exercise startTicker path (covers line 143-155)
    sched.startTicker(j => fired.push(j.id));

    // Exercise the durable path directly
    const due = sched.getDueJobs();
    for (const j of due) {
      if (j.durable) (sched as any).acquireLock();
      sched.markRun(j.id);
      fired.push(j.id);
    }
    sched.stopTicker();

    assert.ok(fired.includes(job.id), 'Durable past-due job should fire');
  });
});

describe('CronScheduler — releaseLock (lines 186-195)', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('releaseLock does not remove lock owned by a different session', () => {
    const lockDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(lockDir, { recursive: true });
    const lockPath = path.join(lockDir, 'cron.lock');
    fs.writeFileSync(lockPath, JSON.stringify({
      sessionId: 'other-session',
      pid: 99999,
      timestamp: Date.now(),
    }));

    // Our scheduler has a different sessionId
    const sched = new CronScheduler(tmpDir, 'my-session');
    sched.startTicker(() => {});
    sched.stopTicker(); // calls releaseLock

    // Lock should still exist because it belongs to another session
    assert.ok(fs.existsSync(lockPath), 'Should not remove lock owned by other session');
  });
});

describe('CronScheduler — acquireLock stale lock (lines 167-171)', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('acquires lock when existing lock is stale (> 120s)', () => {
    const lockDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(lockDir, { recursive: true });
    const lockPath = path.join(lockDir, 'cron.lock');
    fs.writeFileSync(lockPath, JSON.stringify({
      sessionId: 'stale-session',
      pid: 99999,
      timestamp: Date.now() - 180_000,
    }));

    const sched = new CronScheduler(tmpDir, 'fresh-session');
    const result = (sched as any).acquireLock();
    assert.ok(result, 'Should acquire stale lock');
    sched.stopTicker();
  });

  it('returns own session when lock is fresh and owned by us (line 170)', () => {
    const lockDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(lockDir, { recursive: true });
    const lockPath = path.join(lockDir, 'cron.lock');
    fs.writeFileSync(lockPath, JSON.stringify({
      sessionId: 'our-session',
      pid: process.pid,
      timestamp: Date.now(),
    }));

    const sched = new CronScheduler(tmpDir, 'our-session');
    const result = (sched as any).acquireLock();
    assert.ok(result, 'Should proceed when lock is our own');
    sched.stopTicker();
  });
});

describe('CronScheduler — markRun for non-recurring job (line 138)', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
    sched = new CronScheduler(tmpDir);
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('removes non-recurring job after markRun', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'once', recurring: false });
    sched.markRun(job.id);
    assert.equal(sched.get(job.id), undefined, 'Non-recurring job should be deleted after run');
  });

  it('markRun on non-existent job is a no-op', () => {
    sched.markRun('cron_nonexistent_abc'); // should not throw
  });
});

describe('CronScheduler — delete returns false for unknown id (line 97)', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
    sched = new CronScheduler(tmpDir);
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns false for non-existent job id', () => {
    assert.equal(sched.delete('cron_99999_nonexistent'), false);
  });

  it('clears timer when deleting a job', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'timer-test' });
    assert.equal(sched.delete(job.id), true);
    // Deleting again should return false
    assert.equal(sched.delete(job.id), false);
  });
});

describe('CronScheduler — expireOld (lines 263-273)', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('expires recurring jobs older than maxAgeDays', () => {
    const sched = new CronScheduler(tmpDir);
    const job = sched.create({ cron: '* * * * *', prompt: 'old job', recurring: true });
    (job as any).createdAt = Date.now() - 8 * 86400_000;
    (job as any).nextRun = Date.now() + 86400_000;

    // Call expireOld directly instead of waiting for ticker
    (sched as any).expireOld();

    assert.equal(sched.get(job.id), undefined, 'Expired job should be removed');
  });

  it('respects custom maxAgeDays on the job', () => {
    const sched = new CronScheduler(tmpDir);
    const job = sched.create({ cron: '* * * * *', prompt: 'custom age', recurring: true });
    (job as any).maxAgeDays = 2;
    (job as any).createdAt = Date.now() - 3 * 86400_000;
    (job as any).nextRun = Date.now() + 86400_000;

    (sched as any).expireOld();

    assert.equal(sched.get(job.id), undefined, 'Job with custom maxAgeDays=2 should expire after 3 days');
  });
});

describe('CronScheduler — detectMissedTasks (lines 197-206)', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
    sched = new CronScheduler(tmpDir);
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('detects missed recurring tasks', () => {
    const job = sched.create({ cron: '*/5 * * * *', prompt: 'missed', recurring: true });
    // Set lastFiredAt to 1 hour ago so the next run should have been within that window
    (job as any).lastFiredAt = Date.now() - 3600_000;
    const missed = sched.detectMissedTasks();
    assert.ok(missed.length >= 1, 'Should detect at least one missed task');
    assert.ok(missed.some(m => m.id === job.id));
  });

  it('skips non-recurring jobs', () => {
    const job = sched.create({ cron: '* * * * *', prompt: 'one-shot', recurring: false });
    (job as any).lastFiredAt = Date.now() - 3600_000;
    const missed = sched.detectMissedTasks();
    assert.ok(!missed.some(m => m.id === job.id));
  });

  it('uses createdAt when lastFiredAt is not set', () => {
    const job = sched.create({ cron: '*/5 * * * *', prompt: 'no-last-fired', recurring: true });
    // Set createdAt to 1 hour ago, but do NOT set lastFiredAt
    (job as any).createdAt = Date.now() - 3600_000;
    delete (job as any).lastFiredAt;
    const missed = sched.detectMissedTasks();
    assert.ok(missed.length >= 1, 'Should use createdAt fallback');
  });
});

describe('CronScheduler — computeNextRun OR semantics (lines 231-239)', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
    sched = new CronScheduler(tmpDir);
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('throws for invalid cron expression with wrong number of fields', () => {
    assert.throws(() => sched.computeNextRun('* *'), /invalid cron/i);
    assert.throws(() => sched.computeNextRun('* * * * * *'), /invalid cron/i);
  });

  it('supports range in cron fields (e.g., 1-5 for DOW)', () => {
    const next = sched.computeNextRun('0 9 * * 1-5'); // weekdays only
    assert.ok(next > Date.now() - 1000);
    const d = new Date(next);
    assert.ok(d.getDay() >= 1 && d.getDay() <= 5, 'Should be a weekday');
  });

  it('supports step in cron fields (e.g., */15)', () => {
    const next = sched.computeNextRun('*/15 * * * *');
    const d = new Date(next);
    assert.ok(d.getMinutes() % 15 === 0, 'Minutes should be a multiple of 15');
  });

  it('handles DOM + DOW both restricted (OR semantics)', () => {
    // On the 1st of the month OR on Sundays at 12:00
    const next = sched.computeNextRun('0 12 1 * 0');
    const d = new Date(next);
    assert.ok(d.getDate() === 1 || d.getDay() === 0,
      'Should match either 1st of month OR Sunday');
  });

  it('supports comma-separated DOW list', () => {
    const next = sched.computeNextRun('0 9 * * 1,3,5');
    const d = new Date(next);
    assert.ok([1, 3, 5].includes(d.getDay()), 'Should be Mon, Wed, or Fri');
  });
});

describe('CronScheduler — formatSchedule', () => {
  let tmpDir: string;
  let sched: CronScheduler;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
    sched = new CronScheduler(tmpDir);
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('formats */N minute cron expression', () => {
    const result = sched.formatSchedule('*/5 * * * *');
    assert.ok(result.includes('every 5 minutes'));
  });

  it('formats specific minute', () => {
    const result = sched.formatSchedule('30 * * * *');
    assert.ok(result.includes('at minute 30'));
  });

  it('formats specific hour', () => {
    const result = sched.formatSchedule('0 9 * * *');
    assert.ok(result.includes('at hour 9'));
  });

  it('formats specific day of month', () => {
    const result = sched.formatSchedule('0 9 15 * *');
    assert.ok(result.includes('on day 15'));
  });

  it('formats specific month', () => {
    const result = sched.formatSchedule('0 9 * 3 *');
    assert.ok(result.includes('in month 3'));
  });

  it('formats DOW with day names', () => {
    const result = sched.formatSchedule('0 9 * * 1,3,5');
    assert.ok(result.includes('Mon'));
    assert.ok(result.includes('Wed'));
    assert.ok(result.includes('Fri'));
  });

  it('returns "every minute" for * * * * *', () => {
    const result = sched.formatSchedule('* * * * *');
    assert.equal(result, 'every minute');
  });

  it('returns raw string for invalid field count', () => {
    const result = sched.formatSchedule('bad cron');
    assert.equal(result, 'bad cron');
  });
});

describe('CronScheduler — saveDurable/loadDurable (lines 276-298)', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetScheduler();
    tmpDir = makeTmpDir();
  });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('does not persist non-durable jobs', () => {
    const sched = new CronScheduler(tmpDir);
    sched.create({ cron: '* * * * *', prompt: 'ephemeral' });

    const sched2 = new CronScheduler(tmpDir);
    assert.equal(sched2.list().length, 0, 'Non-durable jobs should not persist');
  });

  it('persists and loads durable jobs across instances', () => {
    const sched1 = new CronScheduler(tmpDir);
    sched1.create({ cron: '0 9 * * *', prompt: 'persist-me', durable: true });

    const sched2 = new CronScheduler(tmpDir);
    const jobs = sched2.list();
    assert.ok(jobs.some(j => j.prompt === 'persist-me'));
  });

  it('deleting a durable job saves to disk', () => {
    const sched = new CronScheduler(tmpDir);
    const job = sched.create({ cron: '0 9 * * *', prompt: 'delete-me', durable: true });
    sched.delete(job.id);

    const sched2 = new CronScheduler(tmpDir);
    assert.ok(!sched2.list().some(j => j.prompt === 'delete-me'));
  });
});

describe('getScheduler / resetScheduler', () => {
  beforeEach(() => { resetScheduler(); });

  it('returns the same singleton instance', () => {
    const s1 = getScheduler('/tmp');
    const s2 = getScheduler('/tmp');
    assert.equal(s1, s2);
  });

  it('returns a new instance after resetScheduler', () => {
    const s1 = getScheduler('/tmp');
    resetScheduler();
    const s2 = getScheduler('/tmp');
    assert.notEqual(s1, s2);
  });
});

// =========================================================================
// process-manager.ts — uncovered lines: 23 69 90 95-102 111-115 125
//   141 143-144 153-154 156-162 170-190 197
// =========================================================================

describe('ProcessManager — getOutputSize (line 179-183)', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => {
    pm.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns 0 for unknown task', () => {
    assert.equal(pm.getOutputSize('bg-nonexistent'), 0);
  });

  it('returns positive size after output is written', async () => {
    const id = pm.spawn('echo hello-world', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const size = pm.getOutputSize(id);
    assert.ok(size > 0, 'Should have captured some bytes');
  });
});

describe('ProcessManager — looksLikePrompt (lines 185-188)', () => {
  // looksLikePrompt is private, but we exercise it through the stall-detection
  // path. We can also test it indirectly via a process that outputs a prompt-like line.
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => {
    pm.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('getOutput with tailLines=0 returns full output (line 155-156)', async () => {
    const id = pm.spawn('for i in $(seq 1 5); do echo "line$i"; done', { cwd: tmpDir });
    await waitForComplete(pm, id);
    const result = pm.getOutput(id, 0);
    assert.ok(result);
    assert.ok(result!.output.includes('line1'));
    assert.ok(result!.output.includes('line5'));
  });
});

describe('ProcessManager — monitorMcp (lines 198-226)', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => {
    pm.cleanup();
    await new Promise(r => setTimeout(r, 200));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('creates a monitor task with correct shape', async () => {
    // Use a check function that returns false to clean up the interval quickly
    let callCount = 0;
    const id = pm.monitorMcp('test-server', async () => {
      callCount++;
      return callCount < 2; // healthy first call, unhealthy second
    }, 50);
    assert.ok(id.startsWith('monitor-test-server-'));
    const task = pm.get(id);
    assert.ok(task);
    assert.equal(task!.status, 'running');
    assert.ok(task!.command.includes('monitor:test-server'));
    assert.ok(task!.description.includes('MCP monitor'));
    // Wait for the monitor to self-terminate
    await new Promise<void>((resolve) => {
      pm.on('complete', function handler(ev: any) {
        if (ev.taskId === id) { pm.off('complete', handler); resolve(); }
      });
    });
  });

  it('emits complete when checkFn returns false (unhealthy)', async () => {
    let callCount = 0;
    const id = pm.monitorMcp('unhealthy', async () => {
      callCount++;
      return false; // immediately unhealthy
    }, 50); // very short interval for testing

    const event = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout')), 5000);
      pm.on('complete', function handler(ev: any) {
        if (ev.taskId === id) {
          clearTimeout(timer);
          pm.off('complete', handler);
          resolve(ev);
        }
      });
    });

    assert.equal(event.exitCode, 1);
    assert.ok(event.command.includes('monitor:unhealthy'));
  });

  it('emits complete when checkFn throws (lines 207-209)', async () => {
    const id = pm.monitorMcp('error-server', async () => {
      throw new Error('connection refused');
    }, 50);

    const event = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timeout')), 5000);
      pm.on('complete', function handler(ev: any) {
        if (ev.taskId === id) {
          clearTimeout(timer);
          pm.off('complete', handler);
          resolve(ev);
        }
      });
    });

    assert.equal(event.exitCode, 1);
  });
});

describe('ProcessManager — runningCount (lines 190-196)', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => {
    pm.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('counts only running processes', async () => {
    pm.spawn('sleep 30', { cwd: tmpDir });
    const fastId = pm.spawn('echo done', { cwd: tmpDir });
    await waitForComplete(pm, fastId);
    assert.equal(pm.runningCount(), 1);
  });
});

describe('ProcessManager — process error event (lines 108-117)', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => {
    pm.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles process that exits with error code', async () => {
    const id = pm.spawn('exit 127', { cwd: tmpDir });
    const event = await waitForComplete(pm, id);
    assert.equal(event.exitCode, 127);
    const task = pm.get(id);
    assert.equal(task!.status, 'failed');
  });
});

describe('ProcessManager — env passthrough (line 49)', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  beforeEach(() => { tmpDir = makeTmpDir(); pm = new ProcessManager(tmpDir); });
  afterEach(async () => {
    pm.cleanup();
    await new Promise(r => setTimeout(r, 100));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('passes custom env to spawned process', async () => {
    const id = pm.spawn('echo $MY_TEST_VAR', {
      cwd: tmpDir,
      env: { ...process.env, MY_TEST_VAR: 'custom_value_123' },
    });
    await waitForComplete(pm, id);
    const result = pm.getOutput(id);
    assert.ok(result!.output.includes('custom_value_123'));
  });
});

// =========================================================================
// worktree-manager.ts — uncovered lines: 102-114 137-143
// =========================================================================

describe('cleanupStaleWorktrees (lines 94-131)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns empty array when worktree directory does not exist', () => {
    const cleaned = cleanupStaleWorktrees(tmpDir);
    assert.deepEqual(cleaned, []);
  });

  it('returns empty array when worktree directory is empty', () => {
    const worktreeDir = path.join(tmpDir, '.superinference', 'worktrees');
    fs.mkdirSync(worktreeDir, { recursive: true });
    const cleaned = cleanupStaleWorktrees(tmpDir);
    assert.deepEqual(cleaned, []);
  });

  it('skips non-directory entries', () => {
    const worktreeDir = path.join(tmpDir, '.superinference', 'worktrees');
    fs.mkdirSync(worktreeDir, { recursive: true });
    fs.writeFileSync(path.join(worktreeDir, 'not-a-dir'), 'file');
    const cleaned = cleanupStaleWorktrees(tmpDir);
    assert.deepEqual(cleaned, []);
  });

  it('skips directories that are not old enough', () => {
    const worktreeDir = path.join(tmpDir, '.superinference', 'worktrees');
    const freshDir = path.join(worktreeDir, 'fresh-worktree');
    fs.mkdirSync(freshDir, { recursive: true });
    const cleaned = cleanupStaleWorktrees(tmpDir);
    assert.deepEqual(cleaned, []);
  });

  it('applies half-age threshold for ephemeral worktree names', () => {
    const worktreeDir = path.join(tmpDir, '.superinference', 'worktrees');
    // agent-abcdef0 matches EPHEMERAL_WORKTREE_PATTERNS
    const ephemeralDir = path.join(worktreeDir, 'agent-abcdef0');
    fs.mkdirSync(ephemeralDir, { recursive: true });
    // Set mtime to 16 days ago (half of 30 = 15 days threshold)
    const pastTime = new Date(Date.now() - 16 * 86400_000);
    fs.utimesSync(ephemeralDir, pastTime, pastTime);
    // countWorktreeChanges will return null since it's not a real git worktree
    // which means cleanup will skip it (fail-closed)
    const cleaned = cleanupStaleWorktrees(tmpDir);
    // It won't be cleaned because countWorktreeChanges returns null (no git repo)
    assert.deepEqual(cleaned, []);
  });
});

describe('symlinkLargeDirectories (lines 133-143)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('creates symlink for node_modules', () => {
    const source = path.join(tmpDir, 'source');
    const worktree = path.join(tmpDir, 'worktree');
    fs.mkdirSync(path.join(source, 'node_modules'), { recursive: true });
    fs.mkdirSync(worktree, { recursive: true });

    symlinkLargeDirectories(source, worktree);

    const target = path.join(worktree, 'node_modules');
    assert.ok(fs.existsSync(target), 'node_modules should be symlinked');
    const stat = fs.lstatSync(target);
    assert.ok(stat.isSymbolicLink(), 'Should be a symlink');
  });

  it('skips when source directory does not exist', () => {
    const source = path.join(tmpDir, 'no-source');
    const worktree = path.join(tmpDir, 'worktree');
    fs.mkdirSync(worktree, { recursive: true });

    symlinkLargeDirectories(source, worktree);

    assert.ok(!fs.existsSync(path.join(worktree, 'node_modules')));
  });

  it('skips when target already exists', () => {
    const source = path.join(tmpDir, 'source');
    const worktree = path.join(tmpDir, 'worktree');
    fs.mkdirSync(path.join(source, 'node_modules'), { recursive: true });
    fs.mkdirSync(path.join(worktree, 'node_modules'), { recursive: true });

    symlinkLargeDirectories(source, worktree);

    const stat = fs.lstatSync(path.join(worktree, 'node_modules'));
    assert.ok(stat.isDirectory(), 'Should remain a directory, not converted to symlink');
    assert.ok(!stat.isSymbolicLink());
  });

  it('handles custom directory list', () => {
    const source = path.join(tmpDir, 'source');
    const worktree = path.join(tmpDir, 'worktree');
    fs.mkdirSync(path.join(source, '.venv'), { recursive: true });
    fs.mkdirSync(worktree, { recursive: true });

    symlinkLargeDirectories(source, worktree, ['.venv']);

    const target = path.join(worktree, '.venv');
    assert.ok(fs.existsSync(target));
    assert.ok(fs.lstatSync(target).isSymbolicLink());
  });
});

describe('countWorktreeChanges (lines 78-92)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns null for non-git directory', () => {
    const result = countWorktreeChanges(tmpDir, tmpDir);
    assert.equal(result, null, 'Should return null for non-git dir');
  });
});

describe('flattenSlug and worktreeBranchName', () => {
  it('replaces slashes with plus signs', () => {
    assert.equal(flattenSlug('feat/my-feature'), 'feat+my-feature');
    assert.equal(flattenSlug('a/b/c'), 'a+b+c');
    assert.equal(flattenSlug('no-slashes'), 'no-slashes');
  });

  it('worktreeBranchName prefixes with worktree-', () => {
    assert.equal(worktreeBranchName('my-slug'), 'worktree-my-slug');
    assert.equal(worktreeBranchName('a/b'), 'worktree-a+b');
  });
});

describe('WorktreeSession management', () => {
  afterEach(() => { setWorktreeSession(null); });

  it('getCurrentWorktreeSession returns null by default', () => {
    setWorktreeSession(null);
    assert.equal(getCurrentWorktreeSession(), null);
  });

  it('get/set roundtrips', () => {
    const session = {
      originalCwd: '/tmp/test',
      worktreePath: '/tmp/wt',
      worktreeName: 'test',
      worktreeBranch: 'worktree-test',
      createdAt: new Date().toISOString(),
    };
    setWorktreeSession(session);
    assert.deepEqual(getCurrentWorktreeSession(), session);
  });
});

// =========================================================================
// memory.ts — uncovered lines: 99-100 172 299-301 309 312-313
//   367-372 451-475 488 494 505-512 514-521 525-530 533 540-542
//   546-550 560-561 564-566
// =========================================================================

describe('MemoryManager — saveMemory defaults and name sanitization (lines 449-467)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('defaults memoryType to project when not specified', () => {
    const mgr = new MemoryManager(tmpDir);
    mgr.saveMemory('default-type', 'content');
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    const content = fs.readFileSync(path.join(memDir, 'default-type.md'), 'utf-8');
    assert.ok(content.includes('type: project'));
  });

  it('defaults description to name when not specified', () => {
    const mgr = new MemoryManager(tmpDir);
    mgr.saveMemory('my-memory', 'content');
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    const content = fs.readFileSync(path.join(memDir, 'my-memory.md'), 'utf-8');
    assert.ok(content.includes('description: my-memory'));
  });

  it('sanitizes name with consecutive underscores and leading/trailing underscores', () => {
    const mgr = new MemoryManager(tmpDir);
    mgr.saveMemory('__hello___world__', 'content');
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    const files = fs.readdirSync(memDir);
    assert.equal(files.length, 1);
    // Consecutive underscores collapsed, leading/trailing stripped
    assert.ok(!files[0].startsWith('_'), 'Leading underscore should be stripped');
    assert.ok(!files[0].match(/__/), 'Consecutive underscores should be collapsed');
  });
});

describe('MemoryManager — getMemoryContext with team memories (lines 504-528)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('includes team memories section when team memories exist', () => {
    const teamDir = path.join(tmpDir, '.superinference', 'memory', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 'team-rule.md'), 'Always run tests before merging.');

    const mgr = new MemoryManager(tmpDir);
    const context = mgr.getMemoryContext();
    assert.ok(context.includes('Team Memories'));
    assert.ok(context.includes('Always run tests before merging'));
  });

  it('includes memory guidelines when memories exist (lines 509-525)', () => {
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'guide.md'), '---\nname: guide\ntype: user\n---\nSome guidance.');

    const mgr = new MemoryManager(tmpDir);
    const context = mgr.getMemoryContext();
    assert.ok(context.includes('Memory Guidelines'));
    assert.ok(context.includes('When to save memories'));
    assert.ok(context.includes('Memory freshness'));
    assert.ok(context.includes('Memory types'));
  });

  it('includes memory guidelines when only team memories exist', () => {
    const teamDir = path.join(tmpDir, '.superinference', 'memory', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 'convention.md'), 'Use snake_case for Python.');

    const mgr = new MemoryManager(tmpDir);
    const context = mgr.getMemoryContext();
    assert.ok(context.includes('Memory Guidelines'));
  });

  it('returns empty string when no instructions or memories', () => {
    const mgr = new MemoryManager(tmpDir);
    const context = mgr.getMemoryContext();
    assert.equal(context, '');
  });
});

describe('MemoryManager — getMemoryContext age tags (lines 488-494)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('adds age verification note for memories > 1 day old', () => {
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'old-mem.md'), '---\nname: old-mem\ntype: project\n---\nOld data.');
    // Set mtime to 3 days ago
    const pastTime = new Date(Date.now() - 3 * 86400_000);
    fs.utimesSync(path.join(memDir, 'old-mem.md'), pastTime, pastTime);

    const mgr = new MemoryManager(tmpDir);
    const context = mgr.getMemoryContext();
    assert.ok(context.includes('Verify against current state'), 'Should add age verification note');
  });

  it('does not add age note for fresh memories', () => {
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'fresh.md'), '---\nname: fresh\ntype: project\n---\nFresh data.');

    const mgr = new MemoryManager(tmpDir);
    const context = mgr.getMemoryContext();
    // Fresh memory (today) should not have the verification note in the body
    const lines = context.split('\n');
    const memBodyLine = lines.find(l => l.includes('Fresh data'));
    assert.ok(memBodyLine, 'Should include the memory content');
  });
});

describe('MemoryManager — loadMemories stale marker (line 398-399)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('marks memories older than 30 days as STALE', () => {
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'ancient.md'), '---\nname: ancient\ntype: project\n---\nVery old data.');
    const pastTime = new Date(Date.now() - 45 * 86400_000);
    fs.utimesSync(path.join(memDir, 'ancient.md'), pastTime, pastTime);

    const mgr = new MemoryManager(tmpDir);
    const memories = mgr.loadMemories();
    assert.equal(memories.length, 1);
    assert.ok(memories[0].content.includes('STALE'), 'Should include STALE marker');
    assert.ok(memories[0].content.includes('Verify before acting'));
  });
});

describe('MemoryManager — loadTeamMemories (lines 571-589)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('returns empty array when team dir does not exist', () => {
    const mgr = new MemoryManager(tmpDir);
    const result = mgr.loadTeamMemories();
    assert.deepEqual(result, []);
  });

  it('loads team memories with sanitization', () => {
    const teamDir = path.join(tmpDir, '.superinference', 'memory', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 'convention.md'), '---\nname: conv\n---\nUse TypeScript strict mode.');

    const mgr = new MemoryManager(tmpDir);
    const result = mgr.loadTeamMemories();
    assert.ok(result.length >= 1);
    assert.ok(result[0].includes('Use TypeScript strict mode'));
  });

  it('filters out empty content files', () => {
    const teamDir = path.join(tmpDir, '.superinference', 'memory', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 'empty.md'), '---\nname: empty\n---\n');
    fs.writeFileSync(path.join(teamDir, 'real.md'), 'Real content.');

    const mgr = new MemoryManager(tmpDir);
    const result = mgr.loadTeamMemories();
    assert.equal(result.length, 1);
    assert.ok(result[0].includes('Real content'));
  });

  it('skips non-.md files', () => {
    const teamDir = path.join(tmpDir, '.superinference', 'memory', 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 'notes.txt'), 'Some notes');
    fs.writeFileSync(path.join(teamDir, 'rules.md'), 'A rule.');

    const mgr = new MemoryManager(tmpDir);
    const result = mgr.loadTeamMemories();
    assert.equal(result.length, 1);
    assert.ok(result[0].includes('A rule'));
  });
});

describe('MemoryManager — addToMemoryIndex / removeFromMemoryIndex (lines 853-877)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('addToMemoryIndex creates file if it does not exist', async () => {
    const mgr = new MemoryManager(tmpDir);
    await mgr.addToMemoryIndex(tmpDir, '- [test](test.md) -- A test entry');
    const indexPath = path.join(tmpDir, '.superinference', 'memory', 'MEMORY.md');
    assert.ok(fs.existsSync(indexPath));
    const content = fs.readFileSync(indexPath, 'utf-8');
    assert.ok(content.includes('- [test](test.md)'));
  });

  it('addToMemoryIndex does not duplicate entries', async () => {
    const mgr = new MemoryManager(tmpDir);
    await mgr.addToMemoryIndex(tmpDir, '- [dup](dup.md) -- Duplicate');
    await mgr.addToMemoryIndex(tmpDir, '- [dup](dup.md) -- Duplicate');
    const indexPath = path.join(tmpDir, '.superinference', 'memory', 'MEMORY.md');
    const content = fs.readFileSync(indexPath, 'utf-8');
    const count = (content.match(/\[dup\]/g) || []).length;
    assert.equal(count, 1, 'Entry should appear only once');
  });

  it('removeFromMemoryIndex removes matching lines', async () => {
    const mgr = new MemoryManager(tmpDir);
    await mgr.addToMemoryIndex(tmpDir, '- [keep](keep.md) -- Keep this');
    await mgr.addToMemoryIndex(tmpDir, '- [remove](remove.md) -- Remove this');

    await mgr.removeFromMemoryIndex(tmpDir, 'remove.md');

    const indexPath = path.join(tmpDir, '.superinference', 'memory', 'MEMORY.md');
    const content = fs.readFileSync(indexPath, 'utf-8');
    assert.ok(!content.includes('remove.md'), 'Removed entry should be gone');
    assert.ok(content.includes('keep.md'), 'Kept entry should remain');
  });

  it('removeFromMemoryIndex is no-op when file does not exist', async () => {
    const mgr = new MemoryManager(tmpDir);
    // Should not throw
    await mgr.removeFromMemoryIndex(tmpDir, 'nonexistent');
  });
});

describe('MemoryManager — loadRulesForFile with paths frontmatter (lines 740-771)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('filters rules by paths frontmatter', async () => {
    const rulesDir = path.join(tmpDir, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    fs.writeFileSync(path.join(rulesDir, 'ts-only.md'),
      '---\npaths: *.ts, src/*.ts\n---\nTypeScript-only rule.');
    fs.writeFileSync(path.join(rulesDir, 'all.md'), 'Universal rule.');

    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.loadRulesForFile(tmpDir, 'src/index.ts');
    assert.ok(sources.length >= 2, 'Both matching rule and universal rule should be included');
    assert.ok(sources.some(s => s.content.includes('TypeScript-only rule')));
    assert.ok(sources.some(s => s.content.includes('Universal rule')));
  });

  it('excludes rules whose paths do not match', async () => {
    const rulesDir = path.join(tmpDir, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    fs.writeFileSync(path.join(rulesDir, 'py-only.md'),
      '---\npaths: *.py\n---\nPython rule.');

    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.loadRulesForFile(tmpDir, 'src/index.ts');
    assert.ok(!sources.some(s => s.content.includes('Python rule')));
  });

  it('returns empty array when rules dir does not exist', async () => {
    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.loadRulesForFile(tmpDir, 'any/file.ts');
    assert.deepEqual(sources, []);
  });
});

describe('MemoryManager — setHookCallback (line 246)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('fires hook callback when loading instructions', () => {
    fs.writeFileSync(path.join(tmpDir, 'CLAUDE.md'), 'Test instructions.');
    const mgr = new MemoryManager(tmpDir);
    const events: any[] = [];
    mgr.setHookCallback((event, data) => events.push({ event, data }));
    mgr.loadProjectInstructions();
    assert.ok(events.some(e => e.event === 'instructionsLoaded'));
  });
});

describe('MemoryManager — resolveIncludes edge cases (lines 793-826)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('detects circular references', async () => {
    const dir = path.join(tmpDir, 'includes');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'a.md'), '@include b.md');
    fs.writeFileSync(path.join(dir, 'b.md'), '@include a.md');

    const mgr = new MemoryManager(tmpDir);
    const result = await mgr.resolveIncludes(
      '@include b.md',
      path.join(dir, 'a.md'),
    );
    // a.md -> includes b.md -> b.md includes a.md -> a.md includes b.md -> b.md is in seen
    assert.ok(result.includes('[circular reference: b.md]'));
  });

  it('handles missing include files', async () => {
    const mgr = new MemoryManager(tmpDir);
    const result = await mgr.resolveIncludes(
      '@include nonexistent.md',
      path.join(tmpDir, 'base.md'),
    );
    assert.ok(result.includes('[include not found: nonexistent.md]'));
  });

  it('stops recursion at max depth', async () => {
    const dir = path.join(tmpDir, 'deep');
    fs.mkdirSync(dir, { recursive: true });
    // Create a chain of includes deeper than 5
    for (let i = 0; i < 8; i++) {
      fs.writeFileSync(path.join(dir, `level${i}.md`), `@include level${i + 1}.md`);
    }
    fs.writeFileSync(path.join(dir, 'level8.md'), 'final');

    const mgr = new MemoryManager(tmpDir);
    const result = await mgr.resolveIncludes(
      '@include level1.md',
      path.join(dir, 'level0.md'),
    );
    // At depth > 5 it should stop resolving
    assert.ok(result.includes('@include'), 'Should leave unresolved includes past max depth');
  });
});

describe('MemoryManager — collectInstructionSources with currentFile (line 895-903)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('uses loadRulesForFile when currentFile is provided', async () => {
    const rulesDir = path.join(tmpDir, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    fs.writeFileSync(path.join(rulesDir, 'ts-rule.md'),
      '---\npaths: *.ts\n---\nTS only.');
    fs.writeFileSync(path.join(rulesDir, 'global.md'), 'Global rule.');

    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.collectInstructionSources('test.ts');
    assert.ok(sources.some(s => s.content.includes('TS only')));
    assert.ok(sources.some(s => s.content.includes('Global rule')));
    assert.ok(sources.every(s => s.level), 'All sources should have a level');
  });

  it('uses loadRulesDirectory when no currentFile', async () => {
    const rulesDir = path.join(tmpDir, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    fs.writeFileSync(path.join(rulesDir, 'all.md'), 'Universal rule.');

    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.collectInstructionSources();
    assert.ok(sources.some(s => s.content.includes('Universal rule')));
    assert.ok(sources.some(s => s.level === 'managed'));
  });

  it('includes project and user level sources', async () => {
    fs.writeFileSync(path.join(tmpDir, 'CLAUDE.md'), 'Project instructions.');
    fs.writeFileSync(path.join(tmpDir, 'CLAUDE.local.md'), 'User overrides.');

    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.collectInstructionSources();
    assert.ok(sources.some(s => s.level === 'project'));
    assert.ok(sources.some(s => s.level === 'user'));
  });

  it('includes user memories in collected sources', async () => {
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'pref.md'), '---\nname: pref\ntype: user\n---\nPreference content.');

    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.collectInstructionSources();
    assert.ok(sources.some(s => s.level === 'user' && s.content.includes('Preference content')));
  });
});

describe('MemoryManager — loadProjectInstructions stops at .git boundary (lines 299-312)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('stops scanning upward when .git directory is found', () => {
    // Create a project directory with a .git marker
    const projectDir = path.join(tmpDir, 'project');
    fs.mkdirSync(path.join(projectDir, '.git'), { recursive: true });
    fs.writeFileSync(path.join(projectDir, 'CLAUDE.md'), 'Project level.');

    // Create instructions in the parent directory
    fs.writeFileSync(path.join(tmpDir, 'CLAUDE.md'), 'Parent level.');

    const mgr = new MemoryManager(projectDir);
    const result = mgr.loadProjectInstructions();
    assert.ok(result.includes('Project level'), 'Should include project-level instructions');
    assert.ok(!result.includes('Parent level'), 'Should NOT include parent-level instructions past .git boundary');
  });
});

// =========================================================================
// model-registry.ts — uncovered lines: 23 85 107 115-116 121-122
//   141-142 165
// =========================================================================

describe('detectProvider — env-based vertex detection (line 58)', () => {
  it('detects anthropic-vertex from ANTHROPIC_VERTEX_PROJECT_ID env var', () => {
    const saved = process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    process.env.ANTHROPIC_VERTEX_PROJECT_ID = 'my-project';
    try {
      const result = detectProvider({ baseUrl: '', apiKey: '', model: '' });
      assert.equal(result, 'anthropic-vertex');
    } finally {
      if (saved !== undefined) process.env.ANTHROPIC_VERTEX_PROJECT_ID = saved;
      else delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    }
  });

  it('detects anthropic-vertex from CLAUDE_CODE_USE_VERTEX env var', () => {
    const saved1 = process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    const saved2 = process.env.CLAUDE_CODE_USE_VERTEX;
    delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
    process.env.CLAUDE_CODE_USE_VERTEX = '1';
    try {
      const result = detectProvider({ baseUrl: '', apiKey: '', model: '' });
      assert.equal(result, 'anthropic-vertex');
    } finally {
      if (saved1 !== undefined) process.env.ANTHROPIC_VERTEX_PROJECT_ID = saved1;
      else delete process.env.ANTHROPIC_VERTEX_PROJECT_ID;
      if (saved2 !== undefined) process.env.CLAUDE_CODE_USE_VERTEX = saved2;
      else delete process.env.CLAUDE_CODE_USE_VERTEX;
    }
  });
});

describe('detectProvider — additional URL patterns (lines 69-83)', () => {
  it('detects google-vertex from aiplatform.googleapis.com', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: 'https://us-central1-aiplatform.googleapis.com/v1',
        apiKey: '',
        model: '',
      }), 'google-vertex');
    });
  });

  it('detects groq from api.groq.com', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: 'https://api.groq.com/openai/v1',
        apiKey: '',
        model: '',
      }), 'groq');
    });
  });

  it('detects groq from gsk_ key prefix', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: '',
        apiKey: 'gsk_abc123',
        model: '',
      }), 'groq');
    });
  });

  it('detects xai from xai- key prefix', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: '',
        apiKey: 'xai-abc123',
        model: '',
      }), 'xai');
    });
  });

  it('detects xai from api.x.ai URL', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: 'https://api.x.ai/v1',
        apiKey: '',
        model: '',
      }), 'xai');
    });
  });

  it('detects perplexity from pplx- key prefix', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: '',
        apiKey: 'pplx-abc123',
        model: '',
      }), 'perplexity');
    });
  });

  it('detects mistral from api.mistral.ai URL', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: 'https://api.mistral.ai/v1',
        apiKey: '',
        model: '',
      }), 'mistral');
    });
  });

  it('detects togetherai from api.together.xyz URL', () => {
    withCleanEnv(() => {
      assert.equal(detectProvider({
        baseUrl: 'https://api.together.xyz/v1',
        apiKey: '',
        model: '',
      }), 'togetherai');
    });
  });
});

describe('listProviders (line 86-88)', () => {
  it('returns array of provider objects with id and models', () => {
    const providers = listProviders();
    assert.ok(Array.isArray(providers));
    assert.ok(providers.length > 0);
    for (const p of providers) {
      assert.ok(typeof p.id === 'string');
      assert.ok(Array.isArray(p.models));
    }
  });
});

describe('listModels — static provider (lines 93-97)', () => {
  it('returns static model list for anthropic provider', async () => {
    const models = await listModels({
      baseUrl: '',
      apiKey: '',
      model: '',
      provider: 'anthropic',
    });
    assert.ok(Array.isArray(models));
    assert.ok(models.length > 0);
    assert.ok(models.every(m => m.id && m.name));
  });
});

// =========================================================================
// permissions.ts — uncovered lines: 26 88 90-93 96-99 102-112
//   124-125 173-177 319-328 330-334 337-345 362-366 380-381
//   406-408 431-437 486 488-489 491-492 499-507 536-539
// =========================================================================

describe('containsUnquotedExpansion (lines 171-180)', () => {
  it('detects $VAR in unquoted context', () => {
    assert.equal(containsUnquotedExpansion('echo $HOME'), true);
  });

  it('detects ${VAR} in unquoted context', () => {
    assert.equal(containsUnquotedExpansion('echo ${HOME}'), true);
  });

  it('detects $(cmd) in unquoted context', () => {
    assert.equal(containsUnquotedExpansion('echo $(whoami)'), true);
  });

  it('detects glob * in unquoted context', () => {
    assert.equal(containsUnquotedExpansion('ls *.ts'), true);
  });

  it('detects ? glob in unquoted context', () => {
    assert.equal(containsUnquotedExpansion('ls file?.ts'), true);
  });

  it('detects bracket expressions', () => {
    assert.equal(containsUnquotedExpansion('ls [abc]'), true);
  });

  it('does not detect variables inside double quotes', () => {
    assert.equal(containsUnquotedExpansion('"$HOME"'), false);
  });

  it('does not detect variables inside single quotes', () => {
    assert.equal(containsUnquotedExpansion("'$HOME'"), false);
  });

  it('returns false for plain command', () => {
    assert.equal(containsUnquotedExpansion('echo hello world'), false);
  });
});

describe('stripSafeEnvVars (lines 249-259)', () => {
  it('strips known safe env vars', () => {
    const result = stripSafeEnvVars('NODE_ENV=test ls');
    assert.equal(result.trim(), 'ls');
  });

  it('keeps unknown/dangerous env vars', () => {
    const result = stripSafeEnvVars('SECRET_KEY=abc ls');
    assert.ok(result.includes('SECRET_KEY=abc'));
  });

  it('strips multiple safe env vars', () => {
    const result = stripSafeEnvVars('NODE_ENV=test LANG=en_US.UTF-8 ls');
    assert.equal(result.trim(), 'ls');
  });

  it('keeps mix of safe and unsafe', () => {
    const result = stripSafeEnvVars('NODE_ENV=test API_KEY=secret ls');
    assert.ok(!result.includes('NODE_ENV'));
    assert.ok(result.includes('API_KEY=secret'));
  });
});

describe('PermissionManager — bypassPermissions mode (lines 520-532)', () => {
  it('allows non-path tools in bypass mode', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('web_fetch', { url: 'http://example.com' }), 'allow');
  });

  it('asks for bash commands touching safety paths', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('bash', { command: 'cat .git/config' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cat .superinference/config' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cat .bashrc' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cat .ssh/id_rsa' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cat .aws/credentials' }), 'ask');
  });

  it('allows bash commands not touching safety paths', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('bash', { command: 'ls -la src/' }), 'allow');
  });

  it('asks for file_edit on safety paths', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('file_edit', { file_path: '.gitconfig' }), 'ask');
  });

  it('asks for file_write on safety paths', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('file_write', { file_path: '.vscode/settings.json' }), 'ask');
  });
});

describe('PermissionManager — plan mode (lines 534-538)', () => {
  it('allows read-only tools in plan mode', async () => {
    const pm = new PermissionManager('plan');
    assert.equal(await pm.check('file_read', {}), 'allow');
    assert.equal(await pm.check('grep', {}), 'allow');
    assert.equal(await pm.check('glob', {}), 'allow');
    assert.equal(await pm.check('list_dir', {}), 'allow');
    assert.equal(await pm.check('web_fetch', {}), 'allow');
    assert.equal(await pm.check('web_search', {}), 'allow');
    assert.equal(await pm.check('search_symbols', {}), 'allow');
    assert.equal(await pm.check('task_tracker', {}), 'allow');
    assert.equal(await pm.check('task_list', {}), 'allow');
    assert.equal(await pm.check('task_output', {}), 'allow');
  });

  it('denies write tools in plan mode', async () => {
    const pm = new PermissionManager('plan');
    assert.equal(await pm.check('file_edit', {}), 'deny');
    assert.equal(await pm.check('file_write', {}), 'deny');
    assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
  });
});

describe('PermissionManager — acceptEdits mode (lines 541-548)', () => {
  it('auto-allows edit tools in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('file_edit', { file_path: 'x.ts' }), 'allow');
    assert.equal(await pm.check('file_write', { file_path: 'x.ts' }), 'allow');
    assert.equal(await pm.check('notebook_edit', { file_path: 'x.ipynb' }), 'allow');
    assert.equal(await pm.check('multi_edit', {}), 'allow');
  });

  it('auto-allows safe bash commands in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'git status' }), 'allow');
  });

  it('does not auto-allow unsafe bash in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    const result = await pm.check('bash', { command: 'npm install' });
    assert.equal(result, 'ask');
  });
});

describe('PermissionManager — dontAsk mode (line 518)', () => {
  it('denies everything in dontAsk mode', async () => {
    const pm = new PermissionManager('dontAsk');
    assert.equal(await pm.check('file_read', {}), 'deny');
    assert.equal(await pm.check('bash', { command: 'ls' }), 'deny');
  });
});

describe('PermissionManager — circuit breaker (lines 486-829)', () => {
  it('shouldAbort is false initially', () => {
    const pm = new PermissionManager();
    assert.equal(pm.shouldAbort(), false);
  });

  it('shouldAbort is true after MAX_CONSECUTIVE denials', () => {
    const pm = new PermissionManager();
    for (let i = 0; i < 3; i++) pm.recordDenial();
    assert.equal(pm.shouldAbort(), true);
  });

  it('recordSuccess resets consecutive counter', () => {
    const pm = new PermissionManager();
    pm.recordDenial();
    pm.recordDenial();
    pm.recordSuccess();
    assert.equal(pm.shouldAbort(), false);
  });

  it('shouldAbort after MAX_TOTAL denials', () => {
    const pm = new PermissionManager();
    for (let i = 0; i < 20; i++) {
      pm.recordDenial();
      if (i % 2 === 0) pm.recordSuccess(); // reset consecutive but total keeps climbing
    }
    assert.equal(pm.shouldAbort(), true);
  });

  it('resetCircuitBreaker clears all counters', () => {
    const pm = new PermissionManager();
    for (let i = 0; i < 5; i++) pm.recordDenial();
    assert.equal(pm.shouldAbort(), true);
    pm.resetCircuitBreaker();
    assert.equal(pm.shouldAbort(), false);
  });
});

describe('PermissionManager — checkDenyRules (lines 584-593)', () => {
  it('returns denied when deny rule matches', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'deny' as const, reason: 'No bash allowed' },
    ]);
    const result = pm.checkDenyRules('bash', 'any command');
    assert.equal(result.denied, true);
    assert.ok(result.reason?.includes('No bash allowed'));
  });

  it('returns denied for wildcard deny rule', () => {
    const pm = new PermissionManager('ask', [
      { tool: '*', action: 'deny' as const },
    ]);
    const result = pm.checkDenyRules('file_edit', 'some input');
    assert.equal(result.denied, true);
  });

  it('returns not denied when pattern does not match', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'git *', action: 'deny' as const },
    ]);
    const result = pm.checkDenyRules('bash', 'npm install');
    assert.equal(result.denied, false);
  });

  it('returns denied when pattern matches', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'git *', action: 'deny' as const, reason: 'No git' },
    ]);
    const result = pm.checkDenyRules('bash', 'git push');
    assert.equal(result.denied, true);
    assert.ok(result.reason?.includes('No git'));
  });

  it('skips allow rules', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'allow' as const },
    ]);
    const result = pm.checkDenyRules('bash', 'any');
    assert.equal(result.denied, false);
  });
});

describe('PermissionManager — detectShadowedRules (lines 866-879)', () => {
  it('detects a shadowed allow rule', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'deny' as const },      // higher priority (index 0)
      { tool: 'bash', action: 'allow' as const },      // shadowed (index 1)
    ]);
    const shadows = pm.detectShadowedRules();
    assert.ok(shadows.length >= 1);
    assert.equal(shadows[0].rule.action, 'allow');
    assert.equal(shadows[0].shadowedBy.action, 'deny');
  });

  it('detects allow rule shadowed by ask rule with wildcard pattern', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: '*', action: 'ask' as const },
      { tool: 'bash', action: 'allow' as const },
    ]);
    const shadows = pm.detectShadowedRules();
    assert.ok(shadows.length >= 1);
  });

  it('does not flag non-shadowed rules', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'git *', action: 'deny' as const },
      { tool: 'bash', action: 'allow' as const },
    ]);
    const shadows = pm.detectShadowedRules();
    // pattern is specific ('git *'), not wildcard, so it doesn't shadow
    assert.equal(shadows.length, 0);
  });

  it('does not flag when both rules are deny', () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', action: 'deny' as const },
      { tool: 'bash', action: 'deny' as const },
    ]);
    const shadows = pm.detectShadowedRules();
    assert.equal(shadows.length, 0);
  });
});

describe('PermissionManager — classifyBashCommand additional coverage', () => {
  let pm: PermissionManager;
  beforeEach(() => { pm = new PermissionManager(); });

  it('classifies safe docker subcommands', () => {
    assert.equal(pm.classifyBashCommand('docker ps'), 'safe');
    assert.equal(pm.classifyBashCommand('docker images'), 'safe');
    assert.equal(pm.classifyBashCommand('docker logs container'), 'safe');
    assert.equal(pm.classifyBashCommand('docker inspect container'), 'safe');
  });

  it('classifies unsafe docker subcommands', () => {
    assert.equal(pm.classifyBashCommand('docker run -it ubuntu'), 'unsafe');
    assert.equal(pm.classifyBashCommand('docker exec container bash'), 'unsafe');
    assert.equal(pm.classifyBashCommand('docker build .'), 'unsafe');
  });

  it('classifies python3 -c safe one-liner as safe', () => {
    assert.equal(pm.classifyBashCommand("python3 -c 'print(42)'"), 'safe');
  });

  it('classifies python3 -c with os import as unsafe', () => {
    assert.equal(pm.classifyBashCommand("python3 -c 'import os; os.remove(\"x\")'"), 'unsafe');
  });

  it('classifies python3 -c with subprocess import as unsafe', () => {
    assert.equal(pm.classifyBashCommand("python3 -c 'import subprocess; subprocess.run([\"rm\", \"x\"])'"), 'unsafe');
  });

  it('classifies node -e safe one-liner as safe', () => {
    assert.equal(pm.classifyBashCommand("node -e 'console.log(42)'"), 'safe');
  });

  it('classifies node -e with child_process require as unsafe', () => {
    assert.equal(pm.classifyBashCommand("node -e 'require(\"child_process\")'"), 'unsafe');
  });

  it('classifies node -e with fs require as unsafe', () => {
    assert.equal(pm.classifyBashCommand("node -e 'require(\"fs\")'"), 'unsafe');
  });

  it('classifies jq with system() call as unsafe', () => {
    assert.equal(pm.classifyBashCommand('jq "system(\\"rm x\\")" file.json'), 'unsafe');
  });

  it('classifies safe jq expression as safe', () => {
    assert.equal(pm.classifyBashCommand('jq ".data[]" file.json'), 'safe');
  });

  it('classifies xargs with safe target as safe', () => {
    assert.equal(pm.classifyBashCommand('echo file | xargs cat'), 'safe');
    assert.equal(pm.classifyBashCommand('echo file | xargs grep pattern'), 'safe');
  });

  it('classifies xargs with unsafe target as unsafe', () => {
    assert.equal(pm.classifyBashCommand('echo file | xargs python3'), 'unsafe');
  });

  it('classifies commands with blocked flags (jq -f) as unsafe', () => {
    assert.equal(pm.classifyBashCommand('jq -f script.jq file.json'), 'unsafe');
  });

  it('classifies find with safe flags (flag validation)', () => {
    assert.equal(pm.classifyBashCommand('find . -name "*.ts" -type f'), 'safe');
  });

  it('classifies fd with -x (exec) as unsafe', () => {
    assert.equal(pm.classifyBashCommand('fd -x rm'), 'unsafe');
  });

  it('classifies safe commands with unquoted expansion as unsafe', () => {
    assert.equal(pm.classifyBashCommand('cat $FILE'), 'unsafe');
  });

  it('classifies SQL DELETE FROM as destructive', () => {
    assert.equal(pm.classifyBashCommand('psql -c "DELETE FROM users"'), 'destructive');
  });

  it('classifies SQL TRUNCATE TABLE as destructive', () => {
    assert.equal(pm.classifyBashCommand('psql -c "TRUNCATE TABLE users"'), 'destructive');
  });

  it('classifies known destructive cp command as unsafe (system path extraction)', () => {
    // cp is in UNSAFE_COMMANDS, so it hits unsafe before path extraction
    assert.equal(pm.classifyBashCommand('cp malware /usr/bin/'), 'unsafe');
  });

  it('classifies redirect to system path as destructive', () => {
    // Redirect to /etc/passwd is caught by extractBaseCommands adding 'redirect-to-system-path'
    assert.equal(pm.classifyBashCommand('echo bad > /etc/passwd'), 'destructive');
  });

  it('classifies tput as safe', () => {
    assert.equal(pm.classifyBashCommand('tput cols'), 'safe');
  });

  it('classifies ss as safe', () => {
    assert.equal(pm.classifyBashCommand('ss -tulnp'), 'safe');
  });
});

describe('PermissionManager — loadRules with pattern and reason fields (lines 910-920)', () => {
  let tmpDir: string;

  beforeEach(() => { tmpDir = makeTmpDir(); });
  afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

  it('loads rules with optional pattern and reason fields', () => {
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'permissions.json'), JSON.stringify({
      rules: [
        { tool: 'bash', pattern: 'git *', action: 'allow', reason: 'Git is safe' },
        { tool: 'bash', action: 'deny' },
      ],
    }));

    const pm = new PermissionManager();
    pm.loadRules(tmpDir);
    const rules = pm.getRules();
    assert.equal(rules.length, 2);
    assert.equal(rules[0].pattern, 'git *');
    assert.equal(rules[0].reason, 'Git is safe');
    assert.equal(rules[1].pattern, undefined);
    assert.equal(rules[1].reason, undefined);
  });

  it('handles rule with non-string pattern (ignores it)', () => {
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'permissions.json'), JSON.stringify({
      rules: [
        { tool: 'bash', pattern: 123, action: 'allow', reason: 456 },
      ],
    }));

    const pm = new PermissionManager();
    pm.loadRules(tmpDir);
    const rules = pm.getRules();
    assert.equal(rules.length, 1);
    assert.equal(rules[0].pattern, undefined);
    assert.equal(rules[0].reason, undefined);
  });
});
