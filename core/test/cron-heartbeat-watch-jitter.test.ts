import { describe, it, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { CronScheduler, DEFAULT_CRON_JITTER, type CronJitterConfig } from '../src/cron';
import { projectStateDir } from '../src/reference-journal';

// ---------------------------------------------------------------------------
// Shared tmp-dir plumbing
// ---------------------------------------------------------------------------
const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-hb-'));
  tmpDirs.push(d);
  return d;
}
function lockPathFor(cwd: string): string {
  return path.join(projectStateDir(cwd), 'cron.lock');
}
function tasksPathFor(cwd: string): string {
  return path.join(projectStateDir(cwd), 'scheduled_tasks.json');
}
function writeLock(cwd: string, lock: Record<string, unknown>): void {
  fs.mkdirSync(projectStateDir(cwd), { recursive: true });
  fs.writeFileSync(lockPathFor(cwd), JSON.stringify(lock));
}
function readLock(cwd: string): { sessionId?: string; pid?: number; timestamp?: number } {
  return JSON.parse(fs.readFileSync(lockPathFor(cwd), 'utf-8'));
}

type Internals = {
  acquireLock(): boolean;
  heartbeatLock(): void;
  tick(cb: (j: unknown) => void): void;
  holdsLock: boolean;
  exitRelease: (() => void) | null;
};
function internals(s: CronScheduler): Internals {
  return s as unknown as Internals;
}

/** A PID that is overwhelmingly unlikely to be a live process (so isProcessRunning
 *  returns false) — exercises the dead-pid branch deterministically. */
const DEAD_PID = 0x7fffffff;

function durableJob(id: string, cron = '0 0 * * *'): Record<string, unknown> {
  return {
    id, cron, prompt: `job ${id}`, recurring: true, durable: true,
    createdAt: Date.now(),
  };
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (pred()) return true;
    await new Promise(r => setTimeout(r, 25));
  }
  return pred();
}

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

// ---------------------------------------------------------------------------
// Gap — lock lifetime heartbeat (mtime/timestamp refresh) + onCompromised
// ---------------------------------------------------------------------------
describe('cron lock — heartbeat refresh + compromise detection', () => {
  it('refreshes the lock timestamp on heartbeat while the lease is held', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'hb-session');
    assert.equal(internals(sched).acquireLock(), true);
    // Simulate time passing by rewinding the stored heartbeat far into the past
    // (still our session), as a stale-looking but owned lock would read.
    const old = Date.now() - 90_000;
    writeLock(cwd, { sessionId: 'hb-session', pid: process.pid, timestamp: old });

    internals(sched).heartbeatLock();

    const after = readLock(cwd);
    assert.equal(after.sessionId, 'hb-session');
    assert.ok(after.timestamp! > old, 'heartbeat must advance the lock timestamp');
    assert.ok(Date.now() - after.timestamp! < 5_000, 'refreshed timestamp must be recent');
    sched.stopTicker();
  });

  it('yields the lease when another session has taken over the lock', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'mine');
    assert.equal(internals(sched).acquireLock(), true);
    assert.equal(internals(sched).holdsLock, true);
    // A different session overwrites the lock while we hold it.
    writeLock(cwd, { sessionId: 'thief', pid: process.pid, timestamp: Date.now() });

    internals(sched).heartbeatLock();

    assert.equal(internals(sched).holdsLock, false, 'compromised lease must be dropped');
    assert.equal(internals(sched).exitRelease, null, 'exit-release must be unregistered on compromise');
    sched.stopTicker();
  });

  it('yields the lease when the lock file was removed externally', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'mine2');
    assert.equal(internals(sched).acquireLock(), true);
    fs.unlinkSync(lockPathFor(cwd));

    internals(sched).heartbeatLock();

    assert.equal(internals(sched).holdsLock, false);
    sched.stopTicker();
  });

  it('heartbeat is a no-op when we do not hold the lock', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'never-owned');
    // No acquire → holdsLock false.
    internals(sched).heartbeatLock();
    assert.equal(fs.existsSync(lockPathFor(cwd)), false, 'no lock file should be written');
    sched.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// Gap — staleness via refreshed heartbeat liveness (not a fixed one-shot stamp)
// ---------------------------------------------------------------------------
describe('cron lock — staleness uses refreshed heartbeat liveness', () => {
  it('does NOT reclaim a dead-pid owner whose heartbeat is still fresh', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'contender');
    // Owner pid is dead, but its heartbeat timestamp was just refreshed → alive.
    writeLock(cwd, { sessionId: 'live-via-heartbeat', pid: DEAD_PID, timestamp: Date.now() });

    assert.equal(internals(sched).acquireLock(), false,
      'a fresh heartbeat must keep a cross-host/dead-pid owner alive');
    const lock = readLock(cwd);
    assert.equal(lock.sessionId, 'live-via-heartbeat', 'foreign lock must be left intact');
    sched.stopTicker();
  });

  it('reclaims a dead-pid owner whose heartbeat has gone stale', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'contender2');
    writeLock(cwd, { sessionId: 'dead', pid: DEAD_PID, timestamp: Date.now() - 200_000 });

    assert.equal(internals(sched).acquireLock(), true, 'a stale heartbeat must be reclaimed');
    const lock = readLock(cwd);
    assert.equal(lock.sessionId, 'contender2');
    sched.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// Gap — runtime file-watch of scheduled_tasks.json (external add/remove)
// ---------------------------------------------------------------------------
describe('CronScheduler — live watch of the durable state file', () => {
  it('picks up a durable job added to the file by another process', async () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'watch-add');
    sched.startTicker(() => {});
    assert.equal(sched.list().length, 0);

    fs.mkdirSync(projectStateDir(cwd), { recursive: true });
    fs.writeFileSync(tasksPathFor(cwd), JSON.stringify([durableJob('cron_1_ext')], null, 2));

    const appeared = await waitFor(() => sched.get('cron_1_ext') !== undefined);
    assert.ok(appeared, 'externally-added durable job must appear live via the watcher');
    sched.stopTicker();
  });

  it('drops a durable job removed from the file by another process', async () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'watch-del');
    sched.startTicker(() => {}); // watch starts on the (empty) dir
    fs.mkdirSync(projectStateDir(cwd), { recursive: true });
    fs.writeFileSync(tasksPathFor(cwd), JSON.stringify([durableJob('cron_9_ext')], null, 2));
    assert.ok(await waitFor(() => sched.get('cron_9_ext') !== undefined), 'job should load first');

    // External edit removes it.
    fs.writeFileSync(tasksPathFor(cwd), JSON.stringify([], null, 2));
    const gone = await waitFor(() => sched.get('cron_9_ext') === undefined);
    assert.ok(gone, 'externally-removed durable job must be dropped live');
    sched.stopTicker();
  });

  it('startTicker/stopTicker open and close exactly one watcher', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'watch-idemp');
    sched.startTicker(() => {});
    const w1 = (sched as unknown as { watcher: unknown }).watcher;
    sched.startTicker(() => {}); // idempotent
    const w2 = (sched as unknown as { watcher: unknown }).watcher;
    assert.equal(w1, w2, 'second startTicker must not open a second watcher');
    assert.ok(w1, 'a watcher should be open');
    sched.stopTicker();
    assert.equal((sched as unknown as { watcher: unknown }).watcher, null, 'stopTicker must close the watcher');
  });
});

// ---------------------------------------------------------------------------
// Gap — per-tick runtime jitter tuning (live operator override)
// ---------------------------------------------------------------------------
describe('CronScheduler — live jitter retuning per tick', () => {
  const ENV_KEYS = [
    'AMI_CRON_JITTER_FRAC', 'AMI_CRON_JITTER_CAP_MS',
    'AMI_CRON_ONESHOT_JITTER_MS', 'AMI_CRON_ONESHOT_MINUTE_MOD',
  ];
  afterEach(() => { for (const k of ENV_KEYS) delete process.env[k]; });

  function tick(sched: CronScheduler): void {
    internals(sched).tick(() => {});
  }

  it('defaults to the construction baseline when no env override is set', () => {
    const base: CronJitterConfig = { ...DEFAULT_CRON_JITTER, recurringFrac: 0.2 };
    const sched = new CronScheduler(mkTmp(), 'jit-base', base);
    tick(sched);
    assert.equal(sched.getJitterConfig().recurringFrac, 0.2);
    sched.stopTicker();
  });

  it('applies an env override on the next tick, then reverts when cleared', () => {
    const sched = new CronScheduler(mkTmp(), 'jit-live');
    process.env.AMI_CRON_JITTER_FRAC = '0.5';
    process.env.AMI_CRON_JITTER_CAP_MS = '1000';
    tick(sched);
    assert.equal(sched.getJitterConfig().recurringFrac, 0.5);
    assert.equal(sched.getJitterConfig().recurringCapMs, 1000);
    // Clear → must revert to the baseline without a restart.
    delete process.env.AMI_CRON_JITTER_FRAC;
    delete process.env.AMI_CRON_JITTER_CAP_MS;
    tick(sched);
    assert.equal(sched.getJitterConfig().recurringFrac, DEFAULT_CRON_JITTER.recurringFrac);
    assert.equal(sched.getJitterConfig().recurringCapMs, DEFAULT_CRON_JITTER.recurringCapMs);
    sched.stopTicker();
  });

  it('ignores an invalid override value (falls back to baseline)', () => {
    const sched = new CronScheduler(mkTmp(), 'jit-bad');
    process.env.AMI_CRON_JITTER_FRAC = 'not-a-number';
    process.env.AMI_CRON_ONESHOT_JITTER_MS = '-5';
    tick(sched);
    assert.equal(sched.getJitterConfig().recurringFrac, DEFAULT_CRON_JITTER.recurringFrac);
    assert.equal(sched.getJitterConfig().oneShotMaxMs, DEFAULT_CRON_JITTER.oneShotMaxMs);
    sched.stopTicker();
  });

  it('never lets a zero minute-mod override (division guard) through', () => {
    const sched = new CronScheduler(mkTmp(), 'jit-zero');
    process.env.AMI_CRON_ONESHOT_MINUTE_MOD = '0';
    tick(sched);
    assert.equal(sched.getJitterConfig().oneShotMinuteMod, DEFAULT_CRON_JITTER.oneShotMinuteMod,
      'a 0 minute-mod must fall back to the baseline to avoid modulo-by-zero');
    sched.stopTicker();
  });
});
