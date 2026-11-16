import { describe, it, after, afterEach, beforeEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { CronScheduler, releaseAllCronLocks, matchField } from '../src/cron';
import { projectStateDir } from '../src/reference-journal';
import { fsHooks } from '../src/logger';

// ---------------------------------------------------------------------------
// Shared tmp-dir plumbing
// ---------------------------------------------------------------------------
const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'cron-lock-'));
  tmpDirs.push(d);
  return d;
}
function lockPathFor(cwd: string): string {
  return path.join(projectStateDir(cwd), 'cron.lock');
}
function writeLock(cwd: string, lock: Record<string, unknown>): void {
  const dir = projectStateDir(cwd);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(lockPathFor(cwd), JSON.stringify(lock));
}
// Private accessors (mirrors the existing cron test style of reaching into
// TS-private members for deterministic coverage of internal branches).
type LockInternals = {
  acquireLock(): boolean;
  releaseLock(): void;
  lastBlockedBy: string | null;
  exitRelease: (() => void) | null;
};
function internals(s: CronScheduler): LockInternals {
  return s as unknown as LockInternals;
}

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

// ---------------------------------------------------------------------------
// Gap A — idempotent re-acquire after --resume rewrites the lock's PID
// ---------------------------------------------------------------------------
describe('cron lock — idempotent re-acquire (resume PID rewrite)', () => {
  it('rewrites the stored pid when the sessionId matches but the pid changed', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'resumed-session');
    // Simulate a lock left by a prior process of THIS session (restored via --resume):
    // same sessionId, a stale/different pid.
    const stalePid = process.pid + 1;
    writeLock(cwd, { sessionId: 'resumed-session', pid: stalePid, timestamp: Date.now() });

    assert.equal(internals(sched).acquireLock(), true, 'own lock must re-acquire idempotently');

    const lock = JSON.parse(fs.readFileSync(lockPathFor(cwd), 'utf-8'));
    assert.equal(lock.sessionId, 'resumed-session');
    assert.equal(lock.pid, process.pid, 'pid must be rewritten to the live process');
    sched.stopTicker();
  });

  it('does not rewrite when the pid already matches (no-op idempotent path)', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'same-session');
    writeLock(cwd, { sessionId: 'same-session', pid: process.pid, timestamp: 111 });

    assert.equal(internals(sched).acquireLock(), true);
    const lock = JSON.parse(fs.readFileSync(lockPathFor(cwd), 'utf-8'));
    // Unchanged timestamp proves the file was not rewritten on the matching-pid path.
    assert.equal(lock.timestamp, 111, 'matching-pid re-acquire must not rewrite the lock');
    sched.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// Gap B — de-duplicated contention logging (lastBlockedBy)
// ---------------------------------------------------------------------------
describe('cron lock — de-duplicated contention logging', () => {
  afterEach(() => mock.restoreAll());

  it('logs "held by" once per distinct live owner, then stays quiet while polling', () => {
    const cwd = mkTmp();
    // A DIFFERENT, live owner (use our own pid so isProcessRunning() is true).
    writeLock(cwd, { sessionId: 'owner-X', pid: process.pid, timestamp: Date.now() });
    const sched = new CronScheduler(cwd, 'passive-session');

    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });

    // Poll the live owner three times (as the 5s ticker would).
    assert.equal(internals(sched).acquireLock(), false);
    assert.equal(internals(sched).acquireLock(), false);
    assert.equal(internals(sched).acquireLock(), false);

    const held = lines.filter(l => l.includes('cron lock held by session owner-X'));
    assert.equal(held.length, 1, 'contention must be logged exactly once per owner, not every poll');
    assert.equal(internals(sched).lastBlockedBy, 'owner-X');
  });

  it('logs again when the blocking owner changes', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'passive-session');
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: string, line: string) => { lines.push(line); });

    writeLock(cwd, { sessionId: 'owner-A', pid: process.pid, timestamp: Date.now() });
    internals(sched).acquireLock();
    writeLock(cwd, { sessionId: 'owner-B', pid: process.pid, timestamp: Date.now() });
    internals(sched).acquireLock();

    assert.equal(lines.filter(l => l.includes('held by session owner-A')).length, 1);
    assert.equal(lines.filter(l => l.includes('held by session owner-B')).length, 1);
  });

  it('resets the suppression state on successful acquire', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'my-session');
    // First blocked by a live owner.
    writeLock(cwd, { sessionId: 'owner-Z', pid: process.pid, timestamp: Date.now() });
    internals(sched).acquireLock();
    assert.equal(internals(sched).lastBlockedBy, 'owner-Z');
    // Owner goes away (stale/dead pid) → we acquire → suppression cleared.
    writeLock(cwd, { sessionId: 'owner-Z', pid: 0x7fffffff, timestamp: 0 });
    assert.equal(internals(sched).acquireLock(), true);
    assert.equal(internals(sched).lastBlockedBy, null, 'acquire must clear lastBlockedBy');
    sched.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// Gap C — process-exit lock release via cleanup registry
// ---------------------------------------------------------------------------
describe('cron lock — release on process exit via cleanup registry', () => {
  it('registers an exit release on acquire and frees the lock via releaseAllCronLocks', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'exit-session');
    assert.equal(internals(sched).acquireLock(), true);
    assert.ok(fs.existsSync(lockPathFor(cwd)), 'lock file should exist after acquire');
    assert.equal(typeof internals(sched).exitRelease, 'function', 'acquire must register an exit release');

    // Simulate an abrupt process exit (stopTicker was never called).
    releaseAllCronLocks();
    assert.equal(fs.existsSync(lockPathFor(cwd)), false, 'exit cleanup must remove our lock');
  });

  it('unregisters the exit release on releaseLock so exit cleanup is a no-op afterwards', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'tidy-session');
    internals(sched).acquireLock();
    internals(sched).releaseLock();
    assert.equal(internals(sched).exitRelease, null, 'releaseLock must unregister the exit release');
    // Re-create a foreign lock; exit cleanup must not touch it now.
    writeLock(cwd, { sessionId: 'someone-else', pid: process.pid, timestamp: Date.now() });
    releaseAllCronLocks();
    assert.ok(fs.existsSync(lockPathFor(cwd)), 'already-released scheduler must not remove others on exit');
  });

  it('registerExitRelease is idempotent (one registration across repeated acquires)', () => {
    const cwd = mkTmp();
    const sched = new CronScheduler(cwd, 'idem-session');
    internals(sched).acquireLock();
    const first = internals(sched).exitRelease;
    internals(sched).acquireLock();
    assert.equal(internals(sched).exitRelease, first, 'repeated acquire must not re-register a new release');
    sched.stopTicker();
  });
});

// ---------------------------------------------------------------------------
// Gap D — computeNextRun coarse skip-ahead (behaviour-preserving optimization)
// ---------------------------------------------------------------------------
describe('computeNextRun — skip-ahead equals the brute-force minute-walk oracle', () => {
  let sched: CronScheduler;
  beforeEach(() => { sched = new CronScheduler(mkTmp(), 'oracle-session'); });

  // The former O(minutes) reference implementation, verbatim, used as the oracle.
  const SCAN_HORIZON_MIN = 4 * 366 * 24 * 60;
  function oracle(cron: string, fromTime: number): number | null {
    const parts = cron.trim().split(/\s+/);
    const [minSpec, hourSpec, domSpec, monSpec, dowSpec] = parts;
    const check = new Date(fromTime);
    check.setSeconds(0, 0);
    check.setMinutes(check.getMinutes() + 1);
    const domRestricted = domSpec !== '*';
    const dowRestricted = dowSpec !== '*';
    for (let i = 0; i < SCAN_HORIZON_MIN; i++) {
      const minMatch = matchField(check.getMinutes(), minSpec, 0, 59);
      const hourMatch = matchField(check.getHours(), hourSpec, 0, 23);
      const monMatch = matchField(check.getMonth() + 1, monSpec, 1, 12);
      const domMatch = matchField(check.getDate(), domSpec, 1, 31);
      const dowMatch = matchField(check.getDay(), dowSpec, 0, 6);
      const dayMatch = (domRestricted && dowRestricted) ? (domMatch || dowMatch) : (domMatch && dowMatch);
      if (minMatch && hourMatch && monMatch && dayMatch) return check.getTime();
      check.setMinutes(check.getMinutes() + 1);
    }
    return null;
  }

  const crons = [
    '* * * * *',
    '0 9 * * *',
    '30 14 * * 1',
    '0 9 15 * *',
    '0 9 15 * 1',       // dom+dow OR semantics
    '0 0 1 1 *',        // yearly (sparse)
    '15 3 29 2 *',      // Feb 29 — only on leap years
    '*/7 */3 * * *',
    '0 0 * * 0-7',      // DOW 7≡0 span
    '59 23 31 12 *',
    '0 12 31 4 *',      // Apr 31 never exists but every other month... (dom only)
  ];
  const bases = [
    Date.parse('2026-01-01T00:00:00'),
    Date.parse('2026-02-27T23:58:00'),
    Date.parse('2027-12-31T23:59:30'),
    Date.parse('2024-02-28T12:00:00'), // leap year
    Date.parse('2026-11-16T08:30:00'),
  ];

  it('produces identical results to the oracle across a cron × base matrix', () => {
    for (const cron of crons) {
      for (const base of bases) {
        assert.equal(
          sched.computeNextRun(cron, base),
          oracle(cron, base),
          `mismatch for cron "${cron}" at base ${new Date(base).toISOString()}`,
        );
      }
    }
  });

  it('resolves a sparse yearly cron to the correct Jan-1 instant', () => {
    const base = Date.parse('2026-06-15T10:00:00');
    const next = sched.computeNextRun('0 0 1 1 *', base)!;
    const d = new Date(next);
    assert.equal(d.getFullYear(), 2027);
    assert.equal(d.getMonth(), 0);
    assert.equal(d.getDate(), 1);
    assert.equal(d.getHours(), 0);
    assert.equal(d.getMinutes(), 0);
  });

  it('returns null for a structurally-valid but unsatisfiable schedule (Feb 30)', () => {
    assert.equal(sched.computeNextRun('0 0 30 2 *', Date.parse('2026-01-01T00:00:00')), null);
  });

  it('finds the next leap-day (Feb 29) match within the horizon', () => {
    const base = Date.parse('2026-03-01T00:00:00');
    const next = sched.computeNextRun('0 0 29 2 *', base)!;
    assert.ok(next, 'Feb 29 should resolve to the next leap year within the 4-year horizon');
    const d = new Date(next);
    assert.equal(d.getMonth(), 1);
    assert.equal(d.getDate(), 29);
    assert.equal(d.getFullYear(), 2028);
  });
});
