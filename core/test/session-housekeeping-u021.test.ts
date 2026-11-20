/**
 * U021 — once-per-day, cross-process-safe session housekeeping (retention) throttle.
 *
 * Parity target: the reference's backgroundHousekeeping, which throttles the cleanup
 * sweep to "once per day" with "marker files and locks" and "skip[s] immediately if
 * another process holds the lock". AMI already had `cleanupOldSessions` (retention),
 * but it ran unconditionally on every engine start (and would run in every one of N
 * concurrent processes). This suite covers the new throttle boundary:
 *   - `isSessionCleanupEnabled` — the single "is retention on?" predicate;
 *   - `isHousekeepingDue` — the due/not-due throttle predicate (incl. self-heal on a
 *      corrupt future marker);
 *   - `maybeRunDailyHousekeeping` — marker throttle + cross-process lock-skip +
 *      double-check-under-lock, with injected deps (no real time / fs / lock race);
 *   - `SessionManager.cleanupOldSessionsThrottled` — the wired store-level method
 *      (disabled short-circuit, real-fs throttle, and two-process serialization).
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  isSessionCleanupEnabled,
  isHousekeepingDue,
  maybeRunDailyHousekeeping,
  HOUSEKEEPING_PERIOD_MS,
  HOUSEKEEPING_MARKER_FILE,
  HOUSEKEEPING_LOCK_SUBDIR,
} from '../src/session';

const DAY_MS = 24 * 60 * 60 * 1000;

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-housekeep-'));
}

function writeSessionFile(dir: string, id: string, ageDays: number): string {
  fs.mkdirSync(dir, { recursive: true });
  const p = path.join(dir, `${id}.json`);
  fs.writeFileSync(p, JSON.stringify({ id, messages: [] }));
  const when = (Date.now() - ageDays * DAY_MS) / 1000;
  fs.utimesSync(p, when, when);
  return p;
}

/** A lock stub: always-acquired, records release calls. */
function okLock(): { lock: { acquired: boolean; release: () => void }; released: () => number } {
  let releases = 0;
  return {
    lock: { acquired: true, release: () => { releases++; } },
    released: () => releases,
  };
}

describe('isSessionCleanupEnabled', () => {
  it('is false for a missing / non-finite / non-positive window', () => {
    assert.equal(isSessionCleanupEnabled(undefined), false);
    assert.equal(isSessionCleanupEnabled(0), false);
    assert.equal(isSessionCleanupEnabled(-3), false);
    assert.equal(isSessionCleanupEnabled(NaN), false);
    assert.equal(isSessionCleanupEnabled(Infinity), false);
  });

  it('is true for any finite positive window (incl. a fractional day)', () => {
    assert.equal(isSessionCleanupEnabled(1), true);
    assert.equal(isSessionCleanupEnabled(0.5), true);
    assert.equal(isSessionCleanupEnabled(30), true);
  });
});

describe('isHousekeepingDue', () => {
  const now = 1_000_000_000_000;
  it('is due when never run (null marker)', () => {
    assert.equal(isHousekeepingDue(null, now), true);
  });
  it('is NOT due when the last run is within the period', () => {
    assert.equal(isHousekeepingDue(now - (DAY_MS - 1), now), false);
  });
  it('is due at exactly one period elapsed', () => {
    assert.equal(isHousekeepingDue(now - DAY_MS, now), true);
  });
  it('is due when more than one period has elapsed', () => {
    assert.equal(isHousekeepingDue(now - 3 * DAY_MS, now), true);
  });
  it('self-heals: a FUTURE (corrupt / clock-skewed) marker is treated as due', () => {
    assert.equal(isHousekeepingDue(now + DAY_MS, now), true);
  });
  it('honours a custom period', () => {
    assert.equal(isHousekeepingDue(now - 500, now, 1000), false);
    assert.equal(isHousekeepingDue(now - 1000, now, 1000), true);
  });
});

describe('maybeRunDailyHousekeeping (throttle + cross-process lock-skip)', () => {
  it('skips (returns false) and never locks when a sweep ran within the period', () => {
    let taskRuns = 0;
    let lockCalls = 0;
    const ran = maybeRunDailyHousekeeping('/dir', () => { taskRuns++; }, {
      now: () => 1000,
      readMarker: () => 1000 - 10, // 10ms ago → not due
      acquireLock: () => { lockCalls++; return { acquired: true, release: () => {} }; },
    });
    assert.equal(ran, false);
    assert.equal(taskRuns, 0, 'task must not run when throttled');
    assert.equal(lockCalls, 0, 'the lock is not even attempted when not due');
  });

  it('runs the task, stamps the marker with now, and releases the lock when due', () => {
    let taskRuns = 0;
    let written: { p: string; ts: number } | null = null;
    const { lock, released } = okLock();
    let lockDir = '';
    const ran = maybeRunDailyHousekeeping('/store', () => { taskRuns++; }, {
      now: () => 5_000,
      readMarker: () => null, // never run → due
      writeMarker: (p, ts) => { written = { p, ts }; },
      acquireLock: (d) => { lockDir = d; return lock; },
    });
    assert.equal(ran, true);
    assert.equal(taskRuns, 1);
    assert.deepEqual(written, { p: path.join('/store', HOUSEKEEPING_MARKER_FILE), ts: 5_000 });
    assert.equal(lockDir, path.join('/store', HOUSEKEEPING_LOCK_SUBDIR));
    assert.equal(released(), 1, 'the lock is released after a successful run');
  });

  it('skips (returns false) without running the task when another process holds the lock', () => {
    let taskRuns = 0;
    let wrote = false;
    const ran = maybeRunDailyHousekeeping('/store', () => { taskRuns++; }, {
      now: () => 5_000,
      readMarker: () => null, // due
      writeMarker: () => { wrote = true; },
      acquireLock: () => ({ acquired: false, release: () => {} }),
    });
    assert.equal(ran, false);
    assert.equal(taskRuns, 0, 'another live sweeper → do not pile on');
    assert.equal(wrote, false, 'no marker write when we did not sweep');
  });

  it('double-checks under the lock: a racer that finished first means we skip and release', () => {
    let taskRuns = 0;
    const { lock, released } = okLock();
    // First read (pre-lock) → due (null); second read (under lock) → fresh marker.
    const reads: Array<number | null> = [null, 9_999];
    let readIdx = 0;
    const ran = maybeRunDailyHousekeeping('/store', () => { taskRuns++; }, {
      now: () => 10_000,
      readMarker: () => reads[Math.min(readIdx++, reads.length - 1)],
      acquireLock: () => lock,
    });
    assert.equal(ran, false);
    assert.equal(taskRuns, 0, 'the racer already swept — do not sweep again');
    assert.equal(released(), 1, 'the lock is released even when the re-check skips');
  });

  it('releases the lock even when the task throws', () => {
    const { lock, released } = okLock();
    assert.throws(() => {
      maybeRunDailyHousekeeping('/store', () => { throw new Error('boom'); }, {
        now: () => 1,
        readMarker: () => null,
        acquireLock: () => lock,
      });
    }, /boom/);
    assert.equal(released(), 1, 'finally releases the lock on a throwing task');
  });
});

describe('SessionManager.cleanupOldSessionsThrottled', () => {
  let dir: string;
  beforeEach(() => { dir = makeTempDir(); });
  afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ } });

  it('short-circuits (no marker, no lock) when retention is disabled', () => {
    let lockCalls = 0;
    const mgr = new SessionManager('', dir);
    const res = mgr.cleanupOldSessionsThrottled(undefined, {
      acquireLock: () => { lockCalls++; return { acquired: true, release: () => {} }; },
    });
    assert.deepEqual(res, { ran: false, removed: 0 });
    assert.equal(lockCalls, 0, 'disabled retention never touches the housekeeping lock');
    assert.equal(fs.existsSync(path.join(dir, HOUSEKEEPING_MARKER_FILE)), false, 'no marker written');
  });

  it('runs the real sweep on first call, then is throttled on an immediate second call', () => {
    const stale1 = writeSessionFile(dir, 'aaaaaaaaaaaaaaaa', 10);
    const mgr = new SessionManager('', dir);

    const first = mgr.cleanupOldSessionsThrottled(7);
    assert.equal(first.ran, true, 'first call sweeps (no marker yet → due)');
    assert.equal(first.removed, 1);
    assert.equal(fs.existsSync(stale1), false, 'the 10d-old session is deleted');
    assert.equal(fs.existsSync(path.join(dir, HOUSEKEEPING_MARKER_FILE)), true, 'marker stamped');

    // A SECOND stale file appears, but a sweep ran <24h ago → the next call is throttled.
    const stale2 = writeSessionFile(dir, 'bbbbbbbbbbbbbbbb', 20);
    const second = mgr.cleanupOldSessionsThrottled(7);
    assert.equal(second.ran, false, 'second call within the period is throttled');
    assert.equal(second.removed, 0);
    assert.equal(fs.existsSync(stale2), true, 'the throttled call does NOT delete the new stale file');
  });

  it('serializes two concurrent processes over one store: only one sweeps per period', () => {
    writeSessionFile(dir, 'cccccccccccccccc', 30);
    // Two SessionManagers over the SAME dir stand in for two processes.
    const procA = new SessionManager('', dir);
    const procB = new SessionManager('', dir);

    const a = procA.cleanupOldSessionsThrottled(7);
    const b = procB.cleanupOldSessionsThrottled(7);

    const ranCount = (a.ran ? 1 : 0) + (b.ran ? 1 : 0);
    assert.equal(ranCount, 1, 'exactly one of the two processes performs the per-period sweep');
    assert.equal((a.removed) + (b.removed), 1, 'the single sweep removed the one stale session');
  });

  it('re-sweeps once the throttle period elapses (marker backdated past the period)', () => {
    const mgr = new SessionManager('', dir);
    // First sweep: nothing to remove, but it stamps the marker.
    assert.equal(mgr.cleanupOldSessionsThrottled(7).ran, true);
    // Backdate the marker older than the 24h period so the next call is due again.
    const markerPath = path.join(dir, HOUSEKEEPING_MARKER_FILE);
    fs.writeFileSync(markerPath, String(Date.now() - 2 * HOUSEKEEPING_PERIOD_MS));

    const stale = writeSessionFile(dir, 'dddddddddddddddd', 40);
    const again = mgr.cleanupOldSessionsThrottled(7);
    assert.equal(again.ran, true, 'past the period → due again');
    assert.equal(again.removed, 1);
    assert.equal(fs.existsSync(stale), false, 'the re-enabled sweep deletes the stale session');
  });
});
