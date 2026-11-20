import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  SESSION_LOCK_FILE,
  isPidAlive,
  acquireSessionLock,
  isSessionLocked,
  checkSessionLock,
} from '../src/session';

function tmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-lock-pid-'));
  after(() => fs.rmSync(d, { recursive: true, force: true }));
  return d;
}

function writeLock(dir: string, pid: number, startedAt = Date.now()): string {
  fs.mkdirSync(dir, { recursive: true });
  const lockPath = path.join(dir, SESSION_LOCK_FILE);
  fs.writeFileSync(lockPath, JSON.stringify({ pid, startedAt, execPath: '/x' }));
  return lockPath;
}

// U021 — sentinel/corrupt PID liveness guard (parity: the reference's pidLock isProcessRunning
// `pid <= 1` special-case). A lock whose recorded PID parses to 0 or 1 (corruption, a
// truncated/zero-padded write, or a planted value) must be treated as DEAD so the stale
// lock is reclaimable, instead of `process.kill(0|1, 0)` spuriously reporting it live and
// wedging the lock forever. These are red-green discriminators for the guard:
// `isPidAlive(0)`/`isPidAlive(1)` would otherwise return true (own process group / init).
describe('isPidAlive — sentinel/corrupt PID guard (pid <= 1)', () => {
  it('treats PID 0 as dead (never the caller\'s own process group)', () => {
    // Without the guard, process.kill(0, 0) signals this process's OWN group and
    // succeeds → isPidAlive(0) would wrongly return true.
    assert.equal(isPidAlive(0), false);
  });

  it('treats PID 1 (init/systemd) as dead — never a legitimate lock holder', () => {
    // Without the guard, process.kill(1, 0) returns true (root) or throws EPERM
    // (non-root, mapped to true) → isPidAlive(1) would wrongly report init alive.
    assert.equal(isPidAlive(1), false);
  });

  it('treats a negative PID as dead', () => {
    assert.equal(isPidAlive(-1), false);
  });
});

describe('session lock reclaim with a sentinel PID (end-to-end, real default deps)', () => {
  it('reclaims and acquires over a FRESH lock whose recorded pid is 0', () => {
    const dir = tmpDir();
    // startedAt=now so the time-based stale fallback would still consider an
    // unverifiable holder ACTIVE — only the pid<=1 liveness guard makes this
    // reclaimable. (Without the guard: isPidAlive(0)=true, getProcessCommand(0)=null,
    // age<=stale ⇒ lockHolderActive=true ⇒ blocked.)
    writeLock(dir, 0, Date.now());
    const res = acquireSessionLock(dir); // real DEFAULT_LOCK_DEPS — no injection
    assert.equal(res.acquired, true);
    res.release();
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
  });

  it('isSessionLocked reports a fresh pid-1 lock as NOT locked (reclaimable)', () => {
    const dir = tmpDir();
    writeLock(dir, 1, Date.now());
    assert.equal(isSessionLocked(dir), false);
  });

  it('checkSessionLock recovers a fresh pid-0 lock and reports {free}', () => {
    const dir = tmpDir();
    const lockPath = writeLock(dir, 0, Date.now());
    assert.deepEqual(checkSessionLock(dir), { kind: 'free' });
    assert.equal(fs.existsSync(lockPath), false); // stale sentinel lock recovered
  });
});
