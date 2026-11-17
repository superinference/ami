import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  SESSION_LOCK_FILE,
  LOCK_FALLBACK_STALE_MS,
  acquireSessionLock,
  isSessionLocked,
  isSessionLockHeldLocally,
  checkSessionLock,
} from '../src/session';

function tmpDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-lock-u021-'));
  after(() => fs.rmSync(d, { recursive: true, force: true }));
  return d;
}

// U021-w70: computer-use-lock parity — fresh/re-entrant/blocked tri-state,
// zero-syscall local ownership, check-without-acquire with stale recovery, and
// the WSL cross-OS PID-probe boundary guard.

describe('acquireSessionLock — fresh vs re-entrant flag', () => {
  it('reports fresh:true on the first acquisition and fresh:false on re-entry', () => {
    const dir = tmpDir();
    const first = acquireSessionLock(dir);
    assert.equal(first.acquired, true);
    assert.equal(first.fresh, true);

    const again = acquireSessionLock(dir);
    assert.equal(again.acquired, true);
    assert.equal(again.fresh, false);
    assert.equal(again.pid, process.pid);

    // Re-entrant release is a no-op: the on-disk lock the fresh handle owns stays.
    again.release();
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), true);
    first.release();
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
  });

  it('reports blocked (acquired:false, no fresh) for a live OTHER holder', () => {
    const dir = tmpDir();
    // A foreign lock whose PID is alive (point at ourselves but a different-looking
    // command so lockHolderActive treats it as a live other process).
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, SESSION_LOCK_FILE),
      JSON.stringify({ pid: 999_999, startedAt: Date.now(), execPath: '/x' }),
    );
    const res = acquireSessionLock(dir, { isPidAlive: () => true, getProcessCommand: () => 'ami' });
    assert.equal(res.acquired, false);
    assert.equal(res.fresh, undefined);
    assert.equal(res.pid, 999_999);
  });
});

describe('isSessionLockHeldLocally — zero-syscall ownership', () => {
  it('is false before acquire, true while held, false after release (no disk read)', () => {
    const dir = tmpDir();
    assert.equal(isSessionLockHeldLocally(dir), false);
    const handle = acquireSessionLock(dir);
    assert.equal(isSessionLockHeldLocally(dir), true);
    handle.release();
    assert.equal(isSessionLockHeldLocally(dir), false);
  });

  it('does not report ownership of a lock written by another process', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, SESSION_LOCK_FILE),
      JSON.stringify({ pid: 424_242, startedAt: Date.now(), execPath: '/x' }),
    );
    // No fresh acquire happened here → the in-process map knows nothing about it,
    // even though a lock file exists on disk.
    assert.equal(isSessionLockHeldLocally(dir), false);
  });
});

describe('checkSessionLock — check-without-acquire + stale recovery', () => {
  it('returns {free} and creates no lock when none exists', () => {
    const dir = tmpDir();
    assert.deepEqual(checkSessionLock(dir), { kind: 'free' });
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false); // never acquired
  });

  it('returns {held_by_self} when this process owns it', () => {
    const dir = tmpDir();
    const handle = acquireSessionLock(dir);
    const res = checkSessionLock(dir);
    assert.equal(res.kind, 'held_by_self');
    assert.equal((res as { pid: number }).pid, process.pid);
    handle.release();
  });

  it('returns {blocked} for a live other holder and does NOT unlink it', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, SESSION_LOCK_FILE),
      JSON.stringify({ pid: 777, startedAt: Date.now(), execPath: '/x' }),
    );
    const res = checkSessionLock(dir, { isPidAlive: () => true, getProcessCommand: () => 'ami' });
    assert.equal(res.kind, 'blocked');
    assert.equal((res as { pid: number }).pid, 777);
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), true); // left intact
  });

  it('recovers a STALE dead-PID lock (unlinks it) and reports {free} without acquiring', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 888, startedAt: 0, execPath: '/x' }));
    const res = checkSessionLock(dir, { isPidAlive: () => false });
    assert.deepEqual(res, { kind: 'free' });
    assert.equal(fs.existsSync(lockPath), false); // stale lock was recovered
  });

  it('recovers a corrupt/unparseable lock and reports {free}', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    fs.writeFileSync(lockPath, 'not-json');
    assert.deepEqual(checkSessionLock(dir), { kind: 'free' });
    assert.equal(fs.existsSync(lockPath), false);
  });
});

describe('lockHolderActive WSL cross-OS guard (via checkSessionLock deps)', () => {
  it('on WSL, a dead-but-recent PID is treated as held (not reclaimed)', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    const now = Date.now();
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 5150, startedAt: now, execPath: '/x' }));
    // isPidAlive:false (a Windows PID is unprobeable from WSL) but isWsl:true and
    // the lock is fresh → it must NOT be reclaimed.
    const res = checkSessionLock(dir, { isPidAlive: () => false, isWsl: () => true, now: () => now + 1000 });
    assert.equal(res.kind, 'blocked');
    assert.equal(fs.existsSync(lockPath), true);
  });

  it('on WSL, a dead PID older than the fallback window IS reclaimed', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 5151, startedAt: 0, execPath: '/x' }));
    const res = checkSessionLock(dir, {
      isPidAlive: () => false,
      isWsl: () => true,
      now: () => LOCK_FALLBACK_STALE_MS + 10,
    });
    assert.deepEqual(res, { kind: 'free' });
    assert.equal(fs.existsSync(lockPath), false);
  });

  it('OFF WSL, a dead PID is reclaimed immediately regardless of age', () => {
    const dir = tmpDir();
    fs.mkdirSync(dir, { recursive: true });
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    const now = Date.now();
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 5152, startedAt: now, execPath: '/x' }));
    // Same fresh lock, but not WSL → reclaimed on liveness alone.
    assert.equal(isSessionLocked(dir, { isPidAlive: () => false, isWsl: () => false }), false);
    const res = checkSessionLock(dir, { isPidAlive: () => false, isWsl: () => false, now: () => now + 1 });
    assert.deepEqual(res, { kind: 'free' });
    assert.equal(fs.existsSync(lockPath), false);
  });
});
