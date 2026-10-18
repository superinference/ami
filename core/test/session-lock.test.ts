import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  SESSION_LOCK_FILE,
  acquireSessionLock,
  isSessionLocked,
  isPidAlive,
} from '../src/session';

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'session-lock-'));
}

describe('isPidAlive', () => {
  it('returns true for the current process', () => {
    assert.equal(isPidAlive(process.pid), true);
  });

  it('returns false for a nonexistent PID', () => {
    // PID 2^30 is extremely unlikely to exist
    assert.equal(isPidAlive(1 << 30), false);
  });

  it('returns false for PID 0 on most systems (init is special)', () => {
    // process.kill(0, 0) sends to the current process group — but
    // isPidAlive wraps it so we simply check it doesn't throw.
    const result = isPidAlive(0);
    assert.equal(typeof result, 'boolean');
  });
});

describe('SESSION_LOCK_FILE constant', () => {
  it('is .session.lock', () => {
    assert.equal(SESSION_LOCK_FILE, '.session.lock');
  });
});

describe('acquireSessionLock', () => {
  it('succeeds on a new directory', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    lock.release();
  });

  it('creates the lock file with the current PID', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const lock = acquireSessionLock(dir);
    const content = fs.readFileSync(path.join(dir, SESSION_LOCK_FILE), 'utf-8');
    assert.equal(content, String(process.pid));
    lock.release();
  });

  it('release removes the lock file', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    lock.release();
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
  });

  it('fails when lock is held by the current process (alive PID)', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const first = acquireSessionLock(dir);
    assert.equal(first.acquired, true);

    const second = acquireSessionLock(dir);
    assert.equal(second.acquired, false);
    assert.equal(second.pid, process.pid);

    first.release();
  });

  it('reclaims a stale lock (dead PID)', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    // Write a lock file with a PID that doesn't exist
    const stalePid = (1 << 30) + 7;
    fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), String(stalePid));

    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    const content = fs.readFileSync(path.join(dir, SESSION_LOCK_FILE), 'utf-8');
    assert.equal(content, String(process.pid));
    lock.release();
  });

  it('reclaims a lock with a non-numeric PID', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), 'not-a-number');

    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    lock.release();
  });

  it('creates the session directory if it does not exist', () => {
    const parent = tmpDir();
    const dir = path.join(parent, 'nested', 'session');
    after(() => fs.rmSync(parent, { recursive: true, force: true }));

    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    assert.equal(fs.existsSync(dir), true);
    lock.release();
  });

  it('second attempt after release succeeds', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const first = acquireSessionLock(dir);
    first.release();

    const second = acquireSessionLock(dir);
    assert.equal(second.acquired, true);
    second.release();
  });

  it('release is idempotent (calling twice does not throw)', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const lock = acquireSessionLock(dir);
    lock.release();
    assert.doesNotThrow(() => lock.release());
  });
});

describe('isSessionLocked', () => {
  it('returns false for an unlocked directory', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    assert.equal(isSessionLocked(dir), false);
  });

  it('returns true for a locked directory', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const lock = acquireSessionLock(dir);
    assert.equal(isSessionLocked(dir), true);
    lock.release();
  });

  it('returns false after release', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    const lock = acquireSessionLock(dir);
    lock.release();
    assert.equal(isSessionLocked(dir), false);
  });

  it('returns false for a stale lock file', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), String((1 << 30) + 9));
    assert.equal(isSessionLocked(dir), false);
  });

  it('returns false for a non-numeric lock file', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));

    fs.writeFileSync(path.join(dir, SESSION_LOCK_FILE), 'garbage');
    assert.equal(isSessionLocked(dir), false);
  });

  it('returns false when the directory does not exist', () => {
    assert.equal(isSessionLocked('/tmp/nonexistent-session-lock-test-dir-9999'), false);
  });
});
