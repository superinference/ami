import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  SESSION_LOCK_FILE,
  acquireSessionLock,
  checkSessionLock,
  isSessionLocked,
  isSessionLockingDisabled,
  LOCK_FALLBACK_STALE_MS,
  LOCK_RETRY_MAX_DELAY_MS,
  LOCK_RETRY_DEFAULT_DELAY_MS,
  type SessionLockDeps,
} from '../src/session';

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'session-lock-opts-'));
}

/** Write a lock file on disk owned by a foreign pid (simulates another holder). */
function writeForeignLock(dir: string, pid: number, startedAt = Date.now()): string {
  const lockPath = path.join(dir, SESSION_LOCK_FILE);
  fs.writeFileSync(lockPath, JSON.stringify({ pid, startedAt, execPath: '/x' }));
  return lockPath;
}

/** Run `fn` with AMI_DISABLE_SESSION_LOCK set to `value`, restoring the prior env. */
function withLockDisabledEnv<T>(value: string | undefined, fn: () => T): T {
  const prev = process.env.AMI_DISABLE_SESSION_LOCK;
  if (value === undefined) delete process.env.AMI_DISABLE_SESSION_LOCK;
  else process.env.AMI_DISABLE_SESSION_LOCK = value;
  try {
    return fn();
  } finally {
    if (prev === undefined) delete process.env.AMI_DISABLE_SESSION_LOCK;
    else process.env.AMI_DISABLE_SESSION_LOCK = prev;
  }
}

describe('retry-backoff tuning constants', () => {
  it('expose a bounded max delay and a sane default base delay', () => {
    assert.equal(LOCK_RETRY_MAX_DELAY_MS, 1000);
    assert.equal(LOCK_RETRY_DEFAULT_DELAY_MS, 50);
    assert.ok(LOCK_RETRY_DEFAULT_DELAY_MS < LOCK_RETRY_MAX_DELAY_MS);
  });
});

describe('isSessionLockingDisabled env knob', () => {
  it('defaults to false (locking ON)', () => {
    assert.equal(isSessionLockingDisabled({}), false);
  });

  it('is true when AMI_DISABLE_SESSION_LOCK is truthy', () => {
    assert.equal(isSessionLockingDisabled({ AMI_DISABLE_SESSION_LOCK: '1' }), true);
    assert.equal(isSessionLockingDisabled({ AMI_DISABLE_SESSION_LOCK: 'true' }), true);
  });

  it('is false for an explicit falsy / empty value', () => {
    assert.equal(isSessionLockingDisabled({ AMI_DISABLE_SESSION_LOCK: '' }), false);
    assert.equal(isSessionLockingDisabled({ AMI_DISABLE_SESSION_LOCK: '0' }), false);
    assert.equal(isSessionLockingDisabled({ AMI_DISABLE_SESSION_LOCK: 'false' }), false);
  });
});

describe('AMI_DISABLE_SESSION_LOCK bypasses the locking mechanism', () => {
  it('acquireSessionLock succeeds without writing a lock file when disabled', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    withLockDisabledEnv('1', () => {
      const lock = acquireSessionLock(dir);
      assert.equal(lock.acquired, true);
      assert.equal(lock.fresh, false); // never fires a fresh-gated side effect
      assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
      lock.release(); // no-op, must not throw
    });
  });

  it('acquire succeeds even when a LIVE foreign holder owns the dir', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    // A live, verified foreign holder would normally block (acquired:false)...
    writeForeignLock(dir, 999_999);
    const deps: Partial<SessionLockDeps> = { isPidAlive: () => true, getProcessCommand: () => 'ami' };
    const blocked = acquireSessionLock(dir, deps);
    assert.equal(blocked.acquired, false);
    // ...but with the knob set, the dir reads as free and the acquire succeeds.
    withLockDisabledEnv('1', () => {
      const ok = acquireSessionLock(dir, deps);
      assert.equal(ok.acquired, true);
    });
  });

  it('checkSessionLock reports free and isSessionLocked reports false when disabled', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999);
    const deps: Partial<SessionLockDeps> = { isPidAlive: () => true, getProcessCommand: () => 'ami' };
    // Without the knob: blocked / locked.
    assert.equal(checkSessionLock(dir, deps).kind, 'blocked');
    assert.equal(isSessionLocked(dir, deps), true);
    // With the knob: free / unlocked.
    withLockDisabledEnv('1', () => {
      assert.equal(checkSessionLock(dir, deps).kind, 'free');
      assert.equal(isSessionLocked(dir, deps), false);
    });
  });
});

describe('acquireSessionLock retries/retryDelayMs (contended-holder wait)', () => {
  it('default (no retries) fails immediately against a live holder', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999);
    let aliveProbes = 0;
    const deps: Partial<SessionLockDeps> = {
      isPidAlive: () => { aliveProbes++; return true; },
      getProcessCommand: () => 'ami',
    };
    const res = acquireSessionLock(dir, deps);
    assert.equal(res.acquired, false);
    assert.equal(res.pid, 999_999);
    assert.equal(aliveProbes, 1); // probed exactly once — no waiting
  });

  it('retries and wins once the live holder goes away mid-wait', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999);
    let aliveProbes = 0;
    const deps: Partial<SessionLockDeps> = {
      // Alive for the first two probes (blocked), then gone → reclaimable.
      isPidAlive: () => { aliveProbes++; return aliveProbes <= 2; },
      getProcessCommand: () => 'ami',
    };
    const res = acquireSessionLock(dir, deps, { retries: 3, retryDelayMs: 1 });
    assert.equal(res.acquired, true);
    assert.equal(aliveProbes, 3); // blocked twice, reclaimed on the third
    res.release();
  });

  it('gives up after exhausting retries against a persistent holder', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999);
    let aliveProbes = 0;
    const deps: Partial<SessionLockDeps> = {
      isPidAlive: () => { aliveProbes++; return true; },
      getProcessCommand: () => 'ami',
    };
    const res = acquireSessionLock(dir, deps, { retries: 2, retryDelayMs: 1 });
    assert.equal(res.acquired, false);
    assert.equal(aliveProbes, 3); // initial attempt + 2 retries
  });

  it('treats a zero/negative retryDelayMs as a valid no-wait delay', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999);
    let aliveProbes = 0;
    const deps: Partial<SessionLockDeps> = {
      isPidAlive: () => { aliveProbes++; return aliveProbes <= 1; },
      getProcessCommand: () => 'ami',
    };
    const res = acquireSessionLock(dir, deps, { retries: 2, retryDelayMs: 0 });
    assert.equal(res.acquired, true);
    res.release();
  });
});

describe('acquireSessionLock / checkSessionLock stale override', () => {
  // A live but UNVERIFIABLE holder (getProcessCommand === null) is active only while
  // its age is within the stale window — so the stale option directly moves the
  // reclaim boundary.
  function unverifiableDeps(now: number): Partial<SessionLockDeps> {
    return { isPidAlive: () => true, getProcessCommand: () => null, now: () => now };
  }

  it('reclaims a lock aged past a small custom stale window on acquire', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999, 0); // startedAt = 0
    const deps = unverifiableDeps(10_000); // age = 10s
    // Default window (2h) would still consider it active → blocked.
    assert.equal(acquireSessionLock(dir, deps).acquired, false);
    // A 5s stale window makes the 10s-old lock reclaimable → acquired.
    const res = acquireSessionLock(dir, deps, { stale: 5_000 });
    assert.equal(res.acquired, true);
    res.release();
  });

  it('keeps a lock within a large custom stale window blocked on acquire', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999, 0);
    const deps = unverifiableDeps(LOCK_FALLBACK_STALE_MS + 1_000); // past the default window
    // Past the default window → reclaimable by default.
    const def = acquireSessionLock(dir, deps);
    assert.equal(def.acquired, true);
    def.release();
    // But a larger custom window keeps it active → blocked.
    writeForeignLock(dir, 999_999, 0);
    const res = acquireSessionLock(dir, deps, { stale: LOCK_FALLBACK_STALE_MS * 10 });
    assert.equal(res.acquired, false);
  });

  it('checkSessionLock honors a custom stale window', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    writeForeignLock(dir, 999_999, 0);
    const deps = unverifiableDeps(10_000);
    // Default window: active → blocked.
    assert.equal(checkSessionLock(dir, deps).kind, 'blocked');
    // Small window: aged out → free (and recovered).
    writeForeignLock(dir, 999_999, 0);
    assert.equal(checkSessionLock(dir, deps, { stale: 5_000 }).kind, 'free');
  });
});

describe('acquireSessionLock onCompromised callback', () => {
  it('fires when our held lock is taken over before release', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    let reason: string | null = null;
    const lock = acquireSessionLock(dir, undefined, { onCompromised: (r) => { reason = r; } });
    assert.equal(lock.acquired, true);
    // Simulate another process reclaiming our lock (it now carries a foreign pid).
    writeForeignLock(dir, process.pid + 1);
    lock.release();
    assert.ok(reason, 'onCompromised should have been invoked');
    assert.match(reason!, /taken over by pid/);
    // The foreign lock must NOT have been deleted (we no longer own it).
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), true);
  });

  it('does NOT fire on a clean release', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    let fired = false;
    const lock = acquireSessionLock(dir, undefined, { onCompromised: () => { fired = true; } });
    lock.release();
    assert.equal(fired, false);
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
  });

  it('swallows a throwing onCompromised callback (release never throws)', () => {
    const dir = tmpDir();
    after(() => fs.rmSync(dir, { recursive: true, force: true }));
    const lock = acquireSessionLock(dir, undefined, {
      onCompromised: () => { throw new Error('boom'); },
    });
    writeForeignLock(dir, process.pid + 1);
    assert.doesNotThrow(() => lock.release());
  });
});
