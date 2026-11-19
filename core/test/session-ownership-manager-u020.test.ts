/**
 * U020 — SessionManager single-writer ownership wiring (orca fencing token).
 *
 * The fence primitives (runtimeFence on the lock, assertSessionFence,
 * evaluateSessionAcquisition, process-identity fail-closed probe) are unit-tested
 * in session-ownership-fence-orca.test.ts. THIS file tests the SessionManager
 * CONSUMER wiring that makes them live: acquireOwnership / assertOwnership /
 * ownershipGuard / releaseOwnership and the fence assert inside save() — i.e. a
 * superseded owner's save/rollback is rejected instead of corrupting the new
 * owner's transcript. These are the methods the engine wires into its session-
 * write + checkpoint-rollback paths.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  SessionManager,
  SESSION_LOCK_FILE,
  SessionFenceError,
  type Session,
  type SessionLockDeps,
} from '../src/session';
import { asSessionId } from '../src/types';

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'u020-own-'));
}

function makeSession(id: string, over: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages: [{ role: 'user', content: 'hello world' }],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'http://localhost' },
    ...over,
  };
}

/** Overwrite the on-disk lock so it looks like a DIFFERENT owner advanced the
 *  fence (a takeover) — simulating a second AMI instance reclaiming the session. */
function simulateTakeover(sessionDir: string, fence: number): void {
  const lockPath = path.join(sessionDir, SESSION_LOCK_FILE);
  fs.writeFileSync(
    lockPath,
    JSON.stringify({
      pid: process.pid + 1,
      startedAt: Date.now(),
      execPath: '/other',
      runtimeFence: fence,
    }),
  );
}

describe('U020 SessionManager.acquireOwnership — fresh acquire', () => {
  let dir: string;
  beforeEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; dir = tmpDir(); });
  afterEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; });

  it('records fence 1 on a brand-new session and writes the lock', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    const res = sm.acquireOwnership();
    assert.equal(res.acquired, true);
    assert.equal(res.fence, 1);
    assert.equal(sm.ownershipFence(), 1);
    assert.ok(fs.existsSync(path.join(sm.getSessionDir(), SESSION_LOCK_FILE)));
    sm.releaseOwnership();
  });

  it('is idempotent — a second acquire returns the held fence, no re-lock', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    const first = sm.acquireOwnership();
    const second = sm.acquireOwnership();
    assert.deepEqual(second, { acquired: true, fence: first.fence });
    sm.releaseOwnership();
  });
});

describe('U020 SessionManager.save — fence-asserted single writer', () => {
  let dir: string;
  beforeEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; dir = tmpDir(); });
  afterEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; });

  it('save succeeds while the held fence is still current', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    sm.acquireOwnership();
    assert.doesNotThrow(() => sm.save(makeSession('s-ok')));
    assert.equal(sm.load('s-ok')!.id, 's-ok');
    sm.releaseOwnership();
  });

  it('save THROWS SessionFenceError (writes nothing) after a takeover superseded the fence', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    sm.acquireOwnership(); // fence 1
    sm.save(makeSession('s-pre')); // lands normally
    simulateTakeover(sm.getSessionDir(), 2); // another owner advanced to fence 2
    assert.throws(
      () => sm.save(makeSession('s-blocked')),
      (e: unknown) => e instanceof SessionFenceError && e.expectedFence === 1 && e.currentFence === 2,
    );
    // The superseded write never landed.
    assert.equal(sm.load('s-blocked'), null);
    sm.releaseOwnership();
  });

  it('a manager that never acquired ownership saves unguarded (unchanged behavior)', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    fs.mkdirSync(sm.getSessionDir(), { recursive: true });
    simulateTakeover(sm.getSessionDir(), 9); // irrelevant — no ownership held
    assert.equal(sm.ownershipFence(), undefined);
    assert.doesNotThrow(() => sm.save(makeSession('s-free')));
    assert.equal(sm.load('s-free')!.id, 's-free');
  });
});

describe('U020 SessionManager.assertOwnership / ownershipGuard', () => {
  let dir: string;
  beforeEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; dir = tmpDir(); });
  afterEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; });

  it('assertOwnership is a no-op when no ownership is held', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    assert.doesNotThrow(() => sm.assertOwnership());
  });

  it('ownershipGuard() returns a thunk that throws once a newer owner took over', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    sm.acquireOwnership();
    const guard = sm.ownershipGuard();
    assert.doesNotThrow(guard); // still current
    simulateTakeover(sm.getSessionDir(), 5);
    assert.throws(guard, SessionFenceError);
    sm.releaseOwnership();
  });
});

describe('U020 SessionManager.releaseOwnership', () => {
  let dir: string;
  beforeEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; dir = tmpDir(); });
  afterEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; });

  it('forgets the fence and unlinks the lock so a peer can take over', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    sm.acquireOwnership();
    const lockPath = path.join(sm.getSessionDir(), SESSION_LOCK_FILE);
    assert.ok(fs.existsSync(lockPath));
    sm.releaseOwnership();
    assert.equal(sm.ownershipFence(), undefined);
    assert.equal(fs.existsSync(lockPath), false);
    // Post-release, a stale/takeover fence can no longer block a save.
    simulateTakeover(sm.getSessionDir(), 42);
    assert.doesNotThrow(() => sm.save(makeSession('s-after-release')));
  });

  it('is idempotent (a second release is a no-op)', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    sm.acquireOwnership();
    sm.releaseOwnership();
    assert.doesNotThrow(() => sm.releaseOwnership());
  });
});

describe('U020 SessionManager.acquireOwnership — blocked + locking-disabled', () => {
  let dir: string;
  beforeEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; dir = tmpDir(); });
  afterEach(() => { delete process.env.AMI_DISABLE_SESSION_LOCK; });

  it('a verified-live OTHER holder blocks the acquire (no fence recorded, save stays unguarded)', () => {
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    sm.getSessionDir(); // ensure dir resolved
    // Seed a lock held by a different, provably-live, identity-matching pid.
    fs.mkdirSync(sm.getSessionDir(), { recursive: true });
    const otherPid = process.pid + 1000;
    const startMs = 123456;
    fs.writeFileSync(
      path.join(sm.getSessionDir(), SESSION_LOCK_FILE),
      JSON.stringify({ pid: otherPid, startedAt: Date.now(), execPath: '/x', processStartTimeMs: startMs, runtimeFence: 3 }),
    );
    const deps: Partial<SessionLockDeps> = {
      isPidAlive: (p) => p === otherPid,
      readProcessStartTimeMs: (p) => (p === otherPid ? startMs : null),
    };
    const res = sm.acquireOwnership(deps);
    assert.equal(res.acquired, false);
    assert.equal(res.blocked, true);
    assert.ok(typeof res.message === 'string' && res.message.includes(String(otherPid)));
    assert.equal(sm.ownershipFence(), undefined);
  });

  it('with AMI_DISABLE_SESSION_LOCK set, acquire succeeds with no enforceable fence', () => {
    process.env.AMI_DISABLE_SESSION_LOCK = '1';
    const sm = new SessionManager(dir, path.join(dir, 'sessions'));
    const res = sm.acquireOwnership();
    assert.equal(res.acquired, true);
    assert.equal(res.fence, undefined);
    assert.equal(sm.ownershipFence(), undefined);
    // A takeover fence cannot block a save when locking is disabled.
    assert.doesNotThrow(() => sm.save(makeSession('s-nolock')));
  });
});
