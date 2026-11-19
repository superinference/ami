import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  SESSION_LOCK_FILE,
  acquireSessionLock,
  readSessionFence,
  isSessionFenceCurrent,
  assertSessionFence,
  SessionFenceError,
  evaluateSessionAcquisition,
  safeAppendTranscript,
  type SessionLockDeps,
  type SessionLockPayload,
} from '../src/session';

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'sess-fence-'));
}

/** Seed a lock file on disk with a full identity-bearing payload. */
function seedLock(dir: string, payload: Partial<SessionLockPayload> & { pid: number }): string {
  const lockPath = path.join(dir, SESSION_LOCK_FILE);
  const full: SessionLockPayload = {
    startedAt: Date.now(),
    execPath: '/x',
    ...payload,
  };
  fs.writeFileSync(lockPath, JSON.stringify(full));
  return lockPath;
}

function backdate(lockPath: string, agoMs: number): void {
  const t = (Date.now() - agoMs) / 1000;
  fs.utimesSync(lockPath, t, t);
}

const ABSENT_PID = 2 ** 30;

describe('session ownership — monotonic fence on acquire/reclaim', () => {
  afterEach(() => { /* temp dirs are per-test */ });

  it('a fresh acquire records runtimeFence 1 + identity', () => {
    const dir = tmpDir();
    const h = acquireSessionLock(dir);
    assert.equal(h.acquired, true);
    assert.equal(h.runtimeFence, 1);
    const payload = JSON.parse(fs.readFileSync(path.join(dir, SESSION_LOCK_FILE), 'utf-8'));
    assert.equal(payload.runtimeFence, 1);
    assert.equal(payload.pid, process.pid);
    assert.ok(typeof payload.spawnToken === 'string');
    assert.ok('processStartTimeMs' in payload);
    h.release();
  });

  it('reclaiming a provably-dead identity owner bumps the fence (prior + 1)', () => {
    const dir = tmpDir();
    // Prior owner: a dead pid, identity-bearing, fence 7.
    seedLock(dir, { pid: ABSENT_PID, processStartTimeMs: 1234, spawnToken: 'old', runtimeFence: 7 });
    const deps: Partial<SessionLockDeps> = { isPidAlive: (p) => p === process.pid };
    const h = acquireSessionLock(dir, deps);
    assert.equal(h.acquired, true);
    assert.equal(h.runtimeFence, 8); // 7 + 1
    h.release();
  });
});

describe('session ownership — fail-closed takeover (the behavior change)', () => {
  it('does NOT steal an identity-bearing lock whose live owner is unverifiable, even when mtime is ancient', () => {
    const dir = tmpDir();
    const lockPath = seedLock(dir, {
      pid: 55555,
      processStartTimeMs: null, // host couldn't record it
      spawnToken: null,
      runtimeFence: 3,
    });
    backdate(lockPath, 10 * 60 * 60 * 1000); // 10h — far past the legacy stale window
    // The seeded pid is "alive" but has no reuse-safe evidence ⇒ indeterminate.
    const deps: Partial<SessionLockDeps> = { isPidAlive: (p) => p === 55555 || p === process.pid };
    const h = acquireSessionLock(dir, deps);
    // Fail-closed: a stale-mtime indeterminate owner is NOT reclaimed.
    assert.equal(h.acquired, false);
    assert.equal(h.pid, 55555);
  });

  it('DOES steal an identity-bearing lock whose owner PID is absent (exited)', () => {
    const dir = tmpDir();
    seedLock(dir, { pid: ABSENT_PID, processStartTimeMs: null, spawnToken: null, runtimeFence: 2 });
    const deps: Partial<SessionLockDeps> = { isPidAlive: (p) => p === process.pid };
    const h = acquireSessionLock(dir, deps);
    assert.equal(h.acquired, true);
    assert.equal(h.runtimeFence, 3);
    h.release();
  });

  it('DOES steal when the live PID identity mismatches (PID reuse)', () => {
    const dir = tmpDir();
    seedLock(dir, { pid: 55555, processStartTimeMs: 1000, spawnToken: null, runtimeFence: 4 });
    const deps: Partial<SessionLockDeps> = {
      isPidAlive: (p) => p === 55555 || p === process.pid,
      // The live pid 55555 now reports a very different start time ⇒ reuse.
      readProcessStartTimeMs: (p) => (p === 55555 ? 999_999_999 : 0),
    };
    const h = acquireSessionLock(dir, deps);
    assert.equal(h.acquired, true);
    assert.equal(h.runtimeFence, 5);
    h.release();
  });
});

describe('session ownership — fence read/assert helpers', () => {
  it('readSessionFence / isSessionFenceCurrent reflect the on-disk fence', () => {
    const dir = tmpDir();
    seedLock(dir, { pid: process.pid, runtimeFence: 5 });
    assert.equal(readSessionFence(dir), 5);
    assert.equal(isSessionFenceCurrent(dir, 5), true);
    assert.equal(isSessionFenceCurrent(dir, 4), false);
  });

  it('readSessionFence is null when there is no lock', () => {
    assert.equal(readSessionFence(tmpDir()), null);
  });

  it('assertSessionFence throws SessionFenceError when superseded', () => {
    const dir = tmpDir();
    seedLock(dir, { pid: process.pid, runtimeFence: 9 });
    assert.doesNotThrow(() => assertSessionFence(dir, 9));
    try {
      assertSessionFence(dir, 8);
      assert.fail('expected SessionFenceError');
    } catch (err) {
      assert.ok(err instanceof SessionFenceError);
      assert.equal((err as SessionFenceError).expectedFence, 8);
      assert.equal((err as SessionFenceError).currentFence, 9);
    }
  });

  it('assertSessionFence is a no-op for a legacy lock without a fence', () => {
    const dir = tmpDir();
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    fs.writeFileSync(lockPath, JSON.stringify({ pid: process.pid, startedAt: Date.now(), execPath: '/x' }));
    assert.equal(readSessionFence(dir), null);
    assert.doesNotThrow(() => assertSessionFence(dir, 123));
    assert.equal(isSessionFenceCurrent(dir, 123), true);
  });
});

describe('session ownership — evaluateSessionAcquisition (fail closed)', () => {
  const payload = (fence: number): SessionLockPayload => ({
    pid: 10, startedAt: 0, execPath: '/x', runtimeFence: fence,
  });

  it('grants fence 1 on a null (unrecorded) owner', () => {
    assert.deepEqual(
      evaluateSessionAcquisition({ payload: null, expectedFence: 0, probe: { outcome: 'exited' } }),
      { decision: 'granted', nextFence: 1 },
    );
  });

  it('refuses on a fence mismatch (CAS stale)', () => {
    const d = evaluateSessionAcquisition({ payload: payload(5), expectedFence: 4, probe: { outcome: 'exited' } });
    assert.deepEqual(d, { decision: 'refused', reason: 'fenceStale' });
  });

  it('refuses a verified-live owner', () => {
    const d = evaluateSessionAcquisition({ payload: payload(5), expectedFence: 5, probe: { outcome: 'live', matchedOn: ['spawn-token'] } });
    assert.deepEqual(d, { decision: 'refused', reason: 'ownerAlive' });
  });

  it('refuses an unverifiable owner (indeterminate ⇒ ownerUnproven)', () => {
    const d = evaluateSessionAcquisition({ payload: payload(5), expectedFence: 5, probe: { outcome: 'indeterminate', reason: 'x' } });
    assert.deepEqual(d, { decision: 'refused', reason: 'ownerUnproven' });
  });

  it('grants (advancing the fence) only when the owner is provably gone', () => {
    const d = evaluateSessionAcquisition({ payload: payload(5), expectedFence: 5, probe: { outcome: 'identity-mismatch', field: 'process-start-time' } });
    assert.deepEqual(d, { decision: 'granted', nextFence: 6 });
  });
});

describe('session ownership — safeAppendTranscript fence assert', () => {
  it('appends when the held fence is still current', () => {
    const dir = tmpDir();
    seedLock(dir, { pid: process.pid, runtimeFence: 2 });
    const transcript = path.join(dir, 't.jsonl');
    safeAppendTranscript(transcript, { v: 1 }, { sessionDir: dir, expectedFence: 2 });
    assert.equal(fs.readFileSync(transcript, 'utf-8').trim(), JSON.stringify({ v: 1 }));
  });

  it('rejects (throws) and writes nothing when a newer owner took over', () => {
    const dir = tmpDir();
    seedLock(dir, { pid: process.pid, runtimeFence: 3 }); // new owner advanced to 3
    const transcript = path.join(dir, 't.jsonl');
    assert.throws(
      () => safeAppendTranscript(transcript, { v: 2 }, { sessionDir: dir, expectedFence: 2 }),
      SessionFenceError,
    );
    assert.equal(fs.existsSync(transcript), false); // superseded write never landed
  });

  it('appends normally with no ownership argument (back-compat)', () => {
    const dir = tmpDir();
    const transcript = path.join(dir, 't.jsonl');
    safeAppendTranscript(transcript, { v: 3 });
    assert.ok(fs.readFileSync(transcript, 'utf-8').includes('"v":3'));
  });
});
