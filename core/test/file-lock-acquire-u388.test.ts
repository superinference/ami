/**
 * U388 — acquire-and-HOLD file-lock handle API + canonical PID-liveness probe.
 *
 * Covers the proper-lockfile-style `lock()/lockSync()`/release handle that the
 * canonical lock primitive (utils/file-lock) gained: acquire at one point, release
 * much later (not a scoped `withFileLock(fn)` wrapper). Also pins the ONE canonical
 * `isPidAlive` probe that session.ts and cron.ts now delegate to (dedup rule).
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  acquireFileLock,
  acquireFileLockSync,
  isPidAlive,
} from '../src/utils/file-lock';
import { isPidAlive as sessionIsPidAlive } from '../src/session';
import { isProcessRunning } from '../src/cron';

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'file-lock-acq-'));
}
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

function deadPid(): number {
  for (let p = 2; p < 200_000; p++) {
    try {
      process.kill(p, 0);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ESRCH') return p;
    }
  }
  throw new Error('could not find a dead pid');
}

test('acquireFileLock creates the lock and release() removes it', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const handle = await acquireFileLock(target);
  assert.equal(fs.existsSync(`${target}.lock`), true, 'lock held after acquire');
  assert.equal(handle.compromised, false);
  await handle.release();
  assert.equal(fs.existsSync(`${target}.lock`), false, 'lock gone after release');
});

test('acquireFileLock release() is idempotent', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const handle = await acquireFileLock(target);
  await handle.release();
  await assert.doesNotReject(handle.release()); // second release is a no-op
  assert.equal(fs.existsSync(`${target}.lock`), false);
});

test('acquireFileLock blocks a second holder until the first releases', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const first = await acquireFileLock(target, { retries: 0 });
  // Second acquire with no retries must fail while the first is held.
  await assert.rejects(
    acquireFileLock(target, { retries: 0 }),
    /Could not acquire lock/,
  );
  // After release, a fresh acquire succeeds.
  await first.release();
  const second = await acquireFileLock(target, { retries: 0 });
  assert.equal(fs.existsSync(`${target}.lock`), true);
  await second.release();
});

test('acquireFileLock heartbeat keeps a long hold from being stolen', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  const handle = await acquireFileLock(target, { staleMs: 60, refreshMs: 15 });
  const before = fs.statSync(lockFile).mtimeMs;
  await sleep(120); // 2× staleMs — without a heartbeat the lock would look stale
  const after = fs.statSync(lockFile).mtimeMs;
  assert.ok(after >= before, 'heartbeat refreshed the lock mtime');
  // A waiter must still be blocked (holder proved liveness via the refresh).
  await assert.rejects(acquireFileLock(target, { retries: 1, minTimeout: 1, maxTimeout: 2, staleMs: 60 }), /Could not acquire lock/);
  await handle.release();
});

test('acquireFileLock surfaces onCompromised + handle.compromised when taken over', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  let fired: Error | null = null;
  const handle = await acquireFileLock(target, {
    staleMs: 100,
    refreshMs: 15,
    onCompromised: (e) => { fired = e; },
  });
  fs.writeFileSync(lockFile, String(process.pid + 1)); // another pid takes it over
  await sleep(60); // let the heartbeat notice
  assert.ok(fired, 'onCompromised fired');
  assert.match((fired as unknown as Error).message, /taken over/);
  assert.equal(handle.compromised, true);
  await handle.release();
});

test('acquireFileLock steals a lock whose recorded PID is dead', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  fs.writeFileSync(lockFile, String(deadPid())); // fresh mtime, dead holder
  const handle = await acquireFileLock(target, { staleMs: 60_000, retries: 1 });
  assert.equal(fs.existsSync(lockFile), true);
  await handle.release();
  assert.equal(fs.existsSync(lockFile), false);
});

test('acquireFileLockSync acquire → release round-trip', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const handle = acquireFileLockSync(target);
  assert.equal(fs.existsSync(`${target}.lock`), true);
  assert.equal(handle.compromised, false);
  handle.release();
  assert.equal(fs.existsSync(`${target}.lock`), false);
  handle.release(); // idempotent
});

test('acquireFileLockSync release detects a stolen lock via onCompromised', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  let fired: Error | null = null;
  const handle = acquireFileLockSync(target, { onCompromised: (e) => { fired = e; } });
  fs.writeFileSync(lockFile, String(process.pid + 1)); // stolen while held
  handle.release();
  assert.ok(fired, 'onCompromised fired on release');
  assert.equal(handle.compromised, true);
  // The thief's lock is NOT deleted (it belongs to them now).
  assert.equal(fs.existsSync(lockFile), true);
  fs.unlinkSync(lockFile);
});

test('isPidAlive: canonical probe — sentinel/invalid PIDs are dead, self is alive', () => {
  assert.equal(isPidAlive(0), false);   // own process group
  assert.equal(isPidAlive(1), false);   // init/systemd — never a holder
  assert.equal(isPidAlive(-1), false);  // negative
  assert.equal(isPidAlive(1.5), false); // non-integer
  assert.equal(isPidAlive(2_147_483_646), false); // (almost certainly) dead
  assert.equal(isPidAlive(process.pid), true);    // ourselves
});

test('session.isPidAlive and cron.isProcessRunning delegate to the one probe', () => {
  // Identical results across the three former copies — proves consolidation.
  for (const pid of [0, 1, -1, 2_147_483_646, process.pid]) {
    assert.equal(sessionIsPidAlive(pid), isPidAlive(pid), `session probe parity for ${pid}`);
    assert.equal(isProcessRunning(pid), isPidAlive(pid), `cron probe parity for ${pid}`);
  }
});
