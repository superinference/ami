import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { withFileLockSync } from '../src/utils/file-lock';

// ---------------------------------------------------------------------------
// withFileLockSync — the synchronous sibling of withFileLock, used by the config
// cross-process advisory-lock path. Shares the SAME lockfile format + stale-steal.
// ---------------------------------------------------------------------------

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'file-lock-sync-'));
}

/** Seed a held lock in its on-disk format: an mkdir'd DIRECTORY `${target}.lock`
 *  whose `pid` entry records the holder PID. */
function seedLock(target: string, pid: number | string): string {
  const lockDir = `${target}.lock`;
  fs.mkdirSync(lockDir, { recursive: true });
  fs.writeFileSync(path.join(lockDir, 'pid'), String(pid));
  return lockDir;
}

/** Read the holder PID recorded inside a lock directory. */
function lockPid(target: string): string {
  return fs.readFileSync(path.join(`${target}.lock`, 'pid'), 'utf-8');
}

test('withFileLockSync runs fn, returns its result, and releases the lock', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  let lockSeenDuringSection = false;
  const out = withFileLockSync(target, () => {
    lockSeenDuringSection = fs.existsSync(`${target}.lock`);
    return 42;
  });
  assert.equal(out, 42);
  assert.equal(lockSeenDuringSection, true, 'lock file exists for the duration of the section');
  assert.equal(fs.existsSync(`${target}.lock`), false, 'lock released after fn');
});

test('withFileLockSync releases the lock even when fn throws', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  assert.throws(() => withFileLockSync(target, () => { throw new Error('boom'); }), /boom/);
  assert.equal(fs.existsSync(`${target}.lock`), false, 'lock released after throw');
});

test('withFileLockSync steals a lock whose holder PID is dead, then runs', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  // A lock file recording a provably-dead PID (PID 1 is init on Linux and alive,
  // so synthesize an impossible one). Use a very large PID unlikely to exist.
  const deadPid = 2 ** 31 - 1;
  seedLock(target, deadPid);
  const out = withFileLockSync(target, () => 'ran', { retries: 0 });
  assert.equal(out, 'ran', 'dead holder reclaimed immediately so fn ran with retries:0');
  assert.equal(fs.existsSync(`${target}.lock`), false, 'lock released');
});

test('withFileLockSync fires onCompromised and leaves the thief lock when stolen mid-section', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const errs: string[] = [];
  const out = withFileLockSync(target, () => {
    // Simulate a waiter judging us stale and stealing the lock: overwrite the lock
    // dir's pid entry so it records a DIFFERENT pid than ours.
    fs.writeFileSync(path.join(`${target}.lock`, 'pid'), String(process.pid + 1));
    return 'done';
  }, { onCompromised: (e) => errs.push(e.message) });
  assert.equal(out, 'done');
  assert.equal(errs.length, 1, 'onCompromised fired exactly once');
  assert.match(errs[0]!, /lost or taken over/);
  // We must NOT delete the thief's lock.
  assert.equal(fs.existsSync(`${target}.lock`), true, 'successor lock left intact');
  assert.equal(lockPid(target), String(process.pid + 1));
});

test('withFileLockSync throws after exhausting retries on a live-held lock', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  // A lock held by THIS live process (our own PID) — never stale, never stealable.
  seedLock(target, process.pid);
  assert.throws(
    () => withFileLockSync(target, () => 'never', { retries: 1, minTimeout: 1, maxTimeout: 1, staleMs: 10 ** 9 }),
    /Could not acquire lock/,
  );
  // The pre-existing lock must survive (we never owned it, so never delete it).
  assert.equal(lockPid(target), String(process.pid));
});
