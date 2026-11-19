import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { withFileLock, withFileLockSync, readLockFence } from '../src/utils/file-lock';

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'flock-fc-'));
}

const ABSENT_PID = 2 ** 30;

/** Seed a held lock dir in the on-disk format, optionally with identity + fence. */
function seedLock(
  target: string,
  opts: { pid: number; ageSec?: number; identity?: Record<string, unknown>; fence?: number },
): string {
  const lockDir = `${target}.lock`;
  fs.mkdirSync(lockDir, { recursive: true });
  fs.writeFileSync(path.join(lockDir, 'pid'), String(opts.pid));
  if (opts.identity) fs.writeFileSync(path.join(lockDir, 'identity'), JSON.stringify(opts.identity));
  if (opts.fence !== undefined) fs.writeFileSync(path.join(lockDir, 'fence'), String(opts.fence));
  if (opts.ageSec !== undefined) {
    const t = Date.now() / 1000 - opts.ageSec;
    fs.utimesSync(lockDir, t, t);
  }
  return lockDir;
}

describe('file-lock — fail-closed CAS', () => {
  it('records identity + fence (1) on a fresh failClosed acquire', async () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    let feInside: number | null = null;
    await withFileLock(target, () => { feInside = readLockFence(target); }, { failClosed: true });
    assert.equal(feInside, 1);
    // Entries cleaned up on release.
    assert.equal(fs.existsSync(`${target}.lock`), false);
  });

  it('fail-closed does NOT steal a stale-mtime lock held by a LIVE pid (no identity)', () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    // Live holder (our own pid), very old mtime, no identity recorded.
    seedLock(target, { pid: process.pid, ageSec: 24 * 3600 });
    assert.throws(
      () => withFileLockSync(target, () => 'x', { failClosed: true, retries: 0, staleMs: 1 }),
      /Could not acquire lock/,
    );
  });

  it('LEGACY mode DOES steal that same stale-mtime lock (discriminator)', () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    seedLock(target, { pid: process.pid, ageSec: 24 * 3600 });
    const out = withFileLockSync(target, () => 'ok', { retries: 0, staleMs: 1 });
    assert.equal(out, 'ok');
  });

  it('fail-closed DOES steal a lock whose PID is absent (exited) and bumps the fence', async () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    seedLock(target, { pid: ABSENT_PID, identity: { pid: ABSENT_PID, processStartTimeMs: null, spawnToken: null }, fence: 4 });
    let feInside: number | null = null;
    const out = await withFileLock(target, () => { feInside = readLockFence(target); return 'done'; }, { failClosed: true, retries: 0 });
    assert.equal(out, 'done');
    assert.equal(feInside, 5); // 4 + 1
  });

  it('fail-closed steals an absent-PID lock that has NO identity recorded', () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    seedLock(target, { pid: ABSENT_PID });
    const out = withFileLockSync(target, () => 'ok', { failClosed: true, retries: 0 });
    assert.equal(out, 'ok');
  });

  it('fail-closed will NOT steal a lock recording OUR own live identity', () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    seedLock(target, {
      pid: process.pid,
      identity: { pid: process.pid, processStartTimeMs: 123, spawnToken: 'x' },
      ageSec: 24 * 3600,
    });
    assert.throws(
      () => withFileLockSync(target, () => 'x', { failClosed: true, retries: 0, staleMs: 1 }),
      /Could not acquire lock/,
    );
  });

  it('readLockFence is null when no lock exists', () => {
    const dir = tmp();
    assert.equal(readLockFence(path.join(dir, 'nope')), null);
  });

  it('treats a corrupt identity file (no pid) as no identity ⇒ falls back to PID presence', () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    const lockDir = `${target}.lock`;
    fs.mkdirSync(lockDir, { recursive: true });
    fs.writeFileSync(path.join(lockDir, 'pid'), String(ABSENT_PID));
    fs.writeFileSync(path.join(lockDir, 'identity'), '{"not":"identity"}'); // valid JSON, no pid
    // No usable identity + absent PID ⇒ reclaimable under failClosed.
    const out = withFileLockSync(target, () => 'ok', { failClosed: true, retries: 0 });
    assert.equal(out, 'ok');
  });

  it('readLockFence returns null for a non-numeric fence file', () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    const lockDir = `${target}.lock`;
    fs.mkdirSync(lockDir, { recursive: true });
    fs.writeFileSync(path.join(lockDir, 'fence'), 'not-a-number');
    assert.equal(readLockFence(target), null);
  });

  it('legacy reclaim also advances the fence', () => {
    const dir = tmp();
    const target = path.join(dir, 'res');
    seedLock(target, { pid: ABSENT_PID, fence: 10, ageSec: 1 });
    let feInside: number | null = null;
    withFileLockSync(target, () => { feInside = readLockFence(target); }, { retries: 0 });
    assert.equal(feInside, 11);
  });
});
