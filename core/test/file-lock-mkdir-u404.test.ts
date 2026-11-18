import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { withFileLock, withFileLockSync } from '../src/utils/file-lock';

// ---------------------------------------------------------------------------
// U404 — NFS-safe atomic acquisition. The lock must be acquired via an atomic
// `mkdir` of `${target}.lock` (a DIRECTORY), which is atomic even over NFS /
// network mounts, instead of an `open(O_EXCL)` file create whose exclusivity is
// not reliably atomic over NFS (two sessions sharing a networked state dir could
// both believe they hold an O_EXCL lock). This mirrors proper-lockfile and AMI's
// own oauth `withRefreshLock`, consolidating on ONE canonical mkdir-based lock.
// ---------------------------------------------------------------------------

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'file-lock-mkdir-'));
}

const PID_ENTRY = 'pid';

test('withFileLock acquires via an atomic mkdir (lock is a DIRECTORY, not a file)', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lock = `${target}.lock`;
  let wasDirDuringSection = false;
  let pidEntryWritten = false;
  await withFileLock(target, () => {
    const st = fs.statSync(lock);
    wasDirDuringSection = st.isDirectory();
    pidEntryWritten =
      fs.existsSync(path.join(lock, PID_ENTRY)) &&
      fs.readFileSync(path.join(lock, PID_ENTRY), 'utf-8').trim() === String(process.pid);
  });
  assert.equal(wasDirDuringSection, true, 'lock must be a directory created via mkdir');
  assert.equal(pidEntryWritten, true, 'holder PID recorded inside the lock directory');
  assert.equal(fs.existsSync(lock), false, 'lock directory removed on release');
});

test('withFileLockSync acquires via an atomic mkdir (lock is a DIRECTORY)', () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lock = `${target}.lock`;
  const isDir = withFileLockSync(target, () => fs.statSync(lock).isDirectory());
  assert.equal(isDir, true, 'sync lock must be a directory created via mkdir');
  assert.equal(fs.existsSync(lock), false, 'lock directory removed on release');
});

test('withFileLock steals a stale mkdir-lock DIRECTORY left by a crashed holder', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lock = `${target}.lock`;
  // A crashed holder leaves the directory behind with an old mtime and no / dead PID.
  fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, PID_ENTRY), '99999');
  const old = Date.now() / 1000 - 60;
  fs.utimesSync(lock, old, old);
  const out = await withFileLock(target, () => 'ok', { staleMs: 1000, retries: 2, minTimeout: 1, maxTimeout: 2 });
  assert.equal(out, 'ok');
  assert.equal(fs.existsSync(lock), false, 'stale lock dir stolen then released');
});

test('withFileLock tolerates a half-created lock dir (mkdir done, PID not yet written)', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lock = `${target}.lock`;
  // Directory exists but holds no pid entry (a holder that mkdir'd but has not yet
  // written its PID). readLockPid returns null, so the mtime fallback governs the
  // steal — here the dir is old, so it is reclaimed.
  fs.mkdirSync(lock);
  const old = Date.now() / 1000 - 60;
  fs.utimesSync(lock, old, old);
  const out = await withFileLock(target, () => 'recovered', { staleMs: 1000, retries: 2, minTimeout: 1, maxTimeout: 2 });
  assert.equal(out, 'recovered');
});

test('concurrent mkdir contention serializes writers with no lost update', async () => {
  const dir = tmp();
  const target = path.join(dir, 'counter.json');
  fs.writeFileSync(target, JSON.stringify({ n: 0 }));
  await Promise.all(
    Array.from({ length: 16 }, () =>
      withFileLock(target, () => {
        const cur = JSON.parse(fs.readFileSync(target, 'utf-8')) as { n: number };
        cur.n += 1;
        fs.writeFileSync(target, JSON.stringify(cur));
      }),
    ),
  );
  assert.equal((JSON.parse(fs.readFileSync(target, 'utf-8')) as { n: number }).n, 16);
});
