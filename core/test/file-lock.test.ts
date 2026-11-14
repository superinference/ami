import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { withFileLock, DEFAULT_LOCK_OPTIONS } from '../src/utils/file-lock';

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'file-lock-'));
}

test('withFileLock runs fn and returns its result', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const out = await withFileLock(target, () => 42);
  assert.equal(out, 42);
  assert.equal(fs.existsSync(`${target}.lock`), false, 'lock released after fn');
});

test('withFileLock releases the lock even when fn throws', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  await assert.rejects(withFileLock(target, () => { throw new Error('boom'); }), /boom/);
  assert.equal(fs.existsSync(`${target}.lock`), false, 'lock released after throw');
});

test('withFileLock serializes concurrent critical sections (no lost update)', async () => {
  const dir = tmp();
  const target = path.join(dir, 'counter.json');
  fs.writeFileSync(target, JSON.stringify({ n: 0 }));
  // 20 concurrent read-modify-write cycles. Without locking these race and lose
  // updates; with locking the final value must be exactly 20.
  await Promise.all(
    Array.from({ length: 20 }, () =>
      withFileLock(target, () => {
        const cur = JSON.parse(fs.readFileSync(target, 'utf-8')) as { n: number };
        cur.n += 1;
        // Yield to maximize interleaving pressure.
        fs.writeFileSync(target, JSON.stringify(cur));
      }),
    ),
  );
  const final = JSON.parse(fs.readFileSync(target, 'utf-8')) as { n: number };
  assert.equal(final.n, 20);
});

test('withFileLock steals a stale lock left by a crashed holder', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  // Simulate a crashed holder: a lock file with an old mtime.
  fs.writeFileSync(lockFile, '99999');
  const old = Date.now() / 1000 - 60; // 60s ago
  fs.utimesSync(lockFile, old, old);
  const out = await withFileLock(target, () => 'ok', { staleMs: 1000, retries: 2 });
  assert.equal(out, 'ok');
});

test('withFileLock gives up after retries when lock stays held and fresh', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  fs.writeFileSync(lockFile, '1'); // fresh (now), never released
  await assert.rejects(
    withFileLock(target, () => 'never', { retries: 2, minTimeout: 1, maxTimeout: 2, staleMs: 10_000 }),
    /Could not acquire lock/,
  );
  fs.unlinkSync(lockFile);
});

test('DEFAULT_LOCK_OPTIONS matches the documented swarm budget', () => {
  assert.equal(DEFAULT_LOCK_OPTIONS.retries, 30);
  assert.equal(DEFAULT_LOCK_OPTIONS.minTimeout, 5);
  assert.equal(DEFAULT_LOCK_OPTIONS.maxTimeout, 100);
});
