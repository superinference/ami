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
  assert.equal(DEFAULT_LOCK_OPTIONS.staleMs, 30_000);
});

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Find a PID that is provably not alive (process.kill(pid, 0) raises ESRCH). */
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

test('withFileLock steals a lock whose holder PID is dead even if its mtime is fresh', async () => {
  // Discriminates PID-liveness from mtime-only staleness: the lock mtime is
  // brand new (well within staleMs), so a mtime-only steal would NOT fire, but
  // the recorded holder is dead, so the lock must be reclaimed immediately.
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  fs.writeFileSync(lockFile, String(deadPid())); // fresh mtime (now), dead holder
  const out = await withFileLock(target, () => 'stolen', {
    staleMs: 60_000, // large: mtime path alone would never steal
    retries: 2,
    minTimeout: 1,
    maxTimeout: 2,
  });
  assert.equal(out, 'stolen');
  assert.equal(fs.existsSync(lockFile), false, 'lock released after fn');
});

test('withFileLock does NOT steal a fresh lock held by a live PID', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  fs.writeFileSync(lockFile, String(process.pid)); // our own live PID, fresh mtime
  await assert.rejects(
    withFileLock(target, () => 'never', { retries: 2, minTimeout: 1, maxTimeout: 2, staleMs: 60_000 }),
    /Could not acquire lock/,
  );
  fs.unlinkSync(lockFile);
});

test('withFileLock steals via mtime fallback when the holder PID is unreadable', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  // Corrupt/non-numeric and empty PID content exercise readLockPid's null paths;
  // the old mtime then drives the steal.
  for (const content of ['not-a-pid', '', '0']) {
    fs.writeFileSync(lockFile, content);
    const old = Date.now() / 1000 - 60;
    fs.utimesSync(lockFile, old, old);
    const out = await withFileLock(target, () => 'ok', { staleMs: 1000, retries: 2, minTimeout: 1, maxTimeout: 2 });
    assert.equal(out, 'ok');
  }
});

test('heartbeat keeps a long critical section from being falsely stolen', async () => {
  // Two concurrent sections on the same target, with a fn that runs far longer
  // than staleMs. Without the mtime-refresh heartbeat the waiter would judge the
  // holder stale and steal the lock mid-run (overlap). With it, the holder stays
  // fresh and the sections remain mutually exclusive.
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  let active = 0;
  let overlap = false;
  const section = () =>
    withFileLock(
      target,
      async () => {
        active += 1;
        if (active > 1) overlap = true;
        await sleep(300); // 3× staleMs
        active -= 1;
      },
      { staleMs: 100, retries: 80, minTimeout: 5, maxTimeout: 20 },
    );
  await Promise.all([section(), section()]);
  assert.equal(overlap, false, 'critical sections must not overlap');
});

test('onCompromised fires when the lock is deleted underneath the holder', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  let fired = false;
  await withFileLock(
    target,
    async () => {
      fs.unlinkSync(`${target}.lock`); // external removal
      await sleep(140); // span several refresh ticks (exercises the fired-once guard)
    },
    { staleMs: 100, refreshMs: 20, onCompromised: () => { fired = true; } },
  );
  assert.equal(fired, true);
});

test('onCompromised fires when another process takes over the lock', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const lockFile = `${target}.lock`;
  const errors: Error[] = [];
  await withFileLock(
    target,
    async () => {
      fs.writeFileSync(lockFile, String(process.pid + 1)); // a different PID now holds it
      await sleep(80);
    },
    { staleMs: 100, refreshMs: 20, onCompromised: (e) => { errors.push(e); } },
  );
  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /taken over/);
});

test('heartbeat tolerates a throwing onCompromised handler and a missing callback', async () => {
  const dir = tmp();
  // Throwing handler must not crash the timer / reject the lock.
  const t1 = path.join(dir, 'a.json');
  await assert.doesNotReject(
    withFileLock(
      t1,
      async () => { fs.unlinkSync(`${t1}.lock`); await sleep(60); },
      { staleMs: 100, refreshMs: 20, onCompromised: () => { throw new Error('handler boom'); } },
    ),
  );
  // Compromise with no handler at all must be a no-op (optional-chaining path).
  const t2 = path.join(dir, 'b.json');
  await assert.doesNotReject(
    withFileLock(t2, async () => { fs.unlinkSync(`${t2}.lock`); await sleep(60); }, { staleMs: 100, refreshMs: 20 }),
  );
});

test('refreshMs:0 disables the heartbeat', async () => {
  const dir = tmp();
  const target = path.join(dir, 'state.json');
  const out = await withFileLock(target, () => 7, { refreshMs: 0 });
  assert.equal(out, 7);
  assert.equal(fs.existsSync(`${target}.lock`), false);
});
