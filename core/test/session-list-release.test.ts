import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  SESSION_LOCK_FILE,
  acquireSessionLock,
} from '../src/session';

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-session-lr-'));
}

// -----------------------------------------------------------------------------
// list() — two-pass stat-then-parse with limit/offset pagination (gap: Claude's
// listSessionsImpl stat-first + READ_BATCH pagination; agentSdkTypes listSessions
// limit/offset). Window selection is by mtime; final entries sorted by date desc.
// -----------------------------------------------------------------------------

describe('SessionManager.list pagination (two-pass stat-then-parse)', () => {
  let tmpDir: string;
  let sessionsDir: string;
  let sm: SessionManager;

  beforeEach(() => {
    tmpDir = makeTempDir();
    sessionsDir = path.join(tmpDir, 'sessions');
    fs.mkdirSync(sessionsDir, { recursive: true });
    sm = new SessionManager(tmpDir, sessionsDir);
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  /** Write a session JSON directly so both on-disk `updatedAt` (drives the final
   *  date sort) and file mtime (drives the candidate window) are controlled. */
  function writeSession(id: string, isoDate: string, mtimeMs: number): void {
    const file = path.join(sessionsDir, `${id}.json`);
    fs.writeFileSync(
      file,
      JSON.stringify({
        id,
        messages: [{ role: 'user', content: `prompt ${id}` }],
        createdAt: isoDate,
        updatedAt: isoDate,
        provider: { model: 'm', baseUrl: 'u' },
      }),
    );
    const t = new Date(mtimeMs);
    fs.utimesSync(file, t, t);
  }

  it('returns all entries sorted by date descending when no limit is given', () => {
    writeSession('a', '2021-01-01T00:00:00.000Z', Date.now() - 3000);
    writeSession('b', '2022-01-01T00:00:00.000Z', Date.now() - 2000);
    writeSession('c', '2023-01-01T00:00:00.000Z', Date.now() - 1000);

    const ids = sm.list().map(e => e.id);
    assert.deepEqual(ids, ['c', 'b', 'a']);
  });

  it('limit returns only the N freshest-by-mtime sessions', () => {
    writeSession('a', '2021-01-01T00:00:00.000Z', Date.now() - 3000);
    writeSession('b', '2022-01-01T00:00:00.000Z', Date.now() - 2000);
    writeSession('c', '2023-01-01T00:00:00.000Z', Date.now() - 1000);

    const ids = sm.list({ limit: 2 }).map(e => e.id);
    assert.deepEqual(ids, ['c', 'b']);
  });

  it('offset skips the freshest sessions', () => {
    writeSession('a', '2021-01-01T00:00:00.000Z', Date.now() - 3000);
    writeSession('b', '2022-01-01T00:00:00.000Z', Date.now() - 2000);
    writeSession('c', '2023-01-01T00:00:00.000Z', Date.now() - 1000);

    assert.deepEqual(sm.list({ offset: 1 }).map(e => e.id), ['b', 'a']);
    assert.deepEqual(sm.list({ limit: 1, offset: 1 }).map(e => e.id), ['b']);
    assert.deepEqual(sm.list({ offset: 2 }).map(e => e.id), ['a']);
  });

  it('selects the window by mtime, not by recorded date (two-pass discriminator)', () => {
    // 'new' has the NEWEST mtime but the OLDEST recorded date. A naive
    // parse-all-then-limit by date would drop it from a limit:1 window; the
    // stat-first pass keeps it because its mtime is freshest.
    writeSession('old-but-touched', '2000-01-01T00:00:00.000Z', Date.now());
    writeSession('recent-date', '2030-01-01T00:00:00.000Z', Date.now() - 5000);

    const ids = sm.list({ limit: 1 }).map(e => e.id);
    assert.deepEqual(ids, ['old-but-touched']);
  });

  it('limit <= 0 and offset past the end return an empty list without throwing', () => {
    writeSession('a', '2021-01-01T00:00:00.000Z', Date.now() - 1000);
    assert.deepEqual(sm.list({ limit: 0 }), []);
    assert.deepEqual(sm.list({ offset: 99 }), []);
  });

  it('reports fileSize from the single stat pass', () => {
    writeSession('a', '2021-01-01T00:00:00.000Z', Date.now() - 1000);
    const [entry] = sm.list({ limit: 1 });
    assert.equal(typeof entry.fileSize, 'number');
    assert.ok((entry.fileSize ?? 0) > 0);
  });
});

// -----------------------------------------------------------------------------
// acquireSessionLock release() — ownership-verified, idempotent release (gap:
// Claude's computerUseLock releaseComputerUseLock only unlinks when the lock is
// still ours). A lock reclaimed by another live process must not be deleted by
// the original holder's release().
// -----------------------------------------------------------------------------

describe('acquireSessionLock release is ownership-verified', () => {
  let dir: string;

  beforeEach(() => {
    dir = makeTempDir();
  });

  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('removes the lock when we still own it', () => {
    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);
    lock.release();
    assert.equal(fs.existsSync(path.join(dir, SESSION_LOCK_FILE)), false);
  });

  it('does NOT delete a lock that another process has since reclaimed', () => {
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    const lock = acquireSessionLock(dir);
    assert.equal(lock.acquired, true);

    // Simulate a foreign process reclaiming the (apparently stale) lock: the file
    // now carries a different pid than ours.
    const foreign = JSON.stringify({ pid: 999999, startedAt: Date.now(), execPath: '/other/ami' });
    fs.writeFileSync(lockPath, foreign);

    lock.release();

    // The foreign lock must survive our release — deleting it would be a
    // boundary violation (we no longer own it).
    assert.equal(fs.existsSync(lockPath), true);
    const after = fs.readFileSync(lockPath, 'utf-8');
    assert.equal(JSON.parse(after).pid, 999999);
  });

  it('release remains idempotent after a foreign reclaim (no throw)', () => {
    const lockPath = path.join(dir, SESSION_LOCK_FILE);
    const lock = acquireSessionLock(dir);
    fs.writeFileSync(lockPath, JSON.stringify({ pid: 999999, startedAt: Date.now(), execPath: '' }));
    assert.doesNotThrow(() => { lock.release(); lock.release(); });
  });
});
