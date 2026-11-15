import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { SessionManager } from '../src/session';
import type { Session } from '../src/session';

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-session-robust-'));
}

function writeSessionFile(dir: string, id: string, date: string): void {
  const session: Session = {
    id: id as Session['id'],
    messages: [{ role: 'user', content: `hello from ${id}` }],
    createdAt: date,
    updatedAt: date,
    provider: { model: 'test-model', baseUrl: 'http://localhost' },
  };
  fs.writeFileSync(path.join(dir, `${id}.json`), JSON.stringify(session));
}

describe('SessionManager.listFiles — readdir degradation', () => {
  let base: string;

  beforeEach(() => {
    base = makeTempDir();
  });

  afterEach(() => {
    fs.rmSync(base, { recursive: true, force: true });
  });

  // RED on the pre-fix code: listFiles() did `if (!existsSync) return []` then an
  // unguarded readdirSync. When the session-dir path EXISTS but is not a directory
  // (ENOTDIR) — or is a present-but-unreadable dir (EACCES), or vanished after the
  // existsSync check (ENOENT race) — readdirSync threw and list()/loadLatest() blew
  // up instead of reporting "no sessions". The ENOTDIR case is deterministic and
  // root-independent, so it discriminates the fix from the old behavior.
  it('list() degrades to [] when the session dir path is not a directory (ENOTDIR)', () => {
    const notADir = path.join(base, 'session-store');
    fs.writeFileSync(notADir, 'i am a file, not a directory');
    const sm = new SessionManager(base, notADir);
    assert.ok(fs.existsSync(notADir), 'precondition: path exists (so the old existsSync guard would pass)');
    assert.deepEqual(sm.list(), []);
  });

  it('loadLatest() degrades to null when the session dir path is not a directory', () => {
    const notADir = path.join(base, 'session-store');
    fs.writeFileSync(notADir, 'i am a file, not a directory');
    const sm = new SessionManager(base, notADir);
    assert.equal(sm.loadLatest(), null);
  });

  it('list() still returns [] for a genuinely absent session dir (ENOENT)', () => {
    const absent = path.join(base, 'does', 'not', 'exist');
    const sm = new SessionManager(base, absent);
    assert.deepEqual(sm.list(), []);
    assert.equal(sm.loadLatest(), null);
  });
});

describe('SessionManager.list — deterministic tie-break', () => {
  let base: string;
  let dir: string;

  beforeEach(() => {
    base = makeTempDir();
    dir = path.join(base, 'sessions');
    fs.mkdirSync(dir, { recursive: true });
  });

  afterEach(() => {
    fs.rmSync(base, { recursive: true, force: true });
  });

  // RED on the pre-fix code: the final list() comparator returned only the date
  // delta, so same-timestamp sessions kept their input order (the mtime-desc
  // candidate order) — a result that depends on filesystem mtime, not a stable
  // key. Here all three sessions share one updatedAt, and their mtimes are set so
  // mtime-desc order is a,b,c. The old code therefore returns [a,b,c]; the fix
  // breaks the tie on the session id (descending), returning [c,b,a].
  it('orders same-timestamp sessions deterministically by id, independent of mtime', () => {
    const sameDate = '2024-01-01T00:00:00.000Z';
    for (const id of ['session-a', 'session-b', 'session-c']) {
      writeSessionFile(dir, id, sameDate);
    }
    const atime = new Date();
    // mtime-desc order a > b > c — the opposite of the id-desc order the fix yields.
    fs.utimesSync(path.join(dir, 'session-a.json'), atime, new Date(3000));
    fs.utimesSync(path.join(dir, 'session-b.json'), atime, new Date(2000));
    fs.utimesSync(path.join(dir, 'session-c.json'), atime, new Date(1000));

    const sm = new SessionManager(base, dir);
    const ids = sm.list().map(e => e.id);
    assert.deepEqual(ids, ['session-c', 'session-b', 'session-a']);
  });

  it('keeps date as the primary key; tie-break only applies within equal dates', () => {
    writeSessionFile(dir, 'session-a', '2024-01-02T00:00:00.000Z'); // newest date
    writeSessionFile(dir, 'session-z', '2024-01-01T00:00:00.000Z'); // older date
    const sm = new SessionManager(base, dir);
    const ids = sm.list().map(e => e.id);
    // Newer date wins regardless of id ordering.
    assert.deepEqual(ids, ['session-a', 'session-z']);
  });
});
