/**
 * U021 — cross-session blob-cache garbage collection (orphaned-blob GC).
 *
 * Parity target: the reference's cross-session cache GC (`cleanupOldImageCaches`,
 * imageStore.ts L126-167) — read the per-session cache base, delete the session dirs
 * that no longer belong to a live session, then rmdir the base when it ends up empty.
 *
 * AMI has no disk-backed pasted-image cache, but its content-addressed BLOB store
 * (`<sessionDir>/blobs/<sessionId>/`) IS a disk-backed per-session cache that, before
 * this, was only cleaned when a session was deleted through `delete()` — a blob dir
 * left behind by an out-of-band JSON removal, a crash before the JSON landed, or a
 * sync that dropped the JSON grew the store forever. This suite covers the new sweep:
 *   - `collectOrphanedCacheDirs` — the pure "which dirs have no live owner?" boundary;
 *   - `SessionManager.cleanupOrphanedBlobs` — the wired, real-fs sweep (orphan removed,
 *      LIVE session's blobs kept, base rmdir-when-empty, stray non-dir left alone,
 *      absent base = no-op, count correct).
 *
 * The AMI divergence from the reference is deliberate and asserted: a NON-current but
 * still-live session's blobs are KEPT (they back its `--resume`), unlike Claude's
 * keep-only-current image sweep.
 *
 * RED/GREEN: `collectOrphanedCacheDirs` and `SessionManager.cleanupOrphanedBlobs` do
 * not exist before this unit — the import + method call fail to compile/run on revert.
 */

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  collectOrphanedCacheDirs,
  BLOBS_SUBDIR,
  type Session,
} from '../src/session';

const tempDirs: string[] = [];
function makeTempDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-blobgc-'));
  tempDirs.push(d);
  return d;
}

afterEach(() => {
  while (tempDirs.length) {
    const d = tempDirs.pop()!;
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

/** Save a session carrying one over-long (>5000 char) user message so save()
 *  offloads it to a blob sidecar, creating `<sessionDir>/blobs/<id>/`. */
function saveWithBlob(mgr: SessionManager, id: string): Session {
  const session: Session = {
    id: id as Session['id'],
    messages: [{ role: 'user', content: 'x'.repeat(6000) }],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'test', baseUrl: 'http://localhost' },
  };
  mgr.save(session);
  return session;
}

describe('collectOrphanedCacheDirs', () => {
  it('returns dir names with no live owner (and keeps every live one)', () => {
    const dirs = ['a', 'b', 'c', 'd'];
    const orphans = collectOrphanedCacheDirs(dirs, ['a', 'c']);
    assert.deepEqual(orphans.sort(), ['b', 'd']);
  });

  it('accepts a Set as well as any iterable for liveIds', () => {
    assert.deepEqual(collectOrphanedCacheDirs(['a', 'b'], new Set(['a'])), ['b']);
    assert.deepEqual(collectOrphanedCacheDirs(['a', 'b'], (function* () { yield 'b'; })()), ['a']);
  });

  it('returns everything when nothing is live, and nothing when all are live', () => {
    assert.deepEqual(collectOrphanedCacheDirs(['a', 'b'], []), ['a', 'b']);
    assert.deepEqual(collectOrphanedCacheDirs(['a', 'b'], ['a', 'b']), []);
    assert.deepEqual(collectOrphanedCacheDirs([], ['a']), []);
  });
});

describe('SessionManager.cleanupOrphanedBlobs', () => {
  it('removes an orphaned blob dir but KEEPS a live (non-current) session\'s blobs', () => {
    const dir = makeTempDir();
    const mgr = new SessionManager('', dir);
    const live = saveWithBlob(mgr, 'session-live-1');
    const orphan = saveWithBlob(mgr, 'session-orphan-1');

    const blobsBase = path.join(dir, BLOBS_SUBDIR);
    assert.ok(fs.existsSync(path.join(blobsBase, 'session-live-1')), 'live blob dir created');
    assert.ok(fs.existsSync(path.join(blobsBase, 'session-orphan-1')), 'orphan blob dir created');

    // Orphan the second session: remove only its JSON, leaving its blob dir behind
    // (the exact state a crash-before-JSON / out-of-band delete / partial sync leaves).
    fs.unlinkSync(path.join(dir, `${orphan.id}.json`));

    const removed = mgr.cleanupOrphanedBlobs();
    assert.equal(removed, 1, 'exactly one orphaned blob dir swept');
    assert.ok(!fs.existsSync(path.join(blobsBase, 'session-orphan-1')), 'orphan blob dir removed');
    assert.ok(fs.existsSync(path.join(blobsBase, 'session-live-1')), 'live session blobs preserved');
    // The live session must still fully rehydrate from its kept blobs.
    assert.equal((mgr.load(live.id) as Session).messages[0].content, 'x'.repeat(6000));
  });

  it('rmdirs the blob base when every dir ends up orphaned', () => {
    const dir = makeTempDir();
    const mgr = new SessionManager('', dir);
    const s = saveWithBlob(mgr, 'session-only-1');
    const blobsBase = path.join(dir, BLOBS_SUBDIR);
    assert.ok(fs.existsSync(blobsBase));

    fs.unlinkSync(path.join(dir, `${s.id}.json`));
    const removed = mgr.cleanupOrphanedBlobs();
    assert.equal(removed, 1);
    assert.ok(!fs.existsSync(blobsBase), 'empty blob base is rmdir-ed');
  });

  it('is a no-op (returns 0) when there is no blob base dir at all', () => {
    const dir = makeTempDir();
    fs.mkdirSync(dir, { recursive: true });
    const mgr = new SessionManager('', dir);
    assert.equal(mgr.cleanupOrphanedBlobs(), 0);
  });

  it('leaves a stray non-dir entry under the blob base untouched', () => {
    const dir = makeTempDir();
    const mgr = new SessionManager('', dir);
    saveWithBlob(mgr, 'session-keep-1'); // keeps the base alive + has a live owner
    const blobsBase = path.join(dir, BLOBS_SUBDIR);
    const strayFile = path.join(blobsBase, 'README.txt');
    fs.writeFileSync(strayFile, 'not a session dir');

    const removed = mgr.cleanupOrphanedBlobs();
    assert.equal(removed, 0, 'a non-dir entry is never counted/removed');
    assert.ok(fs.existsSync(strayFile), 'stray file preserved');
    assert.ok(fs.existsSync(path.join(blobsBase, 'session-keep-1')), 'live blob dir preserved');
  });

  it('sweeps multiple orphans in one pass and reports the count', () => {
    const dir = makeTempDir();
    const mgr = new SessionManager('', dir);
    saveWithBlob(mgr, 'session-live-x');
    const o1 = saveWithBlob(mgr, 'session-orphan-a');
    const o2 = saveWithBlob(mgr, 'session-orphan-b');
    fs.unlinkSync(path.join(dir, `${o1.id}.json`));
    fs.unlinkSync(path.join(dir, `${o2.id}.json`));

    const removed = mgr.cleanupOrphanedBlobs();
    assert.equal(removed, 2);
    const blobsBase = path.join(dir, BLOBS_SUBDIR);
    assert.deepEqual(fs.readdirSync(blobsBase), ['session-live-x']);
  });
});
