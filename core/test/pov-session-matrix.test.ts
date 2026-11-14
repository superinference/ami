import { describe, it, before, beforeEach, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  SessionDiffTracker,
  assertPathWithin,
} from '../src/session';
import type { Session, SessionListEntry, FileChange } from '../src/session';

// --------------------------------------------------------------------------
// Helpers — all state lives in fs.mkdtempSync temp dirs (cleaned in after()).
// --------------------------------------------------------------------------

const tmpDirs: string[] = [];
function makeTempDir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'pov-session-'));
  tmpDirs.push(d);
  return d;
}

function makeSession(id: string, messages: Session['messages'] = []): Session {
  return {
    id,
    messages,
    createdAt: '2026-01-01T00:00:00.000Z',
    updatedAt: '2026-01-01T00:00:00.000Z',
    provider: { model: 'test-model', baseUrl: 'http://localhost' },
  };
}

// Isolate any journal/config/session I/O that falls back to the data dir.
let dataDir: string;
let prevDataDir: string | undefined;

before(() => {
  prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
  dataDir = makeTempDir();
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
});

after(() => {
  if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
  else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ==========================================================================
// SessionManager: save / list / load round-trip
// ==========================================================================

describe('SessionManager — save/list/load round-trip', () => {
  let dir: string;
  let sm: SessionManager;

  beforeEach(() => {
    dir = makeTempDir();
    sm = new SessionManager(dir, dir); // override -> sessionDir === dir
  });

  it('getSessionDir reflects the override directory', () => {
    assert.equal(sm.getSessionDir(), dir);
  });

  it('save writes a <id>.json file and ensures the directory', () => {
    const s = makeSession('s-write', [{ role: 'user', content: 'hi' }]);
    sm.save(s);
    assert.ok(fs.existsSync(path.join(dir, 's-write.json')));
  });

  it('round-trips messages, provider, and settings by id', () => {
    const s = makeSession('round-1', [
      { role: 'user', content: 'question one' },
      { role: 'assistant', content: 'answer one' },
      { role: 'tool', tool_call_id: 'tc1', content: 'tool output' },
    ]);
    s.customTitle = 'My Session';
    s.settings = { skill: 'reviewer', thinkingLevel: 'high', outputStyle: 'terse', goal: 'ship' };
    sm.save(s);

    const loaded = sm.load('round-1');
    assert.ok(loaded);
    assert.equal(loaded!.id, 'round-1');
    assert.equal(loaded!.customTitle, 'My Session');
    assert.deepEqual(loaded!.provider, { model: 'test-model', baseUrl: 'http://localhost' });
    assert.deepEqual(loaded!.settings, {
      skill: 'reviewer', thinkingLevel: 'high', outputStyle: 'terse', goal: 'ship',
    });
    assert.deepEqual(loaded!.messages, s.messages);
  });

  it('save() refreshes updatedAt to now (not the stale value on the object)', () => {
    const s = makeSession('ts-1', [{ role: 'user', content: 'x' }]);
    const before = Date.now();
    sm.save(s);
    const loaded = sm.load('ts-1')!;
    const saved = new Date(loaded.updatedAt).getTime();
    assert.ok(saved >= before - 1000, 'updatedAt should be a fresh timestamp');
    assert.notEqual(loaded.updatedAt, '2026-01-01T00:00:00.000Z');
  });

  it('list returns SessionListEntry metadata (id/date/preview/messageCount/fileSize) for each saved session', () => {
    sm.save(makeSession('l-1', [{ role: 'user', content: 'first message here' }]));
    sm.save(makeSession('l-2', [
      { role: 'user', content: 'second one' },
      { role: 'assistant', content: 'reply' },
    ]));

    const entries: SessionListEntry[] = sm.list();
    assert.equal(entries.length, 2);
    for (const e of entries) {
      assert.equal(typeof e.id, 'string');
      assert.equal(typeof e.date, 'string');
      assert.equal(typeof e.preview, 'string');
      assert.equal(typeof e.messageCount, 'number');
      assert.equal(typeof e.fileSize, 'number');
      assert.ok(e.fileSize! > 0);
    }
    const byId = new Map(entries.map(e => [e.id, e]));
    assert.equal(byId.get('l-1')!.messageCount, 1);
    assert.equal(byId.get('l-2')!.messageCount, 2);
  });

  it('list preview is the first user message, flattened and trimmed', () => {
    sm.save(makeSession('p-1', [
      { role: 'assistant', content: 'assistant leads' },
      { role: 'user', content: '  hello\nworld  ' },
    ]));
    const [entry] = sm.list();
    assert.equal(entry.preview, 'hello world');
  });

  it('list preview is "(no messages)" when there is no user message', () => {
    sm.save(makeSession('p-empty', [{ role: 'assistant', content: 'only assistant' }]));
    const [entry] = sm.list();
    assert.equal(entry.preview, '(no messages)');
  });

  it('list preview truncates long first messages to 100 chars + ellipsis', () => {
    const long = 'z'.repeat(250);
    sm.save(makeSession('p-long', [{ role: 'user', content: long }]));
    const [entry] = sm.list();
    assert.equal(entry.preview.length, 103); // 100 + '...'
    assert.ok(entry.preview.endsWith('...'));
  });

  it('list is sorted by date descending (most recent first)', () => {
    const a = makeSession('date-old', [{ role: 'user', content: 'old' }]);
    a.updatedAt = '2020-01-01T00:00:00.000Z';
    const b = makeSession('date-new', [{ role: 'user', content: 'new' }]);
    b.updatedAt = '2030-01-01T00:00:00.000Z';
    // save() rewrites updatedAt, so patch the files on disk directly to control order.
    sm.save(a);
    sm.save(b);
    patchFileField(path.join(dir, 'date-old.json'), 'updatedAt', '2020-01-01T00:00:00.000Z');
    patchFileField(path.join(dir, 'date-new.json'), 'updatedAt', '2030-01-01T00:00:00.000Z');
    const entries = sm.list();
    assert.deepEqual(entries.map(e => e.id), ['date-new', 'date-old']);
  });

  it('list on an empty / nonexistent directory returns []', () => {
    const freshDir = path.join(makeTempDir(), 'never-created');
    const freshSm = new SessionManager(freshDir, freshDir);
    assert.deepEqual(freshSm.list(), []);
  });

  it('loadLatest returns the most recently modified intact session', () => {
    sm.save(makeSession('older', [{ role: 'user', content: 'older' }]));
    sm.save(makeSession('newer', [{ role: 'user', content: 'newer' }]));
    // Bump newer's mtime to guarantee ordering regardless of fs resolution.
    const future = new Date(Date.now() + 60_000);
    fs.utimesSync(path.join(dir, 'newer.json'), future, future);
    const latest = sm.loadLatest();
    assert.ok(latest);
    assert.equal(latest!.id, 'newer');
  });

  it('loadLatest returns null when no sessions exist', () => {
    assert.equal(sm.loadLatest(), null);
  });

  it('delete removes the session file and its blob sidecar', () => {
    const big = 'B'.repeat(6000);
    sm.save(makeSession('del-1', [{ role: 'user', content: big }]));
    const file = path.join(dir, 'del-1.json');
    const blobDir = path.join(dir, 'blobs', 'del-1');
    assert.ok(fs.existsSync(file));
    assert.ok(fs.existsSync(blobDir));
    sm.delete('del-1');
    assert.ok(!fs.existsSync(file));
    assert.ok(!fs.existsSync(blobDir));
    assert.equal(sm.load('del-1'), null);
  });

  it('delete of a nonexistent session is a no-op (no throw)', () => {
    assert.doesNotThrow(() => sm.delete('ghost'));
  });
});

// Overwrite a single top-level JSON field on disk (controls sort order etc).
function patchFileField(filePath: string, field: string, value: string): void {
  const obj = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  obj[field] = value;
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf-8');
}

// ==========================================================================
// SessionManager: large-content blob offload + rehydrate
// ==========================================================================

describe('SessionManager — blob offload / rehydrate', () => {
  let dir: string;
  let sm: SessionManager;

  beforeEach(() => {
    dir = makeTempDir();
    sm = new SessionManager(dir, dir);
  });

  it('offloads over-long user content to a blob and rehydrates it on load', () => {
    const big = 'A'.repeat(6000);
    sm.save(makeSession('blob-1', [{ role: 'user', content: big }]));

    // On disk the content is a blob ref, not the full string.
    const onDisk = JSON.parse(fs.readFileSync(path.join(dir, 'blob-1.json'), 'utf-8'));
    assert.equal(typeof onDisk.messages[0].content, 'object');
    assert.equal(onDisk.messages[0].content.__siSessionBlob.length, 64); // sha256 hex

    // load() restores the full string.
    const loaded = sm.load('blob-1')!;
    assert.equal(loaded.messages[0].content, big);
  });

  it('keeps assistant content inline even when long (never offloaded)', () => {
    const big = 'C'.repeat(6000);
    sm.save(makeSession('blob-assist', [{ role: 'assistant', content: big }]));
    const onDisk = JSON.parse(fs.readFileSync(path.join(dir, 'blob-assist.json'), 'utf-8'));
    assert.equal(typeof onDisk.messages[0].content, 'string');
    assert.equal(onDisk.messages[0].content.length, 6000);
  });

  it('keeps short content inline (no blob dir created)', () => {
    sm.save(makeSession('blob-short', [{ role: 'user', content: 'tiny' }]));
    assert.ok(!fs.existsSync(path.join(dir, 'blobs', 'blob-short')));
  });

  it('falls back to preview text when the blob is missing on load', () => {
    const big = 'D'.repeat(6000);
    sm.save(makeSession('blob-gone', [{ role: 'user', content: big }]));
    // Remove the blob sidecar, simulating a lost/corrupt blob.
    fs.rmSync(path.join(dir, 'blobs', 'blob-gone'), { recursive: true, force: true });
    const loaded = sm.load('blob-gone')!;
    assert.match(loaded.messages[0].content as string, /blob unavailable/);
  });

  it('list preview uses the blob ref preview without rehydrating', () => {
    const big = 'start-of-message ' + 'E'.repeat(6000);
    sm.save(makeSession('blob-prev', [{ role: 'user', content: big }]));
    const [entry] = sm.list();
    assert.ok(entry.preview.startsWith('start-of-message'));
  });

  it('identical large content dedupes to one content-addressed blob file', () => {
    const big = 'F'.repeat(6000);
    sm.save(makeSession('dedupe', [
      { role: 'user', content: big },
      { role: 'tool', tool_call_id: 't', content: big },
    ]));
    const blobs = fs.readdirSync(path.join(dir, 'blobs', 'dedupe'));
    assert.equal(blobs.length, 1);
  });
});

// ==========================================================================
// SessionManager: missing / corrupt file handling
// ==========================================================================

describe('SessionManager — missing / corrupt handling', () => {
  let dir: string;
  let sm: SessionManager;

  beforeEach(() => {
    dir = makeTempDir();
    sm = new SessionManager(dir, dir);
  });

  it('load of a missing session returns null', () => {
    assert.equal(sm.load('nope'), null);
  });

  it('load of a corrupt JSON file returns null (no throw)', () => {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'broken.json'), '{ not valid json', 'utf-8');
    assert.equal(sm.load('broken'), null);
  });

  it('list skips corrupt files and returns only the intact ones', () => {
    sm.save(makeSession('good', [{ role: 'user', content: 'ok' }]));
    fs.writeFileSync(path.join(dir, 'bad.json'), 'not json at all', 'utf-8');
    const entries = sm.list();
    assert.deepEqual(entries.map(e => e.id), ['good']);
  });

  it('list ignores non-.json files in the directory', () => {
    sm.save(makeSession('real', [{ role: 'user', content: 'ok' }]));
    fs.writeFileSync(path.join(dir, 'notes.txt'), 'ignore me', 'utf-8');
    fs.writeFileSync(path.join(dir, '.session.lock'), '123', 'utf-8');
    const entries = sm.list();
    assert.deepEqual(entries.map(e => e.id), ['real']);
  });

  it('loadLatest falls through a corrupt newest file to an intact older one', () => {
    sm.save(makeSession('intact', [{ role: 'user', content: 'intact' }]));
    // Write a corrupt file and make it the newest by mtime.
    const badPath = path.join(dir, 'corrupt.json');
    fs.writeFileSync(badPath, '{ broken', 'utf-8');
    const future = new Date(Date.now() + 120_000);
    fs.utimesSync(badPath, future, future);
    const latest = sm.loadLatest();
    assert.ok(latest);
    assert.equal(latest!.id, 'intact');
  });

  it('loadLatest returns null when every file is corrupt', () => {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'a.json'), 'x', 'utf-8');
    fs.writeFileSync(path.join(dir, 'b.json'), 'y', 'utf-8');
    assert.equal(sm.loadLatest(), null);
  });

  it('sanitizes path-separator ids so saved files stay inside the session dir', () => {
    const s = makeSession('a/b/c', [{ role: 'user', content: 'nested id' }]);
    sm.save(s);
    // Saved under the sanitized flat name, round-trips via the same raw id.
    assert.ok(fs.existsSync(path.join(dir, 'a_b_c.json')));
    const loaded = sm.load('a/b/c');
    assert.ok(loaded);
    assert.equal(loaded!.id, 'a_b_c');
  });

  it('sanitizes traversal ids (..) so load cannot escape the dir', () => {
    const s = makeSession('../escape', [{ role: 'user', content: 'x' }]);
    sm.save(s);
    assert.ok(!fs.existsSync(path.join(path.dirname(dir), 'escape.json')));
    assert.ok(sm.load('../escape'));
  });
});

// ==========================================================================
// SessionDiffTracker
// ==========================================================================

function change(partial: Partial<FileChange> & Pick<FileChange, 'path' | 'type'>): FileChange {
  return { timestamp: 1000, ...partial };
}

describe('SessionDiffTracker', () => {
  let t: SessionDiffTracker;

  beforeEach(() => {
    t = new SessionDiffTracker();
  });

  it('starts empty', () => {
    assert.equal(t.hasChanges(), false);
    assert.equal(t.getChangeCount(), 0);
    assert.deepEqual(t.getChanges(), []);
    assert.deepEqual(t.getModifiedPaths(), []);
    assert.deepEqual(t.getSummary(), { creates: 0, modifies: 0, deletes: 0, renames: 0, uniquePaths: 0 });
  });

  it('records create/modify/delete/rename and aggregates a summary', () => {
    assert.equal(t.recordChange(change({ path: 'a.ts', type: 'create' })), true);
    t.recordChange(change({ path: 'a.ts', type: 'modify' }));
    t.recordChange(change({ path: 'b.ts', type: 'modify' }));
    t.recordChange(change({ path: 'c.ts', type: 'delete' }));
    t.recordChange(change({ path: 'd.ts', type: 'rename' }));

    assert.equal(t.hasChanges(), true);
    assert.equal(t.getChangeCount(), 5);
    assert.deepEqual(t.getSummary(), {
      creates: 1, modifies: 2, deletes: 1, renames: 1, uniquePaths: 4,
    });
  });

  it('getModifiedPaths returns unique paths only', () => {
    t.recordChange(change({ path: 'x.ts', type: 'create' }));
    t.recordChange(change({ path: 'x.ts', type: 'modify' }));
    t.recordChange(change({ path: 'y.ts', type: 'modify' }));
    assert.deepEqual(t.getModifiedPaths().sort(), ['x.ts', 'y.ts']);
  });

  it('getChangesForPath filters by exact path', () => {
    t.recordChange(change({ path: 'x.ts', type: 'create' }));
    t.recordChange(change({ path: 'x.ts', type: 'modify' }));
    t.recordChange(change({ path: 'y.ts', type: 'modify' }));
    const forX = t.getChangesForPath('x.ts');
    assert.equal(forX.length, 2);
    assert.ok(forX.every(c => c.path === 'x.ts'));
    assert.deepEqual(t.getChangesForPath('absent.ts'), []);
  });

  it('getChangesByType filters by type', () => {
    t.recordChange(change({ path: 'a', type: 'create' }));
    t.recordChange(change({ path: 'b', type: 'create' }));
    t.recordChange(change({ path: 'c', type: 'delete' }));
    assert.equal(t.getChangesByType('create').length, 2);
    assert.equal(t.getChangesByType('delete').length, 1);
    assert.deepEqual(t.getChangesByType('rename'), []);
  });

  it('getChangesSince is inclusive of the timestamp boundary', () => {
    t.recordChange(change({ path: 'old', type: 'modify', timestamp: 100 }));
    t.recordChange(change({ path: 'edge', type: 'modify', timestamp: 200 }));
    t.recordChange(change({ path: 'new', type: 'modify', timestamp: 300 }));
    const since = t.getChangesSince(200);
    assert.deepEqual(since.map(c => c.path), ['edge', 'new']);
  });

  it('preserves optional FileChange fields (toolCallId, originalContent, size)', () => {
    t.recordChange(change({
      path: 'z.ts', type: 'modify', toolCallId: 'call-9', originalContent: 'before', size: 42,
    }));
    const [c] = t.getChanges();
    assert.equal(c.toolCallId, 'call-9');
    assert.equal(c.originalContent, 'before');
    assert.equal(c.size, 42);
  });

  it('enforces maxChanges — further records are rejected', () => {
    const capped = new SessionDiffTracker(2);
    assert.equal(capped.recordChange(change({ path: '1', type: 'create' })), true);
    assert.equal(capped.recordChange(change({ path: '2', type: 'create' })), true);
    assert.equal(capped.recordChange(change({ path: '3', type: 'create' })), false);
    assert.equal(capped.getChangeCount(), 2);
  });

  it('clear resets changes and seen paths', () => {
    t.recordChange(change({ path: 'a', type: 'create' }));
    t.clear();
    assert.equal(t.hasChanges(), false);
    assert.equal(t.getChangeCount(), 0);
    assert.deepEqual(t.getModifiedPaths(), []);
    assert.equal(t.getSummary().uniquePaths, 0);
  });
});

// ==========================================================================
// assertPathWithin containment
// ==========================================================================

describe('assertPathWithin', () => {
  it('allows a path nested inside the base dir', () => {
    assert.doesNotThrow(() => assertPathWithin('/base/dir/file.json', '/base/dir'));
    assert.doesNotThrow(() => assertPathWithin('/base/dir/a/b/c.json', '/base/dir'));
  });

  it('allows the base dir itself', () => {
    assert.doesNotThrow(() => assertPathWithin('/base/dir', '/base/dir'));
  });

  it('rejects a parent-traversal escape', () => {
    assert.throws(
      () => assertPathWithin('/base/dir/../evil.json', '/base/dir'),
      /Path traversal blocked/,
    );
  });

  it('rejects a sibling-prefix directory that is not actually contained', () => {
    assert.throws(
      () => assertPathWithin('/base/dir-sibling/file.json', '/base/dir'),
      /Path traversal blocked/,
    );
  });

  it('rejects an unrelated absolute path', () => {
    assert.throws(
      () => assertPathWithin('/etc/passwd', '/base/dir'),
      /Path traversal blocked/,
    );
  });

  it('resolves relative path segments before comparing', () => {
    assert.doesNotThrow(() => assertPathWithin('/base/dir/sub/../file.json', '/base/dir'));
  });
});
