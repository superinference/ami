import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { SessionDiffTracker, FileChange } from '../src/session';

function makeChange(overrides: Partial<FileChange> = {}): FileChange {
  return {
    path: '/src/app.ts',
    type: 'modify',
    timestamp: 1000,
    ...overrides,
  };
}

describe('SessionDiffTracker', () => {
  it('defaults maxChanges to 10000', () => {
    const t = new SessionDiffTracker();
    for (let i = 0; i < 10_000; i++) {
      assert.ok(t.recordChange(makeChange({ path: `/f${i}`, timestamp: i })));
    }
    assert.equal(t.getChangeCount(), 10_000);
    assert.equal(t.recordChange(makeChange({ path: '/overflow' })), false);
  });

  it('recordChange adds a change', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange());
    assert.equal(t.getChangeCount(), 1);
  });

  it('recordChange returns true when successful', () => {
    const t = new SessionDiffTracker();
    assert.equal(t.recordChange(makeChange()), true);
  });

  it('recordChange returns false when at max capacity', () => {
    const t = new SessionDiffTracker(2);
    t.recordChange(makeChange({ path: '/a' }));
    t.recordChange(makeChange({ path: '/b' }));
    assert.equal(t.recordChange(makeChange({ path: '/c' })), false);
    assert.equal(t.getChangeCount(), 2);
  });

  it('getChanges returns all changes in order', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ path: '/first', timestamp: 1 }));
    t.recordChange(makeChange({ path: '/second', timestamp: 2 }));
    t.recordChange(makeChange({ path: '/third', timestamp: 3 }));
    const changes = t.getChanges();
    assert.equal(changes.length, 3);
    assert.equal(changes[0].path, '/first');
    assert.equal(changes[1].path, '/second');
    assert.equal(changes[2].path, '/third');
  });

  it('getChangesForPath filters by path', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ path: '/a.ts', timestamp: 1 }));
    t.recordChange(makeChange({ path: '/b.ts', timestamp: 2 }));
    t.recordChange(makeChange({ path: '/a.ts', timestamp: 3 }));
    const filtered = t.getChangesForPath('/a.ts');
    assert.equal(filtered.length, 2);
    assert.ok(filtered.every(c => c.path === '/a.ts'));
  });

  it('getChangesForPath returns empty for unknown path', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ path: '/a.ts' }));
    assert.deepEqual(t.getChangesForPath('/nope.ts'), []);
  });

  it('getModifiedPaths returns unique paths', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ path: '/a.ts', timestamp: 1 }));
    t.recordChange(makeChange({ path: '/b.ts', timestamp: 2 }));
    t.recordChange(makeChange({ path: '/a.ts', timestamp: 3 }));
    const paths = t.getModifiedPaths();
    assert.equal(paths.length, 2);
    assert.ok(paths.includes('/a.ts'));
    assert.ok(paths.includes('/b.ts'));
  });

  it('getChangeCount returns correct count', () => {
    const t = new SessionDiffTracker();
    assert.equal(t.getChangeCount(), 0);
    t.recordChange(makeChange());
    assert.equal(t.getChangeCount(), 1);
    t.recordChange(makeChange({ path: '/other' }));
    assert.equal(t.getChangeCount(), 2);
  });

  it('getChangesSince filters by timestamp', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ timestamp: 100 }));
    t.recordChange(makeChange({ timestamp: 200 }));
    t.recordChange(makeChange({ timestamp: 300 }));
    const since = t.getChangesSince(200);
    assert.equal(since.length, 2);
    assert.equal(since[0].timestamp, 200);
    assert.equal(since[1].timestamp, 300);
  });

  it('getChangesByType filters creates', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ type: 'create', path: '/new.ts' }));
    t.recordChange(makeChange({ type: 'modify', path: '/old.ts' }));
    const creates = t.getChangesByType('create');
    assert.equal(creates.length, 1);
    assert.equal(creates[0].path, '/new.ts');
  });

  it('getChangesByType filters modifies', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ type: 'create', path: '/a' }));
    t.recordChange(makeChange({ type: 'modify', path: '/b' }));
    t.recordChange(makeChange({ type: 'modify', path: '/c' }));
    assert.equal(t.getChangesByType('modify').length, 2);
  });

  it('getChangesByType filters deletes', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ type: 'delete', path: '/gone.ts' }));
    t.recordChange(makeChange({ type: 'modify', path: '/kept.ts' }));
    const deletes = t.getChangesByType('delete');
    assert.equal(deletes.length, 1);
    assert.equal(deletes[0].path, '/gone.ts');
  });

  it('getChangesByType filters renames', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ type: 'rename', path: '/new-name.ts' }));
    t.recordChange(makeChange({ type: 'modify', path: '/other.ts' }));
    assert.equal(t.getChangesByType('rename').length, 1);
  });

  it('hasChanges returns false initially', () => {
    const t = new SessionDiffTracker();
    assert.equal(t.hasChanges(), false);
  });

  it('hasChanges returns true after recording', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange());
    assert.equal(t.hasChanges(), true);
  });

  it('getSummary counts by type correctly', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ type: 'create', path: '/a' }));
    t.recordChange(makeChange({ type: 'create', path: '/b' }));
    t.recordChange(makeChange({ type: 'modify', path: '/c' }));
    t.recordChange(makeChange({ type: 'delete', path: '/d' }));
    t.recordChange(makeChange({ type: 'rename', path: '/e' }));
    t.recordChange(makeChange({ type: 'rename', path: '/f' }));
    t.recordChange(makeChange({ type: 'rename', path: '/g' }));
    const s = t.getSummary();
    assert.equal(s.creates, 2);
    assert.equal(s.modifies, 1);
    assert.equal(s.deletes, 1);
    assert.equal(s.renames, 3);
  });

  it('getSummary counts unique paths', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ path: '/x', type: 'create', timestamp: 1 }));
    t.recordChange(makeChange({ path: '/y', type: 'modify', timestamp: 2 }));
    t.recordChange(makeChange({ path: '/x', type: 'modify', timestamp: 3 }));
    assert.equal(t.getSummary().uniquePaths, 2);
  });

  it('clear resets everything', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange({ path: '/a' }));
    t.recordChange(makeChange({ path: '/b' }));
    assert.equal(t.hasChanges(), true);
    t.clear();
    assert.equal(t.hasChanges(), false);
    assert.equal(t.getChangeCount(), 0);
    assert.deepEqual(t.getModifiedPaths(), []);
  });

  it('preserves optional fields', () => {
    const t = new SessionDiffTracker();
    t.recordChange({
      path: '/src/index.ts',
      type: 'modify',
      timestamp: 500,
      toolCallId: 'tc_123',
      originalContent: 'old code',
      size: 4096,
    });
    const c = t.getChanges()[0];
    assert.equal(c.toolCallId, 'tc_123');
    assert.equal(c.originalContent, 'old code');
    assert.equal(c.size, 4096);
  });

  it('getChanges returns readonly snapshot', () => {
    const t = new SessionDiffTracker();
    t.recordChange(makeChange());
    const changes = t.getChanges();
    assert.equal(changes.length, 1);
  });
});
