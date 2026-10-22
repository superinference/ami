import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveProvenance,
  isProvenanceTrusted,
  ProvenanceTracker,
  summarizeProvenance,
} from '../src/provenance';

describe('resolveProvenance', () => {
  const managed = new Set(['org-config', 'managed-settings']);
  const builtin = new Set(['core', 'engine']);

  it('returns builtin for builtin source', () => {
    assert.equal(resolveProvenance('bash', 'core', managed, builtin), 'builtin');
  });

  it('returns builtin for another builtin source', () => {
    assert.equal(resolveProvenance('grep', 'engine', managed, builtin), 'builtin');
  });

  it('returns managed for managed source', () => {
    assert.equal(resolveProvenance('custom-tool', 'org-config', managed, builtin), 'managed');
  });

  it('returns managed for another managed source', () => {
    assert.equal(resolveProvenance('audit-tool', 'managed-settings', managed, builtin), 'managed');
  });

  it('returns user for known non-managed source', () => {
    assert.equal(resolveProvenance('my-plugin', 'user-installed', managed, builtin), 'user');
  });

  it('returns unknown for empty source', () => {
    assert.equal(resolveProvenance('mystery', '', managed, builtin), 'unknown');
  });

  it('returns unknown for "unknown" source string', () => {
    assert.equal(resolveProvenance('mystery', 'unknown', managed, builtin), 'unknown');
  });

  it('builtin takes priority over managed when source is in both', () => {
    const both = new Set(['shared']);
    assert.equal(resolveProvenance('tool', 'shared', both, both), 'builtin');
  });
});

describe('isProvenanceTrusted', () => {
  it('returns true for builtin', () => {
    assert.equal(isProvenanceTrusted('builtin'), true);
  });

  it('returns true for managed', () => {
    assert.equal(isProvenanceTrusted('managed'), true);
  });

  it('returns false for user', () => {
    assert.equal(isProvenanceTrusted('user'), false);
  });

  it('returns false for unknown', () => {
    assert.equal(isProvenanceTrusted('unknown'), false);
  });
});

describe('ProvenanceTracker', () => {
  it('register creates record with correct tier', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    const rec = tracker.register('bash', 'core');
    assert.equal(rec.tier, 'builtin');
    assert.equal(rec.name, 'bash');
    assert.equal(rec.source, 'core');
  });

  it('register marks trusted as verified', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set());
    const rec = tracker.register('policy-tool', 'org');
    assert.equal(rec.integrity, 'verified');
  });

  it('register marks untrusted as unverified', () => {
    const tracker = new ProvenanceTracker();
    const rec = tracker.register('random-plugin', 'npm');
    assert.equal(rec.integrity, 'unverified');
  });

  it('register sets verifiedAt timestamp', () => {
    const tracker = new ProvenanceTracker();
    const before = Date.now();
    const rec = tracker.register('tool', 'src');
    assert.ok(rec.verifiedAt >= before);
  });

  it('getRecord returns the record', () => {
    const tracker = new ProvenanceTracker();
    tracker.register('my-tool', 'user-dir');
    const rec = tracker.getRecord('my-tool');
    assert.ok(rec);
    assert.equal(rec.name, 'my-tool');
  });

  it('getRecord returns undefined for unknown', () => {
    const tracker = new ProvenanceTracker();
    assert.equal(tracker.getRecord('nonexistent'), undefined);
  });

  it('getTier returns correct tier', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['builtin-src']));
    tracker.register('bash', 'builtin-src');
    assert.equal(tracker.getTier('bash'), 'builtin');
  });

  it('getTier returns unknown for unregistered', () => {
    const tracker = new ProvenanceTracker();
    assert.equal(tracker.getTier('nope'), 'unknown');
  });

  it('isTrusted returns true for builtin', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    tracker.register('bash', 'core');
    assert.equal(tracker.isTrusted('bash'), true);
  });

  it('isTrusted returns true for managed', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set());
    tracker.register('policy', 'org');
    assert.equal(tracker.isTrusted('policy'), true);
  });

  it('isTrusted returns false for user', () => {
    const tracker = new ProvenanceTracker();
    tracker.register('plugin', 'npm-pkg');
    assert.equal(tracker.isTrusted('plugin'), false);
  });

  it('isTrusted returns false for unregistered', () => {
    const tracker = new ProvenanceTracker();
    assert.equal(tracker.isTrusted('ghost'), false);
  });

  it('recordUntrustedAccess tracks access for untrusted', () => {
    const tracker = new ProvenanceTracker();
    tracker.register('shady', 'random');
    tracker.recordUntrustedAccess('shady');
    assert.equal(tracker.getUntrustedAccess().length, 1);
    assert.equal(tracker.getUntrustedAccess()[0].name, 'shady');
  });

  it('recordUntrustedAccess does not track trusted', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    tracker.register('bash', 'core');
    tracker.recordUntrustedAccess('bash');
    assert.equal(tracker.getUntrustedAccess().length, 0);
  });

  it('getAllRecords returns a copy of all records', () => {
    const tracker = new ProvenanceTracker();
    tracker.register('a', 'x');
    tracker.register('b', 'y');
    const all = tracker.getAllRecords();
    assert.equal(all.size, 2);
    assert.ok(all.has('a'));
    assert.ok(all.has('b'));
  });

  it('getRecordsByTier filters correctly', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set(['core']));
    tracker.register('bash', 'core');
    tracker.register('grep', 'core');
    tracker.register('policy', 'org');
    tracker.register('plugin', 'npm');
    const builtins = tracker.getRecordsByTier('builtin');
    assert.equal(builtins.length, 2);
    const managed = tracker.getRecordsByTier('managed');
    assert.equal(managed.length, 1);
    const user = tracker.getRecordsByTier('user');
    assert.equal(user.length, 1);
  });

  it('clear resets everything', () => {
    const tracker = new ProvenanceTracker();
    tracker.register('tool', 'src');
    tracker.recordUntrustedAccess('tool');
    tracker.clear();
    assert.equal(tracker.getAllRecords().size, 0);
    assert.equal(tracker.getUntrustedAccess().length, 0);
  });

  it('register overwrites previous record for same name', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set());
    tracker.register('tool', 'npm');
    assert.equal(tracker.getTier('tool'), 'user');
    tracker.register('tool', 'org');
    assert.equal(tracker.getTier('tool'), 'managed');
  });
});

describe('summarizeProvenance', () => {
  it('returns zeroed tiers for an empty tracker', () => {
    const summary = summarizeProvenance(new ProvenanceTracker());
    assert.deepEqual(summary.tiers, { builtin: 0, managed: 0, user: 0, unknown: 0 });
    assert.equal(summary.total, 0);
    assert.equal(summary.untrustedCount, 0);
    assert.deepEqual(summary.records, []);
  });

  it('counts records per tier and reports untrusted count', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set(['core']));
    tracker.register('bash', 'core');     // builtin
    tracker.register('grep', 'core');     // builtin
    tracker.register('policy', 'org');    // managed
    tracker.register('plugin', 'npm');    // user
    tracker.register('mystery', '');      // unknown
    const summary = summarizeProvenance(tracker);
    assert.deepEqual(summary.tiers, { builtin: 2, managed: 1, user: 1, unknown: 1 });
    assert.equal(summary.total, 5);
    // user + unknown are not trusted
    assert.equal(summary.untrustedCount, 2);
    assert.ok(summary.records.some(r => r.name === 'plugin' && r.tier === 'user'));
  });
});
