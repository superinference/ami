import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveProvenance,
  ProvenanceTracker,
  summarizeProvenance,
} from '../src/provenance';

// Wave1 coverage hardening for src/provenance.ts.
// These tests pin behavioural variants NOT exercised by provenance.test.ts:
// single-argument constructor, case/whitespace exactness of the `unknown`
// sentinel, and untrusted-access accounting for unregistered names. They are
// written red-green: each asserts a value that would change if the relevant
// branch were altered.

describe('provenance wave1 — single-argument constructor default', () => {
  it('defaults builtinSources to empty when only managedSources is passed', () => {
    // Only one arg: managed set provided, builtin set must default to empty.
    const tracker = new ProvenanceTracker(new Set(['org-config']));
    // A source that is managed resolves to managed (and is trusted).
    const managed = tracker.register('policy', 'org-config');
    assert.equal(managed.tier, 'managed');
    assert.equal(tracker.isTrusted('policy'), true);
    // 'core' would be builtin only if a builtin set contained it — it does not,
    // because builtinSources defaulted to empty. So it must fall through to user.
    const notBuiltin = tracker.register('bash', 'core');
    assert.equal(notBuiltin.tier, 'user');
    assert.equal(tracker.isTrusted('bash'), false);
    assert.equal(notBuiltin.integrity, 'unverified');
  });
});

describe('provenance wave1 — `unknown` sentinel is exact-match only', () => {
  const managed = new Set<string>();
  const builtin = new Set<string>();

  it('treats a case-variant of "unknown" as a real user source, not unknown', () => {
    // Guard against a case-folding bypass: only the exact lowercase sentinel
    // collapses to 'unknown'. 'Unknown'/'UNKNOWN' are ordinary user sources.
    assert.equal(resolveProvenance('t', 'Unknown', managed, builtin), 'user');
    assert.equal(resolveProvenance('t', 'UNKNOWN', managed, builtin), 'user');
    assert.equal(resolveProvenance('t', ' unknown', managed, builtin), 'user');
    assert.equal(resolveProvenance('t', 'unknown ', managed, builtin), 'user');
    // The exact sentinel still collapses.
    assert.equal(resolveProvenance('t', 'unknown', managed, builtin), 'unknown');
  });

  it('treats a whitespace-only source as a truthy user source, not unknown', () => {
    // ' ' is truthy and !== 'unknown', so the `source && source !== 'unknown'`
    // guard yields 'user' — whitespace is not a trust-equivalent of empty.
    assert.equal(resolveProvenance('t', ' ', managed, builtin), 'user');
    // Empty string is falsy → short-circuits to 'unknown'.
    assert.equal(resolveProvenance('t', '', managed, builtin), 'unknown');
  });
});

describe('provenance wave1 — untrusted access for unregistered names', () => {
  it('records access to a never-registered name (tier unknown → untrusted)', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    // Never registered: getTier → 'unknown' → not trusted → must be recorded.
    tracker.recordUntrustedAccess('ghost');
    const access = tracker.getUntrustedAccess();
    assert.equal(access.length, 1);
    assert.equal(access[0].name, 'ghost');
    assert.equal(access[0].tier, 'unknown');
    assert.ok(typeof access[0].timestamp === 'number');
  });

  it('accumulates multiple untrusted accesses while skipping trusted ones', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set(['core']));
    tracker.register('trusted-builtin', 'core');
    tracker.register('trusted-managed', 'org');
    tracker.register('shady', 'npm');
    tracker.recordUntrustedAccess('trusted-builtin'); // skipped
    tracker.recordUntrustedAccess('trusted-managed'); // skipped
    tracker.recordUntrustedAccess('shady');           // recorded
    tracker.recordUntrustedAccess('also-unregistered'); // recorded (unknown)
    const names = tracker.getUntrustedAccess().map(a => a.name);
    assert.deepEqual(names, ['shady', 'also-unregistered']);
  });
});

describe('provenance wave1 — tier filtering and summary edge cases', () => {
  it('getRecordsByTier returns empty array for a tier with no matches', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    tracker.register('bash', 'core');
    assert.deepEqual(tracker.getRecordsByTier('managed'), []);
    assert.deepEqual(tracker.getRecordsByTier('unknown'), []);
  });

  it('summarizeProvenance counts unknown sources toward untrustedCount', () => {
    const tracker = new ProvenanceTracker();
    tracker.register('a', '');     // unknown
    tracker.register('b', 'npm');  // user
    const summary = summarizeProvenance(tracker);
    assert.equal(summary.tiers.unknown, 1);
    assert.equal(summary.tiers.user, 1);
    assert.equal(summary.untrustedCount, 2);
  });
});
