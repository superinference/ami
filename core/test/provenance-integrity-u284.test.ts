import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  ProvenanceTracker,
  PROVENANCE_INTEGRITY_INFO,
  PROVENANCE_INTEGRITY_ORDER,
  describeIntegrity,
  type ProvenanceIntegrity,
} from '../src/provenance';

// U284 (gap: "ProvenanceRecord.integrity is marked 'verified' with no actual
// verification"). register() never performs a content hash/signature check, so it
// must NOT claim 'verified' from the origin tier alone — that is a false integrity
// signal. A trusted origin tier is honestly reported as 'attested' (tier-only trust);
// 'verified' is reserved for a real cryptographic check. These assertions discriminate
// the fix from the old behaviour (which returned 'verified' for every trusted tier).

describe('U284 — register integrity is honest (no false verified)', () => {
  it('marks a builtin-tier source as attested, never verified', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    const rec = tracker.register('bash', 'core');
    assert.equal(rec.tier, 'builtin');
    assert.equal(rec.integrity, 'attested');
    assert.notEqual(rec.integrity, 'verified');
  });

  it('marks a managed-tier source as attested, never verified', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set());
    const rec = tracker.register('policy', 'org');
    assert.equal(rec.tier, 'managed');
    assert.equal(rec.integrity, 'attested');
  });

  it('marks an untrusted (user) source as unverified', () => {
    const tracker = new ProvenanceTracker();
    const rec = tracker.register('plugin', 'npm');
    assert.equal(rec.tier, 'user');
    assert.equal(rec.integrity, 'unverified');
  });

  it('marks an unknown source as unverified', () => {
    const tracker = new ProvenanceTracker();
    const rec = tracker.register('mystery', '');
    assert.equal(rec.tier, 'unknown');
    assert.equal(rec.integrity, 'unverified');
  });

  it('no registered record is ever marked verified from tier alone', () => {
    const tracker = new ProvenanceTracker(new Set(['org']), new Set(['core']));
    tracker.register('a', 'core');
    tracker.register('b', 'org');
    tracker.register('c', 'npm');
    tracker.register('d', '');
    for (const rec of tracker.getAllRecords().values()) {
      assert.notEqual(rec.integrity, 'verified', `${rec.name} must not claim verified`);
    }
  });
});

describe('U284 — integrity legend vocabulary', () => {
  it('describes every integrity state including attested', () => {
    const states: ProvenanceIntegrity[] = ['verified', 'attested', 'unverified', 'failed'];
    for (const s of states) {
      const info = PROVENANCE_INTEGRITY_INFO[s];
      assert.ok(info.label.length > 0, `${s} has a label`);
      assert.ok(info.description.length > 0, `${s} has a description`);
      assert.equal(describeIntegrity(s), info);
    }
    // 'attested' is explicit that content was NOT independently verified.
    assert.match(PROVENANCE_INTEGRITY_INFO.attested.description, /not independently verified/i);
    assert.equal(PROVENANCE_INTEGRITY_INFO.attested.severity, 'caution');
    // 'verified' now reflects a real cryptographic check, not origin trust.
    assert.match(PROVENANCE_INTEGRITY_INFO.verified.description, /cryptographically|hash/i);
  });

  it('exposes a stable best-to-worst display order', () => {
    assert.deepEqual([...PROVENANCE_INTEGRITY_ORDER], ['verified', 'attested', 'unverified', 'failed']);
  });
});
