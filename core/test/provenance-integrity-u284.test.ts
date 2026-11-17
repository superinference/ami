import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  ProvenanceTracker,
  PROVENANCE_INTEGRITY_INFO,
  PROVENANCE_INTEGRITY_ORDER,
  describeIntegrity,
  computeContentSignature,
  verifyContentSignature,
  summarizeProvenance,
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

// U284 (second half of the gap): the declared 'verified' and 'failed' integrity
// arms must be REACHABLE via a real content check, not dead enum values. register()
// derives only 'attested'/'unverified' from the tier; verifyIntegrity() performs an
// actual sha256 comparison against an expected signature and is the only path that
// can produce 'verified' (match) or 'failed' (mismatch — the tampering/MITM analog).

describe('U284 — content signature primitives', () => {
  it('computeContentSignature is a deterministic sha256 hex', () => {
    const a = computeContentSignature('hello');
    const b = computeContentSignature('hello');
    const c = computeContentSignature('hell0');
    assert.equal(a, b, 'same bytes -> same signature');
    assert.notEqual(a, c, 'different bytes -> different signature');
    assert.match(a, /^[0-9a-f]{64}$/, 'sha256 hex');
    // Matches Node's own sha256 of the same input.
    assert.equal(a, require('node:crypto').createHash('sha256').update('hello').digest('hex'));
  });

  it('verifyContentSignature returns verified on match, failed on mismatch', () => {
    const sig = computeContentSignature('payload');
    const ok = verifyContentSignature('payload', sig);
    assert.equal(ok.integrity, 'verified');
    assert.equal(ok.actual, sig);
    assert.equal(ok.expected, sig);
    const bad = verifyContentSignature('tampered', sig);
    assert.equal(bad.integrity, 'failed');
    assert.equal(bad.expected, sig);
    assert.notEqual(bad.actual, sig);
  });

  it('verifyContentSignature fails cleanly on a wrong-length expected signature', () => {
    // timingSafeEqual requires equal-length buffers; unequal length must be a
    // mismatch, not a throw.
    const r = verifyContentSignature('payload', 'deadbeef');
    assert.equal(r.integrity, 'failed');
  });
});

describe('U284 — ProvenanceTracker.verifyIntegrity (reachable verified/failed)', () => {
  it('promotes a registered record to verified when content matches', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    tracker.register('bash', 'core');
    assert.equal(tracker.getRecord('bash')!.integrity, 'attested');
    const expected = computeContentSignature('BASH-BODY-v1');
    const result = tracker.verifyIntegrity('bash', 'BASH-BODY-v1', expected);
    assert.equal(result, 'verified');
    const rec = tracker.getRecord('bash')!;
    assert.equal(rec.integrity, 'verified');
    assert.equal(rec.signature, expected);
  });

  it('marks a record failed when content does NOT match the expected signature', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    tracker.register('bash', 'core');
    const expected = computeContentSignature('BASH-BODY-v1');
    const result = tracker.verifyIntegrity('bash', 'BASH-BODY-TAMPERED', expected);
    assert.equal(result, 'failed');
    assert.equal(tracker.getRecord('bash')!.integrity, 'failed');
    // A failed integrity check is auditable as an untrusted access, even for a
    // trusted (builtin) origin tier.
    const audit = tracker.getUntrustedAccess();
    assert.ok(audit.some(a => a.name === 'bash' && a.tier === 'builtin'));
  });

  it('returns undefined for an unknown source (nothing registered)', () => {
    const tracker = new ProvenanceTracker();
    assert.equal(tracker.verifyIntegrity('ghost', 'x', computeContentSignature('x')), undefined);
    assert.equal(tracker.recordSignature('ghost', 'x'), undefined);
  });

  it('recordSignature stores a baseline without claiming verified', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    tracker.register('bash', 'core');
    const integrity = tracker.recordSignature('bash', 'BASELINE');
    // Baseline recorded, but integrity stays the honest tier-derived 'attested'.
    assert.equal(integrity, 'attested');
    const rec = tracker.getRecord('bash')!;
    assert.equal(rec.integrity, 'attested');
    assert.equal(rec.signature, computeContentSignature('BASELINE'));
    // A later verify against that baseline confirms no drift -> verified.
    assert.equal(tracker.verifyIntegrity('bash', 'BASELINE', rec.signature!), 'verified');
  });
});

describe('U284 — summarizeProvenance surfaces integrity accounting', () => {
  it('counts each integrity state and highlights failed checks', () => {
    const tracker = new ProvenanceTracker(new Set(), new Set(['core']));
    tracker.register('ok', 'core');       // attested
    tracker.register('bad', 'core');      // attested -> will fail
    tracker.register('plugin', 'npm');    // unverified
    tracker.verifyIntegrity('ok', 'body', computeContentSignature('body'));       // verified
    tracker.verifyIntegrity('bad', 'tampered', computeContentSignature('body'));  // failed
    const summary = summarizeProvenance(tracker);
    assert.equal(summary.integrity.verified, 1);
    assert.equal(summary.integrity.failed, 1);
    assert.equal(summary.integrity.unverified, 1);
    assert.equal(summary.integrity.attested, 0);
    assert.equal(summary.failedIntegrityCount, 1);
    assert.equal(summary.total, 3);
  });
});
