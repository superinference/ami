import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  PROVENANCE_TIERS,
  PROVENANCE_TIER_INFO,
  PROVENANCE_INTEGRITY_INFO,
  RISK_CAPABILITIES,
  describeTier,
  describeIntegrity,
  provenanceWarning,
  elevatedCapabilities,
  hasElevatedCapabilities,
  assessProvenanceRisk,
  ProvenanceTracker,
  summarizeProvenance,
  isProvenanceTrusted,
  type ProvenanceIntegrity,
} from '../src/provenance';

// Parity batch C — two provenance.ts gaps, closed additively:
//  (1) Human-readable trust/provenance wording (PROVENANCE_TIER_INFO +
//      integrity info + warning copy surfaced in ProvenanceSummary).
//  (2) ProvenanceRecord carries concrete risk-capability flags
//      (hooks/bash/env), populated at register(), fed into the summary and
//      a capability-aware risk assessment.
// Every assertion is chosen to discriminate the fix from the old behaviour.

describe('provenance C — tier info wording (gap 1)', () => {
  it('has a descriptor for every tier with correct trust/severity', () => {
    for (const tier of PROVENANCE_TIERS) {
      const info = PROVENANCE_TIER_INFO[tier];
      assert.ok(info.label.length > 0, `tier ${tier} has a label`);
      assert.ok(info.description.length > 0, `tier ${tier} has a description`);
      // Trust flag must agree with the origin-only trust primitive.
      assert.equal(info.trusted, isProvenanceTrusted(tier));
    }
    // Trusted tiers are 'ok'; untrusted carry caution/warning.
    assert.equal(PROVENANCE_TIER_INFO.builtin.severity, 'ok');
    assert.equal(PROVENANCE_TIER_INFO.managed.severity, 'ok');
    assert.equal(PROVENANCE_TIER_INFO.user.severity, 'caution');
    assert.equal(PROVENANCE_TIER_INFO.unknown.severity, 'warning');
  });

  it('describeTier returns the matching descriptor object', () => {
    assert.equal(describeTier('user'), PROVENANCE_TIER_INFO.user);
    assert.equal(describeTier('unknown').trusted, false);
    assert.equal(describeTier('builtin').trusted, true);
  });

  it('has a descriptor for every integrity state', () => {
    const states: ProvenanceIntegrity[] = ['verified', 'unverified', 'failed'];
    for (const s of states) {
      assert.ok(PROVENANCE_INTEGRITY_INFO[s].label.length > 0);
      assert.ok(PROVENANCE_INTEGRITY_INFO[s].description.length > 0);
    }
    assert.equal(PROVENANCE_INTEGRITY_INFO.verified.severity, 'ok');
    assert.equal(PROVENANCE_INTEGRITY_INFO.unverified.severity, 'caution');
    assert.equal(PROVENANCE_INTEGRITY_INFO.failed.severity, 'warning');
  });

  it('describeIntegrity returns the matching descriptor object', () => {
    assert.equal(describeIntegrity('failed'), PROVENANCE_INTEGRITY_INFO.failed);
  });

  it('provenanceWarning is null when nothing is untrusted', () => {
    assert.equal(provenanceWarning(0), null);
    assert.equal(provenanceWarning(-1), null);
  });

  it('provenanceWarning uses singular copy for exactly one untrusted source', () => {
    const w = provenanceWarning(1);
    assert.ok(w);
    assert.match(w, /^1 untrusted source loaded/);
    assert.doesNotMatch(w, /^1 untrusted sources/);
  });

  it('provenanceWarning uses plural copy for multiple untrusted sources', () => {
    const w = provenanceWarning(3);
    assert.ok(w);
    assert.match(w, /^3 untrusted sources /);
  });

  it('summary carries warning copy only when untrusted sources exist', () => {
    const clean = new ProvenanceTracker(new Set(), new Set(['core']));
    clean.register('bash', 'core');
    assert.equal(summarizeProvenance(clean).warning, null);

    const dirty = new ProvenanceTracker();
    dirty.register('plugin', 'npm'); // user → untrusted
    const sum = summarizeProvenance(dirty);
    assert.equal(sum.untrustedCount, 1);
    assert.ok(sum.warning && sum.warning.includes('1 untrusted source'));
  });
});

describe('provenance C — risk-capability flags (gap 2)', () => {
  it('register defaults every capability flag to false when none supplied', () => {
    const t = new ProvenanceTracker();
    const rec = t.register('plain', 'npm');
    assert.deepEqual(rec.capabilities, {
      hasHooks: false,
      hasBashAllow: false,
      hasDangerousEnv: false,
    });
    assert.equal(hasElevatedCapabilities(rec), false);
    assert.deepEqual(elevatedCapabilities(rec), []);
  });

  it('register populates concrete risk flags from supplied capabilities', () => {
    const t = new ProvenanceTracker();
    const rec = t.register('risky', 'npm', { hasHooks: true, hasBashAllow: true });
    assert.equal(rec.capabilities.hasHooks, true);
    assert.equal(rec.capabilities.hasBashAllow, true);
    assert.equal(rec.capabilities.hasDangerousEnv, false);
    assert.deepEqual(elevatedCapabilities(rec).sort(), ['hasBashAllow', 'hasHooks']);
    assert.equal(hasElevatedCapabilities(rec), true);
  });

  it('normalizes non-strict-true values to false (only === true counts)', () => {
    const t = new ProvenanceTracker();
    // @ts-expect-error — exercise runtime coercion of a truthy non-boolean.
    const rec = t.register('coerce', 'npm', { hasHooks: 1, hasDangerousEnv: 'yes' });
    assert.equal(rec.capabilities.hasHooks, false);
    assert.equal(rec.capabilities.hasDangerousEnv, false);
  });

  it('RISK_CAPABILITIES enumerates exactly the three known flags', () => {
    assert.deepEqual([...RISK_CAPABILITIES], ['hasHooks', 'hasBashAllow', 'hasDangerousEnv']);
  });

  it('assessProvenanceRisk: trusted origin with no risky caps needs no review', () => {
    const t = new ProvenanceTracker(new Set(), new Set(['core']));
    const rec = t.register('bash', 'core');
    const a = assessProvenanceRisk(rec);
    assert.equal(a.tier, 'builtin');
    assert.equal(a.tierTrusted, true);
    assert.deepEqual(a.elevatedCapabilities, []);
    assert.equal(a.requiresReview, false);
  });

  it('assessProvenanceRisk: trusted origin WITH risky caps requires review', () => {
    const t = new ProvenanceTracker(new Set(['org']), new Set());
    const rec = t.register('policy', 'org', { hasDangerousEnv: true });
    const a = assessProvenanceRisk(rec);
    assert.equal(a.tierTrusted, true); // origin still trusted
    assert.deepEqual(a.elevatedCapabilities, ['hasDangerousEnv']);
    assert.equal(a.requiresReview, true); // but concrete risk forces review
  });

  it('assessProvenanceRisk: untrusted origin always requires review', () => {
    const t = new ProvenanceTracker();
    const rec = t.register('plugin', 'npm'); // user, no caps
    const a = assessProvenanceRisk(rec);
    assert.equal(a.tierTrusted, false);
    assert.deepEqual(a.elevatedCapabilities, []);
    assert.equal(a.requiresReview, true);
  });

  it('summary reports elevatedRiskCount and reviewCount from concrete risk', () => {
    const t = new ProvenanceTracker(new Set(['org']), new Set(['core']));
    t.register('bash', 'core');                          // trusted, no caps → no review
    t.register('policy', 'org', { hasHooks: true });     // trusted, caps → review + elevated
    t.register('plugin', 'npm');                         // untrusted → review, not elevated
    const sum = summarizeProvenance(t);
    assert.equal(sum.total, 3);
    assert.equal(sum.untrustedCount, 1);
    assert.equal(sum.elevatedRiskCount, 1); // only 'policy' has a risky cap
    assert.equal(sum.reviewCount, 2);       // 'policy' (caps) + 'plugin' (untrusted)
  });
});
