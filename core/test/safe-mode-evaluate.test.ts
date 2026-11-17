import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  evaluateSafeMode,
  shouldEnableSafeMode,
  RESOURCE_WHY,
  describeBlocked,
  BLOCKED_RESOURCES,
  RESOURCE_DESCRIPTIONS,
} from '../src/safe-mode';

// U122 — reasoned trust verdict + independent granular trigger sources.

describe('evaluateSafeMode :: reasoned verdict', () => {
  it('returns a negative verdict with no reason when no dangerous indicator is present', () => {
    assert.deepEqual(evaluateSafeMode('/p', {}), { enabled: false });
  });

  it('names the untrusted-project trigger', () => {
    const d = evaluateSafeMode('/p', { untrustedProject: true });
    assert.equal(d.enabled, true);
    assert.match(d.reason!, /untrusted project/i);
  });

  it('names the cloned-from-unknown trigger', () => {
    const d = evaluateSafeMode('/p', { clonedFromUnknown: true });
    assert.equal(d.enabled, true);
    assert.match(d.reason!, /unknown source/i);
  });

  it('names the config-escalation trigger', () => {
    const d = evaluateSafeMode('/p', { hasConfigEscalation: true });
    assert.equal(d.enabled, true);
    assert.match(d.reason!, /credential|command helper|environment/i);
  });

  it('engages on dangerous env vars alone (independent source)', () => {
    const d = evaluateSafeMode('/p', { hasDangerousEnvVars: true });
    assert.equal(d.enabled, true);
    assert.match(d.reason!, /environment variables/i);
  });

  it('engages on hooks alone (independent source)', () => {
    const d = evaluateSafeMode('/p', { hasHooks: true });
    assert.equal(d.enabled, true);
    assert.match(d.reason!, /hooks/i);
  });

  it('engages on bash execution alone (independent source)', () => {
    const d = evaluateSafeMode('/p', { hasBashExecution: true });
    assert.equal(d.enabled, true);
    assert.match(d.reason!, /bash/i);
  });

  it('retains the prompt-override + MCP pairing (MCP alone does NOT engage)', () => {
    assert.equal(evaluateSafeMode('/p', { hasUntrustedMcp: true }).enabled, false);
    assert.equal(evaluateSafeMode('/p', { hasPromptOverride: true }).enabled, false);
    const d = evaluateSafeMode('/p', { hasPromptOverride: true, hasUntrustedMcp: true });
    assert.equal(d.enabled, true);
    assert.match(d.reason!, /prompt override/i);
  });

  it('trusted-ancestor inheritance suppresses the untrusted/cloned flags', () => {
    assert.equal(evaluateSafeMode('/p/sub', { untrustedProject: true, trustedRoots: ['/p'] }).enabled, false);
    assert.equal(evaluateSafeMode('/p/sub', { clonedFromUnknown: true, trustedRoots: ['/p'] }).enabled, false);
    // hard escalation signals still fire inside a trusted root
    assert.equal(evaluateSafeMode('/p/sub', { hasHooks: true, trustedRoots: ['/p'] }).enabled, true);
  });

  it('shouldEnableSafeMode delegates to evaluateSafeMode().enabled', () => {
    assert.equal(shouldEnableSafeMode('/p', { hasHooks: true }), true);
    assert.equal(shouldEnableSafeMode('/p', {}), false);
    assert.equal(shouldEnableSafeMode('/p', { hasBashExecution: true }), true);
  });
});

describe('safe-mode :: per-resource risk (why)', () => {
  it('has a non-empty why for every resource, distinct from its label', () => {
    for (const r of BLOCKED_RESOURCES) {
      assert.ok(RESOURCE_WHY[r] && RESOURCE_WHY[r].length > 0, `missing why for ${r}`);
      assert.notEqual(RESOURCE_WHY[r], RESOURCE_DESCRIPTIONS[r], `why must differ from label for ${r}`);
    }
  });

  it('describeBlocked carries the why alongside label + tier', () => {
    for (const row of describeBlocked()) {
      assert.equal(row.why, RESOURCE_WHY[row.resource]);
      assert.notEqual(row.why, row.label);
    }
  });
});
