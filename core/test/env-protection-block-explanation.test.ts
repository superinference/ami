import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  EnvProtectionGuard,
  PIN_CATEGORIES,
  PIN_CATEGORY_RISK,
  explainPinnedBlock,
} from '../src/env-protection';

// ---------------------------------------------------------------------------
// U108 — Blocked-env-var explanation message (why it is dangerous).
//
// Parity: Claude's ManagedSettingsSecurityDialog / managedEnvConstants.ts
// explain the concrete risk of a blocked env var in plain language rather than
// surfacing a raw internal category slug. AMI's checkAndBlock must do the same:
// enrich the reason with a per-category risk explanation + a remediation pointer.
// ---------------------------------------------------------------------------

describe('PIN_CATEGORY_RISK — per-category plain-language risk', () => {
  it('has a non-empty explanation for every pin category', () => {
    for (const category of PIN_CATEGORIES) {
      const risk = PIN_CATEGORY_RISK[category];
      assert.equal(typeof risk, 'string');
      assert.ok(risk.length > 0, `missing risk explanation for ${category}`);
    }
  });

  it('describes code-loading as arbitrary code injection', () => {
    assert.match(PIN_CATEGORY_RISK.code_loading, /inject arbitrary code/i);
  });

  it('describes policy as controlling where policy/auth is read from', () => {
    assert.match(PIN_CATEGORY_RISK.policy, /policy|auth/i);
  });

  it('describes telemetry as controlling telemetry/tracking', () => {
    assert.match(PIN_CATEGORY_RISK.telemetry, /telemetry|track/i);
  });
});

describe('explainPinnedBlock', () => {
  it('names the variable, the category, the risk, and a remediation pointer', () => {
    const msg = explainPinnedBlock('LD_PRELOAD', 'code_loading');
    assert.match(msg, /LD_PRELOAD/);
    assert.match(msg, /pinned/);
    assert.match(msg, /code_loading/);
    assert.match(msg, /inject arbitrary code/i);
    // remediation pointer so the block is actionable, not an opaque dead end
    assert.match(msg, /trusted source/i);
  });

  it('produces a distinct risk clause per category', () => {
    const codeMsg = explainPinnedBlock('LD_PRELOAD', 'code_loading');
    const telemetryMsg = explainPinnedBlock('DO_NOT_TRACK', 'telemetry');
    assert.notEqual(codeMsg, telemetryMsg);
    assert.match(telemetryMsg, /telemetry|track/i);
  });
});

describe('EnvProtectionGuard.checkAndBlock — enriched reason', () => {
  it('explains WHY a code-loading var is blocked, not just the slug', () => {
    const guard = new EnvProtectionGuard();
    const result = guard.checkAndBlock('LD_PRELOAD', 'tool-call');
    assert.equal(result.blocked, true);
    assert.equal(result.category, 'code_loading');
    // Keep backwards-observable tokens (pinned + category) …
    assert.ok(result.reason.includes('pinned'));
    assert.ok(result.reason.includes('code_loading'));
    // … AND add the plain-language risk + remediation.
    assert.match(result.reason, /inject arbitrary code/i);
    assert.match(result.reason, /trusted source/i);
  });

  it('explains a dynamically-pinned var via the dynamic_policy category', () => {
    const guard = new EnvProtectionGuard();
    guard.mergePinnedFromPolicy(['MY_COMPANY_INTERNAL_SETTING']);
    const result = guard.checkAndBlock('MY_COMPANY_INTERNAL_SETTING', 'env-set');
    assert.equal(result.blocked, true);
    assert.equal(result.category, 'dynamic_policy');
    assert.ok(result.reason.includes('dynamic_policy'));
    assert.match(result.reason, new RegExp(PIN_CATEGORY_RISK.dynamic_policy.slice(0, 12), 'i'));
  });

  it('explains a telemetry var with the telemetry risk clause', () => {
    const guard = new EnvProtectionGuard();
    const result = guard.checkAndBlock('DO_NOT_TRACK', 'bash');
    assert.equal(result.category, 'telemetry');
    assert.match(result.reason, /telemetry|track/i);
  });
});
