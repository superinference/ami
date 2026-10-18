import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { EnvProtectionGuard, getPinCategory, isPinnedVariable } from '../src/env-protection';

// ---------------------------------------------------------------------------
// Gap 4 + Gap 8: env protection — checkAndBlock + dynamic pins
// ---------------------------------------------------------------------------

describe('getPinCategory', () => {
  it('returns policy for SUPERINFERENCE settings variables', () => {
    assert.equal(getPinCategory('SUPERINFERENCE_MANAGED_SETTINGS_PATH'), 'policy');
    assert.equal(getPinCategory('SUPERINFERENCE_CONFIG_DIR'), 'policy');
  });

  it('returns code_loading for LD_PRELOAD and NODE_OPTIONS', () => {
    assert.equal(getPinCategory('LD_PRELOAD'), 'code_loading');
    assert.equal(getPinCategory('NODE_OPTIONS'), 'code_loading');
    assert.equal(getPinCategory('DYLD_INSERT_LIBRARIES'), 'code_loading');
  });

  it('returns telemetry for telemetry control variables', () => {
    assert.equal(getPinCategory('DISABLE_TELEMETRY'), 'telemetry');
    assert.equal(getPinCategory('DO_NOT_TRACK'), 'telemetry');
  });

  it('returns null for non-pinned variables', () => {
    assert.equal(getPinCategory('MY_CUSTOM_VAR'), null);
    assert.equal(getPinCategory('FOO'), null);
    assert.equal(getPinCategory('DEBUG'), null);
  });

  it('is case-insensitive', () => {
    assert.equal(getPinCategory('ld_preload'), 'code_loading');
    assert.equal(getPinCategory('Node_Options'), 'code_loading');
  });
});

describe('EnvProtectionGuard — checkAndBlock', () => {
  it('returns blocked:true for statically pinned variables', () => {
    const guard = new EnvProtectionGuard();
    const result = guard.checkAndBlock('LD_PRELOAD', 'tool-call');
    assert.equal(result.blocked, true);
    assert.equal(result.variable, 'LD_PRELOAD');
    assert.equal(result.category, 'code_loading');
    assert.ok(result.reason.includes('pinned'));
  });

  it('returns blocked:false for non-pinned variables', () => {
    const guard = new EnvProtectionGuard();
    const result = guard.checkAndBlock('MY_VAR', 'tool-call');
    assert.equal(result.blocked, false);
    assert.equal(result.variable, 'MY_VAR');
    assert.equal(result.category, null);
    assert.equal(result.reason, '');
  });

  it('records the attempt when blocked', () => {
    const guard = new EnvProtectionGuard();
    guard.checkAndBlock('NODE_OPTIONS', 'bash');
    const attempts = guard.getBlockedAttempts();
    assert.equal(attempts.length, 1);
    assert.equal(attempts[0].variable, 'NODE_OPTIONS');
    assert.equal(attempts[0].category, 'code_loading');
    assert.equal(attempts[0].source, 'bash');
  });

  it('does not record attempts for non-pinned variables', () => {
    const guard = new EnvProtectionGuard();
    guard.checkAndBlock('FOO', 'bash');
    assert.equal(guard.getBlockedAttempts().length, 0);
  });

  it('handles case-insensitive matching', () => {
    const guard = new EnvProtectionGuard();
    const result = guard.checkAndBlock('ld_preload', 'tool-call');
    assert.equal(result.blocked, true);
    assert.equal(result.variable, 'LD_PRELOAD');
  });
});

describe('EnvProtectionGuard — dynamic pins (Gap 8)', () => {
  it('mergePinnedFromPolicy adds dynamic pins', () => {
    const guard = new EnvProtectionGuard();
    guard.mergePinnedFromPolicy(['CUSTOM_SECRET', 'INTERNAL_KEY']);
    assert.ok(guard.isDynamicallyPinned('CUSTOM_SECRET'));
    assert.ok(guard.isDynamicallyPinned('INTERNAL_KEY'));
  });

  it('dynamic pins are case-insensitive', () => {
    const guard = new EnvProtectionGuard();
    guard.mergePinnedFromPolicy(['custom_secret']);
    assert.ok(guard.isDynamicallyPinned('CUSTOM_SECRET'));
    assert.ok(guard.isDynamicallyPinned('custom_secret'));
    assert.ok(guard.isDynamicallyPinned('Custom_Secret'));
  });

  it('checkAndBlock blocks dynamically pinned variables', () => {
    const guard = new EnvProtectionGuard();
    guard.mergePinnedFromPolicy(['MY_COMPANY_INTERNAL_SETTING']);
    const result = guard.checkAndBlock('MY_COMPANY_INTERNAL_SETTING', 'env-set');
    assert.equal(result.blocked, true);
    assert.equal(result.category, 'dynamic_policy');
    assert.ok(result.reason.includes('pinned'));
  });

  it('dynamic + static pins combined: static takes precedence for category', () => {
    const guard = new EnvProtectionGuard();
    guard.mergePinnedFromPolicy(['LD_PRELOAD']);
    const result = guard.checkAndBlock('LD_PRELOAD', 'tool');
    assert.equal(result.blocked, true);
    assert.equal(result.category, 'code_loading');
  });

  it('non-dynamically-pinned var remains unblocked', () => {
    const guard = new EnvProtectionGuard();
    guard.mergePinnedFromPolicy(['PINNED_ONE']);
    assert.equal(guard.isDynamicallyPinned('NOT_PINNED'), false);
    const result = guard.checkAndBlock('NOT_PINNED', 'tool');
    assert.equal(result.blocked, false);
  });

  it('checkSet returns null for unpinned vars', () => {
    const guard = new EnvProtectionGuard();
    const attempt = guard.checkSet('HARMLESS_VAR', 'bash');
    assert.equal(attempt, null);
  });

  it('checkSet records statically pinned vars', () => {
    const guard = new EnvProtectionGuard();
    const attempt = guard.checkSet('NODE_OPTIONS', 'bash');
    assert.ok(attempt !== null);
    assert.equal(attempt!.category, 'code_loading');
    assert.equal(attempt!.variable, 'NODE_OPTIONS');
  });

  it('clear resets the blocked attempts log', () => {
    const guard = new EnvProtectionGuard();
    guard.checkAndBlock('LD_PRELOAD', 'tool');
    assert.equal(guard.getBlockedAttempts().length, 1);
    guard.clear();
    assert.equal(guard.getBlockedAttempts().length, 0);
  });
});
