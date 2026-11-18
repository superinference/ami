import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w116 — reason-differentiated wording for WHY bypass/auto-allow is
// blocked. setDisableBypass now carries a human reason, exposed via
// disableBypassReason() and folded into autoModeUnavailableReason() so
// /permissions and the Shift+Tab message can explain the block instead of
// surfacing a bare boolean. Parity: Claude's getAutoModeUnavailableReason /
// getAutoModeUnavailableNotification.
// ---------------------------------------------------------------------------

describe('PermissionManager — disableBypassReason', () => {
  it('defaults to undefined when bypass is permitted', () => {
    const pm = new PermissionManager('ask');
    assert.equal(pm.isDisableBypass(), false);
    assert.equal(pm.disableBypassReason(), undefined);
  });

  it('carries the reason supplied when disabling bypass', () => {
    const pm = new PermissionManager('ask');
    pm.setDisableBypass(true, 'blocked by managed policy (disableBypassPermissionsMode)');
    assert.equal(pm.isDisableBypass(), true);
    assert.equal(
      pm.disableBypassReason(),
      'blocked by managed policy (disableBypassPermissionsMode)',
    );
  });

  it('records no reason when disabled without one (back-compat single-arg call)', () => {
    const pm = new PermissionManager('ask');
    pm.setDisableBypass(true);
    assert.equal(pm.isDisableBypass(), true);
    assert.equal(pm.disableBypassReason(), undefined);
  });

  it('clears a stale reason when bypass is re-enabled', () => {
    const pm = new PermissionManager('ask');
    pm.setDisableBypass(true, 'org policy');
    assert.equal(pm.disableBypassReason(), 'org policy');
    pm.setDisableBypass(false);
    assert.equal(pm.isDisableBypass(), false);
    assert.equal(pm.disableBypassReason(), undefined);
  });

  it('folds its own bypass killswitch into autoModeUnavailableReason as "settings"', () => {
    const pm = new PermissionManager('ask');
    // Available by default.
    assert.equal(pm.autoModeUnavailableReason(), null);
    // Killswitch armed → the manager self-reports a settings-category block
    // without the caller re-deriving disabledBySettings.
    pm.setDisableBypass(true, 'blocked by managed policy');
    assert.equal(pm.autoModeUnavailableReason(), 'settings');
    // Lifting it restores availability.
    pm.setDisableBypass(false);
    assert.equal(pm.autoModeUnavailableReason(), null);
  });

  it('an explicit caller-supplied disabledBySettings still wins', () => {
    const pm = new PermissionManager('ask');
    // Caller forces the gate even though the manager's killswitch is off.
    assert.equal(pm.autoModeUnavailableReason({ disabledBySettings: true }), 'settings');
  });

  it('bypass killswitch does not mask a tripped circuit breaker when both set via precedence', () => {
    const pm = new PermissionManager('ask');
    // Trip the circuit breaker (MAX_CONSECUTIVE = 3).
    pm.recordDenial();
    pm.recordDenial();
    pm.recordDenial();
    assert.equal(pm.shouldAbort(), true);
    // Settings (killswitch) has higher precedence than circuit-breaker in
    // classifyAutoModeUnavailableReason, so settings wins when both are active.
    pm.setDisableBypass(true, 'policy');
    assert.equal(pm.autoModeUnavailableReason(), 'settings');
    // With the killswitch off, the live circuit breaker is reported instead.
    pm.setDisableBypass(false);
    assert.equal(pm.autoModeUnavailableReason(), 'circuit-breaker');
  });
});
