import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  AUTO_MODE_UNAVAILABLE_REASONS,
  classifyAutoModeUnavailableReason,
  getAutoModeUnavailableNotification,
  type AutoModeUnavailableReason,
  type AutoModeAvailabilityState,
} from '../src/auto-mode';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w78): auto-mode availability gating with reason classification.
//
// Parity: Claude's getAutoModeUnavailableReason / getAutoModeUnavailableNotification
// (utils/permissions/permissionSetup.ts) + useAutoModeUnavailableNotification —
// a reason-tagged one-shot notice when an elevated autonomy mode cannot be entered.
// ---------------------------------------------------------------------------

test('the three reasons are exactly settings / circuit-breaker / model', () => {
  assert.deepEqual([...AUTO_MODE_UNAVAILABLE_REASONS], ['settings', 'circuit-breaker', 'model']);
});

test('null when auto mode is fully available', () => {
  const state: AutoModeAvailabilityState = {
    disabledBySettings: false,
    circuitBroken: false,
    modelSupportsAutoMode: true,
  };
  assert.equal(classifyAutoModeUnavailableReason(state), null);
});

test('settings disable takes precedence over everything', () => {
  assert.equal(
    classifyAutoModeUnavailableReason({
      disabledBySettings: true,
      circuitBroken: true,
      modelSupportsAutoMode: false,
    }),
    'settings',
  );
});

test('circuit breaker reported when settings allow but breaker tripped', () => {
  assert.equal(
    classifyAutoModeUnavailableReason({
      disabledBySettings: false,
      circuitBroken: true,
      modelSupportsAutoMode: false,
    }),
    'circuit-breaker',
  );
});

test('model incapability reported last', () => {
  assert.equal(
    classifyAutoModeUnavailableReason({
      disabledBySettings: false,
      circuitBroken: false,
      modelSupportsAutoMode: false,
    }),
    'model',
  );
});

test('every reason has a distinct non-empty notification string', () => {
  const seen = new Set<string>();
  for (const r of AUTO_MODE_UNAVAILABLE_REASONS) {
    const text = getAutoModeUnavailableNotification(r);
    assert.ok(text.length > 0);
    assert.ok(!seen.has(text));
    seen.add(text);
  }
});

// --- wiring: the manager reports unavailability from its own circuit breaker

test('PermissionManager.autoModeUnavailableReason uses its live circuit breaker', () => {
  const pm = new PermissionManager('ask');
  assert.equal(pm.autoModeUnavailableReason(), null);

  // Trip the circuit breaker (MAX_CONSECUTIVE = 3 denials).
  pm.recordDenial();
  pm.recordDenial();
  pm.recordDenial();
  assert.equal(pm.shouldAbort(), true);
  assert.equal(pm.autoModeUnavailableReason(), 'circuit-breaker');
});

test('manager honors injected settings/model gates with correct precedence', () => {
  const pm = new PermissionManager('ask');
  assert.equal(pm.autoModeUnavailableReason({ disabledBySettings: true }), 'settings');
  assert.equal(pm.autoModeUnavailableReason({ modelSupportsAutoMode: false }), 'model');
  // settings beats model
  assert.equal(
    pm.autoModeUnavailableReason({ disabledBySettings: true, modelSupportsAutoMode: false }),
    'settings',
  );
});

const _r: AutoModeUnavailableReason = 'model';
void _r;
