import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { MANAGED_ONLY_KEYS, canOverride, filterManagedKeys } from '../src/settings-guard';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// Gap 6: disableBypassPermissionsMode
// ---------------------------------------------------------------------------

describe('settings-guard — MANAGED_ONLY_KEYS', () => {
  it('contains disableBypassPermissionsMode', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('disableBypassPermissionsMode'));
  });

  it('contains other expected managed-only keys', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('allowManagedModsOnly'));
    assert.ok(MANAGED_ONLY_KEYS.has('allowedMcpServers'));
    assert.ok(MANAGED_ONLY_KEYS.has('processWrapper'));
  });
});

describe('settings-guard — canOverride', () => {
  it('rejects non-managed sources for disableBypassPermissionsMode', () => {
    assert.equal(canOverride('disableBypassPermissionsMode', 'global', 'managed'), false);
    assert.equal(canOverride('disableBypassPermissionsMode', 'project', 'managed'), false);
    assert.equal(canOverride('disableBypassPermissionsMode', 'cli', 'managed'), false);
  });

  it('allows managed source for disableBypassPermissionsMode', () => {
    assert.equal(canOverride('disableBypassPermissionsMode', 'managed', 'managed'), true);
  });

  it('allows any source for non-managed keys', () => {
    assert.equal(canOverride('theme', 'global', 'managed'), true);
    assert.equal(canOverride('theme', 'project', 'managed'), true);
    assert.equal(canOverride('theme', 'cli', 'managed'), true);
    assert.equal(canOverride('theme', 'managed', 'managed'), true);
  });
});

describe('settings-guard — filterManagedKeys', () => {
  it('strips managed-only keys from user config', () => {
    const config = { theme: 'dark', disableBypassPermissionsMode: true, model: 'opus' };
    const result = filterManagedKeys(config, 'user');
    assert.deepEqual(result.filtered, { theme: 'dark', model: 'opus' });
    assert.deepEqual(result.rejected, ['disableBypassPermissionsMode']);
  });

  it('preserves managed-only keys from managed config', () => {
    const config = { disableBypassPermissionsMode: true, theme: 'dark' };
    const result = filterManagedKeys(config, 'managed');
    assert.deepEqual(result.filtered, { disableBypassPermissionsMode: true, theme: 'dark' });
    assert.deepEqual(result.rejected, []);
  });

  it('strips multiple managed-only keys', () => {
    const config = {
      disableBypassPermissionsMode: true,
      allowManagedModsOnly: true,
      processWrapper: '/usr/bin/sandbox',
      theme: 'light',
    };
    const result = filterManagedKeys(config, 'project');
    assert.deepEqual(result.filtered, { theme: 'light' });
    assert.equal(result.rejected.length, 3);
    assert.ok(result.rejected.includes('disableBypassPermissionsMode'));
    assert.ok(result.rejected.includes('allowManagedModsOnly'));
    assert.ok(result.rejected.includes('processWrapper'));
  });
});

describe('PermissionManager — setMode bypass guard', () => {
  it('setMode rejects bypassPermissions when _disableBypass is true', () => {
    const pm = new PermissionManager();
    pm.setDisableBypass(true);
    const result = pm.setMode('bypassPermissions');
    assert.equal(result, false);
    // Mode should remain 'ask' (the default)
    assert.equal(pm.getMode(), 'ask');
  });

  it('setMode rejects auto-allow when _disableBypass is true', () => {
    const pm = new PermissionManager();
    pm.setDisableBypass(true);
    const result = pm.setMode('auto-allow');
    assert.equal(result, false);
    assert.equal(pm.getMode(), 'ask');
  });

  it('setMode allows bypassPermissions when _disableBypass is false', () => {
    const pm = new PermissionManager();
    pm.setDisableBypass(false);
    const result = pm.setMode('bypassPermissions');
    assert.equal(result, true);
    assert.equal(pm.getMode(), 'bypassPermissions');
  });

  it('setMode allows auto-allow when _disableBypass is false', () => {
    const pm = new PermissionManager();
    const result = pm.setMode('auto-allow');
    assert.equal(result, true);
    assert.equal(pm.getMode(), 'auto-allow');
  });

  it('setMode allows non-bypass modes even when _disableBypass is true', () => {
    const pm = new PermissionManager();
    pm.setDisableBypass(true);

    assert.equal(pm.setMode('ask'), true);
    assert.equal(pm.getMode(), 'ask');

    assert.equal(pm.setMode('deny-all'), true);
    assert.equal(pm.getMode(), 'deny-all');

    assert.equal(pm.setMode('acceptEdits'), true);
    assert.equal(pm.getMode(), 'acceptEdits');

    assert.equal(pm.setMode('plan'), true);
    assert.equal(pm.getMode(), 'plan');
  });

  it('setMode returns true for normal mode changes', () => {
    const pm = new PermissionManager();
    assert.equal(pm.setMode('deny-all'), true);
    assert.equal(pm.getMode(), 'deny-all');
  });

  it('isDisableBypass reflects the current state', () => {
    const pm = new PermissionManager();
    assert.equal(pm.isDisableBypass(), false);
    pm.setDisableBypass(true);
    assert.equal(pm.isDisableBypass(), true);
    pm.setDisableBypass(false);
    assert.equal(pm.isDisableBypass(), false);
  });

  it('mode stays unchanged after a blocked setMode call', () => {
    const pm = new PermissionManager('deny-all');
    pm.setDisableBypass(true);
    pm.setMode('bypassPermissions');
    // Must remain deny-all, not default or bypass
    assert.equal(pm.getMode(), 'deny-all');
  });
});
