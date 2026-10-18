import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MANAGED_ONLY_KEYS,
  TIER_LOCKED_KEYS,
  canOverride,
  getEffectiveTier,
  SettingsGuard,
} from '../src/settings-guard';

describe('MANAGED_ONLY_KEYS', () => {
  it('contains allowManagedModsOnly', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('allowManagedModsOnly'));
  });
  it('contains allowedMcpServers', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('allowedMcpServers'));
  });
  it('contains managedMcpServers', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('managedMcpServers'));
  });
  it('contains prependPlugins', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('prependPlugins'));
  });
  it('contains appendPlugins', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('appendPlugins'));
  });
  it('contains enabledPlugins', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('enabledPlugins'));
  });
  it('contains strictKnownMarketplaces', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('strictKnownMarketplaces'));
  });
  it('contains processWrapper', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('processWrapper'));
  });
  it('contains allowModsToOverrideDenyRules', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('allowModsToOverrideDenyRules'));
  });
});

describe('TIER_LOCKED_KEYS', () => {
  it('contains permissionMode', () => {
    assert.ok(TIER_LOCKED_KEYS.has('permissionMode'));
  });
  it('contains maxTurns', () => {
    assert.ok(TIER_LOCKED_KEYS.has('maxTurns'));
  });
  it('contains maxBudgetUsd', () => {
    assert.ok(TIER_LOCKED_KEYS.has('maxBudgetUsd'));
  });
  it('contains maxSessionMs', () => {
    assert.ok(TIER_LOCKED_KEYS.has('maxSessionMs'));
  });
});

describe('canOverride', () => {
  it('allows managed to set managed-only key', () => {
    assert.equal(canOverride('allowManagedModsOnly', 'managed', 'managed'), true);
  });
  it('denies project from setting managed-only key', () => {
    assert.equal(canOverride('allowManagedModsOnly', 'project', 'managed'), false);
  });
  it('denies local from setting managed-only key', () => {
    assert.equal(canOverride('allowedMcpServers', 'local', 'managed'), false);
  });
  it('denies global from setting managed-only key', () => {
    assert.equal(canOverride('prependPlugins', 'global', 'managed'), false);
  });
  it('denies env from setting managed-only key', () => {
    assert.equal(canOverride('enabledPlugins', 'env', 'managed'), false);
  });
  it('denies cli from setting managed-only key', () => {
    assert.equal(canOverride('strictKnownMarketplaces', 'cli', 'managed'), false);
  });
  it('allows managed to override tier-locked key over managed', () => {
    assert.equal(canOverride('permissionMode', 'managed', 'managed'), true);
  });
  it('allows global to override tier-locked key over project', () => {
    assert.equal(canOverride('maxTurns', 'global', 'project'), true);
  });
  it('denies local from overriding managed tier-locked key', () => {
    assert.equal(canOverride('permissionMode', 'local', 'managed'), false);
  });
  it('denies cli from overriding global tier-locked key', () => {
    assert.equal(canOverride('maxBudgetUsd', 'cli', 'global'), false);
  });
  it('allows normal key override from any tier', () => {
    assert.equal(canOverride('someCustomKey', 'local', 'managed'), true);
  });
  it('allows env tier for normal keys', () => {
    assert.equal(canOverride('customSetting', 'env', 'global'), true);
  });
  it('allows cli tier for normal keys', () => {
    assert.equal(canOverride('customSetting', 'cli', 'managed'), true);
  });
  it('allows same tier for tier-locked key', () => {
    assert.equal(canOverride('maxSessionMs', 'project', 'project'), true);
  });
});

describe('getEffectiveTier', () => {
  it('returns managed for managed-only key with managed source', () => {
    assert.equal(
      getEffectiveTier('allowManagedModsOnly', [
        { tier: 'managed' },
        { tier: 'project' },
      ]),
      'managed',
    );
  });
  it('returns null for managed-only key without managed source', () => {
    assert.equal(
      getEffectiveTier('allowedMcpServers', [
        { tier: 'global' },
        { tier: 'project' },
      ]),
      null,
    );
  });
  it('returns highest-priority for tier-locked key', () => {
    assert.equal(
      getEffectiveTier('permissionMode', [
        { tier: 'project' },
        { tier: 'managed' },
        { tier: 'cli' },
      ]),
      'managed',
    );
  });
  it('returns global over local for tier-locked key', () => {
    assert.equal(
      getEffectiveTier('maxTurns', [
        { tier: 'local' },
        { tier: 'global' },
      ]),
      'global',
    );
  });
  it('returns lowest-priority (CLI) for normal key', () => {
    assert.equal(
      getEffectiveTier('customKey', [
        { tier: 'managed' },
        { tier: 'global' },
        { tier: 'cli' },
      ]),
      'cli',
    );
  });
  it('returns env over project for normal key', () => {
    assert.equal(
      getEffectiveTier('customKey', [
        { tier: 'project' },
        { tier: 'env' },
      ]),
      'env',
    );
  });
  it('returns null for empty sources', () => {
    assert.equal(getEffectiveTier('anyKey', []), null);
  });
  it('returns single source tier for single source', () => {
    assert.equal(
      getEffectiveTier('customKey', [{ tier: 'local' }]),
      'local',
    );
  });
});

describe('SettingsGuard', () => {
  it('checkOverride allows valid override', () => {
    const guard = new SettingsGuard();
    assert.equal(guard.checkOverride('customKey', 'local', 'global'), true);
  });
  it('checkOverride blocks managed-only key from project', () => {
    const guard = new SettingsGuard();
    assert.equal(guard.checkOverride('allowManagedModsOnly', 'project', 'managed'), false);
  });
  it('checkOverride blocks tier-locked key override', () => {
    const guard = new SettingsGuard();
    assert.equal(guard.checkOverride('permissionMode', 'cli', 'managed'), false);
  });
  it('getBlockedOverrides tracks blocked attempts', () => {
    const guard = new SettingsGuard();
    guard.checkOverride('allowedMcpServers', 'local', 'managed');
    const blocked = guard.getBlockedOverrides();
    assert.equal(blocked.length, 1);
    assert.equal(blocked[0].key, 'allowedMcpServers');
    assert.equal(blocked[0].source, 'local');
    assert.ok(blocked[0].reason.includes('managed config'));
  });
  it('getBlockedOverrides includes reason for tier-locked', () => {
    const guard = new SettingsGuard();
    guard.checkOverride('maxBudgetUsd', 'env', 'global');
    const blocked = guard.getBlockedOverrides();
    assert.equal(blocked.length, 1);
    assert.ok(blocked[0].reason.includes('locked'));
  });
  it('getBlockedCount returns correct count', () => {
    const guard = new SettingsGuard();
    guard.checkOverride('allowManagedModsOnly', 'local', 'managed');
    guard.checkOverride('permissionMode', 'cli', 'managed');
    assert.equal(guard.getBlockedCount(), 2);
  });
  it('getBlockedCount is 0 initially', () => {
    const guard = new SettingsGuard();
    assert.equal(guard.getBlockedCount(), 0);
  });
  it('clear resets blocked overrides', () => {
    const guard = new SettingsGuard();
    guard.checkOverride('allowManagedModsOnly', 'local', 'managed');
    assert.equal(guard.getBlockedCount(), 1);
    guard.clear();
    assert.equal(guard.getBlockedCount(), 0);
    assert.equal(guard.getBlockedOverrides().length, 0);
  });
});
