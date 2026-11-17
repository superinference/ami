import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  SETTINGS_TIERS,
  canOverride,
  getEffectiveTier,
  getSettingsTierDisplayName,
  getSettingsTierDisplayNameLowercase,
  getSettingsTierShortName,
  getSettingsTierLabels,
  getSettingsLayerOrder,
  resolveAppliedSettings,
} from '../src/settings-guard';
import { mergeConfigs } from '../src/config';

// U056 — settings cascade: plugin base tier (lowest-priority default layer),
// flag tier (--settings file between local and env), and the layered
// effective-vs-applied distinction. Parity: Claude's settingsCache plugin base,
// SETTING_SOURCES flagSettings layer, and SDK get_settings effective/applied split.

describe('settings tiers include plugin + flag (managed first, cli last preserved)', () => {
  it('registers the plugin base tier and the flag tier', () => {
    assert.ok(SETTINGS_TIERS.includes('plugin' as never));
    assert.ok(SETTINGS_TIERS.includes('flag' as never));
  });
  it('keeps managed first and cli last (display-order invariant)', () => {
    assert.equal(SETTINGS_TIERS[0], 'managed');
    assert.equal(SETTINGS_TIERS[SETTINGS_TIERS.length - 1], 'cli');
  });
  it('gives plugin + flag distinct human prose in all three registers', () => {
    assert.equal(getSettingsTierShortName('plugin'), 'Plugin');
    assert.notEqual(getSettingsTierDisplayName('plugin'), 'plugin');
    assert.equal(
      getSettingsTierDisplayNameLowercase('plugin'),
      getSettingsTierDisplayNameLowercase('plugin').toLowerCase(),
    );
    assert.equal(getSettingsTierShortName('flag'), 'Flag');
    assert.notEqual(getSettingsTierDisplayName('flag'), 'flag');
    assert.equal(
      getSettingsTierDisplayNameLowercase('flag'),
      getSettingsTierDisplayNameLowercase('flag').toLowerCase(),
    );
  });
  it('labels the whole cascade including plugin + flag', () => {
    const tiers = getSettingsTierLabels().map(l => l.tier);
    assert.ok(tiers.includes('plugin'));
    assert.ok(tiers.includes('flag'));
    assert.deepEqual(tiers, [...SETTINGS_TIERS]);
  });
});

describe('getSettingsLayerOrder — explicit low→high merge ordering', () => {
  it('orders every tier from lowest-priority (plugin base) to highest (cli)', () => {
    assert.deepEqual(getSettingsLayerOrder(), [
      'plugin',
      'managed',
      'global',
      'project',
      'local',
      'flag',
      'env',
      'cli',
    ]);
  });
  it('plugin is the base and cli is the top of the cascade', () => {
    const order = getSettingsLayerOrder();
    assert.equal(order[0], 'plugin');
    assert.equal(order[order.length - 1], 'cli');
  });
});

describe('canOverride — plugin + flag tier governance', () => {
  it('plugin may not set a managed-only key', () => {
    assert.equal(canOverride('allowManagedModsOnly', 'plugin', 'managed'), false);
  });
  it('plugin may not override a tier-locked key owned by a higher tier', () => {
    assert.equal(canOverride('permissionMode', 'plugin', 'local'), false);
    assert.equal(canOverride('maxTurns', 'plugin', 'global'), false);
  });
  it('flag may override a tier-locked key owned by local/project/user', () => {
    assert.equal(canOverride('permissionMode', 'flag', 'local'), true);
    assert.equal(canOverride('maxTurns', 'flag', 'project'), true);
  });
  it('flag may NOT override a tier-locked key owned by managed policy', () => {
    assert.equal(canOverride('permissionMode', 'flag', 'managed'), false);
  });
  it('flag may not set a managed-only key', () => {
    assert.equal(canOverride('allowManagedPermissionRulesOnly', 'flag', 'managed'), false);
  });
});

describe('getEffectiveTier — plugin base + flag precedence for normal keys', () => {
  it('plugin loses to every other tier for a normal key (it is the base default)', () => {
    assert.equal(getEffectiveTier('customKey', [{ tier: 'plugin' }, { tier: 'global' }]), 'global');
    assert.equal(getEffectiveTier('customKey', [{ tier: 'plugin' }, { tier: 'managed' }]), 'managed');
  });
  it('plugin is the effective tier when it is the only source', () => {
    assert.equal(getEffectiveTier('customKey', [{ tier: 'plugin' }]), 'plugin');
  });
  it('flag overrides local/project/global for a normal key', () => {
    assert.equal(getEffectiveTier('customKey', [{ tier: 'local' }, { tier: 'flag' }]), 'flag');
    assert.equal(getEffectiveTier('customKey', [{ tier: 'flag' }, { tier: 'env' }]), 'env');
  });
  it('managed wins a tier-locked key over flag; flag wins over local', () => {
    assert.equal(
      getEffectiveTier('permissionMode', [{ tier: 'flag' }, { tier: 'managed' }, { tier: 'local' }]),
      'managed',
    );
    assert.equal(
      getEffectiveTier('permissionMode', [{ tier: 'flag' }, { tier: 'local' }]),
      'flag',
    );
  });
});

describe('resolveAppliedSettings — effective (disk merge) vs applied (runtime)', () => {
  it('returns the disk-merged effective unchanged and applied with runtime overlaid', () => {
    const { effective, applied } = resolveAppliedSettings(
      { model: 'sonnet', maxTurns: 10 },
      { model: 'opus', effort: 'high' },
    );
    assert.deepEqual(effective, { model: 'sonnet', maxTurns: 10 });
    assert.deepEqual(applied, { model: 'opus', maxTurns: 10, effort: 'high' });
    assert.equal(applied.model, 'opus');
  });
  it('does not mutate the effective input object', () => {
    const eff = { model: 'a' };
    const { applied } = resolveAppliedSettings(eff, { model: 'b' });
    assert.equal(eff.model, 'a');
    assert.equal(applied.model, 'b');
  });
});

describe('mergeConfigs — plugin base + flag layers', () => {
  it('plugin base provides defaults that higher tiers override', () => {
    const merged = mergeConfigs(
      {}, {}, null, { model: 'global-model' }, null, null, undefined,
      { model: 'plugin-model', maxTurns: 7 },
    );
    // global overrides the plugin base model; the plugin-only default survives.
    assert.equal(merged.model, 'global-model');
    assert.equal(merged.maxTurns, 7);
  });
  it('flag layer overrides local but is overridden by env', () => {
    const flagOverridesLocal = mergeConfigs(
      {}, {}, null, null, { model: 'local-model' }, null, { model: 'flag-model' },
    );
    assert.equal(flagOverridesLocal.model, 'flag-model');

    const envBeatsFlag = mergeConfigs(
      {}, { model: 'env-model' }, null, null, { model: 'local-model' }, null, { model: 'flag-model' },
    );
    assert.equal(envBeatsFlag.model, 'env-model');
  });
  it('plugin base is the lowest layer — managed defaults beat it', () => {
    const merged = mergeConfigs(
      {}, {}, null, null, null, { maxTurns: 3 }, undefined, { maxTurns: 99 },
    );
    assert.equal(merged.maxTurns, 3);
  });
});
