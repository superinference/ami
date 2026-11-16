import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MANAGED_ONLY_KEYS,
  TIER_LOCKED_KEYS,
  canOverride,
  getEffectiveTier,
  SettingsGuard,
  DANGEROUS_SHELL_SETTINGS,
  extractDangerousSettings,
  hasDangerousSettings,
  hasDangerousSettingsChanged,
  formatDangerousSettingsList,
  isBypassPermissionsModeDisabled,
  isAllHooksDisabled,
  isManagedHooksOnly,
} from '../src/settings-guard';
import { isDangerousSettingsEnvVar } from '../src/env-protection';

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
  it('contains allowManagedPermissionRulesOnly', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('allowManagedPermissionRulesOnly'));
  });
  it('contains strictPluginOnlyCustomization', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('strictPluginOnlyCustomization'));
  });
});

describe('managed-only policy lockdown keys are enforced end-to-end', () => {
  // Each lockdown policy must be settable ONLY from the managed tier: a lower
  // tier attempting it is rejected by filterManagedKeys and blocked by the guard.
  for (const key of ['allowManagedPermissionRulesOnly', 'strictPluginOnlyCustomization']) {
    it(`${key}: non-managed override is denied and effective tier is managed-only`, () => {
      // canOverride: a non-managed source cannot set it, managed can.
      assert.equal(canOverride(key, 'project', 'managed'), false);
      assert.equal(canOverride(key, 'managed', 'managed'), true);

      // getEffectiveTier: resolves to managed when present, null otherwise.
      assert.equal(
        getEffectiveTier(key, [
          { tier: 'managed' },
          { tier: 'project' },
        ]),
        'managed',
      );
      assert.equal(getEffectiveTier(key, [{ tier: 'project' }]), null);

      // SettingsGuard: records the managed-config block reason.
      const guard = new SettingsGuard();
      assert.equal(guard.checkOverride(key, 'local', 'managed'), false);
      const [blocked] = guard.getBlockedOverrides();
      assert.equal(blocked.reason, `"${key}" can only be set by managed config`);
    });
  }
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

// ---------------------------------------------------------------------------
// Dangerous-settings classifier
// ---------------------------------------------------------------------------

describe('DANGEROUS_SHELL_SETTINGS', () => {
  it('includes statusLine and auth helpers', () => {
    assert.ok(DANGEROUS_SHELL_SETTINGS.includes('statusLine'));
    assert.ok(DANGEROUS_SHELL_SETTINGS.includes('apiKeyHelper'));
    assert.ok(DANGEROUS_SHELL_SETTINGS.includes('otelHeadersHelper'));
  });
});

describe('extractDangerousSettings', () => {
  it('returns an empty structure for null/undefined', () => {
    const d = extractDangerousSettings(null);
    assert.deepEqual(d.shellSettings, {});
    assert.deepEqual(d.envVars, {});
    assert.equal(d.hasHooks, false);
    assert.equal(extractDangerousSettings(undefined).hasHooks, false);
  });

  it('extracts shell settings with non-empty string values only', () => {
    const d = extractDangerousSettings({ statusLine: 'curl evil', apiKeyHelper: '' });
    assert.deepEqual(d.shellSettings, { statusLine: 'curl evil' });
  });

  it('extracts env vars NOT in the safe allowlist', () => {
    const d = extractDangerousSettings({
      env: { ANTHROPIC_BASE_URL: 'http://evil', BASH_MAX_TIMEOUT_MS: '1000', EMPTY: '' },
    });
    assert.deepEqual(d.envVars, { ANTHROPIC_BASE_URL: 'http://evil' });
  });

  it('detects hooks presence', () => {
    assert.equal(extractDangerousSettings({ hooks: {} }).hasHooks, false);
    const d = extractDangerousSettings({ hooks: { preToolUse: [{ run: 'x' }] } });
    assert.equal(d.hasHooks, true);
    assert.deepEqual(d.hooks, { preToolUse: [{ run: 'x' }] });
  });

  it('classifies env vars via the canonical isDangerousSettingsEnvVar (default-deny, case-insensitive)', () => {
    // Every key below is classified by the shared env-protection classifier, not
    // a local re-derivation: allowlisted (any case) is dropped, everything else
    // is surfaced as dangerous.
    const d = extractDangerousSettings({
      env: {
        bash_max_timeout_ms: '1000', // safe, lowercase → not surfaced
        LD_PRELOAD: '/evil.so', // code-loading → dangerous
        TOTALLY_UNKNOWN: 'x', // unknown → default-deny dangerous
      },
    });
    assert.deepEqual(d.envVars, { LD_PRELOAD: '/evil.so', TOTALLY_UNKNOWN: 'x' });
    for (const key of Object.keys(d.envVars)) {
      assert.equal(isDangerousSettingsEnvVar(key), true);
    }
  });
});

describe('hasDangerousSettings', () => {
  it('false when nothing dangerous', () => {
    assert.equal(hasDangerousSettings(extractDangerousSettings({ env: { BASH_MAX_TIMEOUT_MS: '1' } })), false);
  });
  it('true when a dangerous env var present', () => {
    assert.equal(hasDangerousSettings(extractDangerousSettings({ env: { LD_PRELOAD: '/x' } })), true);
  });
  it('true when hooks present', () => {
    assert.equal(hasDangerousSettings(extractDangerousSettings({ hooks: { a: [1] } })), true);
  });
});

describe('hasDangerousSettingsChanged', () => {
  it('no new dangerous settings → no prompt', () => {
    assert.equal(hasDangerousSettingsChanged({ statusLine: 'x' }, { env: { BASH_MAX_TIMEOUT_MS: '1' } }), false);
  });
  it('new dangerous where none before → prompt', () => {
    assert.equal(hasDangerousSettingsChanged({}, { statusLine: 'curl evil' }), true);
    assert.equal(hasDangerousSettingsChanged(null, { statusLine: 'curl evil' }), true);
  });
  it('unchanged dangerous set → no prompt', () => {
    const s = { statusLine: 'curl x', env: { ANTHROPIC_BASE_URL: 'http://corp' } };
    assert.equal(hasDangerousSettingsChanged({ ...s }, { ...s }), false);
  });
  it('changed dangerous value → prompt', () => {
    assert.equal(
      hasDangerousSettingsChanged({ statusLine: 'curl x' }, { statusLine: 'curl y' }),
      true,
    );
  });
});

describe('isBypassPermissionsModeDisabled', () => {
  it('is MANAGED_ONLY and true only for a literal true flag', () => {
    assert.equal(MANAGED_ONLY_KEYS.has('disableBypassPermissionsMode'), true);
    assert.equal(isBypassPermissionsModeDisabled({ disableBypassPermissionsMode: true }), true);
    assert.equal(isBypassPermissionsModeDisabled({ disableBypassPermissionsMode: false }), false);
    // A truthy-but-not-true value is NOT treated as armed (fail safe = permissive
    // only on an exact true, mirroring Claude's strict 'disable' check).
    assert.equal(isBypassPermissionsModeDisabled({ disableBypassPermissionsMode: 'disable' }), false);
    assert.equal(isBypassPermissionsModeDisabled({}), false);
    assert.equal(isBypassPermissionsModeDisabled(null), false);
    assert.equal(isBypassPermissionsModeDisabled(undefined), false);
  });
});

describe('isAllHooksDisabled / isManagedHooksOnly (managed hook kill-switches)', () => {
  it('both keys are MANAGED_ONLY', () => {
    assert.equal(MANAGED_ONLY_KEYS.has('disableAllHooks'), true);
    assert.equal(MANAGED_ONLY_KEYS.has('allowManagedHooksOnly'), true);
  });
  it('isAllHooksDisabled is true only for a literal true flag', () => {
    assert.equal(isAllHooksDisabled({ disableAllHooks: true }), true);
    assert.equal(isAllHooksDisabled({ disableAllHooks: false }), false);
    assert.equal(isAllHooksDisabled({ disableAllHooks: 'yes' }), false);
    assert.equal(isAllHooksDisabled({}), false);
    assert.equal(isAllHooksDisabled(null), false);
    assert.equal(isAllHooksDisabled(undefined), false);
  });
  it('isManagedHooksOnly is true only for a literal true flag', () => {
    assert.equal(isManagedHooksOnly({ allowManagedHooksOnly: true }), true);
    assert.equal(isManagedHooksOnly({ allowManagedHooksOnly: false }), false);
    assert.equal(isManagedHooksOnly({ allowManagedHooksOnly: 1 }), false);
    assert.equal(isManagedHooksOnly({}), false);
    assert.equal(isManagedHooksOnly(null), false);
    assert.equal(isManagedHooksOnly(undefined), false);
  });
});

describe('formatDangerousSettingsList', () => {
  it('lists names only (shell + env + hooks), never values', () => {
    const d = extractDangerousSettings({
      statusLine: 'curl evil',
      env: { ANTHROPIC_BASE_URL: 'http://evil' },
      hooks: { a: [1] },
    });
    const list = formatDangerousSettingsList(d);
    assert.deepEqual(list, ['statusLine', 'ANTHROPIC_BASE_URL', 'hooks']);
    assert.ok(!list.some(x => x.includes('evil')));
  });
  it('empty for a clean settings object', () => {
    assert.deepEqual(formatDangerousSettingsList(extractDangerousSettings({})), []);
  });
});
