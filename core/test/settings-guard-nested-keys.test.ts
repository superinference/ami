import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  MANAGED_ONLY_NESTED_KEYS,
  TIER_LOCKED_NESTED_KEYS,
  isManagedOnlyKey,
  isTierLockedKey,
  canOverride,
  getEffectiveTier,
  filterManagedKeys,
  SettingsGuard,
  MANAGED_ONLY_KEYS,
  TIER_LOCKED_KEYS,
} from '../src/settings-guard';
import { configToolWriteBlockReason } from '../src/tools/config-tool';

// U056 gap: nested-key (sub-key) granularity for managed/locked settings.
// Parity: the reference's settings.ts validNestedKeys — permissions/sandbox/hooks are
// expanded one level deep so a sub-path (`permissions.deny`,
// `sandbox.allowUnsandboxedCommands`) is governed INDEPENDENTLY of its siblings.

describe('nested-key governance predicates', () => {
  it('flat managed-only keys reduce to MANAGED_ONLY_KEYS.has (unchanged top-level behavior)', () => {
    for (const key of MANAGED_ONLY_KEYS) {
      assert.equal(isManagedOnlyKey(key), true, key);
    }
    assert.equal(isManagedOnlyKey('someCustomKey'), false);
    assert.equal(isManagedOnlyKey('model'), false);
  });

  it('flat tier-locked keys reduce to TIER_LOCKED_KEYS.has (unchanged top-level behavior)', () => {
    for (const key of TIER_LOCKED_KEYS) {
      assert.equal(isTierLockedKey(key), true, key);
    }
    assert.equal(isTierLockedKey('someCustomKey'), false);
  });

  it('recognizes managed-only nested sub-keys', () => {
    assert.equal(isManagedOnlyKey('sandbox.allowUnsandboxedCommands'), true);
    assert.equal(isManagedOnlyKey('sandbox.autoAllowBashIfSandboxed'), true);
    assert.equal(isManagedOnlyKey('permissions.disableBypassPermissionsMode'), true);
    // The container itself is NOT managed-only (siblings remain overridable).
    assert.equal(isManagedOnlyKey('sandbox'), false);
    assert.equal(isManagedOnlyKey('permissions'), false);
    // An unrelated sub-key of a non-governed container is not governed.
    assert.equal(isManagedOnlyKey('sandbox.image'), false);
    assert.equal(isManagedOnlyKey('permissions.allow'), false);
  });

  it('recognizes tier-locked nested sub-keys', () => {
    assert.equal(isTierLockedKey('permissions.deny'), true);
    assert.equal(isTierLockedKey('permissions.allow'), false);
  });

  it('a sub-key inherits governance from a managed-only ancestor container', () => {
    // enabledPlugins is a flat MANAGED_ONLY key; a hypothetical dotted descendant
    // inherits its managed-only status.
    assert.equal(isManagedOnlyKey('enabledPlugins.foo'), true);
    assert.equal(isManagedOnlyKey('enabledPlugins.foo.bar'), true);
  });

  it('a sub-key inherits lock from a tier-locked ancestor container', () => {
    assert.equal(isTierLockedKey('permissionMode.something'), true);
    assert.equal(isTierLockedKey('maxTurns.x'), true);
  });

  it('the nested sets expose their governed sub-paths', () => {
    assert.ok(MANAGED_ONLY_NESTED_KEYS.has('sandbox.allowUnsandboxedCommands'));
    assert.ok(TIER_LOCKED_NESTED_KEYS.has('permissions.deny'));
  });
});

describe('canOverride — nested-aware', () => {
  it('blocks a non-managed source from setting a managed-only sub-key', () => {
    assert.equal(canOverride('sandbox.allowUnsandboxedCommands', 'project', 'managed'), false);
    assert.equal(canOverride('sandbox.autoAllowBashIfSandboxed', 'local', 'managed'), false);
    assert.equal(canOverride('permissions.disableBypassPermissionsMode', 'cli', 'managed'), false);
  });

  it('allows the managed source to set a managed-only sub-key', () => {
    assert.equal(canOverride('sandbox.allowUnsandboxedCommands', 'managed', 'managed'), true);
  });

  it('blocks a lower tier from overriding a tier-locked sub-key', () => {
    assert.equal(canOverride('permissions.deny', 'local', 'managed'), false);
    assert.equal(canOverride('permissions.deny', 'cli', 'global'), false);
  });

  it('allows a higher/equal tier to set a tier-locked sub-key', () => {
    assert.equal(canOverride('permissions.deny', 'managed', 'project'), true);
    assert.equal(canOverride('permissions.deny', 'project', 'project'), true);
  });

  it('a non-governed sub-key is freely overridable', () => {
    assert.equal(canOverride('permissions.allow', 'project', 'managed'), true);
    assert.equal(canOverride('sandbox.image', 'local', 'managed'), true);
  });

  it('flat-key behavior is unchanged', () => {
    assert.equal(canOverride('allowManagedModsOnly', 'project', 'managed'), false);
    assert.equal(canOverride('permissionMode', 'local', 'managed'), false);
    assert.equal(canOverride('someCustomKey', 'local', 'managed'), true);
  });
});

describe('getEffectiveTier — nested-aware', () => {
  it('managed-only sub-key resolves to managed when present, else null', () => {
    assert.equal(
      getEffectiveTier('sandbox.allowUnsandboxedCommands', [
        { tier: 'project' },
        { tier: 'managed' },
      ]),
      'managed',
    );
    assert.equal(
      getEffectiveTier('sandbox.allowUnsandboxedCommands', [{ tier: 'project' }, { tier: 'local' }]),
      null,
    );
  });

  it('tier-locked sub-key resolves to the highest-trust (lowest-priority) source', () => {
    assert.equal(
      getEffectiveTier('permissions.deny', [{ tier: 'local' }, { tier: 'managed' }, { tier: 'project' }]),
      'managed',
    );
  });

  it('a non-governed sub-key resolves last-wins (highest priority number)', () => {
    assert.equal(
      getEffectiveTier('permissions.allow', [{ tier: 'managed' }, { tier: 'cli' }]),
      'cli',
    );
  });
});

describe('filterManagedKeys — nested descent', () => {
  it('drops a managed-only sub-key from a non-managed container, keeping siblings', () => {
    const { filtered, rejected } = filterManagedKeys(
      { sandbox: { mode: 'container', allowUnsandboxedCommands: true, image: 'node:26' } },
      'project',
    );
    assert.deepEqual(rejected, ['sandbox.allowUnsandboxedCommands']);
    assert.deepEqual(filtered, { sandbox: { mode: 'container', image: 'node:26' } });
  });

  it('drops multiple managed-only sub-keys', () => {
    const { filtered, rejected } = filterManagedKeys(
      { sandbox: { allowUnsandboxedCommands: true, autoAllowBashIfSandboxed: true, keep: 1 } },
      'local',
    );
    assert.deepEqual(rejected.sort(), [
      'sandbox.allowUnsandboxedCommands',
      'sandbox.autoAllowBashIfSandboxed',
    ]);
    assert.deepEqual(filtered, { sandbox: { keep: 1 } });
  });

  it('leaves a managed source untouched (can set the nested keys)', () => {
    const input = { sandbox: { allowUnsandboxedCommands: true } };
    const { filtered, rejected } = filterManagedKeys(input, 'managed');
    assert.deepEqual(rejected, []);
    assert.deepEqual(filtered, input);
  });

  it('a container with no governed sub-keys passes through unchanged', () => {
    const input = { sandbox: { mode: 'off', image: 'node:26' } };
    const { filtered, rejected } = filterManagedKeys(input, 'project');
    assert.deepEqual(rejected, []);
    assert.deepEqual(filtered, input);
    // Identity preserved when nothing is dropped (no needless clone path taken).
    assert.equal(filtered.sandbox, input.sandbox);
  });

  it('flat managed-only key still rejected wholesale; non-object value skips descent', () => {
    const { filtered, rejected } = filterManagedKeys(
      { allowManagedModsOnly: true, model: 'x', sandbox: 'container' },
      'project',
    );
    assert.deepEqual(rejected, ['allowManagedModsOnly']);
    assert.deepEqual(filtered, { model: 'x', sandbox: 'container' });
  });

  it('null and array values are not descended into', () => {
    const { filtered, rejected } = filterManagedKeys(
      { permissions: null, list: [1, 2] },
      'project',
    );
    assert.deepEqual(rejected, []);
    assert.deepEqual(filtered, { permissions: null, list: [1, 2] });
  });
});

describe('SettingsGuard — nested-aware block reason', () => {
  it('names a managed-only sub-key with the admin remedy', () => {
    const g = new SettingsGuard();
    assert.equal(g.checkOverride('sandbox.allowUnsandboxedCommands', 'project', 'managed'), false);
    const blocked = g.getBlockedOverrides();
    assert.equal(blocked.length, 1);
    assert.match(blocked[0].reason, /managed by your organization/);
    assert.match(blocked[0].reason, /sandbox\.allowUnsandboxedCommands/);
  });

  it('names a tier-locked sub-key with the tier remedy', () => {
    const g = new SettingsGuard();
    assert.equal(g.checkOverride('permissions.deny', 'local', 'managed'), false);
    assert.match(g.getBlockedOverrides()[0].reason, /locked at tier "managed"/);
  });
});

describe('config-tool write guard — nested-aware (end-to-end consumer)', () => {
  it('blocks the model-facing config tool from self-setting a managed-only sub-key', () => {
    const reason = configToolWriteBlockReason('sandbox.allowUnsandboxedCommands');
    assert.ok(reason);
    assert.match(reason!, /managed-only setting/);
  });

  it('blocks a tier-locked sub-key write with the security message', () => {
    const reason = configToolWriteBlockReason('permissions.deny');
    assert.ok(reason);
    assert.match(reason!, /tier-locked/);
  });

  it('allows a non-governed sub-key write (e.g. superinference.enforcing)', () => {
    assert.equal(configToolWriteBlockReason('superinference.enforcing'), null);
    assert.equal(configToolWriteBlockReason('permissions.allow'), null);
  });
});
