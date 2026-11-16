import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  areSandboxSettingsLockedByPolicy,
  validateManagedPolicyValues,
} from '../src/settings-guard';
import { CUSTOMIZATION_SURFACES } from '../src/plugin-only-policy';

// U056 — managed-policy guards added to settings-guard.ts (parity:
// areSandboxSettingsLockedByPolicy + doctor managed-policy value validation).

describe('areSandboxSettingsLockedByPolicy', () => {
  it('false for null/undefined/empty managed config', () => {
    assert.equal(areSandboxSettingsLockedByPolicy(null), false);
    assert.equal(areSandboxSettingsLockedByPolicy(undefined), false);
    assert.equal(areSandboxSettingsLockedByPolicy({}), false);
  });

  it('true when managed config pins sandboxExcludedCommands', () => {
    assert.equal(
      areSandboxSettingsLockedByPolicy({ sandboxExcludedCommands: ['git push'] }),
      true,
    );
    // Even an empty-but-present list is an admin-owned setting (parity: value is defined).
    assert.equal(areSandboxSettingsLockedByPolicy({ sandboxExcludedCommands: [] }), true);
  });

  it('true when managed config pins any sandbox exemption sub-setting', () => {
    assert.equal(areSandboxSettingsLockedByPolicy({ sandbox: { mode: 'container' } }), true);
    assert.equal(areSandboxSettingsLockedByPolicy({ sandbox: { mode: 'off' } }), true);
    assert.equal(
      areSandboxSettingsLockedByPolicy({ sandbox: { autoAllowBashIfSandboxed: true } }),
      true,
    );
    assert.equal(
      areSandboxSettingsLockedByPolicy({ sandbox: { allowUnsandboxedCommands: false } }),
      true,
    );
  });

  it('false when managed sandbox object has no exemption sub-settings', () => {
    assert.equal(areSandboxSettingsLockedByPolicy({ sandbox: {} }), false);
    // An unrelated sandbox field (not an exemption knob) does not lock.
    assert.equal(
      areSandboxSettingsLockedByPolicy({ sandbox: { image: 'node:26' } as never }),
      false,
    );
  });

  it('false when sandbox is null or non-object', () => {
    assert.equal(areSandboxSettingsLockedByPolicy({ sandbox: null }), false);
    assert.equal(areSandboxSettingsLockedByPolicy({ sandbox: 'container' as never }), false);
  });
});

describe('validateManagedPolicyValues', () => {
  it('returns no warnings for null/undefined/non-object/clean config', () => {
    assert.deepEqual(validateManagedPolicyValues(null), []);
    assert.deepEqual(validateManagedPolicyValues(undefined), []);
    assert.deepEqual(validateManagedPolicyValues('nope' as never), []);
    assert.deepEqual(validateManagedPolicyValues({}), []);
  });

  it('accepts a valid boolean strictPluginOnlyCustomization', () => {
    assert.deepEqual(validateManagedPolicyValues({ strictPluginOnlyCustomization: true }), []);
    assert.deepEqual(validateManagedPolicyValues({ strictPluginOnlyCustomization: false }), []);
  });

  it('accepts an array of known surfaces', () => {
    assert.deepEqual(
      validateManagedPolicyValues({ strictPluginOnlyCustomization: [...CUSTOMIZATION_SURFACES] }),
      [],
    );
    assert.deepEqual(
      validateManagedPolicyValues({ strictPluginOnlyCustomization: ['hooks'] }),
      [],
    );
  });

  it('warns on a wrong-typed value (object/string/number)', () => {
    const w = validateManagedPolicyValues({ strictPluginOnlyCustomization: { hooks: true } });
    assert.equal(w.length, 1);
    assert.equal(w[0].key, 'strictPluginOnlyCustomization');
    assert.match(w[0].issue, /invalid value \(expected true or an array, got object\)/);
    assert.match(w[0].fix, /silently ignored/);

    assert.match(
      validateManagedPolicyValues({ strictPluginOnlyCustomization: 'hooks' })[0].issue,
      /got string/,
    );
  });

  it('warns on unrecognized surfaces in an array (unknown strings + non-strings)', () => {
    const w = validateManagedPolicyValues({
      strictPluginOnlyCustomization: ['hooks', 'commands', 42],
    });
    assert.equal(w.length, 1);
    assert.match(w[0].issue, /2 value\(s\) this client doesn't recognize/);
    assert.match(w[0].issue, /commands/);
    assert.match(w[0].issue, /42/);
    assert.ok(w[0].fix.includes(CUSTOMIZATION_SURFACES.join(', ')));
  });

  it('no warning for an all-known array even when mixed order', () => {
    assert.deepEqual(
      validateManagedPolicyValues({ strictPluginOnlyCustomization: ['mcp', 'agents'] }),
      [],
    );
  });
});
