import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  SANDBOX_POLICY_LOCK_MESSAGE,
  managedStatusSuffix,
  shouldAllowManagedSandboxDomainsOnly,
  areSandboxSettingsLockedByPolicy,
  MANAGED_ONLY_KEYS,
  canOverride,
  getSettingsJSONSchema,
} from '../src/settings-guard';

// U056 — sandbox managed-policy lock wiring: the affordance message, the
// `(managed)` status suffix, and the `allowManagedSandboxDomainsOnly` managed knob.
// Parity: the reference's SandboxOverridesTab locked-override text +
// areSandboxSettingsLockedByPolicy + shouldAllowManagedSandboxDomainsOnly().

describe('SANDBOX_POLICY_LOCK_MESSAGE', () => {
  it('is the read-only locked-override affordance text', () => {
    assert.match(SANDBOX_POLICY_LOCK_MESSAGE, /managed by a higher-priority configuration/);
    assert.match(SANDBOX_POLICY_LOCK_MESSAGE, /cannot be changed locally/);
  });
});

describe('managedStatusSuffix', () => {
  it('returns " (managed)" when locked, empty when not', () => {
    assert.equal(managedStatusSuffix(true), ' (managed)');
    assert.equal(managedStatusSuffix(false), '');
  });

  it('composes into a status label', () => {
    assert.equal(`container${managedStatusSuffix(true)}`, 'container (managed)');
    assert.equal(`container${managedStatusSuffix(false)}`, 'container');
  });
});

describe('shouldAllowManagedSandboxDomainsOnly', () => {
  it('false for null/undefined/empty/absent', () => {
    assert.equal(shouldAllowManagedSandboxDomainsOnly(null), false);
    assert.equal(shouldAllowManagedSandboxDomainsOnly(undefined), false);
    assert.equal(shouldAllowManagedSandboxDomainsOnly({}), false);
  });

  it('true only for a strict boolean true', () => {
    assert.equal(shouldAllowManagedSandboxDomainsOnly({ allowManagedSandboxDomainsOnly: true }), true);
    // Non-strict-true values do not enable the lockdown.
    assert.equal(shouldAllowManagedSandboxDomainsOnly({ allowManagedSandboxDomainsOnly: false }), false);
    assert.equal(
      shouldAllowManagedSandboxDomainsOnly({ allowManagedSandboxDomainsOnly: 'true' as never }),
      false,
    );
    assert.equal(shouldAllowManagedSandboxDomainsOnly({ allowManagedSandboxDomainsOnly: 1 as never }), false);
  });
});

describe('allowManagedSandboxDomainsOnly is managed-only', () => {
  it('is a MANAGED_ONLY_KEYS entry', () => {
    assert.ok(MANAGED_ONLY_KEYS.has('allowManagedSandboxDomainsOnly'));
  });

  it('a lower tier cannot override it; managed can', () => {
    assert.equal(canOverride('allowManagedSandboxDomainsOnly', 'project', 'managed'), false);
    assert.equal(canOverride('allowManagedSandboxDomainsOnly', 'local', 'global'), false);
    assert.equal(canOverride('allowManagedSandboxDomainsOnly', 'managed', 'managed'), true);
  });

  it('appears in the generated settings JSON Schema as a managed boolean', () => {
    const schema = getSettingsJSONSchema();
    const prop = schema.properties['allowManagedSandboxDomainsOnly'];
    assert.ok(prop, 'key present in schema');
    assert.equal(prop.type, 'boolean');
    assert.equal(prop.managed, true);
  });
});

describe('areSandboxSettingsLockedByPolicy integration with the new helpers', () => {
  it('a domains-only knob alone does NOT lock the exemption surface', () => {
    // domains-only is an egress policy, not an exemption — the exemption-lock check
    // only trips on the exemption sub-settings. The two knobs are independent.
    assert.equal(
      areSandboxSettingsLockedByPolicy({ allowManagedSandboxDomainsOnly: true } as never),
      false,
    );
    assert.equal(
      shouldAllowManagedSandboxDomainsOnly({ allowManagedSandboxDomainsOnly: true }),
      true,
    );
  });
});
