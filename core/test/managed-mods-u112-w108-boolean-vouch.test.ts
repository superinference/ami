import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isModAllowed,
  ManagedModsEnforcer,
  type ModInfo,
  type ManagedModsPolicy,
} from '../src/managed-mods';

/**
 * U112 / w108 — parity with the reference's `managedPlugins.ts` (L16-L20): the managed
 * `enabledPlugins` policy is BOOLEAN-AUTHORITATIVE. Only a boolean `true` entry
 * vouches for a user-tier mod under `allowManagedModsOnly`; the ambiguous legacy
 * `string[]` form (version list / owner-repo allow-array) is NOT an enablement
 * signal and must NOT silently vouch for an un-marketplace-qualified user mod.
 */
const localUserMod: ModInfo = { name: 'my-mod', id: 'my-mod@local', tier: 'user', marketplace: 'local' };

describe('isModAllowed — legacy string[] entry is NOT an enablement vouch (the reference parity)', () => {
  it('a non-empty version-list array does NOT vouch a user mod under allow-only', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': ['1.0.0', '2.0.0'] },
    };
    const result = isModAllowed(localUserMod, policy);
    assert.equal(result.allowed, false);
    assert.ok(result.reason!.includes("is not part of your organization's managed mod set"));
  });

  it('an owner/repo allow-array does NOT vouch a user mod under allow-only', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': ['owner/repo'] },
    };
    assert.equal(isModAllowed(localUserMod, policy).allowed, false);
  });

  it('a boolean true entry still vouches a local user mod (regression lock)', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': true },
    };
    assert.equal(isModAllowed(localUserMod, policy).allowed, true);
  });

  it('ManagedModsEnforcer refuses an array-only user mod end to end', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': ['1.0.0'] },
    };
    const enforcer = new ManagedModsEnforcer(policy);
    assert.equal(enforcer.check(localUserMod), false);
    assert.equal(enforcer.getRefusedCount(), 1);
    assert.ok(enforcer.getRefused()[0].reason.includes("is not part of your organization's managed mod set"));
  });
});
