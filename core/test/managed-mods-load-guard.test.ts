import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { ManagedModsEnforcer, isModAllowed, validateModId } from '../src/managed-mods';
import type { ModInfo, ManagedModsPolicy } from '../src/managed-mods';

// ---------------------------------------------------------------------------
// Gap 3: Managed-mods convenience guard + validateModId
// ---------------------------------------------------------------------------

describe('validateModId', () => {
  it('rejects __proto__', () => {
    assert.equal(validateModId('__proto__'), false);
    assert.equal(validateModId('@evil/__proto__'), false);
    assert.equal(validateModId('pkg/__proto__/hack'), false);
  });

  it('rejects constructor', () => {
    assert.equal(validateModId('constructor'), false);
    assert.equal(validateModId('@scope/constructor'), false);
  });

  it('rejects prototype', () => {
    assert.equal(validateModId('prototype'), false);
  });

  it('rejects toString', () => {
    assert.equal(validateModId('toString'), false);
  });

  it('rejects valueOf', () => {
    assert.equal(validateModId('valueOf'), false);
  });

  it('rejects hasOwnProperty', () => {
    assert.equal(validateModId('hasOwnProperty'), false);
  });

  it('allows normal mod IDs', () => {
    assert.equal(validateModId('my-mod'), true);
    assert.equal(validateModId('@org/my-mod'), true);
    assert.equal(validateModId('cool-plugin-v2'), true);
    assert.equal(validateModId('@superinference/dark-theme'), true);
  });

  it('allows IDs with dots and numbers', () => {
    assert.equal(validateModId('mod-1.2.3'), true);
    assert.equal(validateModId('@scope/mod.v2'), true);
  });
});

describe('isModAllowed', () => {
  it('allows all mods when allowManagedModsOnly is false', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: false };
    const mod: ModInfo = { id: 'user-mod', name: 'user-mod', tier: 'user' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, true);
  });

  it('blocks user mods when allowManagedModsOnly is true', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const mod: ModInfo = { id: 'user-mod', name: 'user-mod', tier: 'user' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, false);
    assert.ok(result.reason?.includes('allowManagedModsOnly'));
  });

  it('allows builtin mods when allowManagedModsOnly is true', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const mod: ModInfo = { id: 'builtin-mod', name: 'builtin-mod', tier: 'builtin' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, true);
  });

  it('allows managed mods when allowManagedModsOnly is true', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const mod: ModInfo = { id: 'org-mod', name: 'org-mod', tier: 'managed' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, true);
  });

  it('allows user mod if enabled in enabledPlugins', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'special-user-mod': true },
    };
    const mod: ModInfo = { id: 'special-user-mod', name: 'special-user-mod', tier: 'user' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, true);
  });

  it('blocks user mod not in enabledPlugins', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'special-user-mod': true },
    };
    const mod: ModInfo = { id: 'other-mod', name: 'other-mod', tier: 'user' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, false);
    assert.ok(result.reason?.includes('allowManagedModsOnly'));
  });

  it('blocks remote marketplace mods even if in enabledPlugins', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'remote-mod': true },
    };
    const mod: ModInfo = { id: 'remote-mod', name: 'remote-mod', tier: 'user', marketplace: 'npm-registry' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, false);
    assert.ok(result.reason?.includes('remote marketplace'));
  });
});

describe('ManagedModsEnforcer.shouldBlock — static convenience', () => {
  it('with allowManagedModsOnly:true blocks user mods', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const mod: ModInfo = { id: 'user-plugin', name: 'user-plugin', tier: 'user' };
    const result = ManagedModsEnforcer.shouldBlock(mod, policy);
    assert.equal(result.blocked, true);
    assert.ok(result.reason?.includes('allowManagedModsOnly'));
  });

  it('with allowManagedModsOnly:true allows managed mods', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const mod: ModInfo = { id: 'org-plugin', name: 'org-plugin', tier: 'managed' };
    const result = ManagedModsEnforcer.shouldBlock(mod, policy);
    assert.equal(result.blocked, false);
  });

  it('with allowManagedModsOnly:false allows all', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: false };
    const mod: ModInfo = { id: 'any-mod', name: 'any-mod', tier: 'user' };
    const result = ManagedModsEnforcer.shouldBlock(mod, policy);
    assert.equal(result.blocked, false);
  });

  it('with null policy allows all', () => {
    const mod: ModInfo = { id: 'any-mod', name: 'any-mod', tier: 'user' };
    const result = ManagedModsEnforcer.shouldBlock(mod, null);
    assert.equal(result.blocked, false);
  });
});

describe('ManagedModsEnforcer — instance methods', () => {
  it('check records refused mods', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const enforcer = new ManagedModsEnforcer(policy);
    const mod: ModInfo = { id: 'bad-mod', name: 'bad-mod', tier: 'user' };
    const allowed = enforcer.check(mod);
    assert.equal(allowed, false);
    const refused = enforcer.getRefused();
    assert.equal(refused.length, 1);
    assert.equal(refused[0].mod.id, 'bad-mod');
  });

  it('check does not record allowed mods', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const enforcer = new ManagedModsEnforcer(policy);
    const mod: ModInfo = { id: 'good-mod', name: 'good-mod', tier: 'managed' };
    enforcer.check(mod);
    assert.equal(enforcer.getRefused().length, 0);
  });

  it('getRefusedCount tracks number of refused', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true };
    const enforcer = new ManagedModsEnforcer(policy);
    enforcer.check({ id: 'a', name: 'a', tier: 'user' });
    enforcer.check({ id: 'b', name: 'b', tier: 'user' });
    assert.equal(enforcer.getRefusedCount(), 2);
  });

  it('updatePolicy changes behavior', () => {
    const enforcer = new ManagedModsEnforcer({ allowManagedModsOnly: true });
    const mod: ModInfo = { id: 'x', name: 'x', tier: 'user' };
    assert.equal(enforcer.check(mod), false);
    enforcer.updatePolicy({ allowManagedModsOnly: false });
    enforcer.clear();
    assert.equal(enforcer.check(mod), true);
  });

  it('clear resets the refused list', () => {
    const enforcer = new ManagedModsEnforcer({ allowManagedModsOnly: true });
    enforcer.check({ id: 'x', name: 'x', tier: 'user' });
    assert.equal(enforcer.getRefusedCount(), 1);
    enforcer.clear();
    assert.equal(enforcer.getRefusedCount(), 0);
  });
});
