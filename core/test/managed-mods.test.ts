import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isModAllowed,
  parseModId,
  normalizeModName,
  modNamesMatch,
  validateModId,
  ManagedModsEnforcer,
  type ModInfo,
  type ManagedModsPolicy,
} from '../src/managed-mods';

const builtinMod: ModInfo = { name: 'core-tool', id: 'core-tool@builtin', tier: 'builtin' };
const managedMod: ModInfo = { name: 'org-tool', id: 'org-tool@managed', tier: 'managed' };
const userMod: ModInfo = { name: 'my-mod', id: 'my-mod@local', tier: 'user', marketplace: 'local' };
const remoteUserMod: ModInfo = { name: 'remote-mod', id: 'remote-mod@npm', tier: 'user', marketplace: 'npm' };

const activePolicy: ManagedModsPolicy = { allowManagedModsOnly: true };
const inactivePolicy: ManagedModsPolicy = { allowManagedModsOnly: false };

describe('isModAllowed', () => {
  it('allows all when allowManagedModsOnly is false', () => {
    assert.deepStrictEqual(isModAllowed(userMod, inactivePolicy), { allowed: true });
  });

  it('allows builtin tier', () => {
    const result = isModAllowed(builtinMod, activePolicy);
    assert.equal(result.allowed, true);
  });

  it('allows managed tier', () => {
    const result = isModAllowed(managedMod, activePolicy);
    assert.equal(result.allowed, true);
  });

  it('denies user tier when policy active', () => {
    const result = isModAllowed(userMod, activePolicy);
    assert.equal(result.allowed, false);
    assert.ok(result.reason);
  });

  it('allows user with enabledPlugins entry (true)', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': true },
    };
    const result = isModAllowed(userMod, policy);
    assert.equal(result.allowed, true);
  });

  it('allows user with enabledPlugins entry (version list)', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': ['1.0.0', '2.0.0'] },
    };
    const result = isModAllowed(userMod, policy);
    assert.equal(result.allowed, true);
  });

  it('denies user from remote marketplace even with enabledPlugins', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'remote-mod@npm': true },
    };
    const result = isModAllowed(remoteUserMod, policy);
    assert.equal(result.allowed, false);
    assert.ok(result.reason!.includes('remote marketplace'));
    assert.ok(result.reason!.includes('npm'));
  });

  it('allows user from local marketplace with enabledPlugins', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': true },
    };
    const mod: ModInfo = { name: 'my-mod', id: 'my-mod@local', tier: 'user', marketplace: 'local-folder' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, true);
  });

  it('allows user from inline marketplace with enabledPlugins', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@inline': true },
    };
    const mod: ModInfo = { name: 'my-mod', id: 'my-mod@inline', tier: 'user', marketplace: 'inline' };
    const result = isModAllowed(mod, policy);
    assert.equal(result.allowed, true);
  });

  it('reason message includes mod name', () => {
    const result = isModAllowed(userMod, activePolicy);
    assert.ok(result.reason!.includes('my-mod'));
  });

  it('denies user with empty enabledPlugins list for their id', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': [] },
    };
    const result = isModAllowed(userMod, policy);
    assert.equal(result.allowed, false);
  });

  it('denies user with enabledPlugins false', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'my-mod@local': false },
    };
    const result = isModAllowed(userMod, policy);
    assert.equal(result.allowed, false);
  });
});

describe('parseModId', () => {
  it('extracts name and marketplace', () => {
    assert.deepStrictEqual(parseModId('my-plugin@npm'), { name: 'my-plugin', marketplace: 'npm' });
  });

  it('handles no @ sign', () => {
    assert.deepStrictEqual(parseModId('standalone'), { name: 'standalone', marketplace: '' });
  });

  it('handles scoped names (@scope/name@npm)', () => {
    assert.deepStrictEqual(parseModId('@scope/name@npm'), { name: '@scope/name', marketplace: 'npm' });
  });

  it('handles id with only leading @', () => {
    assert.deepStrictEqual(parseModId('@onlyscope'), { name: '@onlyscope', marketplace: '' });
  });
});

describe('normalizeModName', () => {
  it('lowercases', () => {
    assert.equal(normalizeModName('MyPlugin'), 'myplugin');
  });

  it('collapses separators', () => {
    assert.equal(normalizeModName('my--plugin__name'), 'my_plugin_name');
  });

  it('strips special chars', () => {
    assert.equal(normalizeModName('my.plugin!v2'), 'my_plugin_v2');
  });

  it('handles empty string', () => {
    assert.equal(normalizeModName(''), '');
  });
});

describe('modNamesMatch', () => {
  it('matches equivalent names', () => {
    assert.equal(modNamesMatch('my-plugin', 'my_plugin'), true);
  });

  it('matches case-insensitive', () => {
    assert.equal(modNamesMatch('MyPlugin', 'myplugin'), true);
  });

  it('rejects different names', () => {
    assert.equal(modNamesMatch('plugin-a', 'plugin-b'), false);
  });
});

describe('validateModId', () => {
  it('accepts normal IDs', () => {
    assert.ok(validateModId('my-mod@npm'));
    assert.ok(validateModId('simple-plugin'));
    assert.ok(validateModId('@scope/pkg@npm'));
  });

  it('rejects __proto__', () => {
    assert.equal(validateModId('__proto__'), false);
  });

  it('rejects constructor in path', () => {
    assert.equal(validateModId('foo/constructor'), false);
  });

  it('rejects prototype in id', () => {
    assert.equal(validateModId('pkg@prototype'), false);
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

  it('accepts IDs containing but not matching dangerous strings', () => {
    assert.ok(validateModId('my-prototype-tool'));
    assert.ok(validateModId('constructors'));
  });
});

describe('ManagedModsEnforcer', () => {
  it('check allows builtin', () => {
    const enforcer = new ManagedModsEnforcer(activePolicy);
    assert.equal(enforcer.check(builtinMod), true);
  });

  it('check refuses user when policy active', () => {
    const enforcer = new ManagedModsEnforcer(activePolicy);
    assert.equal(enforcer.check(userMod), false);
  });

  it('getRefused tracks refused mods', () => {
    const enforcer = new ManagedModsEnforcer(activePolicy);
    enforcer.check(userMod);
    const refused = enforcer.getRefused();
    assert.equal(refused.length, 1);
    assert.equal(refused[0].mod.name, 'my-mod');
    assert.ok(refused[0].reason.includes('allowManagedModsOnly'));
  });

  it('getRefusedCount returns correct count', () => {
    const enforcer = new ManagedModsEnforcer(activePolicy);
    enforcer.check(builtinMod);
    enforcer.check(userMod);
    enforcer.check(remoteUserMod);
    assert.equal(enforcer.getRefusedCount(), 2);
  });

  it('clear resets refused list', () => {
    const enforcer = new ManagedModsEnforcer(activePolicy);
    enforcer.check(userMod);
    assert.equal(enforcer.getRefusedCount(), 1);
    enforcer.clear();
    assert.equal(enforcer.getRefusedCount(), 0);
  });

  it('updatePolicy switches enforcement', () => {
    const enforcer = new ManagedModsEnforcer(activePolicy);
    assert.equal(enforcer.check(userMod), false);
    enforcer.updatePolicy(inactivePolicy);
    assert.equal(enforcer.check(userMod), true);
  });

  it('static shouldBlock returns blocked:false when policy is null', () => {
    assert.deepEqual(ManagedModsEnforcer.shouldBlock(userMod, null), { blocked: false });
  });

  it('static shouldBlock returns blocked:false when allowManagedModsOnly is false', () => {
    assert.deepEqual(ManagedModsEnforcer.shouldBlock(userMod, inactivePolicy), { blocked: false });
  });

  it('static shouldBlock returns blocked:true for user mod with active policy', () => {
    const result = ManagedModsEnforcer.shouldBlock(userMod, activePolicy);
    assert.equal(result.blocked, true);
    assert.ok(result.reason);
  });

  it('static shouldBlock returns blocked:false for builtin mod', () => {
    const result = ManagedModsEnforcer.shouldBlock(builtinMod, activePolicy);
    assert.equal(result.blocked, false);
  });
});
