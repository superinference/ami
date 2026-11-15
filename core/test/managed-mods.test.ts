import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isModAllowed,
  isModBlockedByPolicy,
  isLocalOrInlineSource,
  getManagedPluginNames,
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

describe('isModBlockedByPolicy (unconditional org force-disable)', () => {
  it('returns true when enabledPlugins[id] === false', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: false, enabledPlugins: { 'my-mod@local': false } };
    assert.equal(isModBlockedByPolicy('my-mod@local', policy), true);
  });

  it('returns false for a true / allow-array / absent entry', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: false,
      enabledPlugins: { 'a@m': true, 'b@m': ['owner/repo'] },
    };
    assert.equal(isModBlockedByPolicy('a@m', policy), false);
    assert.equal(isModBlockedByPolicy('b@m', policy), false);
    assert.equal(isModBlockedByPolicy('missing@m', policy), false);
  });

  it('returns false for null/undefined policy', () => {
    assert.equal(isModBlockedByPolicy('x@m', null), false);
    assert.equal(isModBlockedByPolicy('x@m', undefined), false);
  });

  it('force-disables via isModAllowed even when allowManagedModsOnly is OFF', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: false, enabledPlugins: { 'my-mod@local': false } };
    const result = isModAllowed(userMod, policy);
    assert.equal(result.allowed, false);
    assert.match(result.reason!, /disabled by your organization's policy/);
  });

  it('force-disables a builtin/managed mod too (no tier escape)', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: true, enabledPlugins: { 'org-tool@managed': false } };
    assert.equal(isModAllowed(managedMod, policy).allowed, false);
  });

  it('ManagedModsEnforcer.shouldBlock force-disables with allowManagedModsOnly OFF', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: false, enabledPlugins: { 'my-mod@local': false } };
    const verdict = ManagedModsEnforcer.shouldBlock(userMod, policy);
    assert.equal(verdict.blocked, true);
    assert.match(verdict.reason!, /disabled by your organization's policy/);
  });

  it('ManagedModsEnforcer.shouldBlock still allows a non-listed mod with allowManagedModsOnly OFF', () => {
    const policy: ManagedModsPolicy = { allowManagedModsOnly: false, enabledPlugins: { 'other@m': false } };
    assert.deepEqual(ManagedModsEnforcer.shouldBlock(userMod, policy), { blocked: false });
  });
});

describe('isLocalOrInlineSource (non-spoofable provenance vouch)', () => {
  it('typed source is authoritative: local / inline => true, remote => false', () => {
    assert.equal(isLocalOrInlineSource({ name: 'm', id: 'm@x', tier: 'user', source: 'local' }), true);
    assert.equal(isLocalOrInlineSource({ name: 'm', id: 'm@x', tier: 'user', source: 'inline' }), true);
    assert.equal(isLocalOrInlineSource({ name: 'm', id: 'm@x', tier: 'user', source: 'remote' }), false);
  });

  it('typed remote source overrides a spoofed local-looking marketplace name', () => {
    const spoof: ModInfo = { name: 'm', id: 'm@x', tier: 'user', marketplace: 'local-attacker', source: 'remote' };
    assert.equal(isLocalOrInlineSource(spoof), false);
  });

  it('legacy fallback: absent marketplace is treated as a local in-repo copy', () => {
    assert.equal(isLocalOrInlineSource({ name: 'm', id: 'm@x', tier: 'user' }), true);
  });

  it('legacy fallback: local/inline display-name prefix honored, remote rejected', () => {
    assert.equal(isLocalOrInlineSource({ name: 'm', id: 'm@x', tier: 'user', marketplace: 'local-folder' }), true);
    assert.equal(isLocalOrInlineSource({ name: 'm', id: 'm@x', tier: 'user', marketplace: 'inline' }), true);
    assert.equal(isLocalOrInlineSource({ name: 'm', id: 'm@x', tier: 'user', marketplace: 'npm' }), false);
  });
});

describe('isModAllowed — non-spoofable source vouch under allowManagedModsOnly', () => {
  it('BLOCKS a remote-sourced mod that spoofs a local marketplace name', () => {
    // Pre-fix, marketplace="local-evil".startsWith("local") vouched this remote mod as
    // local and ALLOWED it; the typed source='remote' must now block it.
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'spoof@mkt': true },
    };
    const spoof: ModInfo = {
      name: 'spoof',
      id: 'spoof@mkt',
      tier: 'user',
      marketplace: 'local-evil',
      source: 'remote',
    };
    const result = isModAllowed(spoof, policy);
    assert.equal(result.allowed, false);
    assert.ok(result.reason!.includes('remote marketplace'));
  });

  it('ALLOWS a mod stamped source=local even if its marketplace name looks remote', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'trusted@npm-mirror': true },
    };
    const mod: ModInfo = {
      name: 'trusted',
      id: 'trusted@npm-mirror',
      tier: 'user',
      marketplace: 'npm-mirror',
      source: 'local',
    };
    assert.equal(isModAllowed(mod, policy).allowed, true);
  });

  it('ALLOWS a mod stamped source=inline', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: true,
      enabledPlugins: { 'inl@here': true },
    };
    const mod: ModInfo = { name: 'inl', id: 'inl@here', tier: 'user', marketplace: 'anything', source: 'inline' };
    assert.equal(isModAllowed(mod, policy).allowed, true);
  });
});

describe('getManagedPluginNames (org-locked name set)', () => {
  it('collects every boolean (true OR false) entry, excludes allow-arrays', () => {
    const policy: ManagedModsPolicy = {
      allowManagedModsOnly: false,
      enabledPlugins: { 'on@m': true, 'off@m': false, 'arr@m': ['owner/repo'] },
    };
    const names = getManagedPluginNames(policy);
    assert.equal(names.has('on@m'), true);
    assert.equal(names.has('off@m'), true);
    assert.equal(names.has('arr@m'), false);
    assert.equal(names.size, 2);
  });

  it('returns an empty set for null/undefined or empty policy', () => {
    assert.equal(getManagedPluginNames(null).size, 0);
    assert.equal(getManagedPluginNames(undefined).size, 0);
    assert.equal(getManagedPluginNames({ allowManagedModsOnly: false }).size, 0);
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
