import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  CUSTOMIZATION_SURFACES,
  parseStrictPluginOnlyCustomization,
  isRestrictedToPluginOnly,
  isSourceAdminTrusted,
  ADMIN_TRUSTED_SOURCES,
} from '../src/plugin-only-policy';

test('CUSTOMIZATION_SURFACES is the four lockable surfaces', () => {
  assert.deepEqual([...CUSTOMIZATION_SURFACES], ['skills', 'agents', 'hooks', 'mcp']);
});

test('parse: boolean values pass through unchanged', () => {
  assert.equal(parseStrictPluginOnlyCustomization(true), true);
  assert.equal(parseStrictPluginOnlyCustomization(false), false);
});

test('parse: array keeps known surfaces, drops unknown (forwards-compat, degrade to less-locked)', () => {
  // An old client seeing a future surface name must ignore it, not fail the whole file.
  assert.deepEqual(
    parseStrictPluginOnlyCustomization(['skills', 'commands', 'hooks']),
    ['skills', 'hooks'],
  );
  assert.deepEqual(parseStrictPluginOnlyCustomization(['agents', 'mcp']), ['agents', 'mcp']);
});

test('parse: array of only-unknown surfaces degrades to empty array (locks nothing)', () => {
  assert.deepEqual(parseStrictPluginOnlyCustomization(['commands', 'themes']), []);
});

test('parse: non-array invalid value degrades to undefined (unlocked-for-this-field, never everything-broken)', () => {
  assert.equal(parseStrictPluginOnlyCustomization('skills'), undefined);
  assert.equal(parseStrictPluginOnlyCustomization({ skills: true }), undefined);
  assert.equal(parseStrictPluginOnlyCustomization(42), undefined);
  assert.equal(parseStrictPluginOnlyCustomization(null), undefined);
  assert.equal(parseStrictPluginOnlyCustomization(undefined), undefined);
});

test('parse: array with non-string entries drops them', () => {
  assert.deepEqual(parseStrictPluginOnlyCustomization(['skills', 1, null, 'hooks']), ['skills', 'hooks']);
});

test('isRestrictedToPluginOnly: true locks every surface', () => {
  for (const s of CUSTOMIZATION_SURFACES) {
    assert.equal(isRestrictedToPluginOnly(s, true), true);
  }
});

test('isRestrictedToPluginOnly: array locks only listed surfaces', () => {
  assert.equal(isRestrictedToPluginOnly('hooks', ['skills', 'hooks']), true);
  assert.equal(isRestrictedToPluginOnly('mcp', ['skills', 'hooks']), false);
});

test('isRestrictedToPluginOnly: false / undefined / empty locks nothing', () => {
  assert.equal(isRestrictedToPluginOnly('hooks', false), false);
  assert.equal(isRestrictedToPluginOnly('hooks', undefined), false);
  assert.equal(isRestrictedToPluginOnly('hooks', []), false);
});

test('isRestrictedToPluginOnly: parses a raw (unparsed) value before deciding', () => {
  // Raw managed value as it lands from JSON: unknown surface dropped, hooks still locked.
  const raw = ['hooks', 'commands'];
  assert.equal(isRestrictedToPluginOnly('hooks', raw), true);
  assert.equal(isRestrictedToPluginOnly('mcp', raw), false);
  // Non-array invalid raw value degrades to unlocked, not a throw.
  assert.equal(isRestrictedToPluginOnly('hooks', 'hooks'), false);
});

test('isSourceAdminTrusted: plugin/managed/bundled/built-in are trusted, user/project are not', () => {
  for (const s of ['plugin', 'policySettings', 'built-in', 'builtin', 'bundled']) {
    assert.equal(isSourceAdminTrusted(s), true, s);
  }
  for (const s of ['userSettings', 'projectSettings', 'localSettings', 'project', 'user', undefined]) {
    assert.equal(isSourceAdminTrusted(s), false, String(s));
  }
  assert.ok(ADMIN_TRUSTED_SOURCES.has('plugin'));
});
