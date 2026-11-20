import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  isSharedSettingSource,
  permissionRuleSourceDisplayString,
  type PermissionRuleSource,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// detectShadowedRules — rich UnreachableRule output (shadowType + reason + fix),
// source attribution, sandbox-auto-allow carve-out, single-finding-per-rule.
// Parity: the reference's utils/permissions/shadowedRuleDetection.ts.
// ---------------------------------------------------------------------------

test('isSharedSettingSource: shared tiers (project/policy/command) vs personal', () => {
  for (const s of ['projectSettings', 'policySettings', 'command'] as PermissionRuleSource[]) {
    assert.equal(isSharedSettingSource(s), true, `${s} must be shared`);
  }
  for (const s of ['userSettings', 'localSettings', 'cliArg', 'session', 'flagSettings'] as PermissionRuleSource[]) {
    assert.equal(isSharedSettingSource(s), false, `${s} must be personal`);
  }
  // undefined is treated as personal (the safe default — never attributed to the team).
  assert.equal(isSharedSettingSource(undefined), false);
});

test('permissionRuleSourceDisplayString: prose per source + unknown fallback', () => {
  assert.equal(permissionRuleSourceDisplayString('projectSettings'), 'project settings');
  assert.equal(permissionRuleSourceDisplayString('policySettings'), 'enterprise managed settings');
  assert.equal(permissionRuleSourceDisplayString('localSettings'), 'local settings');
  assert.equal(permissionRuleSourceDisplayString('command'), 'slash command');
  assert.equal(permissionRuleSourceDisplayString(undefined), 'an unknown source');
});

test('detectShadowedRules: deny shadow carries shadowType=deny + reason + fix naming both sources', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'localSettings' },
    { tool: 'bash', action: 'deny', source: 'projectSettings' },
  ]);
  const shadows = pm.detectShadowedRules();
  assert.equal(shadows.length, 1);
  const u = shadows[0]!;
  assert.equal(u.shadowType, 'deny');
  assert.equal(u.rule.pattern, 'ls *');
  assert.equal(u.shadowedBy.action, 'deny');
  assert.equal(u.reason, 'Blocked by "bash" deny rule (from project settings)');
  assert.equal(
    u.fix,
    'Remove the "bash" deny rule from project settings, or remove the specific allow rule from local settings',
  );
});

test('detectShadowedRules: ask shadow (preceding) carries shadowType=ask + reason', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'ask', source: 'userSettings' },
    { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'projectSettings' },
  ]);
  const shadows = pm.detectShadowedRules();
  assert.equal(shadows.length, 1);
  assert.equal(shadows[0]!.shadowType, 'ask');
  assert.equal(shadows[0]!.reason, 'Shadowed by "bash" ask rule (from user settings)');
  assert.match(shadows[0]!.fix, /Remove the "bash" ask rule from user settings/);
});

test('detectShadowedRules: deny beats ask — single finding per rule, deny preferred', () => {
  // Both a preceding ask AND a deny shadow the same allow; only the deny finding emits.
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'ask', source: 'userSettings' },
    { tool: 'bash', action: 'deny', source: 'policySettings' },
    { tool: 'bash', pattern: 'ls *', action: 'allow' },
  ]);
  const shadows = pm.detectShadowedRules();
  assert.equal(shadows.length, 1);
  assert.equal(shadows[0]!.shadowType, 'deny');
});

test('detectShadowedRules: sandbox carve-out — personal bash ask does NOT shadow when sandbox auto-allow on', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'ask', source: 'userSettings' }, // personal
    { tool: 'bash', pattern: 'ls *', action: 'allow' },
  ]);
  // Default (no sandbox) — still shadowed.
  assert.equal(pm.detectShadowedRules().length, 1);
  // With sandbox auto-allow, a PERSONAL ask rule no longer shadows.
  assert.equal(pm.detectShadowedRules({ sandboxAutoAllowEnabled: true }).length, 0);
});

test('detectShadowedRules: sandbox carve-out does NOT apply to shared-source bash ask rules', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'ask', source: 'projectSettings' }, // shared
    { tool: 'bash', pattern: 'ls *', action: 'allow' },
  ]);
  // Shared ask rule still warns even with sandbox on (teammates may lack the sandbox).
  assert.equal(pm.detectShadowedRules({ sandboxAutoAllowEnabled: true }).length, 1);
  assert.equal(pm.detectShadowedRules({ sandboxAutoAllowEnabled: true })[0]!.shadowType, 'ask');
});

test('detectShadowedRules: carve-out only affects bash, not other tools', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'web_fetch', action: 'ask', source: 'userSettings' },
    { tool: 'web_fetch', pattern: 'https://x/*', action: 'allow' },
  ]);
  // A non-bash personal ask rule is unaffected by the bash sandbox carve-out.
  assert.equal(pm.detectShadowedRules({ sandboxAutoAllowEnabled: true }).length, 1);
});

test('detectShadowedRules: sandbox carve-out auto-derived from the manager sandbox policy', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'ask', source: 'userSettings' },
    { tool: 'bash', pattern: 'ls *', action: 'allow' },
  ]);
  pm.setSandboxPolicy({ mode: 'container', autoAllowBashIfSandboxed: true });
  // No explicit option — carve-out derived from the policy makes the ask not shadow.
  assert.equal(pm.detectShadowedRules().length, 0);
  // Policy without the opt-in knob does not enable the carve-out.
  pm.setSandboxPolicy({ mode: 'container' });
  assert.equal(pm.detectShadowedRules().length, 1);
});

test('detectShadowedRules: no source tag → reason/fix cite "an unknown source"', () => {
  const pm = new PermissionManager('ask', [
    { tool: 'bash', action: 'deny' },
    { tool: 'bash', pattern: 'ls *', action: 'allow' },
  ]);
  const u = pm.detectShadowedRules()[0]!;
  assert.equal(u.reason, 'Blocked by "bash" deny rule (from an unknown source)');
  assert.match(u.fix, /from an unknown source/);
});
