import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  AUTO_MODE_DEFAULT_RULES,
  getDefaultExternalAutoModeRules,
  resolveAutoModeRules,
  buildAutoModeClassifierPrompt,
  buildAutoModeClassifierPromptFromConfig,
  type AutoModeRules,
} from '../src/auto-mode';
import { describeRule, PermissionManager, type PermissionRule } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w76): auto-mode (YOLO) natural-language classifier rules + plain-language
// permission-rule previews + managed-rule immutability on removal.
//
// Parity: Claude's settings.autoMode / yoloClassifier (three declarative rule
// categories: allow / soft_deny / environment with per-section REPLACE merge),
// PermissionRuleDescription (describeRule), and the managed policySettings tier
// being read-only to the user (removeRule refuses to delete it).
// ---------------------------------------------------------------------------

test('auto-mode defaults expose all three non-empty categories and are frozen', () => {
  assert.ok(AUTO_MODE_DEFAULT_RULES.allow.length > 0);
  assert.ok(AUTO_MODE_DEFAULT_RULES.soft_deny.length > 0);
  assert.ok(AUTO_MODE_DEFAULT_RULES.environment.length > 0);
  assert.ok(Object.isFrozen(AUTO_MODE_DEFAULT_RULES));
});

test('getDefaultExternalAutoModeRules returns a mutable copy, not the frozen singleton', () => {
  const a = getDefaultExternalAutoModeRules();
  const b = getDefaultExternalAutoModeRules();
  assert.deepEqual(a, { ...AUTO_MODE_DEFAULT_RULES });
  assert.notEqual(a.allow, AUTO_MODE_DEFAULT_RULES.allow); // distinct array
  a.allow.push('mutated');
  assert.ok(!b.allow.includes('mutated')); // copies are independent
  assert.ok(!AUTO_MODE_DEFAULT_RULES.allow.includes('mutated'));
});

test('resolveAutoModeRules with no config returns the defaults', () => {
  assert.deepEqual(resolveAutoModeRules(), getDefaultExternalAutoModeRules());
  assert.deepEqual(resolveAutoModeRules(null), getDefaultExternalAutoModeRules());
});

test('resolveAutoModeRules: a non-empty user section REPLACES defaults; empty/absent falls through', () => {
  const rules = resolveAutoModeRules({ soft_deny: ['Never run rm'] });
  // soft_deny fully replaced (not merged with defaults)
  assert.deepEqual(rules.soft_deny, ['Never run rm']);
  // absent sections keep defaults
  assert.deepEqual(rules.allow, AUTO_MODE_DEFAULT_RULES.allow);
  assert.deepEqual(rules.environment, AUTO_MODE_DEFAULT_RULES.environment);

  // an explicitly-empty section falls through to defaults
  const emptyAllow = resolveAutoModeRules({ allow: [], soft_deny: ['x'] });
  assert.deepEqual(emptyAllow.allow, AUTO_MODE_DEFAULT_RULES.allow);
  assert.deepEqual(emptyAllow.soft_deny, ['x']);
});

test('resolveAutoModeRules does not alias the caller config arrays', () => {
  const config = { allow: ['read only'] };
  const rules = resolveAutoModeRules(config);
  assert.notEqual(rules.allow, config.allow);
  rules.allow.push('tampered');
  assert.deepEqual(config.allow, ['read only']);
});

test('buildAutoModeClassifierPrompt renders each category and marks empty ones (none)', () => {
  const rules: AutoModeRules = {
    allow: ['Read files in the workspace'],
    soft_deny: ['Delete anything'],
    environment: [],
  };
  const prompt = buildAutoModeClassifierPrompt(rules);
  assert.match(prompt, /security classifier/i);
  assert.match(prompt, /- Read files in the workspace/);
  assert.match(prompt, /- Delete anything/);
  // the empty environment section is explicitly surfaced
  assert.match(prompt, /Environment context[^]*\(none\)/);
});

test('buildAutoModeClassifierPromptFromConfig == build(resolve(config)) and surfaces soft_deny', () => {
  const config = { soft_deny: ['Never push to origin'] };
  assert.equal(
    buildAutoModeClassifierPromptFromConfig(config),
    buildAutoModeClassifierPrompt(resolveAutoModeRules(config)),
  );
  assert.match(buildAutoModeClassifierPromptFromConfig(config), /- Never push to origin/);
  // with no config the default soft_deny rules still appear
  assert.match(buildAutoModeClassifierPromptFromConfig(), /Require confirmation/i);
});

// ---- describeRule -----------------------------------------------------------

test('describeRule renders a plain-language preview per action', () => {
  assert.equal(
    describeRule({ tool: 'bash', pattern: 'git status', action: 'allow' }),
    'Allow bash matching "git status"',
  );
  assert.equal(
    describeRule({ tool: 'file_edit', pattern: '/etc/**', action: 'deny', source: 'policySettings' }),
    'Deny file_edit matching "/etc/**" (from enterprise managed settings)',
  );
  assert.equal(
    describeRule({ tool: '*', action: 'ask' }),
    'Ask before any tool calls',
  );
  assert.equal(
    describeRule({ tool: 'bash', pattern: '*', action: 'allow' }),
    'Allow bash calls',
  );
});

test('describeRule describes paramPattern rules', () => {
  assert.equal(
    describeRule({ tool: 'Task', paramPattern: { model: 'opus*' }, action: 'allow' }),
    'Allow Task with model="opus*"',
  );
});

// ---- managed-rule immutability on removeRule --------------------------------

test('removeRule never deletes managed (policySettings) rules, even when the predicate matches', () => {
  const managed: PermissionRule = { tool: 'bash', pattern: 'rm *', action: 'deny', source: 'policySettings' };
  const session: PermissionRule = { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'session' };
  const mgr = new PermissionManager('ask', [managed, session]);

  // Predicate matches EVERYTHING — the managed rule must still survive.
  const removed = mgr.removeRule(() => true);
  assert.equal(removed, 1); // only the session rule was removed
  const remaining = mgr.getRules();
  assert.equal(remaining.length, 1);
  assert.equal(remaining[0]!.source, 'policySettings');
  assert.equal(remaining[0]!.pattern, 'rm *');
});

test('removeRule still removes non-managed rules normally', () => {
  const mgr = new PermissionManager('ask', [
    { tool: 'bash', pattern: 'ls *', action: 'allow', source: 'session' },
    { tool: 'bash', pattern: 'cat *', action: 'allow', source: 'userSettings' },
  ]);
  const removed = mgr.removeRule(r => r.tool === 'bash');
  assert.equal(removed, 2);
  assert.equal(mgr.getRules().length, 0);
});
