import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  PROMPT_RULE_PREFIX,
  createPromptRuleContent,
  isPromptRuleContent,
  extractPromptDescription,
  managedRuleReadOnlyNotice,
  describeRule,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003/w89 — natural-language "prompt rule" content codec + managed/policy
// rule read-only notice.
//
// Parity:
//  - Claude's utils/permissions/bashClassifier.ts: PROMPT_PREFIX /
//    createPromptRuleContent(description) / extractPromptDescription(content).
//    The "yes, and don't ask again — described as …" classifier path persists a
//    reusable natural-language rule (`prompt: <description>`) instead of a glob.
//  - Claude's components/permissions/rules/PermissionRuleList.tsx "Rule details"
//    view: a policySettings rule shows "This rule is configured by managed
//    settings and cannot be modified. Contact your system administrator…" and is
//    not user-editable.
// ---------------------------------------------------------------------------

describe('prompt-rule content codec', () => {
  it('PROMPT_RULE_PREFIX matches the reference PROMPT_PREFIX', () => {
    assert.equal(PROMPT_RULE_PREFIX, 'prompt:');
  });

  it('createPromptRuleContent encodes a trimmed description', () => {
    assert.equal(createPromptRuleContent('run the test suite'), 'prompt: run the test suite');
    assert.equal(createPromptRuleContent('  run the test suite  '), 'prompt: run the test suite');
  });

  it('round-trips through extractPromptDescription', () => {
    const content = createPromptRuleContent('deploy to staging');
    assert.equal(extractPromptDescription(content), 'deploy to staging');
  });

  it('isPromptRuleContent recognizes prompt content (whitespace tolerant)', () => {
    assert.equal(isPromptRuleContent('prompt: x'), true);
    assert.equal(isPromptRuleContent('  prompt: x'), true);
    assert.equal(isPromptRuleContent('git:*'), false);
    assert.equal(isPromptRuleContent(undefined), false);
  });

  it('extractPromptDescription returns null for non-prompt / empty / non-string', () => {
    assert.equal(extractPromptDescription('git:*'), null);
    assert.equal(extractPromptDescription('prompt:   '), null);
    assert.equal(extractPromptDescription(undefined), null);
    // typed as string for the API, but guard against a runtime non-string too
    assert.equal(extractPromptDescription(42 as unknown as string), null);
  });
});

describe('describeRule renders prompt rules as prose', () => {
  it('shows the description instead of the raw glob', () => {
    // RED before wiring: describeRule treated the pattern as a glob and produced
    //   `Allow bash matching "prompt: run the tests"`.
    const out = describeRule({
      tool: 'bash',
      pattern: createPromptRuleContent('run the tests'),
      action: 'allow',
    });
    assert.equal(out, 'Allow bash commands described as "run the tests"');
  });

  it('includes the origin tier for a sourced prompt rule', () => {
    const out = describeRule({
      tool: 'bash',
      pattern: createPromptRuleContent('run the tests'),
      action: 'allow',
      source: 'projectSettings',
    });
    assert.equal(out, 'Allow bash commands described as "run the tests" (from project settings)');
  });

  it('still renders ordinary glob rules unchanged', () => {
    assert.equal(
      describeRule({ tool: 'bash', pattern: 'git status', action: 'allow' }),
      'Allow bash matching "git status"',
    );
  });
});

describe('managedRuleReadOnlyNotice', () => {
  it('returns the managed read-only notice for a policySettings rule', () => {
    const notice = managedRuleReadOnlyNotice('policySettings');
    assert.ok(notice);
    assert.match(notice, /cannot be modified/);
    assert.match(notice, /system administrator/);
  });

  it('returns null for user-editable sources and undefined', () => {
    assert.equal(managedRuleReadOnlyNotice('projectSettings'), null);
    assert.equal(managedRuleReadOnlyNotice('userSettings'), null);
    assert.equal(managedRuleReadOnlyNotice('session'), null);
    assert.equal(managedRuleReadOnlyNotice(undefined), null);
  });
});

describe('PermissionManager rule editability', () => {
  it('flags managed rules as read-only and surfaces the notice', () => {
    const pm = new PermissionManager('ask');
    const managedRule = { tool: 'file_edit', pattern: '/etc/**', action: 'deny' as const, source: 'policySettings' as const };
    assert.equal(pm.isRuleEditable(managedRule), false);
    assert.ok(pm.ruleReadOnlyNotice(managedRule));
  });

  it('treats a user-sourced rule as editable with no notice', () => {
    const pm = new PermissionManager('ask');
    const userRule = { tool: 'bash', pattern: 'git status', action: 'allow' as const, source: 'userSettings' as const };
    assert.equal(pm.isRuleEditable(userRule), true);
    assert.equal(pm.ruleReadOnlyNotice(userRule), null);
  });
});
