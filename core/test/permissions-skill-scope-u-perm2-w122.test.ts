import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'path';
import * as os from 'os';
import {
  getSkillScopeRule,
  buildPermissionSuggestions,
  PermissionManager,
  type PermissionUpdate,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U-perm2 / w122 — Skill-scoped permission narrowing (parity with the reference's
// getClaudeSkillScope, utils/permissions/filesystem.ts L101-157). A write under
// `<configRoot>/skills/<name>/` now offers a NARROW least-privilege "allow edits
// to this skill only" session grant, instead of only the broad config-folder
// grant that also exposes settings.json / hooks/ / other skills. Includes the
// traversal + glob-metachar rejection the reference guards.
// ---------------------------------------------------------------------------

const cwd = path.resolve('/tmp/u-perm2-ws');

function addRuleContents(suggestions: PermissionUpdate[]): string[] {
  const out: string[] = [];
  for (const s of suggestions) {
    if (s.type === 'addRules') {
      for (const r of s.rules) out.push(r.ruleContent);
    }
  }
  return out;
}

test('getSkillScopeRule: a file INSIDE a project .ami skill dir yields a narrow scoped rule', () => {
  const fp = path.join(cwd, '.ami', 'skills', 'myskill', 'SKILL.md');
  const scope = getSkillScopeRule(fp, cwd);
  assert.ok(scope, 'a file under .ami/skills/<name>/ is skill-scoped');
  assert.equal(scope!.skillName, 'myskill');
  assert.equal(scope!.skillDir, path.join(cwd, '.ami', 'skills', 'myskill'));
  assert.equal(scope!.ruleContent, `${path.join(cwd, '.ami', 'skills', 'myskill')}${path.sep}*`);
});

test('getSkillScopeRule: a nested file under .claude skill dir keeps the first segment as the skill name', () => {
  const fp = path.join(cwd, '.claude', 'skills', 'foo', 'nested', 'deep.ts');
  const scope = getSkillScopeRule(fp, cwd);
  assert.ok(scope);
  assert.equal(scope!.skillName, 'foo');
  assert.equal(scope!.ruleContent, `${path.join(cwd, '.claude', 'skills', 'foo')}${path.sep}*`);
});

test('getSkillScopeRule: a GLOBAL ~/.claude/skills/<name>/ file is scoped too', () => {
  const fp = path.join(os.homedir(), '.claude', 'skills', 'g', 'SKILL.md');
  const scope = getSkillScopeRule(fp, cwd);
  assert.ok(scope);
  assert.equal(scope!.skillName, 'g');
  assert.equal(scope!.skillDir, path.join(os.homedir(), '.claude', 'skills', 'g'));
});

test('getSkillScopeRule: a loose file DIRECTLY under skills/ (no skill segment) is NOT scoped', () => {
  const fp = path.join(cwd, '.ami', 'skills', 'README.md');
  assert.equal(getSkillScopeRule(fp, cwd), null);
});

test('getSkillScopeRule: a `..` traversal skill segment is rejected (falls back to broad grant)', () => {
  const fp = path.join(cwd, '.ami', 'skills') + path.sep + '..skill' + path.sep + 'x.md';
  // `..skill` contains `..`
  assert.equal(getSkillScopeRule(fp, cwd), null);
});

test('getSkillScopeRule: a `.` skill segment is rejected', () => {
  const fp = path.join(cwd, '.claude', 'skills') + path.sep + '.' + path.sep + 'x.md';
  assert.equal(getSkillScopeRule(fp, cwd), null);
});

test('getSkillScopeRule: a glob-metachar skill name is rejected (would widen to ALL skills)', () => {
  for (const bad of ['*', 'a?b', 'a[b]']) {
    const fp = path.join(cwd, '.ami', 'skills') + path.sep + bad + path.sep + 'x.md';
    assert.equal(getSkillScopeRule(fp, cwd), null, `skill name ${bad} must be rejected`);
  }
});

test('getSkillScopeRule: a path outside any skill dir is null; empty input is null', () => {
  assert.equal(getSkillScopeRule(path.join(cwd, '.ami', 'settings.json'), cwd), null);
  assert.equal(getSkillScopeRule(path.join(cwd, 'src', 'index.ts'), cwd), null);
  assert.equal(getSkillScopeRule('', cwd), null);
});

test('buildPermissionSuggestions: a skill-file edit offers the NARROW skill rule BEFORE the broad config-folder rule', () => {
  const fp = path.join(process.cwd(), '.ami', 'skills', 'myskill', 'SKILL.md');
  const suggestions = buildPermissionSuggestions('file_edit', { file_path: fp });
  const contents = addRuleContents(suggestions);
  const narrow = `${path.join(process.cwd(), '.ami', 'skills', 'myskill')}${path.sep}*`;
  const broad = `${path.join(process.cwd(), '.ami')}${path.sep}*`;
  assert.ok(contents.includes(narrow), 'the narrow skill-scoped rule is offered');
  assert.ok(contents.includes(broad), 'the broad config-folder rule is still offered (additive)');
  // Least privilege first: the narrow rule precedes the broad one.
  assert.ok(contents.indexOf(narrow) < contents.indexOf(broad), 'narrow before broad');
  // The exact-path rule and the acceptEdits setMode affordance are preserved.
  assert.ok(contents.includes(fp), 'the exact-path allow rule is preserved');
  assert.ok(suggestions.some(s => s.type === 'setMode'), 'the acceptEdits affordance is preserved');
});

test('buildPermissionSuggestions: a NON-skill config file offers only the broad grant (no narrow rule)', () => {
  const fp = path.join(process.cwd(), '.ami', 'settings.json');
  const suggestions = buildPermissionSuggestions('file_write', { file_path: fp });
  const contents = addRuleContents(suggestions);
  const broad = `${path.join(process.cwd(), '.ami')}${path.sep}*`;
  assert.ok(contents.includes(broad), 'broad config-folder grant offered');
  // No skill-scoped rule for a non-skill path.
  assert.ok(!contents.some(c => c.includes(`${path.sep}skills${path.sep}`)), 'no skill rule for a non-skill file');
});

test('e2e: the narrow skill-scoped suggestion is a valid applicable PermissionUpdate (wired end-to-end)', async () => {
  const fp = path.join(process.cwd(), '.claude', 'skills', 'demo', 'SKILL.md');
  const suggestions = buildPermissionSuggestions('file_edit', { file_path: fp });
  const narrow = `${path.join(process.cwd(), '.claude', 'skills', 'demo')}${path.sep}*`;
  const skillSuggestion = suggestions.find(
    s => s.type === 'addRules' && s.rules.some(r => r.ruleContent === narrow),
  );
  assert.ok(skillSuggestion, 'the skill-scoped suggestion exists');
  const pm = new PermissionManager('ask');
  const { applied } = pm.applyUpdate(skillSuggestion!);
  assert.equal(applied, 1, 'the skill-scoped suggestion applies cleanly through applyUpdate');
  // After applying, a file_edit on a file inside that skill is auto-allowed, while a
  // SIBLING skill (not granted) is NOT — proving the narrowing is real, not blanket.
  const inScope = await pm.check('file_edit', { file_path: fp });
  assert.equal(inScope, 'allow', 'an edit inside the granted skill is allowed');
  const sibling = path.join(process.cwd(), '.claude', 'skills', 'other', 'SKILL.md');
  const outScope = await pm.check('file_edit', { file_path: sibling });
  assert.notEqual(outScope, 'allow', 'a sibling skill is NOT covered by the narrow grant');
});
