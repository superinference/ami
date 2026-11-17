import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { scopedToolAllowRules } from '../src/skills';
import { skillTool } from '../src/tools/skill';
import { normalizeToolName } from '../src/permissions';
import type { ToolContext } from '../src/types';

// ===========================================================================
// GAP U023 / M0 (medium): Least-privilege, PATH-scoped tool allowlist for a
// command/skill-spawned subagent (parity with Claude Code's
// `allowedTools: ['Read(~/**)', 'Edit(~/.claude/settings.json)']`). AMI's
// name-only filter (filterToolsForSkill) says WHICH tools a forked skill gets;
// scopedToolAllowRules adds the WHICH-ARGUMENTS half as additive permission
// ALLOW rules wired into the forked sub-engine.
// ===========================================================================

describe('scopedToolAllowRules — parse path/argument-scoped allowlist entries', () => {
  it('returns [] for undefined / empty input', () => {
    assert.deepEqual(scopedToolAllowRules(undefined), []);
    assert.deepEqual(scopedToolAllowRules([]), []);
  });

  it('ignores a bare tool name (no scope → name filter governs)', () => {
    assert.deepEqual(scopedToolAllowRules(['Edit', 'bash']), []);
  });

  it('ignores a category token (selects a class, not an argument scope)', () => {
    // category tokens never carry parens, so they produce no scoped rule
    assert.deepEqual(scopedToolAllowRules(['read-only', 'edit', 'execution']), []);
  });

  it('ignores an empty or bare-* scope (unrestricted)', () => {
    assert.deepEqual(scopedToolAllowRules(['Edit()', 'Read(*)', 'Bash( * )']), []);
  });

  it('maps Tool(scope) → an allow rule with the scope as the arg pattern', () => {
    assert.deepEqual(scopedToolAllowRules(['Read(~/**)']), [
      { tool: 'Read', pattern: '~/**', action: 'allow' },
    ]);
  });

  it('produces one rule per scoped entry, in order', () => {
    assert.deepEqual(
      scopedToolAllowRules(['Read(~/**)', 'Edit(~/.claude/settings.json)']),
      [
        { tool: 'Read', pattern: '~/**', action: 'allow' },
        { tool: 'Edit', pattern: '~/.claude/settings.json', action: 'allow' },
      ],
    );
  });

  it('applies the normalize callback to the authored tool name', () => {
    assert.deepEqual(scopedToolAllowRules(['Read(~/**)'], normalizeToolName), [
      { tool: 'file_read', pattern: '~/**', action: 'allow' },
    ]);
  });

  it('keeps a Bash command-prefix scope verbatim as the pattern', () => {
    assert.deepEqual(scopedToolAllowRules(['Bash(git commit:*)'], normalizeToolName), [
      { tool: 'bash', pattern: 'git commit:*', action: 'allow' },
    ]);
  });

  it('de-duplicates identical tool+scope pairs', () => {
    assert.deepEqual(scopedToolAllowRules(['Read(~/**)', 'Read(~/**)']), [
      { tool: 'Read', pattern: '~/**', action: 'allow' },
    ]);
  });

  it('skips non-string and malformed (unterminated-paren) entries', () => {
    assert.deepEqual(
      scopedToolAllowRules(['Read(~/**)', 42 as unknown as string, 'Edit(oops', '()'] as string[]),
      [{ tool: 'Read', pattern: '~/**', action: 'allow' }],
    );
  });
});

// ---------------------------------------------------------------------------
// End-to-end wiring: a forked skill's scoped allowlist becomes additive ALLOW
// rules on the spawned sub-engine (prepended; parent deny rules still win since
// a matching deny unconditionally wins in the permission engine).
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function fakeEngineFactory(capture: (cfg: any) => void) {
  return (cfg: any) => {
    capture(cfg);
    return {
      submit: async function* () {
        yield { type: 'text_delta', text: 'ok' };
      },
    };
  };
}

function manager(skill: any) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: () => 'do the thing',
    listSkills: () => [skill],
    getAgent: () => null,
    listAgents: () => [],
  } as any;
}

describe('skillTool.execute — forked sub-engine honors a path-scoped allowlist', () => {
  it('prepends the skill scoped allow rules ahead of the parent rules', async () => {
    let captured: any = null;
    const parentRules = [{ tool: 'bash', pattern: 'rm *', action: 'deny' as const }];
    await skillTool.execute(
      { skill: 'scoped' },
      ctx({
        _skillManager: manager({
          name: 'scoped',
          context: 'fork',
          allowedTools: ['Read(~/**)', 'Edit(~/.claude/settings.json)'],
        }),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _parentPermissionRules: parentRules,
        _engineFactory: fakeEngineFactory(c => { captured = c; }),
      }),
    );
    assert.deepEqual(captured.permissionRules, [
      { tool: 'file_read', pattern: '~/**', action: 'allow' },
      { tool: 'file_edit', pattern: '~/.claude/settings.json', action: 'allow' },
      { tool: 'bash', pattern: 'rm *', action: 'deny' },
    ]);
  });

  it('forwards parent rules unchanged when the skill declares no scoped allowlist', async () => {
    let captured: any = null;
    const parentRules = [{ tool: 'bash', pattern: 'rm *', action: 'deny' as const }];
    await skillTool.execute(
      { skill: 'plain' },
      ctx({
        _skillManager: manager({ name: 'plain', context: 'fork', allowedTools: ['Read'] }),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _parentPermissionRules: parentRules,
        _engineFactory: fakeEngineFactory(c => { captured = c; }),
      }),
    );
    assert.deepEqual(captured.permissionRules, parentRules);
  });
});
