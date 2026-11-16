import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  skillTool,
  filterToolsForSkill,
  type SkillToolFilterWarning,
} from '../src/tools/skill';
import { resolveSkillModelOverride } from '../src/model-capabilities';
import { createDefaultTools } from '../src/tools/index';
import type { SkillDefinition } from '../src/skills';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const allTools = createDefaultTools('/tmp').getAll();

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function fakeEngineFactory(events: Array<{ type: string; [k: string]: any }>) {
  return (_cfg: any) => ({
    submit: async function* (_prompt: string) {
      for (const e of events) yield e;
    },
  });
}

// ===========================================================================
// GAP: resolveSkillModelOverride — carry [1m] 1M-context suffix on skill model
// ===========================================================================

describe('resolveSkillModelOverride — carries the session [1m] suffix', () => {
  it('carries [1m] when the session has it and the skill family supports 1M', () => {
    assert.equal(
      resolveSkillModelOverride('claude-opus-5', 'claude-opus-5[1m]'),
      'claude-opus-5[1m]',
    );
    assert.equal(
      resolveSkillModelOverride('claude-sonnet-4-5', 'claude-sonnet-4-5[1m]'),
      'claude-sonnet-4-5[1m]',
    );
  });

  it('does NOT carry [1m] when the skill family has no 1M variant (haiku)', () => {
    assert.equal(
      resolveSkillModelOverride('claude-haiku-4-5', 'claude-opus-5[1m]'),
      'claude-haiku-4-5',
    );
  });

  it('leaves the skill model untouched when it already carries a marker', () => {
    assert.equal(
      resolveSkillModelOverride('claude-opus-5[1m]', 'claude-opus-5[1m]'),
      'claude-opus-5[1m]',
    );
  });

  it('does nothing when the session model has no 1M marker', () => {
    assert.equal(
      resolveSkillModelOverride('claude-opus-5', 'claude-opus-5'),
      'claude-opus-5',
    );
  });
});

// ===========================================================================
// GAP: filterToolsForSkill — scoped `Tool(scope:*)` + Claude-cased normalization
// ===========================================================================

describe('filterToolsForSkill — Claude-cased + scoped allowed-tools', () => {
  it('resolves Claude-cased bare names to AMI tools', () => {
    const skill = { allowedTools: ['Read', 'Grep', 'LS'] } as SkillDefinition;
    const names = filterToolsForSkill(allTools, skill).map(t => t.name).sort();
    assert.deepEqual(names, ['file_read', 'grep', 'list_dir'].sort());
  });

  it('resolves scoped command patterns to the tool-name portion', () => {
    const skill = {
      allowedTools: ['Bash(git diff:*)', 'Bash(git status:*)', 'Read'],
    } as SkillDefinition;
    const names = filterToolsForSkill(allTools, skill).map(t => t.name).sort();
    assert.deepEqual(names, ['bash', 'file_read'].sort());
  });

  it('treats a `*` wildcard as "all tools" (skips the allowlist filter)', () => {
    const skill = { allowedTools: ['*'] } as SkillDefinition;
    const names = filterToolsForSkill(allTools, skill).map(t => t.name);
    // All tools minus the skill tool itself (AMI "all" convention).
    assert.ok(names.includes('bash'));
    assert.ok(names.includes('file_edit'));
    assert.ok(!names.includes('skill'));
    assert.equal(names.length, allTools.filter(t => t.name !== 'skill').length);
  });

  it('normalizes Claude-cased disallowed entries too', () => {
    const skill = { disallowedTools: ['Bash'] } as SkillDefinition;
    const names = filterToolsForSkill(allTools, skill).map(t => t.name);
    assert.ok(!names.includes('bash'));
    assert.ok(names.includes('file_read'));
  });
});

// ===========================================================================
// GAP: filterToolsForSkill — unknown-tool + empty-toolset diagnostics
// ===========================================================================

describe('filterToolsForSkill — diagnostics for misconfigured restrictions', () => {
  it('warns when an allowed entry resolves to no real tool', () => {
    const warnings: SkillToolFilterWarning[] = [];
    const skill = { allowedTools: ['file_read', 'nonexistent_tool'] } as SkillDefinition;
    const names = filterToolsForSkill(allTools, skill, w => warnings.push(w)).map(t => t.name);
    assert.deepEqual(names, ['file_read']);
    const unknown = warnings.find(w => w.kind === 'unknown-tool');
    assert.ok(unknown, 'expected an unknown-tool warning');
    assert.equal(unknown!.entry, 'nonexistent_tool');
    assert.equal(unknown!.list, 'allowed');
  });

  it('warns when a disallowed entry resolves to no real tool', () => {
    const warnings: SkillToolFilterWarning[] = [];
    const skill = { disallowedTools: ['typo_tool'] } as SkillDefinition;
    filterToolsForSkill(allTools, skill, w => warnings.push(w));
    assert.ok(warnings.some(w => w.kind === 'unknown-tool' && w.list === 'disallowed' && w.entry === 'typo_tool'));
  });

  it('warns when the effective toolset resolves to zero tools', () => {
    const warnings: SkillToolFilterWarning[] = [];
    const skill = { allowedTools: ['does_not_exist'] } as SkillDefinition;
    const names = filterToolsForSkill(allTools, skill, w => warnings.push(w)).map(t => t.name);
    assert.deepEqual(names, []);
    assert.ok(warnings.some(w => w.kind === 'empty-toolset'));
  });

  it('emits no warnings for a valid restriction list', () => {
    const warnings: SkillToolFilterWarning[] = [];
    const skill = { allowedTools: ['bash', 'grep'] } as SkillDefinition;
    filterToolsForSkill(allTools, skill, w => warnings.push(w));
    assert.equal(warnings.length, 0);
  });
});

// ===========================================================================
// INTEGRATION (e2e through skillTool.execute) — forked skill wiring
// ===========================================================================

describe('skillTool.execute — forked skill applies U085 behaviors', () => {
  it('carries the [1m] suffix into the forked sub-engine model override', async () => {
    let captured: any = null;
    await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'sub', context: 'fork', model: 'claude-opus-5' }),
          getSkillContent: () => 'go',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'claude-opus-5[1m]' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return fakeEngineFactory([{ type: 'text_delta', text: 'x' }])(cfg);
        },
      }),
    );
    assert.equal(captured.provider.model, 'claude-opus-5[1m]');
  });

  it('surfaces tool-restriction warnings in the forked result output + metadata', async () => {
    const r = await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'sub', context: 'fork', allowedTools: ['file_read', 'bogus_tool'] }),
          getSkillContent: () => 'go',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: fakeEngineFactory([{ type: 'text_delta', text: 'ok' }]),
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /tool-restriction warnings/);
    assert.match(r.output, /bogus_tool/);
    const warnings = (r.metadata as any)?.toolWarnings as SkillToolFilterWarning[];
    assert.ok(Array.isArray(warnings) && warnings.some(w => w.entry === 'bogus_tool'));
  });

  it('resolves a Claude-cased allowed-tools list for the forked sub-engine', async () => {
    let captured: any = null;
    await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'sub', context: 'fork', allowedTools: ['Read', 'Grep'] }),
          getSkillContent: () => 'go',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return fakeEngineFactory([{ type: 'text_delta', text: 'x' }])(cfg);
        },
      }),
    );
    const names = captured.tools.map((t: any) => t.name).sort();
    assert.deepEqual(names, ['file_read', 'grep'].sort());
  });
});
