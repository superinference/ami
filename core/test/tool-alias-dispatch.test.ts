import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  toolMatchesName,
  findToolByName,
} from '../src/tools/tool-registry-guard';
import { createDefaultTools } from '../src/tools';
import { taskKillTool } from '../src/tools/task-kill';
import { taskOutputTool } from '../src/tools/task-output';

// PARITY (additive): back-compat alias-aware tool dispatch — a tool must be
// resolvable by any of its declared `aliases` (renamed/legacy names from old
// transcripts or SDK callers), not only by its primary name. Mirrors Claude's
// `toolMatchesName` / `findToolByName` helpers and the per-tool `aliases` knob.

describe('toolMatchesName', () => {
  it('matches the primary name', () => {
    assert.equal(toolMatchesName({ name: 'task_kill' }, 'task_kill'), true);
  });

  it('matches a declared alias', () => {
    assert.equal(
      toolMatchesName({ name: 'task_kill', aliases: ['KillShell'] }, 'KillShell'),
      true,
    );
  });

  it('does not match an unrelated name', () => {
    assert.equal(
      toolMatchesName({ name: 'task_kill', aliases: ['KillShell'] }, 'grep'),
      false,
    );
  });

  it('returns false when there are no aliases and the name differs', () => {
    assert.equal(toolMatchesName({ name: 'task_kill' }, 'KillShell'), false);
  });
});

describe('findToolByName', () => {
  const tools = [
    { name: 'task_kill', aliases: ['KillShell'] as const },
    { name: 'task_output', aliases: ['AgentOutputTool', 'BashOutputTool'] as const },
    { name: 'grep' },
  ];

  it('resolves by primary name', () => {
    assert.equal(findToolByName(tools, 'grep')?.name, 'grep');
  });

  it('resolves by alias to the canonical tool', () => {
    assert.equal(findToolByName(tools, 'KillShell')?.name, 'task_kill');
    assert.equal(findToolByName(tools, 'BashOutputTool')?.name, 'task_output');
  });

  it('resolves case-insensitively by name or alias (AMI tolerance preserved)', () => {
    assert.equal(findToolByName(tools, 'GREP')?.name, 'grep');
    assert.equal(findToolByName(tools, 'killshell')?.name, 'task_kill');
  });

  it('prefers an exact match over a case-insensitive one', () => {
    const list = [{ name: 'Foo' }, { name: 'foo' }];
    assert.equal(findToolByName(list, 'foo')?.name, 'foo');
  });

  it('returns undefined for an unknown name', () => {
    assert.equal(findToolByName(tools, 'does_not_exist'), undefined);
  });
});

describe('real tools declare back-compat aliases', () => {
  it('task_kill answers to the deprecated KillShell name', () => {
    assert.ok(taskKillTool.aliases?.includes('KillShell'));
    assert.ok(taskKillTool.searchHint && taskKillTool.searchHint.length > 0);
  });

  it('task_output answers to renamed AgentOutputTool / BashOutputTool names', () => {
    assert.ok(taskOutputTool.aliases?.includes('AgentOutputTool'));
    assert.ok(taskOutputTool.aliases?.includes('BashOutputTool'));
  });
});

describe('ToolRegistry honors aliases', () => {
  it('registers tool aliases in the guard and resolves get() by alias', () => {
    const registry = createDefaultTools(process.cwd());
    // Primary name still works.
    assert.equal(registry.get('task_kill')?.name, 'task_kill');
    // Alias resolves to the canonical tool.
    assert.equal(registry.get('KillShell')?.name, 'task_kill');
    assert.equal(registry.get('AgentOutputTool')?.name, 'task_output');
    // No bogus conflicts introduced by the real alias set.
    assert.equal(registry.getConflicts().length, 0);
  });
});
