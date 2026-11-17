import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'node:path';

import { skillTool } from '../src/tools/skill';
import type { SkillDefinition } from '../src/skills';
import { SAFE_SKILL_PROPERTIES } from '../src/skills';
import type { ToolContext, EngineEvent } from '../src/types';
import { PermissionManager } from '../src/permissions';

// ===========================================================================
// U085 (medium) — least-privilege capability ceiling for a FORKED skill,
// wired end-to-end: skill frontmatter → EngineConfig.capabilityCeiling →
// PermissionManager.check denies ahead of the ambient mode (parity: Claude's
// forked-agent createAutoMemCanUseTool).
// ===========================================================================

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/work',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function manager(skill: SkillDefinition) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: (n: string) => (n === skill.name ? 'do the thing' : null),
    listSkills: () => [skill],
  } as any;
}

const forkBase = {
  name: 'sub',
  description: 'd',
  content: 'c',
  filePath: '<test>',
  context: 'fork' as const,
};

describe('skillTool — forked skill sets capabilityCeiling from its sandbox', () => {
  it('threads read-only-bash + write-roots into the sub-engine config', async () => {
    let captured: any;
    const skill = { ...forkBase, sandboxReadOnlyBash: true, sandboxWriteRoots: ['./memory'] } as SkillDefinition;
    const r = await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: manager(skill),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return { async *submit() { yield { type: 'text_delta', text: 'ok' } as EngineEvent; } } as any;
        },
      }),
    );
    assert.ok(!r.isError);
    assert.ok(captured.capabilityCeiling, 'ceiling threaded into sub-engine config');
    assert.equal(captured.capabilityCeiling.readOnlyBashOnly, true);
    assert.deepEqual(captured.capabilityCeiling.writeRoots, [path.resolve('/work', './memory')]);
    // Byline + metadata surface the sandbox (introspection).
    assert.match(r.output, /sandbox: read-only bash, writes confined to 1 dir/);
    assert.deepEqual((r.metadata as any).sandbox, {
      readOnlyBash: true,
      writeRoots: [path.resolve('/work', './memory')],
    });
  });

  it('sets NO ceiling when the skill declares no sandbox', async () => {
    let captured: any;
    const skill = { ...forkBase } as SkillDefinition;
    const r = await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: manager(skill),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          captured = cfg;
          return { async *submit() { yield { type: 'text_delta', text: 'ok' } as EngineEvent; } } as any;
        },
      }),
    );
    assert.ok(!r.isError);
    assert.equal(captured.capabilityCeiling, undefined);
    assert.doesNotMatch(r.output, /sandbox:/);
    assert.equal((r.metadata as any).sandbox, undefined);
  });

  it('read-only-bash alone surfaces the right byline', async () => {
    const skill = { ...forkBase, sandboxReadOnlyBash: true } as SkillDefinition;
    const r = await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: manager(skill),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({ async *submit() { yield { type: 'text_delta', text: 'ok' } as EngineEvent; } }) as any,
      }),
    );
    assert.match(r.output, /sandbox: read-only bash\]/);
  });

  it('sandbox fields are reviewed-safe (do not force a permission prompt)', () => {
    assert.ok(SAFE_SKILL_PROPERTIES.has('sandboxReadOnlyBash'));
    assert.ok(SAFE_SKILL_PROPERTIES.has('sandboxWriteRoots'));
  });
});

describe('PermissionManager — capability ceiling is a HARD cap ahead of every mode', () => {
  it('denies a non-read-only bash command even under auto-allow', async () => {
    const pm = new PermissionManager('auto-allow');
    // Baseline: auto-allow would allow the write command.
    assert.equal(await pm.check('bash', { command: 'echo hi > f.txt' }), 'allow');
    pm.setCapabilityCeiling({ readOnlyBashOnly: true });
    assert.equal(await pm.check('bash', { command: 'echo hi > f.txt' }), 'deny');
    // A read-only command is still allowed.
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
  });

  it('confines writes to the whitelisted roots even under auto-allow', async () => {
    const pm = new PermissionManager('auto-allow');
    const root = path.resolve('/work/memory');
    pm.setCapabilityCeiling({ writeRoots: [root], baseDir: '/work' });
    assert.equal(await pm.check('file_edit', { file_path: path.join(root, 'n.md') }), 'allow');
    assert.equal(await pm.check('file_edit', { file_path: '/work/src/app.ts' }), 'deny');
  });

  it('is inert once cleared (undefined ceiling)', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setCapabilityCeiling({ readOnlyBashOnly: true });
    assert.equal(await pm.check('bash', { command: 'rm -rf x' }), 'deny');
    pm.setCapabilityCeiling(undefined);
    assert.equal(await pm.check('bash', { command: 'rm -rf x' }), 'allow');
  });
});
