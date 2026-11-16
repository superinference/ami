import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  skillTool,
  unsafeSkillProperties,
  skillHasOnlySafeProperties,
  SAFE_SKILL_PROPERTIES,
} from '../src/tools/skill';
import type { SkillDefinition } from '../src/skills';
import type { ToolContext } from '../src/types';
import type { PermissionPromptResult } from '../src/permissions';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp/u085-w65',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function manager(skill: any) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: (n: string) => (n === skill.name ? 'do the thing' : null),
    listSkills: () => [skill],
  } as any;
}

function denyingHandler(result: PermissionPromptResult) {
  const calls: string[] = [];
  return {
    calls,
    handler: {
      showPrompt: async (tool: string, summary: string) => {
        calls.push(`${tool}:${summary}`);
        return result;
      },
    },
  };
}

const baseSkill: SkillDefinition = {
  name: 'sub',
  description: 'd',
  content: 'c',
  filePath: '<test>',
  context: 'fork',
};

// ===========================================================================
// GAP U085 (medium): fail-closed SAFE_SKILL_PROPERTIES allowlist
// ===========================================================================

describe('unsafeSkillProperties — reviewed-safe allowlist', () => {
  it('treats a skill using only declared fields as safe', () => {
    assert.deepEqual(unsafeSkillProperties({ ...baseSkill, model: 'x', allowedTools: ['bash'] }), []);
    assert.ok(skillHasOnlySafeProperties({ ...baseSkill }));
  });

  it('flags an unreviewed property carrying a meaningful value', () => {
    const crafted = { ...baseSkill, evilDirective: 'rm -rf /' } as unknown as SkillDefinition;
    assert.deepEqual(unsafeSkillProperties(crafted), ['evilDirective']);
    assert.equal(skillHasOnlySafeProperties(crafted), false);
  });

  it('ignores unreviewed properties that are empty / nullish', () => {
    const s = { ...baseSkill, blank: undefined, nul: null, arr: [], obj: {} } as unknown as SkillDefinition;
    assert.deepEqual(unsafeSkillProperties(s), []);
  });

  it('keeps the allowlist in sync with the declared skill fields', () => {
    for (const k of ['model', 'effort', 'permissionMode', 'mcpServers', 'defaultThinkingLevel']) {
      assert.ok(SAFE_SKILL_PROPERTIES.has(k), `expected ${k} to be reviewed-safe`);
    }
  });
});

describe('skillTool.execute — fail-closed on unreviewed skill metadata', () => {
  const crafted = { name: 'crafted', context: 'inline', evilField: 'danger' } as unknown as SkillDefinition;

  it('blocks a skill with unreviewed properties when no permission handler is available', async () => {
    const r = await skillTool.execute(
      { skill: 'crafted' },
      ctx({ _skillManager: manager(crafted) }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /unreviewed properties \(evilField\)/);
    assert.match(r.output, /no permission handler/);
  });

  it('prompts for approval of unreviewed metadata and blocks on deny', async () => {
    const h = denyingHandler({ action: 'deny', feedback: 'nope' });
    const r = await skillTool.execute(
      { skill: 'crafted' },
      ctx({ _skillManager: manager(crafted), _permissionPromptHandler: h.handler as any }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /was not approved: nope/);
    assert.equal(h.calls.length, 1);
    assert.match(h.calls[0], /unreviewed properties: evilField/);
  });

  it('runs the skill when the unreviewed-metadata prompt is approved', async () => {
    const h = denyingHandler({ action: 'allow_once' });
    const r = await skillTool.execute(
      { skill: 'crafted' },
      ctx({ _skillManager: manager(crafted), _permissionPromptHandler: h.handler as any }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
  });

  it('an explicit operator allow rule bypasses the safe-property gate (no prompt)', async () => {
    const h = denyingHandler({ action: 'deny' });
    const r = await skillTool.execute(
      { skill: 'crafted' },
      ctx({
        _skillManager: manager(crafted),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'crafted', action: 'allow' }],
        _permissionPromptHandler: h.handler as any,
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
    assert.equal(h.calls.length, 0, 'allow rule must short-circuit the prompt');
  });
});

// ===========================================================================
// GAP U085 (medium): cancelled forked sub-skill surfaces as an error
// ===========================================================================

describe('skillTool.execute — cancelled forked skill reports an error', () => {
  it('returns isError + cancelled metadata + partial output when the parent aborts', async () => {
    const ac = new AbortController();
    const r = await skillTool.execute(
      { skill: 'sub' },
      ctx({
        abortSignal: ac.signal,
        _skillManager: manager(baseSkill),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({
          async *submit() {
            yield { type: 'text_delta', text: 'partial work' };
            ac.abort();
          },
        }) as any,
      }),
    );
    assert.ok(r.isError, 'cancelled sub-skill must be an error');
    assert.match(r.output, /was cancelled before completion/);
    assert.match(r.output, /partial work/);
    assert.equal((r.metadata as any)?.cancelled, true);
  });

  it('a forked skill that completes without an abort still succeeds', async () => {
    const r = await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: manager(baseSkill),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: () => ({
          async *submit() { yield { type: 'text_delta', text: 'done' }; },
        }) as any,
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /done/);
    assert.notEqual((r.metadata as any)?.cancelled, true);
  });
});

// ===========================================================================
// GAP U085 (medium): MCP prompts reachable only by namespaced identifier,
// and MCP lookup failures are surfaced, not swallowed.
// ===========================================================================

describe('skillTool.execute — MCP prompt reachability + error surfacing', () => {
  const mgr = {
    getSkill: () => null,
    getSkillContent: () => null,
    listSkills: () => [],
  } as any;

  it('does NOT resolve an MCP prompt by a guessed BARE name', async () => {
    let getPromptCalled = false;
    const r = await skillTool.execute(
      { skill: 'deploy' },
      ctx({
        _skillManager: mgr,
        _mcpManager: {
          listPrompts: async () => [{ name: 'deploy', server: 'ops' }],
          getPrompt: async () => { getPromptCalled = true; return { messages: [] }; },
        },
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /not found/);
    assert.equal(getPromptCalled, false, 'bare name must not reach getPrompt');
  });

  it('resolves an MCP prompt via its namespaced mcp__server__prompt identifier', async () => {
    const r = await skillTool.execute(
      { skill: 'mcp__ops__deploy' },
      ctx({
        _skillManager: mgr,
        _mcpManager: {
          listPrompts: async () => [{ name: 'deploy', server: 'ops' }],
          getPrompt: async (server: string, name: string) => ({
            messages: [{ role: 'user', content: `${server}/${name}` }],
          }),
        },
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /MCP Skill: mcp__ops__deploy/);
    assert.match(r.output, /ops\/deploy/);
  });

  it('does not cross servers: a namespaced name must match the right server', async () => {
    const r = await skillTool.execute(
      { skill: 'mcp__other__deploy' },
      ctx({
        _skillManager: mgr,
        _mcpManager: {
          listPrompts: async () => [{ name: 'deploy', server: 'ops' }],
        },
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /not found/);
  });

  it('surfaces a transient MCP list error instead of silently reporting not-found', async () => {
    const r = await skillTool.execute(
      { skill: 'mcp__ops__deploy' },
      ctx({
        _skillManager: mgr,
        _mcpManager: {
          listPrompts: async () => { throw new Error('server unreachable'); },
        },
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /failed to load MCP skill "mcp__ops__deploy": server unreachable/);
  });
});
