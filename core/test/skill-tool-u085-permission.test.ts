import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { skillTool, evaluateSkillPermission } from '../src/tools/skill';
import { normalizeToolName } from '../src/permissions';
import type { ToolContext } from '../src/types';
import type { PermissionPromptResult } from '../src/permissions';

// ---------------------------------------------------------------------------
// Helpers
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

function fakeEngineFactory(
  events: Array<{ type: string; [k: string]: any }>,
  capture?: (cfg: any) => void,
) {
  return (cfg: any) => {
    capture?.(cfg);
    return {
      submit: async function* (_prompt: string) {
        for (const e of events) yield e;
      },
    };
  };
}

function manager(skill: any) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: () => 'do the thing',
    listSkills: () => [skill],
  } as any;
}

// ===========================================================================
// GAP U085 (high): Per-skill permission gating — evaluateSkillPermission (pure)
// ===========================================================================

describe('evaluateSkillPermission — per-skill deny/allow/ask keyed by name', () => {
  it('returns undefined when there are no rules', () => {
    assert.equal(evaluateSkillPermission('x', undefined), undefined);
    assert.equal(evaluateSkillPermission('x', []), undefined);
  });

  it('returns undefined when no Skill(...) rule matches (default-ask fall-through)', () => {
    const rules = [{ tool: 'bash', pattern: '*', action: 'deny' as const }];
    assert.equal(evaluateSkillPermission('deploy', rules), undefined);
  });

  it('denies an exact skill-name match', () => {
    const rules = [{ tool: 'skill', pattern: 'dangerous', action: 'deny' as const }];
    assert.equal(evaluateSkillPermission('dangerous', rules), 'deny');
    assert.equal(evaluateSkillPermission('safe', rules), undefined);
  });

  it('denies via a `name:*` prefix scope', () => {
    const rules = [{ tool: 'skill', pattern: 'project:*', action: 'deny' as const }];
    assert.equal(evaluateSkillPermission('project:deploy', rules), 'deny');
    assert.equal(evaluateSkillPermission('user:deploy', rules), undefined);
  });

  it('a pattern-less skill rule matches every skill', () => {
    const rules = [{ tool: 'skill', action: 'deny' as const }];
    assert.equal(evaluateSkillPermission('anything', rules), 'deny');
  });

  it('deny wins over allow (defense-in-depth order)', () => {
    const rules = [
      { tool: 'skill', pattern: 'x', action: 'allow' as const },
      { tool: 'skill', pattern: 'x', action: 'deny' as const },
    ];
    assert.equal(evaluateSkillPermission('x', rules), 'deny');
  });

  it('allow wins over ask when both match and no deny', () => {
    const rules = [
      { tool: 'skill', pattern: 'x', action: 'ask' as const },
      { tool: 'skill', pattern: 'x', action: 'allow' as const },
    ];
    assert.equal(evaluateSkillPermission('x', rules), 'allow');
  });

  it('returns ask for a matching ask rule', () => {
    const rules = [{ tool: 'skill', pattern: 'x', action: 'ask' as const }];
    assert.equal(evaluateSkillPermission('x', rules), 'ask');
  });

  it('honors Claude-cased `Skill` rule tool via the alias map', () => {
    // A rule authored as Skill(dangerous) normalizes its tool to 'skill'.
    assert.equal(normalizeToolName('Skill'), 'skill');
    const rules = [{ tool: 'Skill', pattern: 'dangerous', action: 'deny' as const }];
    assert.equal(evaluateSkillPermission('dangerous', rules), 'deny');
  });
});

// ===========================================================================
// GAP U085 (high): deny rule blocks a model-invoked skill (inline + forked)
// ===========================================================================

describe('skillTool.execute — per-skill deny gate', () => {
  it('blocks an inline skill denied by a Skill(...) rule', async () => {
    const r = await skillTool.execute(
      { skill: 'dangerous' },
      ctx({
        _skillManager: manager({ name: 'dangerous', context: 'inline' }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'dangerous', action: 'deny' }],
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /blocked by a permission rule/);
  });

  it('blocks a forked skill denied by a Skill(...) rule BEFORE spawning the sub-engine', async () => {
    let factoryCalled = false;
    const r = await skillTool.execute(
      { skill: 'dangerous' },
      ctx({
        _skillManager: manager({ name: 'dangerous', context: 'fork' }),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _parentPermissionRules: [{ tool: 'skill', action: 'deny' }],
        _engineFactory: () => {
          factoryCalled = true;
          return { submit: async function* () {} } as any;
        },
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /blocked by a permission rule/);
    assert.equal(factoryCalled, false, 'denied skill must not spawn a sub-engine');
  });

  it('does NOT gate a skill when no Skill(...) rule matches', async () => {
    const r = await skillTool.execute(
      { skill: 'safe' },
      ctx({
        _skillManager: manager({ name: 'safe', context: 'inline' }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'other', action: 'deny' }],
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
  });
});

// ===========================================================================
// GAP U085 (high): ask rule prompts via the forwarded permission handler
// ===========================================================================

describe('skillTool.execute — per-skill ask gate', () => {
  function handler(result: PermissionPromptResult) {
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

  it('runs the skill when the user approves the ask prompt', async () => {
    const h = handler({ action: 'allow_once' });
    const r = await skillTool.execute(
      { skill: 'maybe', args: 'go' },
      ctx({
        _skillManager: manager({ name: 'maybe', context: 'inline' }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'maybe', action: 'ask' }],
        _permissionPromptHandler: h.handler as any,
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
    assert.equal(h.calls.length, 1);
    assert.match(h.calls[0], /skill:skill: maybe go/);
  });

  it('rejects the skill when the user denies the ask prompt, surfacing feedback', async () => {
    const h = handler({ action: 'deny', feedback: 'not now' });
    const r = await skillTool.execute(
      { skill: 'maybe' },
      ctx({
        _skillManager: manager({ name: 'maybe', context: 'inline' }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'maybe', action: 'ask' }],
        _permissionPromptHandler: h.handler as any,
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /was not approved: not now/);
  });

  it('rejects when the prompt is cancelled', async () => {
    const h = handler({ action: 'cancelled' });
    const r = await skillTool.execute(
      { skill: 'maybe' },
      ctx({
        _skillManager: manager({ name: 'maybe', context: 'inline' }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'maybe', action: 'ask' }],
        _permissionPromptHandler: h.handler as any,
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /was not approved/);
  });

  it('an ask rule with no prompt handler proceeds (generic gate already applied)', async () => {
    const r = await skillTool.execute(
      { skill: 'maybe' },
      ctx({
        _skillManager: manager({ name: 'maybe', context: 'inline' }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'maybe', action: 'ask' }],
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
  });
});

// ===========================================================================
// GAP U085 (high): forked skill honors session permission policy (no auto-allow)
// ===========================================================================

describe('skillTool.execute — forked sub-engine permission mode', () => {
  it('defaults the forked sub-engine to ask (not auto-allow)', async () => {
    let captured: any = null;
    await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: manager({ name: 'sub', context: 'fork' }),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: fakeEngineFactory(
          [{ type: 'text_delta', text: 'x' }],
          c => { captured = c; },
        ),
      }),
    );
    assert.equal(captured.permissionMode, 'ask');
  });

  it('forwards the parent permission rules to the forked sub-engine', async () => {
    let captured: any = null;
    const rules = [{ tool: 'bash', pattern: 'rm *', action: 'deny' as const }];
    await skillTool.execute(
      { skill: 'sub' },
      ctx({
        _skillManager: manager({ name: 'sub', context: 'fork' }),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _parentPermissionRules: rules,
        _engineFactory: fakeEngineFactory(
          [{ type: 'text_delta', text: 'x' }],
          c => { captured = c; },
        ),
      }),
    );
    assert.deepEqual(captured.permissionRules, rules);
  });

  it('honors a skill-declared permissionMode: auto-allow for trusted forks', async () => {
    let captured: any = null;
    await skillTool.execute(
      { skill: 'trusted' },
      ctx({
        _skillManager: manager({ name: 'trusted', context: 'fork', permissionMode: 'auto-allow' }),
        _providerConfig: { baseUrl: 'http://t', apiKey: 'k', model: 'm' },
        _engineFactory: fakeEngineFactory(
          [{ type: 'text_delta', text: 'x' }],
          c => { captured = c; },
        ),
      }),
    );
    assert.equal(captured.permissionMode, 'auto-allow');
  });
});
