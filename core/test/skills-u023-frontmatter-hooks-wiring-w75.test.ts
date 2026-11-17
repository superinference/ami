// U023 (w75) — E2E wiring reproducer for per-skill / per-agent frontmatter `hooks`,
// driven through the REAL taskTool.execute + skillTool.execute spawn paths. A mock
// engineFactory captures the sub-engine EngineConfig so we can assert the definition's
// `hooks` are threaded into `config.frontmatterHooks` (which the engine then registers
// on the sub-engine's HookManager — proven to fire in skills-u023-frontmatter-hooks-w75).
//
// RED on revert: without the task.ts / skill.ts wiring, captured config.frontmatterHooks
// is undefined.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import { skillTool } from '../src/tools/skill';
import type { ToolContext } from '../src/types';
import type { SkillDefinition, AgentDefinition } from '../src/skills';
import type { HookConfigEntry } from '../src/hooks';

const HOOK: HookConfigEntry = {
  event: 'preToolUse',
  matcher: 'bash',
  hook: { type: 'command', command: 'echo "DENY: scoped"' },
};
const AGENT_HOOK: HookConfigEntry = {
  event: 'postToolUse',
  hook: { type: 'command', command: 'echo audit' },
};

let tmpDir: string;
beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fm-hooks-wiring-'));
});
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

function gen() {
  return async function* () { yield { type: 'text_delta', text: 'done' } as any; };
}

// ---------------------------------------------------------------------------
// task.ts — an agent's frontmatter hooks reach the sub-engine config
// ---------------------------------------------------------------------------

describe('taskTool — per-agent frontmatter hooks threading', () => {
  function ctx(agentDef: AgentDefinition, captured: { config?: any }): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
      _skillManager: {
        getAgent: () => agentDef,
        listAgents: () => [agentDef],
        getSkill: () => null,
      },
      _engineFactory: (config: any) => { captured.config = config; return { submit: gen() }; },
    } as unknown as ToolContext;
  }

  it('threads agentDef.hooks into subConfig.frontmatterHooks', async () => {
    const cap: { config?: any } = {};
    const agent: AgentDefinition = {
      name: 'auditor', description: 'd', systemPrompt: 'be an auditor', filePath: '<t>',
      tools: ['file_read'], hooks: [AGENT_HOOK],
    };
    const res = await taskTool.execute({ prompt: 'go', subagent_type: 'auditor' }, ctx(agent, cap));
    assert.equal(res.isError, undefined);
    assert.deepEqual(cap.config.frontmatterHooks, [AGENT_HOOK]);
  });

  it('omits frontmatterHooks when the agent declares none', async () => {
    const cap: { config?: any } = {};
    const agent: AgentDefinition = {
      name: 'plain', description: 'd', systemPrompt: 'be plain', filePath: '<t>', tools: ['file_read'],
    };
    await taskTool.execute({ prompt: 'go', subagent_type: 'plain' }, ctx(agent, cap));
    assert.equal(cap.config.frontmatterHooks, undefined);
  });
});

// ---------------------------------------------------------------------------
// skill.ts — a forked skill's (and its target agent's) hooks reach the config
// ---------------------------------------------------------------------------

describe('skillTool — forked skill frontmatter hooks threading', () => {
  function manager(skill: SkillDefinition, agent?: AgentDefinition) {
    return {
      getSkill: (n: string) => (n === skill.name ? skill : null),
      getSkillContent: (n: string) => (n === skill.name ? 'do the thing' : null),
      listSkills: () => [skill],
      getAgent: (n: string) => (agent && n === agent.name ? agent : null),
      listAgents: () => (agent ? [agent] : []),
    } as any;
  }
  function ctx(mgr: any, captured: { config?: any }): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
      _skillManager: mgr,
      _engineFactory: (config: any) => { captured.config = config; return { submit: gen() }; },
    } as unknown as ToolContext;
  }

  it('threads a forked skill\'s own hooks into subConfig.frontmatterHooks', async () => {
    const cap: { config?: any } = {};
    const skill: SkillDefinition = {
      name: 'guarded', description: 'd', content: 'c', filePath: '<t>', context: 'fork', hooks: [HOOK],
    };
    const res = await skillTool.execute({ skill: 'guarded' }, ctx(manager(skill), cap));
    assert.equal(res.isError, false);
    assert.deepEqual(cap.config.frontmatterHooks, [HOOK]);
  });

  it('merges the forked skill\'s hooks with its target agent\'s hooks', async () => {
    const cap: { config?: any } = {};
    const agent: AgentDefinition = {
      name: 'persona', description: 'd', systemPrompt: 'be a persona', filePath: '<t>', hooks: [AGENT_HOOK],
    };
    const skill: SkillDefinition = {
      name: 'guarded', description: 'd', content: 'c', filePath: '<t>', context: 'fork',
      agent: 'persona', hooks: [HOOK],
    };
    const res = await skillTool.execute({ skill: 'guarded' }, ctx(manager(skill, agent), cap));
    assert.equal(res.isError, false);
    assert.deepEqual(cap.config.frontmatterHooks, [HOOK, AGENT_HOOK]);
  });

  it('omits frontmatterHooks when neither the skill nor its agent declares hooks', async () => {
    const cap: { config?: any } = {};
    const skill: SkillDefinition = {
      name: 'plain', description: 'd', content: 'c', filePath: '<t>', context: 'fork',
    };
    await skillTool.execute({ skill: 'plain' }, ctx(manager(skill), cap));
    assert.equal(cap.config.frontmatterHooks, undefined);
  });
});
