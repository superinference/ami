import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  taskTool,
  resetAgentRegistry,
  buildCoordinatorWorkerContext,
  availableAgentTypeNames,
} from '../src/tools/task';
import type { ToolContext } from '../src/types';
import type { SkillManagerLike, AgentDefinition } from '../src/skills';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

function agent(name: string): AgentDefinition {
  return { name, description: `the ${name}`, prompt: '' } as unknown as AgentDefinition;
}

function skillManagerWith(agents: AgentDefinition[]): SkillManagerLike {
  return {
    getSkill: () => null,
    getSkillContent: () => null,
    listSkills: () => [],
    getAgent: (n: string) => agents.find(a => a.name === n) ?? null,
    listAgents: () => agents,
  };
}

// ===========================================================================
// U011-w108 — coordinator affordance surfaces the named-agent-type catalog
// (parity: the reference injects the available config surface — MCP servers +
//  plugin/agent catalog — into an agent's runtime context so it delegates to the
//  specialist it actually has rather than guessing / falling back to general).
// ===========================================================================

describe('U011-w108 — availableAgentTypeNames', () => {
  it('returns the sorted, de-duplicated names from the skill manager roster', () => {
    const c = ctx({ _skillManager: skillManagerWith([agent('reviewer'), agent('explorer'), agent('reviewer')]) });
    assert.deepEqual(availableAgentTypeNames(c), ['explorer', 'reviewer']);
  });

  it('trims blank/whitespace names out so they never shadow the catalog', () => {
    const c = ctx({ _skillManager: skillManagerWith([agent('  spaced  '), agent('   '), agent('')]) });
    assert.deepEqual(availableAgentTypeNames(c), ['spaced']);
  });

  it('returns [] when there is no skill manager', () => {
    assert.deepEqual(availableAgentTypeNames(ctx()), []);
  });

  it('returns [] when the manager lacks a listAgents accessor', () => {
    const c = ctx({ _skillManager: { getSkill: () => null } as unknown as SkillManagerLike });
    assert.deepEqual(availableAgentTypeNames(c), []);
  });

  it('returns [] (never throws) when listAgents throws', () => {
    const c = ctx({
      _skillManager: { listAgents: () => { throw new Error('boom'); } } as unknown as SkillManagerLike,
    });
    assert.deepEqual(availableAgentTypeNames(c), []);
  });
});

describe('U011-w108 — buildCoordinatorWorkerContext agent-type clause', () => {
  it('appends the named agent-type catalog clause when present', () => {
    const content = buildCoordinatorWorkerContext([], ['explorer', 'reviewer']);
    assert.ok(
      content.includes('delegate to these specialized named agent types by setting a worker\'s subagent_type: explorer, reviewer'),
      `agent-type clause missing, got: ${content}`,
    );
  });

  it('omits the agent-type clause when no agent types are known (back-compat default)', () => {
    const content = buildCoordinatorWorkerContext(['github']);
    assert.ok(!content.includes('subagent_type'), `no agent-type clause expected, got: ${content}`);
    // the MCP clause still renders — the new arg is purely additive
    assert.ok(content.includes('connected MCP servers: github'));
  });

  it('renders both the MCP and the agent-type clauses together', () => {
    const content = buildCoordinatorWorkerContext(['linear'], ['reviewer']);
    assert.ok(content.includes('connected MCP servers: linear'));
    assert.ok(content.includes('subagent_type: reviewer'));
  });

  it('stays deterministic across calls (cache-friendly)', () => {
    assert.equal(
      buildCoordinatorWorkerContext(['a'], ['x', 'y']),
      buildCoordinatorWorkerContext(['a'], ['x', 'y']),
    );
  });
});

describe('U011-w108 — END-TO-END: coordinator spawn injects the agent-type catalog', () => {
  beforeEach(() => resetAgentRegistry());

  it('prepends the named-agent-type catalog to a coordinator-mode spawn prompt', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'si-w108-'));
    let capturedPrompt = '';
    try {
      await taskTool.execute(
        { prompt: 'orchestrate the work', mode: 'coordinator' },
        ctx({
          cwd: tmp,
          _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
          _skillManager: skillManagerWith([agent('reviewer'), agent('explorer')]),
          _engineFactory: () => ({
            submit: async function* (prompt: string) {
              capturedPrompt = prompt;
              yield { type: 'text_delta' as const, text: 'done' };
            },
          }),
        } as unknown as Partial<ToolContext>),
      );
      assert.ok(
        capturedPrompt.includes('subagent_type: explorer, reviewer'),
        `coordinator prompt must surface the agent catalog, got: ${capturedPrompt.slice(0, 400)}`,
      );
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
