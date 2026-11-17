// U023-w68 — E2E red-green reproducer for per-agent `skills` preloading (Claude
// parity), driven through the real taskTool.execute path:
//
//  - A named agent that declares `skills: [helper]` must have the helper skill's
//    body inlined into the sub-engine's authoritative system prompt on spawn.
//    RED on revert: without the buildPreloadedSkillsBlock wiring in task.ts, the
//    captured sub-engine config.systemPrompt contains only the agent body.
//
//  - A declared-but-unresolvable preload skill must be surfaced as a note in the
//    agent's returned output (not silently dropped).
//    RED on revert: without the note wiring, the output omits the missing name.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';
import type { SkillDefinition } from '../src/skills';

let tmpDir: string;

beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agent-skills-preload-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function gen() {
  return async function* () {
    yield { type: 'text_delta', text: 'done' } as any;
  };
}

function skill(name: string, content: string, extra: Partial<SkillDefinition> = {}): SkillDefinition {
  return { name, description: 'd', content, filePath: '<test>', ...extra } as SkillDefinition;
}

function ctx(
  agentDef: any,
  skills: Record<string, SkillDefinition>,
  captured: { config?: any },
): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
    _skillManager: {
      getAgent: () => agentDef,
      listAgents: () => [agentDef],
      getSkill: (n: string) => skills[n] ?? null,
      listSkills: () => Object.values(skills),
    },
    _engineFactory: (config: any) => {
      captured.config = config;
      return { submit: gen() };
    },
  } as unknown as ToolContext;
}

describe('taskTool — per-agent skills preload', () => {
  it('inlines a declared skill body into the sub-engine system prompt', async () => {
    const cap: { config?: any } = {};
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'preloader' },
      ctx(
        { name: 'preloader', systemPrompt: 'be a preloader agent', skills: ['helper'], tools: ['file_read'] },
        { helper: skill('helper', 'ALWAYS write a changelog entry.') },
        cap,
      ),
    );
    assert.equal(res.isError, undefined);
    assert.ok(cap.config.systemPrompt.includes('be a preloader agent'), cap.config.systemPrompt);
    assert.ok(cap.config.systemPrompt.includes('# Preloaded Skills'), cap.config.systemPrompt);
    assert.ok(cap.config.systemPrompt.includes('ALWAYS write a changelog entry.'), cap.config.systemPrompt);
  });

  it('surfaces an unresolved preload skill as a note in the output', async () => {
    const cap: { config?: any } = {};
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'preloader' },
      ctx(
        { name: 'preloader', systemPrompt: 'be a preloader agent', skills: ['ghost'], tools: ['file_read'] },
        {},
        cap,
      ),
    );
    assert.ok(res.output.includes('ghost'), res.output);
    assert.ok(/unknown or disabled skill/.test(res.output), res.output);
    // Nothing loaded ⇒ no preload block appended.
    assert.ok(!cap.config.systemPrompt.includes('# Preloaded Skills'), cap.config.systemPrompt);
  });

  it('resolves a plugin-namespaced skill referenced by its bare name (resolveSkillName fallback)', async () => {
    const cap: { config?: any } = {};
    // The skill is registered ONLY under its plugin-namespaced name; the bare
    // "deploy" is unavailable via getSkill. The multi-strategy fallback must still
    // inline it so a plugin-shipped agent's `skills: [deploy]` preload works.
    const namespaced = skill('pkg:deploy', 'Run the deploy checklist.');
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'preloader' },
      ctx(
        { name: 'preloader', systemPrompt: 'be a preloader agent', skills: ['deploy'], tools: ['file_read'] },
        { 'pkg:deploy': namespaced },
        cap,
      ),
    );
    assert.equal(res.isError, undefined);
    assert.ok(cap.config.systemPrompt.includes('# Preloaded Skills'), cap.config.systemPrompt);
    assert.ok(cap.config.systemPrompt.includes('Run the deploy checklist.'), cap.config.systemPrompt);
    // No "unknown or disabled skill" note — it resolved via the fallback.
    assert.ok(!/unknown or disabled skill/.test(res.output), res.output);
  });

  it('does not append a preload block when the agent declares no skills', async () => {
    const cap: { config?: any } = {};
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'plain' },
      ctx({ name: 'plain', systemPrompt: 'be a plain agent', tools: ['file_read'] }, {}, cap),
    );
    assert.ok(!cap.config.systemPrompt.includes('# Preloaded Skills'), cap.config.systemPrompt);
  });
});
