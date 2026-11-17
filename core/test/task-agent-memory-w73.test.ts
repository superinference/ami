// U016-w73 — E2E red-green reproducer for per-agent persistent memory (Claude
// parity: loadAgentMemoryPrompt wired at agent-spawn time), driven through the
// real taskTool.execute path:
//
//  - A named agent that declares `memory: 'project'` must have a "Persistent
//    Agent Memory" block (with its existing MEMORY.md entrypoint) PREPENDED to the
//    sub-engine's authoritative system prompt on spawn.
//    RED on revert: without the buildAgentMemoryPrompt wiring in task.ts, the
//    captured sub-engine config.systemPrompt contains only the agent body.
//
//  - An agent with no `memory` scope gets no agent-memory block.
//    RED on revert: n/a (negative control) — guards against always-on injection.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';
import { getAgentMemoryDir } from '../src/memory';

let tmpDir: string;

beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agent-memory-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function gen() {
  return async function* () {
    yield { type: 'text_delta', text: 'done' } as any;
  };
}

function ctx(agentDef: any, captured: { config?: any }): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
    _skillManager: {
      getAgent: () => agentDef,
      listAgents: () => [agentDef],
      getSkill: () => null,
    },
    _engineFactory: (config: any) => {
      captured.config = config;
      return { submit: gen() };
    },
  } as unknown as ToolContext;
}

describe('taskTool — per-agent persistent memory', () => {
  it('prepends the Persistent Agent Memory block (with entrypoint) to the sub-engine prompt', async () => {
    // Seed an existing project-scope agent-memory entrypoint for this agent type.
    const memDir = getAgentMemoryDir('scribe', 'project', tmpDir);
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'MEMORY.md'), '- [Style](style.md) — the user wants minimal diffs\n');

    const cap: { config?: any } = {};
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'scribe' },
      ctx({ name: 'scribe', systemPrompt: 'be a scribe agent', memory: 'project', tools: ['file_read'] }, cap),
    );

    assert.equal(res.isError, undefined);
    const sp: string = cap.config.systemPrompt;
    assert.ok(sp.includes('# Persistent Agent Memory'), sp);
    assert.ok(sp.includes('the user wants minimal diffs'), sp);
    assert.ok(sp.includes('be a scribe agent'), sp);
    // The memory block comes BEFORE the agent body (prepended).
    assert.ok(sp.indexOf('# Persistent Agent Memory') < sp.indexOf('be a scribe agent'), sp);
  });

  it('does NOT inject an agent-memory block when the agent declares no memory scope', async () => {
    const cap: { config?: any } = {};
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'plain' },
      ctx({ name: 'plain', systemPrompt: 'be a plain agent', tools: ['file_read'] }, cap),
    );
    assert.ok(!cap.config.systemPrompt.includes('# Persistent Agent Memory'), cap.config.systemPrompt);
  });
});
