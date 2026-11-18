// U016 — capability-gated per-agent memory block at agent-spawn time.
//
// Parity with Claude Code: the agent-creation wizard only inserts MemoryStep when
// `isAutoMemoryEnabled()` (CreateAgentWizard.tsx:52) and MemoryStep's
// `getSystemPrompt` only prepends `loadAgentMemoryPrompt(...)` when
// `isAutoMemoryEnabled() && memory && agentType` (MemoryStep.tsx:80). AMI has no
// wizard, so the functional surface is the task tool spawn path: a declared
// `memory` scope must only produce a "Persistent Agent Memory" block when the
// auto-memory capability is active.
//
//  - GREEN (capability on, the default): an agent with `memory: 'project'` gets the
//    block prepended (already covered by task-agent-memory-w73; re-asserted here as
//    the positive control).
//  - RED on revert: with `AMI_DISABLE_AUTO_MEMORY=1`, the block must NOT be
//    injected. Without the `&& isAutoMemoryEnabled()` gate in task.ts this test
//    fails (the block is injected regardless of the capability being disabled).

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';
import { getAgentMemoryDir } from '../src/memory';

let tmpDir: string;
let savedDisable: string | undefined;

beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agent-mem-gate-'));
  savedDisable = process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
  if (savedDisable === undefined) delete process.env.AMI_DISABLE_AUTO_MEMORY;
  else process.env.AMI_DISABLE_AUTO_MEMORY = savedDisable;
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

function seedMemory() {
  const memDir = getAgentMemoryDir('scribe', 'project', tmpDir);
  fs.mkdirSync(memDir, { recursive: true });
  fs.writeFileSync(path.join(memDir, 'MEMORY.md'), '- [Style](style.md) — minimal diffs\n');
}

describe('taskTool — per-agent memory is capability-gated on isAutoMemoryEnabled()', () => {
  it('injects the memory block when auto-memory is enabled (default)', async () => {
    delete process.env.AMI_DISABLE_AUTO_MEMORY;
    seedMemory();
    const cap: { config?: any } = {};
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'scribe' },
      ctx({ name: 'scribe', systemPrompt: 'be a scribe agent', memory: 'project', tools: ['file_read'] }, cap),
    );
    assert.equal(res.isError, undefined);
    assert.ok(cap.config.systemPrompt.includes('# Persistent Agent Memory'), cap.config.systemPrompt);
    assert.ok(cap.config.systemPrompt.includes('minimal diffs'), cap.config.systemPrompt);
  });

  it('does NOT inject the memory block when AMI_DISABLE_AUTO_MEMORY is truthy', async () => {
    process.env.AMI_DISABLE_AUTO_MEMORY = '1';
    seedMemory();
    const cap: { config?: any } = {};
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'scribe' },
      ctx({ name: 'scribe', systemPrompt: 'be a scribe agent', memory: 'project', tools: ['file_read'] }, cap),
    );
    assert.equal(res.isError, undefined);
    assert.ok(
      !cap.config.systemPrompt.includes('# Persistent Agent Memory'),
      `memory block must be inert when the capability is disabled: ${cap.config.systemPrompt}`,
    );
    // The agent body still spawns normally.
    assert.ok(cap.config.systemPrompt.includes('be a scribe agent'), cap.config.systemPrompt);
  });

  it('still injects the block when AMI_DISABLE_AUTO_MEMORY is explicitly falsy', async () => {
    process.env.AMI_DISABLE_AUTO_MEMORY = 'false';
    seedMemory();
    const cap: { config?: any } = {};
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'scribe' },
      ctx({ name: 'scribe', systemPrompt: 'be a scribe agent', memory: 'project', tools: ['file_read'] }, cap),
    );
    assert.ok(cap.config.systemPrompt.includes('# Persistent Agent Memory'), cap.config.systemPrompt);
  });
});
