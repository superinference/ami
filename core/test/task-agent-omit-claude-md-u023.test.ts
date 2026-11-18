// U023 — per-agent `omitClaudeMd` project-context knob (parity with Claude's agent
// `omitClaudeMd`, inverted for AMI's lean-subagent perk). AMI subagents run on a
// REPLACED lean/identity prompt that already omits the project CLAUDE.md/AGENTS.md and
// the session-start git status. The knob lets a NAMED agent OPT BACK IN to the project
// instruction context via `omit-claude-md: false`.
//
//  - resolveAgentProjectContext: returns '' for undefined/true (lean default) and the
//    project instructions for an explicit false.
//  - E2E through the real taskTool.execute spawn path: an agent def with
//    omitClaudeMd:false gets the project instructions appended to its sub-engine
//    systemPrompt; the lean default (undefined) does NOT.
//    RED on revert: without the resolveAgentProjectContext call at the agentSystemPrompt
//    build site, cap.config.systemPrompt never contains the project instructions.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { taskTool, resetAgentRegistry, resolveAgentProjectContext } from '../src/tools/task';
import type { ToolContext } from '../src/types';

let tmpDir: string;
const MARKER = 'PROJECT-CONVENTION: always run the smoke suite before committing.';

beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agent-omitmd-'));
  fs.writeFileSync(path.join(tmpDir, 'CLAUDE.md'), `# Repo rules\n\n${MARKER}\n`);
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function gen(submittedPrompts: string[]) {
  return async function* (p: string) {
    submittedPrompts.push(p);
    yield { type: 'text_delta', text: 'ok' } as any;
  };
}

function ctx(agentDef: any, captured: { config?: any; prompts: string[] }): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
    _skillManager: {
      getAgent: () => agentDef,
      listAgents: () => [agentDef],
    },
    _engineFactory: (config: any) => {
      captured.config = config;
      return { submit: gen(captured.prompts) };
    },
  } as unknown as ToolContext;
}

describe('resolveAgentProjectContext', () => {
  it('returns the project instructions only for an explicit omitClaudeMd === false', () => {
    const optIn = resolveAgentProjectContext(tmpDir, false);
    assert.ok(optIn.includes(MARKER), optIn);
    assert.ok(optIn.startsWith('# Project Instructions'), optIn);
  });

  it('returns "" for the lean default (undefined) and for true', () => {
    assert.equal(resolveAgentProjectContext(tmpDir, undefined), '');
    assert.equal(resolveAgentProjectContext(tmpDir, true), '');
  });

  it('returns "" when opted in but there are no instruction files', () => {
    const bare = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agent-omitmd-bare-'));
    try {
      assert.equal(resolveAgentProjectContext(bare, false), '');
    } finally {
      fs.rmSync(bare, { recursive: true, force: true });
    }
  });
});

describe('taskTool — per-agent omitClaudeMd project-context opt-in', () => {
  it('appends the project instructions to the sub-engine prompt when omitClaudeMd is false', async () => {
    const cap: { config?: any; prompts: string[] } = { prompts: [] };
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'conventionagent' },
      ctx({ name: 'conventionagent', systemPrompt: 'be a convention-aware agent', omitClaudeMd: false, tools: ['file_read'] }, cap),
    );
    assert.equal(res.isError, undefined);
    assert.ok(cap.config.systemPrompt.includes(MARKER), cap.config.systemPrompt);
    assert.ok(cap.config.systemPrompt.includes('be a convention-aware agent'), cap.config.systemPrompt);
  });

  it('keeps the lean default prompt (no project instructions) when omitClaudeMd is unset', async () => {
    const cap: { config?: any; prompts: string[] } = { prompts: [] };
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'leanagent' },
      ctx({ name: 'leanagent', systemPrompt: 'be a lean agent', tools: ['file_read'] }, cap),
    );
    assert.ok(!cap.config.systemPrompt.includes(MARKER), cap.config.systemPrompt);
    assert.ok(cap.config.systemPrompt.includes('be a lean agent'), cap.config.systemPrompt);
  });
});
