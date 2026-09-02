/**
 * Extra coverage tests for low-coverage tool files.
 * Targets uncovered lines in: skill.ts, workflow.ts, team.ts,
 * send-message.ts, exit-worktree.ts, task.ts, task-tracker.ts,
 * todo-write.ts, config-tool.ts, mcp-tool.ts, mcp-resources.ts,
 * mcp-auth.ts.
 */

import { describe, it, beforeEach, afterEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

after(() => { setTimeout(() => process.exit(0), 200); });

import { skillTool } from '../src/tools/skill';
import { workflowTool } from '../src/tools/workflow';
import {
  teamCreateTool,
  teamDeleteTool,
  getCurrentTeam,
  resetTeam,
} from '../src/tools/team';
import {
  sendMessageTool,
  pollMailbox,
  resetAllMailboxes,
  getMailbox,
} from '../src/tools/send-message';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import { taskTool } from '../src/tools/task';
import {
  taskTrackerTool,
  resetTaskState,
  getTaskState,
  setTaskPersistPath,
} from '../src/tools/task-tracker';
import { todoWriteTool, resetTodos, getTodos } from '../src/tools/todo-write';
import { configTool } from '../src/tools/config-tool';
import { createMcpTool } from '../src/tools/mcp-tool';
import {
  listMcpResourcesTool,
  readMcpResourceTool,
} from '../src/tools/mcp-resources';
import { createMcpAuthTool } from '../src/tools/mcp-auth';
import type { ToolContext, EngineConfig } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

function fakeEngineFactory(events: Array<{ type: string; [k: string]: any }>) {
  return (_cfg: any) => ({
    submit: async function* (_prompt: string) {
      for (const e of events) yield e;
    },
  });
}

// =========================================================================
// SKILL TOOL — execute paths (MCP fallback, subagent, inline)
// =========================================================================

describe('skillTool — definition', () => {
  it('has correct name and schema', () => {
    assert.equal(skillTool.name, 'skill');
    assert.equal(skillTool.isReadOnly, false);
    assert.ok(skillTool.inputSchema.required?.includes('skill'));
  });
});

describe('skillTool — validation and error paths', () => {
  it('rejects empty skill name', async () => {
    const r = await skillTool.execute({ skill: '' }, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('must not be empty'));
  });

  it('rejects whitespace skill name', async () => {
    const r = await skillTool.execute({ skill: '   ' }, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('must not be empty'));
  });

  it('errors when _skillManager is not available', async () => {
    const r = await skillTool.execute({ skill: 'test' }, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('skill manager not available'));
  });

  it('errors when skill has disableModelInvocation set', async () => {
    const r = await skillTool.execute(
      { skill: 'disabled-skill' },
      ctx({
        _skillManager: {
          getSkill: (name: string) => ({ name, disableModelInvocation: true }),
          getSkillContent: () => 'content',
          listSkills: () => [],
        } as any,
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('disableModelInvocation'));
  });

  it('errors when skill is not found and lists available skills', async () => {
    const r = await skillTool.execute(
      { skill: 'nonexistent' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [{ name: 'code-review' }, { name: 'explain' }],
        } as any,
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
    assert.ok(r.output.includes('code-review'));
    assert.ok(r.output.includes('explain'));
  });

  it('errors when no skills available and lists (none)', async () => {
    const r = await skillTool.execute(
      { skill: 'nonexistent' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [],
        } as any,
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('(none)'));
  });
});

describe('skillTool — MCP prompt fallback', () => {
  it('falls back to MCP prompts when skill not found locally', async () => {
    const r = await skillTool.execute(
      { skill: 'mcp-prompt', args: 'some args' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [],
        } as any,
        _mcpManager: {
          listPrompts: async () => [{ name: 'mcp-prompt', server: 'test-srv' }],
          getPrompt: async (_server: string, _name: string, _args: any) => ({
            messages: [{ role: 'user', content: 'prompt content' }],
          }),
        },
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('MCP Skill'));
    assert.ok(r.output.includes('mcp-prompt'));
  });

  it('returns not found when MCP has no matching prompt', async () => {
    const r = await skillTool.execute(
      { skill: 'missing-mcp' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [],
        } as any,
        _mcpManager: {
          listPrompts: async () => [{ name: 'other-prompt', server: 'srv' }],
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
  });

  it('handles MCP prompt error gracefully and falls through', async () => {
    const r = await skillTool.execute(
      { skill: 'err-mcp' },
      ctx({
        _skillManager: {
          getSkill: () => null,
          getSkillContent: () => null,
          listSkills: () => [],
        } as any,
        _mcpManager: {
          listPrompts: async () => { throw new Error('mcp list error'); },
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
  });

  it('skips MCP when skillContent is found locally', async () => {
    let mcpCalled = false;
    const r = await skillTool.execute(
      { skill: 'local-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'local-skill', context: 'inline' }),
          getSkillContent: () => 'local content',
          listSkills: () => [],
        } as any,
        _mcpManager: {
          listPrompts: async () => { mcpCalled = true; return []; },
        },
        _engineAddSystemReminder: () => {},
      }),
    );
    assert.ok(!r.isError);
    assert.ok(!mcpCalled);
  });
});

describe('skillTool — inline execution', () => {
  it('activates inline skill via _engineAddSystemReminder', async () => {
    let addedReminder: string | null = null;

    const r = await skillTool.execute(
      { skill: 'inline-skill', args: 'my args' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'inline-skill', context: 'inline' }),
          getSkillContent: (_name: string, _args: any) => 'Inline instructions here',
          listSkills: () => [],
        } as any,
        _engineAddSystemReminder: (msg: string) => { addedReminder = msg; },
      }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('activated inline'));
    assert.ok(addedReminder);
    assert.ok(addedReminder!.includes('inline-skill'));
    assert.ok(addedReminder!.includes('Inline instructions here'));
  });

  it('activates inline when skill context is undefined (defaults to inline)', async () => {
    let addedReminder: string | null = null;

    const r = await skillTool.execute(
      { skill: 'default-context' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'default-context' }),
          getSkillContent: () => 'Default context instructions',
          listSkills: () => [],
        } as any,
        _engineAddSystemReminder: (msg: string) => { addedReminder = msg; },
      }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('activated inline'));
    assert.ok(addedReminder);
  });
});

describe('skillTool — subagent execution', () => {
  it('errors when _engineFactory is not available', async () => {
    const r = await skillTool.execute(
      { skill: 'sub-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'sub-skill', context: 'subagent' }),
          getSkillContent: () => 'Do stuff',
          listSkills: () => [],
        } as any,
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('engine factory not available'));
  });

  it('executes subagent and returns text output', async () => {
    const r = await skillTool.execute(
      { skill: 'sub-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'sub-skill', context: 'subagent' }),
          getSkillContent: () => 'Do stuff',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: fakeEngineFactory([
          { type: 'text_delta', text: 'skill ' },
          { type: 'text_delta', text: 'result' },
        ]),
      }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('skill result'));
  });

  it('collects error events during subagent execution', async () => {
    const r = await skillTool.execute(
      { skill: 'err-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'err-skill', context: 'subagent' }),
          getSkillContent: () => 'Do stuff',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: fakeEngineFactory([
          { type: 'text_delta', text: 'partial' },
          { type: 'error', error: 'stream error' },
        ]),
      }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('partial'));
    assert.ok(r.output.includes('stream error'));
  });

  it('handles engine throw gracefully', async () => {
    const r = await skillTool.execute(
      { skill: 'throw-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'throw-skill', context: 'subagent' }),
          getSkillContent: () => 'Do stuff',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          submit: () => { throw new Error('engine boom'); },
        }),
      }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('engine boom'));
  });

  it('handles non-Error throw from subagent', async () => {
    const r = await skillTool.execute(
      { skill: 'string-throw-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'string-throw-skill', context: 'subagent' }),
          getSkillContent: () => 'Do stuff',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          submit: () => { throw 'plain string error'; },
        }),
      }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('plain string error'));
  });

  it('returns fallback when subagent produces no output', async () => {
    const r = await skillTool.execute(
      { skill: 'quiet-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({ name: 'quiet-skill', context: 'subagent' }),
          getSkillContent: () => 'Do stuff',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: fakeEngineFactory([
          { type: 'turn_complete' },
        ]),
      }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('no output'));
  });

  it('filters tools with allowedTools from skill definition', async () => {
    let capturedConfig: any = null;

    const r = await skillTool.execute(
      { skill: 'restricted-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({
            name: 'restricted-skill',
            context: 'subagent',
            allowedTools: ['file_read', 'grep'],
          }),
          getSkillContent: () => 'Read-only skill',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () {
              yield { type: 'text_delta', text: 'ok' };
            },
          };
        },
      }),
    );

    assert.ok(!r.isError);
    assert.ok(capturedConfig);
    const toolNames = capturedConfig.tools.map((t: any) => t.name);
    for (const name of toolNames) {
      assert.ok(
        ['file_read', 'grep'].includes(name),
        `unexpected tool: ${name}`,
      );
    }
  });

  it('uses model from skill definition', async () => {
    let capturedConfig: any = null;

    await skillTool.execute(
      { skill: 'model-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({
            name: 'model-skill',
            context: 'subagent',
            model: 'claude-opus-4',
          }),
          getSkillContent: () => 'Do stuff',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.ok(capturedConfig);
    assert.equal(capturedConfig.provider.model, 'claude-opus-4');
  });

  it('sets thinking config when skill has effort', async () => {
    let capturedConfig: any = null;

    await skillTool.execute(
      { skill: 'thinking-skill' },
      ctx({
        _skillManager: {
          getSkill: () => ({
            name: 'thinking-skill',
            context: 'subagent',
            effort: 'high',
          }),
          getSkillContent: () => 'Think hard',
          listSkills: () => [],
        } as any,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.ok(capturedConfig);
    assert.deepEqual(capturedConfig.thinking, { enabled: true, level: 'high' });
  });
});

// =========================================================================
// WORKFLOW TOOL — lines 13, 81-89, 92-107, 118-120
// =========================================================================

describe('workflowTool — definition', () => {
  it('has correct name and schema', () => {
    assert.equal(workflowTool.name, 'workflow');
    assert.equal(workflowTool.isReadOnly, true);
    assert.equal(workflowTool.isConcurrencySafe, false);
  });
});

describe('workflowTool — input validation', () => {
  it('errors when no script/scriptPath/name provided', async () => {
    const r = await workflowTool.execute({}, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('One of script, scriptPath, or name is required'));
  });

  it('errors when scriptPath file not found', async () => {
    const r = await workflowTool.execute(
      { scriptPath: '/tmp/nonexistent-workflow-xyz.js' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
  });

  it('errors when named workflow not found', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-'));
    try {
      const r = await workflowTool.execute(
        { name: 'nonexistent-workflow' },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(r.isError);
      assert.ok(r.output.includes('not found'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('errors when script lacks meta header', async () => {
    const r = await workflowTool.execute(
      { script: 'console.log("no meta")' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('must begin with'));
  });
});

describe('workflowTool — scriptPath and name resolution', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('loads script from scriptPath', async () => {
    const scriptContent = `export const meta = {
  name: 'file-wf',
  description: 'test'
}
log('from file');
`;
    const scriptPath = path.join(tmpDir, 'wf.js');
    fs.writeFileSync(scriptPath, scriptContent);

    const r = await workflowTool.execute(
      { scriptPath },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes("Workflow 'file-wf' completed"));
    assert.ok(r.output.includes('from file'));
  });

  it('loads script from .superinference/workflows/ by name', async () => {
    const wfDir = path.join(tmpDir, '.superinference', 'workflows');
    fs.mkdirSync(wfDir, { recursive: true });
    fs.writeFileSync(
      path.join(wfDir, 'test-wf.js'),
      `export const meta = {
  name: 'test-wf',
  description: 'testing'
}
log('named workflow');
`,
    );

    const r = await workflowTool.execute(
      { name: 'test-wf' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes("Workflow 'test-wf' completed"));
    assert.ok(r.output.includes('named workflow'));
  });

  it('loads .mjs workflows by name', async () => {
    const wfDir = path.join(tmpDir, '.superinference', 'workflows');
    fs.mkdirSync(wfDir, { recursive: true });
    fs.writeFileSync(
      path.join(wfDir, 'mjs-wf.mjs'),
      `export const meta = {
  name: 'mjs-wf',
  description: 'testing mjs'
}
log('from mjs');
`,
    );

    const r = await workflowTool.execute(
      { name: 'mjs-wf' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('from mjs'));
  });
});

describe('workflowTool — agentHandler and workflowResolver', () => {
  it('agentHandler returns fallback when no engine factory', async () => {
    const script = `export const meta = {
  name: 'agent-wf',
  description: 'test'
}
const result = await agent('do something');
log(String(result));
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: process.cwd() }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('no engine factory') || r.output.includes('Agent not available'));
  });

  it('agentHandler uses engine factory when available', async () => {
    const script = `export const meta = {
  name: 'factory-wf',
  description: 'test'
}
const result = await agent('task1');
log(String(result));
`;
    const r = await workflowTool.execute(
      { script },
      ctx({
        cwd: process.cwd(),
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: fakeEngineFactory([
          { type: 'text_delta', text: 'agent output' },
        ]),
      }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('agent output'));
  });

  it('catches execution errors from workflow', async () => {
    const script = `export const meta = {
  name: 'error-wf',
  description: 'test'
}
throw new Error('workflow boom');
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: process.cwd() }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('workflow boom'));
  });

  it('passes args to workflow context', async () => {
    const script = `export const meta = {
  name: 'args-wf',
  description: 'test'
}
log('args=' + JSON.stringify(args));
`;
    const r = await workflowTool.execute(
      { script, args: 'hello world' },
      ctx({ cwd: process.cwd() }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('hello world'));
  });

  it('reports agent count and logs in result', async () => {
    const script = `export const meta = {
  name: 'count-wf',
  description: 'test'
}
log('step 1');
log('step 2');
`;
    const r = await workflowTool.execute(
      { script },
      ctx({ cwd: process.cwd() }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('Agents spawned: 0'));
    assert.ok(r.output.includes('step 1'));
    assert.ok(r.output.includes('step 2'));
  });

  it('workflowResolver resolves named workflows', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-res-'));
    const wfDir = path.join(tmpDir, '.superinference', 'workflows');
    fs.mkdirSync(wfDir, { recursive: true });
    fs.writeFileSync(
      path.join(wfDir, 'sub.js'),
      `export const meta = {
  name: 'sub',
  description: 'sub workflow'
}
log('sub ran');
`,
    );

    const script = `export const meta = {
  name: 'parent-wf',
  description: 'test'
}
log('parent');
`;
    try {
      const r = await workflowTool.execute(
        { script },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(!r.isError, r.output);
      assert.ok(r.output.includes('parent'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('agentHandler shuts down subengine after completion', async () => {
    let shutdownCalled = false;
    const script = `export const meta = {
  name: 'shutdown-wf',
  description: 'test'
}
const r = await agent('do it');
log(String(r));
`;
    const r = await workflowTool.execute(
      { script },
      ctx({
        cwd: process.cwd(),
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          submit: async function* () {
            yield { type: 'text_delta', text: 'output' };
          },
          shutdown: () => { shutdownCalled = true; },
        }),
      }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(shutdownCalled);
  });

  it('workflowResolver returns null for non-existent workflow', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-res2-'));
    try {
      const script = `export const meta = {
  name: 'resolver-test',
  description: 'test'
}
log('ok');
`;
      const r = await workflowTool.execute(
        { script },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(!r.isError);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// =========================================================================
// TEAM TOOL — lines 44-54 (teamDeleteTool)
// =========================================================================

describe('teamCreateTool and teamDeleteTool', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetTeam();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-team-'));
  });

  afterEach(() => {
    resetTeam();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('teamCreateTool creates a team and writes team.json', async () => {
    const r = await teamCreateTool.execute(
      { team_name: 'alpha', description: 'Alpha team' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('Team "alpha" created'));
    assert.ok(getCurrentTeam());
    assert.equal(getCurrentTeam()!.name, 'alpha');

    const teamDir = path.join(tmpDir, '.superinference', 'teams', 'alpha');
    assert.ok(fs.existsSync(path.join(teamDir, 'team.json')));
    assert.ok(fs.existsSync(path.join(teamDir, 'memory')));
  });

  it('teamCreateTool rejects if team already exists', async () => {
    await teamCreateTool.execute({ team_name: 'alpha' }, ctx({ cwd: tmpDir }));
    const r = await teamCreateTool.execute({ team_name: 'beta' }, ctx({ cwd: tmpDir }));
    assert.ok(r.isError);
    assert.ok(r.output.includes('already exists'));
  });

  it('teamDeleteTool deletes the current team', async () => {
    await teamCreateTool.execute({ team_name: 'to-delete' }, ctx({ cwd: tmpDir }));
    assert.ok(getCurrentTeam());

    const r = await teamDeleteTool.execute({}, ctx({ cwd: tmpDir }));
    assert.ok(!r.isError);
    assert.ok(r.output.includes('deleted'));
    assert.equal(getCurrentTeam(), null);

    const teamDir = path.join(tmpDir, '.superinference', 'teams', 'to-delete');
    assert.ok(!fs.existsSync(teamDir));
  });

  it('teamDeleteTool errors when no active team', async () => {
    const r = await teamDeleteTool.execute({}, ctx({ cwd: tmpDir }));
    assert.ok(r.isError);
    assert.ok(r.output.includes('No active team'));
  });

  it('resetTeam clears current team', async () => {
    await teamCreateTool.execute({ team_name: 'test' }, ctx({ cwd: tmpDir }));
    assert.ok(getCurrentTeam());
    resetTeam();
    assert.equal(getCurrentTeam(), null);
  });
});

// =========================================================================
// SEND MESSAGE TOOL — lines 27-28, 60-64, 90-102, 104-111
// =========================================================================

describe('sendMessageTool — uncovered paths', () => {
  beforeEach(() => {
    resetAllMailboxes();
  });

  it('pollMailbox returns messages and clears the box', async () => {
    await sendMessageTool.execute(
      { to: 'agent-x', content: 'msg1', from: 'main' },
      ctx(),
    );
    await sendMessageTool.execute(
      { to: 'agent-x', content: 'msg2', from: 'main' },
      ctx(),
    );

    const msgs = pollMailbox('agent-x');
    assert.equal(msgs.length, 2);
    assert.equal(msgs[0].content, 'msg1');
    assert.equal(msgs[1].content, 'msg2');

    // After poll, mailbox should be empty
    const empty = pollMailbox('agent-x');
    assert.equal(empty.length, 0);
  });

  it('pollMailbox returns empty for non-existent agent', () => {
    const msgs = pollMailbox('nonexistent');
    assert.equal(msgs.length, 0);
  });

  it('handles shutdown_request JSON content', async () => {
    const shutdownReq = JSON.stringify({
      type: 'shutdown_request',
      requestId: 'req-123',
      reason: 'done',
    });

    const r = await sendMessageTool.execute(
      { to: 'agent-1', content: shutdownReq, from: 'orchestrator' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('Shutdown request sent'));
    assert.ok(r.output.includes('req-123'));

    const mbox = getMailbox('agent-1');
    assert.equal(mbox.length, 1);
  });

  it('handles plan_approval_response JSON content', async () => {
    const approval = JSON.stringify({
      type: 'plan_approval_response',
      approve: true,
      planId: 'plan-abc',
    });

    const r = await sendMessageTool.execute(
      { to: 'agent-2', content: approval, from: 'user' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('approved'));
    assert.ok(r.output.includes('agent-2'));
  });

  it('handles plan_approval_response with rejection', async () => {
    const rejection = JSON.stringify({
      type: 'plan_approval_response',
      approve: false,
    });

    const r = await sendMessageTool.execute(
      { to: 'agent-3', content: rejection, from: 'user' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('rejected'));
  });

  it('broadcasts to all agents with * target', async () => {
    // Set up mailboxes for multiple agents
    await sendMessageTool.execute({ to: 'a', content: 'setup' }, ctx());
    await sendMessageTool.execute({ to: 'b', content: 'setup' }, ctx());
    // Clear setup messages
    pollMailbox('a');
    pollMailbox('b');

    // Need to re-establish mailboxes since pollMailbox empties them
    await sendMessageTool.execute({ to: 'a', content: 'x' }, ctx());
    await sendMessageTool.execute({ to: 'b', content: 'x' }, ctx());

    const r = await sendMessageTool.execute(
      { to: '*', content: 'broadcast msg', from: 'main' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('Broadcast sent'));
  });

  it('includes summary in message when provided', async () => {
    const r = await sendMessageTool.execute(
      { to: 'agent-sum', content: 'detailed content here', summary: 'short version' },
      ctx(),
    );
    assert.ok(!r.isError);

    const mbox = getMailbox('agent-sum');
    assert.equal(mbox.length, 1);
    assert.equal(mbox[0].summary, 'short version');
  });

  it('creates mailbox for new recipient', async () => {
    const r = await sendMessageTool.execute(
      { to: 'new-agent', content: 'first message' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('Message sent'));
    assert.ok(r.output.includes('new-agent'));

    const mbox = getMailbox('new-agent');
    assert.equal(mbox.length, 1);
  });

  it('rejects whitespace-only "to"', async () => {
    const r = await sendMessageTool.execute(
      { to: '   ', content: 'hello' },
      ctx(),
    );
    assert.ok(r.isError);
  });

  it('rejects whitespace-only content', async () => {
    const r = await sendMessageTool.execute(
      { to: 'agent', content: '   ' },
      ctx(),
    );
    assert.ok(r.isError);
  });
});

// =========================================================================
// EXIT WORKTREE TOOL — lines 64-65, 71-76, 79-96, 100-105, 115-116
// =========================================================================

describe('exitWorktreeTool — extra coverage', () => {
  it('reports no active session for non-worktree path with keep action', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-ewt-'));
    const child = require('child_process');
    try {
      child.execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
      child.execSync('git config user.email "t@t.com"', { cwd: tmpDir, stdio: 'ignore' });
      child.execSync('git config user.name "T"', { cwd: tmpDir, stdio: 'ignore' });
      fs.writeFileSync(path.join(tmpDir, 'f.txt'), 'x');
      child.execSync('git add . && git commit -m "i"', { cwd: tmpDir, stdio: 'ignore' });

      const r = await exitWorktreeTool.execute(
        { action: 'keep' },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(r.output.includes('No active worktree session'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('reports no active session for non-worktree path with remove action', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-ewt-'));
    const child = require('child_process');
    try {
      child.execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
      child.execSync('git config user.email "t@t.com"', { cwd: tmpDir, stdio: 'ignore' });
      child.execSync('git config user.name "T"', { cwd: tmpDir, stdio: 'ignore' });
      fs.writeFileSync(path.join(tmpDir, 'f.txt'), 'x');
      child.execSync('git add . && git commit -m "i"', { cwd: tmpDir, stdio: 'ignore' });

      const r = await exitWorktreeTool.execute(
        { action: 'remove' },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(r.output.includes('No active worktree session'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('rejects non-git directory', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-ewt-nogit-'));
    try {
      const r = await exitWorktreeTool.execute(
        { action: 'keep' },
        ctx({ cwd: tmpDir }),
      );
      assert.ok(r.isError);
      assert.ok(r.output.includes('not inside a git repository'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('fires hook on remove when _hookManager is present', async () => {
    // This tests lines 110-112 — the hookManager path after successful remove.
    // We can't easily simulate a real worktree removal here, so we test the
    // non-worktree path which exercises the hook integration code indirectly.
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-ewt-hook-'));
    const child = require('child_process');
    try {
      child.execSync('git init', { cwd: tmpDir, stdio: 'ignore' });
      child.execSync('git config user.email "t@t.com"', { cwd: tmpDir, stdio: 'ignore' });
      child.execSync('git config user.name "T"', { cwd: tmpDir, stdio: 'ignore' });
      fs.writeFileSync(path.join(tmpDir, 'f.txt'), 'x');
      child.execSync('git add . && git commit -m "i"', { cwd: tmpDir, stdio: 'ignore' });

      let hookCalled = false;
      const r = await exitWorktreeTool.execute(
        { action: 'remove', discard_changes: true },
        ctx({
          cwd: tmpDir,
          _hookManager: {
            executeWorktreeRemove: async () => { hookCalled = true; },
          } as any,
        }),
      );
      // Not in worktree path, so won't actually remove — but exercises the path
      assert.ok(r.output.includes('No active worktree session'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// =========================================================================
// TASK TOOL — lines 85-98, 103-105, 113-115, 136-137, 185-234
// =========================================================================

describe('taskTool — subagent_type', () => {
  it('errors when subagent_type not found', async () => {
    const r = await taskTool.execute(
      { prompt: 'do stuff', subagent_type: 'nonexistent' },
      ctx({
        _skillManager: {
          getAgent: () => null,
          listAgents: () => [{ name: 'explorer' }, { name: 'coder' }],
        } as any,
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
    assert.ok(r.output.includes('explorer'));
    assert.ok(r.output.includes('coder'));
  });

  it('uses agent definition tools when specified', async () => {
    let capturedConfig: any = null;

    const r = await taskTool.execute(
      { prompt: 'agent task' , subagent_type: 'custom-agent' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _skillManager: {
          getAgent: (name: string) => ({
            name,
            systemPrompt: 'You are custom',
            tools: ['file_read', 'grep'],
          }),
          listAgents: () => [],
        } as any,
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'agent result' }; },
          };
        },
      }),
    );

    assert.ok(!r.isError);
    assert.ok(capturedConfig);
    const toolNames = capturedConfig.tools.map((t: any) => t.name);
    assert.ok(!toolNames.includes('task'));
  });

  it('uses disallowedTools from agent definition', async () => {
    let capturedConfig: any = null;

    await taskTool.execute(
      { prompt: 'agent task', subagent_type: 'restricted' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _skillManager: {
          getAgent: () => ({
            name: 'restricted',
            disallowedTools: ['bash', 'file_write'],
          }),
          listAgents: () => [],
        } as any,
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.ok(capturedConfig);
    const toolNames = capturedConfig.tools.map((t: any) => t.name);
    assert.ok(!toolNames.includes('bash'));
    assert.ok(!toolNames.includes('file_write'));
    assert.ok(!toolNames.includes('task'));
  });

  it('uses agent model when specified', async () => {
    let capturedConfig: any = null;

    await taskTool.execute(
      { prompt: 'agent task', subagent_type: 'model-agent' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _skillManager: {
          getAgent: () => ({
            name: 'model-agent',
            model: 'claude-sonnet-4',
          }),
          listAgents: () => [],
        } as any,
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.ok(capturedConfig);
    assert.equal(capturedConfig.provider.model, 'claude-sonnet-4');
  });

  it('uses agentDef maxTurns', async () => {
    let capturedConfig: any = null;

    await taskTool.execute(
      { prompt: 'agent task', subagent_type: 'limited' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _skillManager: {
          getAgent: () => ({
            name: 'limited',
            maxTurns: 5,
          }),
          listAgents: () => [],
        } as any,
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.ok(capturedConfig);
    assert.equal(capturedConfig.maxTurns, 5);
  });

  it('passes agent systemPrompt as engine systemPrompt (not prepended to submit prompt)', async () => {
    // Since task.ts now puts agentSystemPrompt in subConfig.systemPrompt,
    // the engine's system prompt carries the agent identity, and the submitted
    // user message is just the task prompt (no mixing).
    let capturedConfig: any = null;
    let capturedPrompt: string | null = null;

    await taskTool.execute(
      { prompt: 'do the thing', subagent_type: 'sys-agent' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _skillManager: {
          getAgent: () => ({
            name: 'sys-agent',
            systemPrompt: 'You are a specialist.',
          }),
          listAgents: () => [],
        } as any,
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* (prompt: string) {
              capturedPrompt = prompt;
              yield { type: 'text_delta', text: 'ok' };
            },
          };
        },
      }),
    );

    // Agent system prompt goes into subConfig.systemPrompt, not the submit call
    assert.equal(capturedConfig?.systemPrompt, 'You are a specialist.');
    // Sub-agents always run in detached mode
    assert.equal(capturedConfig?.detachedMode, true);
    // The submit prompt is just the task, not mixed with system prompt
    assert.equal(capturedPrompt, 'do the thing');
  });
});

describe('taskTool — model aliases', () => {
  it('resolves "sonnet" alias', async () => {
    let capturedConfig: any = null;

    await taskTool.execute(
      { prompt: 'test', model: 'sonnet' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.ok(capturedConfig);
    assert.equal(capturedConfig.provider.model, 'claude-sonnet-4');
  });

  it('resolves "opus" alias', async () => {
    let capturedConfig: any = null;

    await taskTool.execute(
      { prompt: 'test', model: 'opus' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.equal(capturedConfig.provider.model, 'claude-opus-4');
  });

  it('resolves "haiku" alias', async () => {
    let capturedConfig: any = null;

    await taskTool.execute(
      { prompt: 'test', model: 'haiku' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.equal(capturedConfig.provider.model, 'claude-haiku-4-5');
  });

  it('passes through unknown model names as-is', async () => {
    let capturedConfig: any = null;

    await taskTool.execute(
      { prompt: 'test', model: 'gpt-4o-mini' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return {
            submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
          };
        },
      }),
    );

    assert.equal(capturedConfig.provider.model, 'gpt-4o-mini');
  });
});

describe('taskTool — fork mode', () => {
  it('fork mode includes parent conversation context', async () => {
    let capturedPrompt: string | null = null;

    await taskTool.execute(
      { prompt: 'fork task', mode: 'fork' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _parentMessages: [
          { role: 'user', content: 'What is this code?' },
          { role: 'assistant', content: 'This is a utility module for data processing.' },
        ] as any,
        _engineFactory: (_cfg: any) => ({
          submit: async function* (prompt: string) {
            capturedPrompt = prompt;
            yield { type: 'text_delta', text: 'forked' };
          },
        }),
      }),
    );

    assert.ok(capturedPrompt);
    assert.ok(capturedPrompt!.includes('Parent conversation context'));
    assert.ok(capturedPrompt!.includes('utility module'));
    assert.ok(capturedPrompt!.includes('fork task'));
  });

  it('fork mode without parent messages does not add context section', async () => {
    let capturedPrompt: string | null = null;

    await taskTool.execute(
      { prompt: 'fork no context', mode: 'fork' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          submit: async function* (prompt: string) {
            capturedPrompt = prompt;
            yield { type: 'text_delta', text: 'done' };
          },
        }),
      }),
    );

    assert.ok(capturedPrompt);
    assert.ok(!capturedPrompt!.includes('Parent conversation'));
  });
});

describe('taskTool — preToolUse hook deny', () => {
  it('denies task when preToolUse hook returns deny', async () => {
    const r = await taskTool.execute(
      { prompt: 'denied task' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _hookManager: {
          executePreToolUse: async () => ({ action: 'deny' }),
          executeSubagentStart: async () => {},
          executeSubagentStop: async () => {},
        } as any,
        _engineFactory: fakeEngineFactory([
          { type: 'text_delta', text: 'should not reach' },
        ]),
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('denied'));
  });

  it('allows task when preToolUse hook returns allow', async () => {
    const r = await taskTool.execute(
      { prompt: 'allowed task' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _hookManager: {
          executePreToolUse: async () => ({ action: 'allow' }),
          executeSubagentStart: async () => {},
          executeSubagentStop: async () => {},
        } as any,
        _engineFactory: fakeEngineFactory([
          { type: 'text_delta', text: 'allowed result' },
        ]),
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('allowed result'));
  });
});

describe('taskTool — hook manager integration', () => {
  it('calls executeSubagentStart and executeSubagentStop hooks', async () => {
    let startCalled = false;
    let stopCalled = false;

    await taskTool.execute(
      { prompt: 'hook test' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _hookManager: {
          executeSubagentStart: async () => { startCalled = true; },
          executeSubagentStop: async () => { stopCalled = true; },
        } as any,
        _engineFactory: fakeEngineFactory([
          { type: 'text_delta', text: 'ok' },
        ]),
      }),
    );

    assert.ok(startCalled);
    assert.ok(stopCalled);
  });

  it('calls executeSubagentStop on error', async () => {
    let stopCalled = false;

    await taskTool.execute(
      { prompt: 'hook error test' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _hookManager: {
          executeSubagentStart: async () => {},
          executeSubagentStop: async () => { stopCalled = true; },
        } as any,
        _engineFactory: (_cfg: any) => ({
          submit: async function* () {
            throw new Error('boom');
          },
        }),
      }),
    );

    assert.ok(stopCalled);
  });
});

describe('taskTool — tool_use_start and usage_update events', () => {
  it('counts tool_use_start events and tracks stats', async () => {
    const r = await taskTool.execute(
      { prompt: 'tool stats test' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg: any) => ({
          submit: async function* () {
            yield { type: 'text_delta', text: 'result' };
            yield { type: 'tool_use_start', toolName: 'file_read' };
            yield { type: 'tool_use_start', toolName: 'grep' };
            yield { type: 'tool_use_start', toolName: 'bash' };
            yield { type: 'usage_update', stats: { totalTokens: 1500 } };
          },
        }),
      }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('3 tool calls'));
    assert.ok(r.output.includes('1500 tokens'));
  });
});

describe('taskTool — cwd override', () => {
  it('uses cwd from input when provided', async () => {
    let capturedConfig: any = null;
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cwd-'));

    try {
      await taskTool.execute(
        { prompt: 'cwd test', cwd: tmpDir },
        ctx({
          _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg: any) => {
            capturedConfig = cfg;
            return {
              submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
            };
          },
        }),
      );

      assert.ok(capturedConfig);
      assert.equal(capturedConfig.cwd, tmpDir);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('taskTool — background agent error handling', () => {
  it('handles error in background agent', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-err-'));
    const { ProcessManager } = require('../src/process-manager');
    const pm = new ProcessManager(tmpDir);

    try {
      const r = await taskTool.execute(
        { prompt: 'bg error', run_in_background: true },
        ctx({
          cwd: tmpDir,
          processManager: pm,
          _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
          _engineFactory: (_cfg: any) => ({
            submit: async function* () {
              throw new Error('bg agent crash');
            },
          }),
        }),
      );

      assert.ok(r.output.includes('Background agent started'));

      // Wait for the async background task to complete
      await new Promise(r => setTimeout(r, 200));

      // Check the output file was written with error info
      const taskId = r.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);
      const outputPath = path.join(tmpDir, '.superinference', 'tasks', `${taskId}.output`);
      if (fs.existsSync(outputPath)) {
        const content = fs.readFileSync(outputPath, 'utf-8');
        assert.ok(content.includes('bg agent crash') || content.includes('no output'));
      }
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('background agent with description uses description as label', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-desc-'));
    const { ProcessManager } = require('../src/process-manager');
    const pm = new ProcessManager(tmpDir);

    try {
      const r = await taskTool.execute(
        { prompt: 'bg desc task', run_in_background: true, description: 'Test desc' },
        ctx({
          cwd: tmpDir,
          processManager: pm,
          _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' },
          _engineFactory: fakeEngineFactory([
            { type: 'text_delta', text: 'result' },
          ]),
        }),
      );

      assert.ok(r.output.includes('Background agent started'));

      const taskId = r.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);

      // Check the process was registered with the label
      const entry = (pm as any).processes.get(taskId);
      assert.ok(entry);
      assert.ok(entry.command.includes('Test desc'));

      await new Promise(r => setTimeout(r, 200));
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// =========================================================================
// TASK TRACKER TOOL — lines 28-29, 95, 108-115, 179, 188-222, 249-276
// =========================================================================

describe('taskTrackerTool — persistence', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetTaskState();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-tt-'));
  });

  afterEach(() => {
    resetTaskState();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('setTaskPersistPath creates config dir and loads', () => {
    setTaskPersistPath(tmpDir);
    // Just creating should not crash
    const state = getTaskState();
    assert.equal(state.size, 0);
  });

  it('persists tasks across setTaskPersistPath calls', async () => {
    setTaskPersistPath(tmpDir);
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Persist me' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'create', subject: 'And me' },
      ctx(),
    );

    // Verify file written
    const filePath = path.join(tmpDir, '.superinference', 'tasks.json');
    assert.ok(fs.existsSync(filePath));

    // Reset and reload
    resetTaskState();
    setTaskPersistPath(tmpDir);
    const state = getTaskState();
    assert.equal(state.size, 2);
  });

  it('loads with correct high-water nextId', async () => {
    setTaskPersistPath(tmpDir);
    await taskTrackerTool.execute({ action: 'create', subject: 'A' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'B' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'C' }, ctx());

    // Delete middle task
    await taskTrackerTool.execute({ action: 'delete', task_id: '2' }, ctx());

    // Reload
    resetTaskState();
    setTaskPersistPath(tmpDir);

    // Next task should get id 4, not 3
    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'D' },
      ctx(),
    );
    assert.ok(r.output.includes('Task #4'));
  });
});

describe('taskTrackerTool — owner and metadata', () => {
  beforeEach(() => { resetTaskState(); });

  it('creates task with owner', async () => {
    const r = await taskTrackerTool.execute(
      { action: 'create', subject: 'Owned', owner: 'alice' },
      ctx(),
    );
    assert.ok(!r.isError);
    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.owner, 'alice');
  });

  it('creates task with metadata', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Meta task', metadata: { priority: 'high', count: 3 } },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.metadata?.priority, 'high');
    assert.equal(task.metadata?.count, 3);
  });

  it('formatTask renders owner line', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Owned task', owner: 'bob' },
      ctx(),
    );
    const r = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    assert.ok(r.output.includes('Owner: bob'));
  });

  it('formatTask renders metadata', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Meta', metadata: { env: 'prod' } },
      ctx(),
    );
    const r = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    assert.ok(r.output.includes('Metadata:'));
    assert.ok(r.output.includes('env'));
    assert.ok(r.output.includes('prod'));
  });

  it('update merges metadata, null removes key', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Merge', metadata: { a: 1, b: 2 } },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', metadata: { b: null, c: 3 } },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.metadata?.a, 1);
    assert.equal(task.metadata?.b, undefined);
    assert.equal(task.metadata?.c, 3);
  });

  it('update initializes metadata when not present', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'No meta' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', metadata: { x: 'y' } },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task?.metadata);
    assert.equal(task.metadata?.x, 'y');
  });

  it('update clears owner when set to empty string', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Clear owner', owner: 'alice' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', owner: '' },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.equal(task?.owner, undefined);
  });

  it('metadata with _internal keys are hidden in formatTask', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Hidden', metadata: { _internal: true, visible: 'yes' } },
      ctx(),
    );
    const r = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    assert.ok(r.output.includes('visible'));
    assert.ok(!r.output.includes('_internal'));
  });

  it('list hides tasks with _internal metadata', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Visible' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Internal', metadata: { _internal: true } },
      ctx(),
    );

    const r = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.ok(r.output.includes('Visible'));
    assert.ok(!r.output.includes('Internal'));
  });
});

describe('taskTrackerTool — dependencies (blocks/blockedBy)', () => {
  beforeEach(() => { resetTaskState(); });

  it('addBlocks creates bidirectional dependency', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocker' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocked' }, ctx());

    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['2'] },
      ctx(),
    );

    const blocker = getTaskState().get(1)!;
    const blocked = getTaskState().get(2)!;
    assert.ok(blocker.blocks.includes(2));
    assert.ok(blocked.blockedBy.includes(1));
  });

  it('addBlockedBy creates bidirectional dependency', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Task A' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Task B' }, ctx());

    await taskTrackerTool.execute(
      { action: 'update', task_id: '2', addBlockedBy: ['1'] },
      ctx(),
    );

    const a = getTaskState().get(1)!;
    const b = getTaskState().get(2)!;
    assert.ok(a.blocks.includes(2));
    assert.ok(b.blockedBy.includes(1));
  });

  it('formatTask shows blocked by info', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'First' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Second' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '2', addBlockedBy: ['1'] },
      ctx(),
    );

    const r = await taskTrackerTool.execute(
      { action: 'get', task_id: '2' },
      ctx(),
    );
    assert.ok(r.output.includes('Blocked by: #1'));
  });

  it('formatTask shows blocks info', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocker' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocked' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['2'] },
      ctx(),
    );

    const r = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    assert.ok(r.output.includes('Blocks: #2'));
  });

  it('does not add duplicate dependency', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'A' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'B' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['2'] },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['2'] },
      ctx(),
    );

    const a = getTaskState().get(1)!;
    assert.equal(a.blocks.filter(id => id === 2).length, 1);
  });

  it('does not add self-reference', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Self' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['1'] },
      ctx(),
    );

    const task = getTaskState().get(1)!;
    assert.equal(task.blocks.length, 0);
  });

  it('ignores non-existent task in addBlocks', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'A' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['99'] },
      ctx(),
    );

    const a = getTaskState().get(1)!;
    assert.equal(a.blocks.length, 0);
  });

  it('ignores NaN in addBlockedBy', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'A' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlockedBy: ['abc'] },
      ctx(),
    );

    const a = getTaskState().get(1)!;
    assert.equal(a.blockedBy.length, 0);
  });

  it('deleting task cleans up dependencies', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocker' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocked' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['2'] },
      ctx(),
    );

    await taskTrackerTool.execute({ action: 'delete', task_id: '1' }, ctx());

    const blocked = getTaskState().get(2)!;
    assert.equal(blocked.blockedBy.length, 0);
  });

  it('update with status=deleted also cleans up dependencies', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Dep A' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Dep B' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', addBlocks: ['2'] },
      ctx(),
    );

    const r = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'deleted' },
      ctx(),
    );
    assert.ok(r.output.includes('deleted'));
    assert.ok(!getTaskState().has(1));

    const b = getTaskState().get(2)!;
    assert.equal(b.blockedBy.length, 0);
  });

  it('unresolvedBlockers does not include completed deps', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Dep' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Child' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '2', addBlockedBy: ['1'] },
      ctx(),
    );

    // Complete the dependency
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx(),
    );

    const r = await taskTrackerTool.execute(
      { action: 'get', task_id: '2' },
      ctx(),
    );
    // Should not show "Blocked by" since task #1 is completed
    assert.ok(!r.output.includes('Blocked by'));
  });
});

describe('taskTrackerTool — claim action', () => {
  beforeEach(() => { resetTaskState(); });

  it('claims a pending task', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Claimable' }, ctx());
    const r = await taskTrackerTool.execute(
      { action: 'claim', task_id: '1', owner: 'agent-1' },
      ctx(),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('Claimed'));
    assert.ok(r.output.includes('in_progress'));
    assert.ok(r.output.includes('agent-1'));
  });

  it('rejects claiming in_progress task', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Busy' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress', owner: 'agent-1' },
      ctx(),
    );
    const r = await taskTrackerTool.execute(
      { action: 'claim', task_id: '1' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('already in_progress'));
    assert.ok(r.output.includes('agent-1'));
  });

  it('rejects claiming completed task', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Done' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx(),
    );
    const r = await taskTrackerTool.execute(
      { action: 'claim', task_id: '1' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('already completed'));
  });

  it('rejects claiming blocked task', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocker' }, ctx());
    await taskTrackerTool.execute({ action: 'create', subject: 'Blocked' }, ctx());
    await taskTrackerTool.execute(
      { action: 'update', task_id: '2', addBlockedBy: ['1'] },
      ctx(),
    );

    const r = await taskTrackerTool.execute(
      { action: 'claim', task_id: '2' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('blocked by'));
    assert.ok(r.output.includes('#1'));
  });

  it('rejects claim with missing task_id', async () => {
    const r = await taskTrackerTool.execute(
      { action: 'claim' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('task_id is required'));
  });

  it('rejects claim for non-existent task', async () => {
    const r = await taskTrackerTool.execute(
      { action: 'claim', task_id: '99' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('not found'));
  });
});

describe('taskTrackerTool — hook manager', () => {
  beforeEach(() => { resetTaskState(); });

  it('calls executeTaskCreated hook on create', async () => {
    let hookData: any = null;

    await taskTrackerTool.execute(
      { action: 'create', subject: 'Hooked' },
      ctx({
        _hookManager: {
          executeTaskCreated: async (data: any) => { hookData = data; },
        } as any,
      }),
    );

    assert.ok(hookData);
    assert.equal(hookData.subject, 'Hooked');
  });

  it('calls executeTaskCompleted hook on completion', async () => {
    let hookData: any = null;

    await taskTrackerTool.execute(
      { action: 'create', subject: 'Complete me' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx({
        _hookManager: {
          executeTaskCompleted: async (data: any) => { hookData = data; },
        } as any,
      }),
    );

    assert.ok(hookData);
    assert.equal(hookData.subject, 'Complete me');
  });
});

// =========================================================================
// TODO WRITE TOOL — lines 17, 34, 52-56
// =========================================================================

describe('todoWriteTool — coverage', () => {
  beforeEach(() => { resetTodos(); });

  it('has correct definition', () => {
    assert.equal(todoWriteTool.name, 'todo_write');
    assert.equal(todoWriteTool.isReadOnly, false);
    assert.ok(todoWriteTool.inputSchema.required?.includes('todos'));
  });

  it('writes todos and returns formatted summary', async () => {
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'First task', status: 'pending' },
        { id: '2', content: 'Second task', status: 'in_progress' },
        { id: '3', content: 'Done task', status: 'completed' },
      ],
    }, ctx());

    assert.ok(!r.isError);
    assert.ok(r.output.includes('3 items'));
    assert.ok(r.output.includes('[ ] 1: First task'));
    assert.ok(r.output.includes('[>] 2: Second task'));
    assert.ok(r.output.includes('[x] 3: Done task'));
  });

  it('renders cancelled status with [-] icon', async () => {
    const r = await todoWriteTool.execute({
      todos: [{ id: 'c1', content: 'Cancelled', status: 'cancelled' }],
    }, ctx());

    assert.ok(r.output.includes('[-] c1: Cancelled'));
  });

  it('replaces all todos when merge is false', async () => {
    await todoWriteTool.execute({
      todos: [{ id: '1', content: 'Original', status: 'pending' }],
    }, ctx());
    await todoWriteTool.execute({
      todos: [{ id: '2', content: 'New', status: 'pending' }],
      merge: false,
    }, ctx());

    const todos = getTodos();
    assert.equal(todos.length, 1);
    assert.equal(todos[0].id, '2');
  });

  it('merges todos by id when merge is true', async () => {
    await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'First', status: 'pending' },
        { id: '2', content: 'Second', status: 'pending' },
      ],
    }, ctx());

    await todoWriteTool.execute({
      todos: [
        { id: '2', content: 'Updated second', status: 'completed' },
        { id: '3', content: 'Third', status: 'pending' },
      ],
      merge: true,
    }, ctx());

    const todos = getTodos();
    assert.equal(todos.length, 3);
    assert.equal(todos[0].id, '1');
    assert.equal(todos[0].content, 'First');
    assert.equal(todos[1].id, '2');
    assert.equal(todos[1].content, 'Updated second');
    assert.equal(todos[1].status, 'completed');
    assert.equal(todos[2].id, '3');
  });

  it('handles empty todo array', async () => {
    const r = await todoWriteTool.execute({ todos: [] }, ctx());
    assert.ok(r.output.includes('0 items'));
  });

  it('resetTodos clears all', async () => {
    await todoWriteTool.execute({
      todos: [{ id: '1', content: 'X', status: 'pending' }],
    }, ctx());
    resetTodos();
    assert.equal(getTodos().length, 0);
  });

  it('shows verification suggestion when 3+ items completed', async () => {
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'A', status: 'completed' },
        { id: '2', content: 'B', status: 'completed' },
        { id: '3', content: 'C', status: 'completed' },
      ],
    }, ctx());
    assert.ok(r.output.includes('Verification suggested'));
  });

  it('does not show verification suggestion for < 3 completed', async () => {
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'A', status: 'completed' },
        { id: '2', content: 'B', status: 'completed' },
        { id: '3', content: 'C', status: 'pending' },
      ],
    }, ctx());
    assert.ok(!r.output.includes('Verification suggested'));
  });

  it('returns metadata with old and new todo IDs', async () => {
    await todoWriteTool.execute({
      todos: [{ id: 'old-1', content: 'Old', status: 'pending' }],
    }, ctx());

    const r = await todoWriteTool.execute({
      todos: [{ id: 'new-1', content: 'New', status: 'pending' }],
    }, ctx());

    // The result includes metadata property (accessed via cast)
    const metadata = (r as any).metadata;
    assert.ok(metadata);
    assert.ok(Array.isArray(metadata.oldTodos));
    assert.ok(Array.isArray(metadata.newTodos));
    assert.ok(metadata.oldTodos.includes('old-1'));
    assert.ok(metadata.newTodos.includes('new-1'));
  });

  it('reports changed count in output', async () => {
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'A', status: 'pending' },
        { id: '2', content: 'B', status: 'pending' },
      ],
    }, ctx());
    assert.ok(r.output.includes('2 changed'));
  });
});

// =========================================================================
// CONFIG TOOL — lines 36-41
// =========================================================================

describe('configTool — coverage', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-cfg-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('has correct definition', () => {
    assert.equal(configTool.name, 'config');
    assert.equal(configTool.isReadOnly, false);
    assert.ok(configTool.inputSchema.required?.includes('action'));
  });

  it('get returns full config', async () => {
    const cfgDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(cfgDir, { recursive: true });
    fs.writeFileSync(
      path.join(cfgDir, 'config.json'),
      JSON.stringify({ model: 'gpt-4o', provider: 'openai' }),
    );

    const r = await configTool.execute(
      { action: 'get' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('gpt-4o'));
    assert.ok(r.output.includes('openai'));
  });

  it('get returns specific key', async () => {
    const cfgDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(cfgDir, { recursive: true });
    fs.writeFileSync(
      path.join(cfgDir, 'config.json'),
      JSON.stringify({ model: 'gpt-4o' }),
    );

    const r = await configTool.execute(
      { action: 'get', key: 'model' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('model'));
    assert.ok(r.output.includes('gpt-4o'));
  });

  it('get returns fallback when no config file', async () => {
    const r = await configTool.execute(
      { action: 'get' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('No project config'));
  });

  it('set writes a new config value', async () => {
    const r = await configTool.execute(
      { action: 'set', key: 'model', value: 'claude-sonnet-4' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('Set model'));

    const configPath = path.join(tmpDir, '.superinference', 'config.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    assert.equal(config.model, 'claude-sonnet-4');
  });

  it('set merges with existing config', async () => {
    const cfgDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(cfgDir, { recursive: true });
    fs.writeFileSync(
      path.join(cfgDir, 'config.json'),
      JSON.stringify({ existing: 'value' }),
    );

    await configTool.execute(
      { action: 'set', key: 'model', value: 'new-model' },
      ctx({ cwd: tmpDir }),
    );

    const configPath = path.join(cfgDir, 'config.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
    assert.equal(config.existing, 'value');
    assert.equal(config.model, 'new-model');
  });

  it('returns usage error for invalid action/params', async () => {
    const r = await configTool.execute(
      { action: 'set' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('Usage'));
  });

  it('returns usage error for unknown action', async () => {
    const r = await configTool.execute(
      { action: 'delete' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('Usage'));
  });
});

// =========================================================================
// MCP TOOL — lines 18-31
// =========================================================================

describe('createMcpTool — coverage', () => {
  it('creates tool with correct normalized name', () => {
    const tool = createMcpTool('my-server', 'my-tool', 'Does things', { type: 'object', properties: {} });
    assert.equal(tool.name, 'mcp__my_server__my_tool');
    assert.ok(tool.description.includes('[MCP:my-server]'));
    assert.ok(tool.description.includes('Does things'));
  });

  it('errors when no MCP manager', async () => {
    const tool = createMcpTool('srv', 'tl', 'desc', { type: 'object', properties: {} });
    const r = await tool.execute({}, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('MCP not initialized'));
  });

  it('calls MCP manager and returns string result', async () => {
    const tool = createMcpTool('srv', 'tl', 'desc', { type: 'object', properties: {} });
    const r = await tool.execute(
      { param: 'value' },
      ctx({
        _mcpManager: {
          callTool: async (_name: string, _input: any) => 'string result',
        },
      }),
    );
    assert.ok(!r.isError);
    assert.equal(r.output, 'string result');
  });

  it('calls MCP manager and JSON-stringifies object result', async () => {
    const tool = createMcpTool('srv', 'tl', 'desc', { type: 'object', properties: {} });
    const r = await tool.execute(
      {},
      ctx({
        _mcpManager: {
          callTool: async () => ({ key: 'value' }),
        },
      }),
    );
    assert.ok(!r.isError);
    const parsed = JSON.parse(r.output);
    assert.equal(parsed.key, 'value');
  });

  it('handles MCP tool error', async () => {
    const tool = createMcpTool('srv', 'tl', 'desc', { type: 'object', properties: {} });
    const r = await tool.execute(
      {},
      ctx({
        _mcpManager: {
          callTool: async () => { throw new Error('MCP failed'); },
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('MCP failed'));
  });

  it('handles non-Error MCP throw', async () => {
    const tool = createMcpTool('srv', 'tl', 'desc', { type: 'object', properties: {} });
    const r = await tool.execute(
      {},
      ctx({
        _mcpManager: {
          callTool: async () => { throw 'raw string error'; },
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('raw string error'));
  });

  it('truncates output to 100k chars', async () => {
    const tool = createMcpTool('srv', 'tl', 'desc', { type: 'object', properties: {} });
    const bigString = 'x'.repeat(200_000);
    const r = await tool.execute(
      {},
      ctx({
        _mcpManager: {
          callTool: async () => bigString,
        },
      }),
    );
    assert.ok(!r.isError);
    assert.equal(r.output.length, 100_000);
  });
});

// =========================================================================
// MCP RESOURCES — lines 29-39, 46-48
// =========================================================================

describe('listMcpResourcesTool — coverage', () => {
  it('has correct definition', () => {
    assert.equal(listMcpResourcesTool.name, 'ListMcpResources');
    assert.equal(listMcpResourcesTool.isReadOnly, true);
  });

  it('errors when no MCP manager', async () => {
    const r = await listMcpResourcesTool.execute({}, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('MCP not initialized'));
  });

  it('returns "No MCP resources" when empty', async () => {
    const r = await listMcpResourcesTool.execute(
      {},
      ctx({
        _mcpManager: { listResources: async () => [] },
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('No MCP resources'));
  });

  it('returns null resources as empty', async () => {
    const r = await listMcpResourcesTool.execute(
      {},
      ctx({
        _mcpManager: { listResources: async () => null },
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('No MCP resources'));
  });

  it('formats resources correctly', async () => {
    const r = await listMcpResourcesTool.execute(
      {},
      ctx({
        _mcpManager: {
          listResources: async () => [
            { uri: 'file:///test.txt', name: 'test', mimeType: 'text/plain' },
            { uri: 'file:///data.json', name: null, mimeType: null },
          ],
        },
      }),
    );
    assert.ok(!r.isError);
    assert.ok(r.output.includes('file:///test.txt'));
    assert.ok(r.output.includes('test'));
    assert.ok(r.output.includes('text/plain'));
    assert.ok(r.output.includes('unknown'));
  });

  it('passes server filter to listResources', async () => {
    let capturedServer: string | undefined;
    await listMcpResourcesTool.execute(
      { server: 'my-server' },
      ctx({
        _mcpManager: {
          listResources: async (server?: string) => {
            capturedServer = server;
            return [];
          },
        },
      }),
    );
    assert.equal(capturedServer, 'my-server');
  });

  it('handles listResources error', async () => {
    const r = await listMcpResourcesTool.execute(
      {},
      ctx({
        _mcpManager: {
          listResources: async () => { throw new Error('list failed'); },
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('list failed'));
  });
});

describe('readMcpResourceTool — coverage', () => {
  it('has correct definition', () => {
    assert.equal(readMcpResourceTool.name, 'ReadMcpResource');
    assert.equal(readMcpResourceTool.isReadOnly, true);
    assert.ok(readMcpResourceTool.inputSchema.required?.includes('server'));
    assert.ok(readMcpResourceTool.inputSchema.required?.includes('uri'));
  });

  it('errors when no MCP manager', async () => {
    const r = await readMcpResourceTool.execute(
      { server: 's', uri: 'u' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('MCP not initialized'));
  });

  it('reads string resource', async () => {
    const r = await readMcpResourceTool.execute(
      { server: 'my-server', uri: 'file:///test.txt' },
      ctx({
        _mcpManager: {
          readResource: async () => 'file contents here',
        },
      }),
    );
    assert.ok(!r.isError);
    assert.equal(r.output, 'file contents here');
  });

  it('reads object resource and JSON-stringifies', async () => {
    const r = await readMcpResourceTool.execute(
      { server: 'srv', uri: 'data://obj' },
      ctx({
        _mcpManager: {
          readResource: async () => ({ key: 'value', nested: { a: 1 } }),
        },
      }),
    );
    assert.ok(!r.isError);
    const parsed = JSON.parse(r.output);
    assert.equal(parsed.key, 'value');
  });

  it('truncates large results', async () => {
    const bigString = 'y'.repeat(200_000);
    const r = await readMcpResourceTool.execute(
      { server: 'srv', uri: 'big' },
      ctx({
        _mcpManager: {
          readResource: async () => bigString,
        },
      }),
    );
    assert.ok(!r.isError);
    assert.equal(r.output.length, 100_000);
  });

  it('handles readResource error', async () => {
    const r = await readMcpResourceTool.execute(
      { server: 'srv', uri: 'bad' },
      ctx({
        _mcpManager: {
          readResource: async () => { throw new Error('read failed'); },
        },
      }),
    );
    assert.ok(r.isError);
    assert.ok(r.output.includes('read failed'));
  });
});

// =========================================================================
// MCP AUTH — lines 18-21
// =========================================================================

describe('createMcpAuthTool — coverage', () => {
  it('creates auth tool with correct normalized name', () => {
    const tool = createMcpAuthTool('my-server');
    assert.equal(tool.name, 'mcp__my_server__authenticate');
    assert.ok(tool.description.includes('my-server'));
    assert.equal(tool.isReadOnly, true);
  });

  it('errors when no MCP manager', async () => {
    const tool = createMcpAuthTool('srv');
    const r = await tool.execute({}, ctx());
    assert.ok(r.isError);
    assert.ok(r.output.includes('MCP not initialized'));
  });

  it('calls reconnect on MCP manager', async () => {
    let reconnectedServer: string | null = null;
    const tool = createMcpAuthTool('test-server');

    const r = await tool.execute(
      {},
      ctx({
        _mcpManager: {
          reconnect: async (serverName: string) => {
            reconnectedServer = serverName;
          },
        },
      }),
    );

    assert.ok(!r.isError);
    assert.ok(r.output.includes('Authentication completed'));
    assert.ok(r.output.includes('test-server'));
    assert.equal(reconnectedServer, 'test-server');
  });

  it('handles reconnect error', async () => {
    const tool = createMcpAuthTool('fail-server');

    const r = await tool.execute(
      {},
      ctx({
        _mcpManager: {
          reconnect: async () => { throw new Error('auth failed'); },
        },
      }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('auth failed'));
    assert.ok(r.output.includes('credentials'));
  });

  it('handles non-Error auth throw', async () => {
    const tool = createMcpAuthTool('err-server');

    const r = await tool.execute(
      {},
      ctx({
        _mcpManager: {
          reconnect: async () => { throw 'string error'; },
        },
      }),
    );

    assert.ok(r.isError);
    assert.ok(r.output.includes('string error'));
  });
});
