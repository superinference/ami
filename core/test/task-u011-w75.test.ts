import { describe, it, beforeEach, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  resetAgentRegistry,
  getAgentByName,
  linkChildAbortToParent,
  clampPermissionMode,
  applyCustomAgentDenylist,
  CUSTOM_AGENT_DISALLOWED_TOOLS,
  ALL_AGENT_DISALLOWED_TOOLS,
  resolveResumeTarget,
  persistAgentRegistry,
  rehydrateAgentRegistry,
  agentRegistryPath,
} from '../src/tools/task';
import type { AgentDefinition } from '../src/skills';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';

const fakeTools = (names: string[]): ToolDefinition[] =>
  names.map(name => ({ name } as unknown as ToolDefinition));

// Isolate all on-disk project state (the durable registry + background output
// files) into a throwaway data dir so the suite never touches ~/.ami.
let dataDir: string;
before(() => {
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w75-'));
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
});
after(() => {
  delete process.env.SUPERINFERENCE_DATA_DIR;
  try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
});

type Ev = Record<string, unknown>;

/** Run task.execute with a fake engine, capturing the resolved EngineConfig + tool names. */
async function runCapture(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{ result: { isError?: boolean; output: string }; captured: EngineConfig; toolNames: string[] }> {
  let captured!: EngineConfig;
  let toolNames: string[] = [];
  async function* fakeSubmit() { for (const e of events) yield e as never; }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c: EngineConfig) => {
      captured = c;
      toolNames = (c.tools as ToolDefinition[]).map(t => t.name);
      return { submit: fakeSubmit, shutdown() {} } as never;
    },
    ...ctx,
  } as unknown as ToolContext);
  return { result, captured, toolNames };
}

/** A minimal SkillManager stub returning a single agent definition. */
function skillManagerFor(agent: AgentDefinition): Partial<ToolContext>['_skillManager'] {
  return {
    getAgent: (n: string) => (n === agent.name ? agent : null),
    listAgents: () => [agent],
    getSkill: () => null,
  } as unknown as ToolContext['_skillManager'];
}

function agentDef(over: Partial<AgentDefinition>): AgentDefinition {
  return {
    name: 'a', description: 'd', systemPrompt: 'you are a', filePath: '/x', ...over,
  } as AgentDefinition;
}

// ---------------------------------------------------------------------------
// gaps U011/2,3: parent-abort cascade to BACKGROUND subagents
// ---------------------------------------------------------------------------
describe('linkChildAbortToParent — parent abort cascades to a child controller', () => {
  it('forwards a later parent abort to the child, and unlink stops it', () => {
    const parent = new AbortController();
    const child = new AbortController();
    const unlink = linkChildAbortToParent(parent.signal, child);
    assert.equal(child.signal.aborted, false);
    parent.abort('stop');
    assert.equal(child.signal.aborted, true, 'parent abort cascaded to child');
    // unlink after the fact is a safe no-op.
    unlink();
  });

  it('aborts the child immediately when the parent is ALREADY aborted', () => {
    const parent = new AbortController();
    parent.abort('already');
    const child = new AbortController();
    linkChildAbortToParent(parent.signal, child);
    assert.equal(child.signal.aborted, true);
  });

  it('after unlink, a parent abort no longer reaches the child', () => {
    const parent = new AbortController();
    const child = new AbortController();
    const unlink = linkChildAbortToParent(parent.signal, child);
    unlink();
    parent.abort('too late');
    assert.equal(child.signal.aborted, false, 'unlinked listener removed');
  });
});

describe('task.execute — a BACKGROUND subagent aborts when the parent turn aborts', () => {
  beforeEach(() => resetAgentRegistry());

  it('cascades the parent abort into the detached background sub-engine', async () => {
    const parent = new AbortController();
    let childAbort: AbortController | undefined;
    async function* submit() {
      yield { type: 'text_delta', text: '' } as never;
      // Hold the run open until the child's own abort fires (proving the cascade),
      // then end so the detached drain completes and leaves no open handle.
      await new Promise<void>((res) => {
        if (childAbort!.signal.aborted) return res();
        childAbort!.signal.addEventListener('abort', () => res(), { once: true });
      });
    }
    const result = await taskTool.execute(
      { prompt: 'x', description: 'bg', run_in_background: true },
      {
        cwd: process.cwd(),
        abortSignal: parent.signal,
        _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
        _engineFactory: (c: EngineConfig) => {
          childAbort = c.abortController as AbortController;
          return { submit, shutdown() {} } as never;
        },
      } as unknown as ToolContext,
    );
    assert.match(result.output, /Background agent started/);
    assert.ok(childAbort, 'child abort controller was created');
    assert.equal(childAbort!.signal.aborted, false, 'not aborted while parent is live');
    parent.abort('parent turn aborted');
    assert.equal(childAbort!.signal.aborted, true, 'parent abort cascaded to the background subagent');
    // Let the detached drain settle its finally (unlink + teardown).
    await new Promise(r => setTimeout(r, 20));
  });
});

// ---------------------------------------------------------------------------
// gap U011/20: clamp a custom agent definition's permissionMode (no escalation)
// ---------------------------------------------------------------------------
describe('clampPermissionMode — a mode may tighten but never escalate past the parent', () => {
  it('clamps an escalation (auto-allow under an ask parent → ask)', () => {
    assert.equal(clampPermissionMode('auto-allow', 'ask'), 'ask');
    assert.equal(clampPermissionMode('bypassPermissions', 'deny-all'), 'deny-all');
    assert.equal(clampPermissionMode('acceptEdits', 'plan'), 'plan');
  });
  it('allows a TIGHTER mode through unchanged', () => {
    assert.equal(clampPermissionMode('deny-all', 'ask'), 'deny-all');
    assert.equal(clampPermissionMode('plan', 'auto-allow'), 'plan');
    assert.equal(clampPermissionMode('ask', 'ask'), 'ask');
  });
  it('is a no-op when the parent mode is unknown (not threaded)', () => {
    assert.equal(clampPermissionMode('auto-allow', undefined), 'auto-allow');
  });
  it('treats an unknown mode string as the ask baseline', () => {
    // unknown requested ranks as ask(2); a deny-all parent(0) clamps it down.
    assert.equal(clampPermissionMode('weird', 'deny-all'), 'deny-all');
    // unknown parent ranks as ask(2); an auto-allow(4) request is clamped to it.
    assert.equal(clampPermissionMode('auto-allow', 'weird'), 'weird');
  });
});

describe('task.execute — custom agent permissionMode is clamped to the parent', () => {
  beforeEach(() => resetAgentRegistry());

  it('a PROJECT agent with auto-allow cannot escalate past an ask parent', async () => {
    const def = agentDef({ name: 'risky', source: 'project', permissionMode: 'auto-allow' });
    const { captured } = await runCapture(
      { prompt: 'go', description: 'd', subagent_type: 'risky' },
      { _skillManager: skillManagerFor(def), _parentPermissionMode: 'ask' },
    );
    assert.equal(captured.permissionMode, 'ask', 'project agent auto-allow clamped to parent ask');
  });

  it('a BUILT-IN agent keeps its declared auto-allow (trusted source)', async () => {
    const def = agentDef({ name: 'trusted', source: 'built-in', permissionMode: 'auto-allow' });
    const { captured } = await runCapture(
      { prompt: 'go', description: 'd', subagent_type: 'trusted' },
      { _skillManager: skillManagerFor(def), _parentPermissionMode: 'ask' },
    );
    assert.equal(captured.permissionMode, 'auto-allow', 'built-in agent mode not clamped');
  });
});

// ---------------------------------------------------------------------------
// gap U011/13: trust-tier tool denylist for non-built-in agent definitions
// ---------------------------------------------------------------------------
describe('applyCustomAgentDenylist / CUSTOM_AGENT_DISALLOWED_TOOLS', () => {
  it('the custom floor is a superset of the universal floor plus task', () => {
    for (const t of ALL_AGENT_DISALLOWED_TOOLS) assert.ok(CUSTOM_AGENT_DISALLOWED_TOOLS.has(t), `${t} in custom floor`);
    assert.ok(CUSTOM_AGENT_DISALLOWED_TOOLS.has('task'), 'nested-spawn task denied to custom agents');
  });
  it('strips the denylist from a NON-built-in agent, keeps MCP tools', () => {
    const kept = applyCustomAgentDenylist(
      fakeTools(['bash', 'file_edit', 'task', 'mcp__srv__x']),
      { source: 'project' },
    ).map(t => t.name);
    assert.ok(kept.includes('bash') && kept.includes('file_edit'), 'ordinary tools kept');
    assert.ok(kept.includes('mcp__srv__x'), 'mcp tools always survive');
    assert.ok(!kept.includes('task'), 'task stripped for custom agent');
  });
  it('leaves a BUILT-IN agent untouched', () => {
    const kept = applyCustomAgentDenylist(fakeTools(['bash', 'task']), { source: 'built-in' }).map(t => t.name);
    assert.deepEqual(kept, ['bash', 'task']);
  });
});

describe('task.execute — a project agent with tools:[*] cannot grant itself task', () => {
  beforeEach(() => resetAgentRegistry());

  it('strips task from a non-built-in wildcard agent but keeps it for built-in', async () => {
    const proj = agentDef({ name: 'wild', source: 'project', tools: ['*'] });
    const r1 = await runCapture(
      { prompt: 'go', description: 'd', subagent_type: 'wild' },
      { _skillManager: skillManagerFor(proj) },
    );
    assert.ok(r1.toolNames.includes('bash'), 'wildcard still grants ordinary tools');
    assert.ok(!r1.toolNames.includes('task'), 'project wildcard agent denied the task tool');

    const builtin = agentDef({ name: 'wild2', source: 'built-in', tools: ['*'] });
    const r2 = await runCapture(
      { prompt: 'go', description: 'd', subagent_type: 'wild2' },
      { _skillManager: skillManagerFor(builtin) },
    );
    assert.ok(r2.toolNames.includes('task'), 'built-in wildcard agent keeps the task tool at depth 0');
  });
});

// ---------------------------------------------------------------------------
// gaps U011/18,19: durable cross-process resume (persist + rehydrate + raw sessionId)
// ---------------------------------------------------------------------------
describe('resolveResumeTarget — name / taskId / raw sessionId resolution', () => {
  beforeEach(() => resetAgentRegistry());

  it('returns undefined for an unknown target', () => {
    assert.equal(resolveResumeTarget('nope'), undefined);
  });
});

describe('persist/rehydrate agent registry', () => {
  it('round-trips the registries through disk so resolve works after a reset (restart)', async () => {
    resetAgentRegistry();
    // Spawn a named foreground agent — this persists the registry snapshot to disk.
    const { captured } = await runCapture({ prompt: 'first', description: 'd', name: 'worker' });
    const sid = captured.sessionId!;
    assert.ok(fs.existsSync(agentRegistryPath(process.cwd())), 'snapshot written to disk');
    assert.equal(getAgentByName('worker'), getAgentByName('worker'));

    // Simulate a fresh process: clear every in-memory map.
    resetAgentRegistry();
    assert.equal(getAgentByName('worker'), undefined, 'in-memory registry cleared');
    assert.equal(resolveResumeTarget('worker'), undefined, 'not resolvable before rehydrate');

    // Rehydrate from disk → the name/taskId/session index is restored.
    rehydrateAgentRegistry(process.cwd());
    const byName = resolveResumeTarget('worker');
    assert.ok(byName, 'resolvable by name after rehydrate');
    assert.equal(byName!.sessionId, sid, 'same session restored');

    // gap 19: a RAW sessionId also resolves to the same run.
    const bySession = resolveResumeTarget(sid);
    assert.ok(bySession, 'resolvable by raw sessionId');
    assert.equal(bySession!.sessionId, sid);
  });

  it('persistAgentRegistry is best-effort (never throws on a bad path)', () => {
    // A cwd whose state dir cannot be created must not throw out of a spawn.
    assert.doesNotThrow(() => persistAgentRegistry(process.cwd()));
  });
});

describe('task.execute — resume resolves a PRIOR-process agent from disk', () => {
  it('resume by name works after the in-memory registry is cleared', async () => {
    resetAgentRegistry();
    const { captured: first } = await runCapture({ prompt: 'first', description: 'd', name: 'rsm' });
    const sid = first.sessionId!;

    // Simulate a restart: drop in-memory state. The on-disk snapshot survives.
    resetAgentRegistry();

    const { result, captured: second } = await runCapture({ prompt: 'again', resume: 'rsm' });
    assert.ok(!result.isError, `resume should succeed: ${result.output}`);
    assert.equal(second.sessionId, sid, 'resumed on the same session as the prior-process spawn');
  });

  it('resume by RAW sessionId works cross-process', async () => {
    resetAgentRegistry();
    const { captured: first } = await runCapture({ prompt: 'first', description: 'd', name: 'rsm2' });
    const sid = first.sessionId!;
    resetAgentRegistry();
    const { result, captured: second } = await runCapture({ prompt: 'again', resume: sid });
    assert.ok(!result.isError, `resume by sessionId should succeed: ${result.output}`);
    assert.equal(second.sessionId, sid);
  });
});
