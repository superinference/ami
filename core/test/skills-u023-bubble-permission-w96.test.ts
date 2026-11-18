// U023-w96 — `bubble` permission mode as an AGENT-DEFINITION knob (parity with
// Claude Code's async/background-agent `bubble` permission mode, runAgent.ts:L440-L463).
//
// AMI already resolved the `'bubble'` sentinel on the fork/`permission_mode` TOOL-INPUT
// path (resolveBubblePermissionMode + FORK_BUBBLE_PERMISSION_MODE). This closes the
// matching gap for a NAMED agent DEFINITION: an agent file may now DECLARE
// `permission-mode: bubble` (accepted as a declarable mode, surfaced on
// AgentDefinition.permissionMode) and the task spawn path resolves it to the parent's
// LIVE stance so every permission decision bubbles up to the parent terminal instead
// of auto-denying — rather than being rejected as an unknown mode.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  AGENT_PERMISSION_MODES,
  AGENT_DECLARABLE_PERMISSION_MODES,
  BUBBLE_PERMISSION_MODE,
  validateAgentDefinition,
  type AgentDefinition,
  type SkillManagerLike,
} from '../src/skills';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';
import type { SubagentContext } from '../src/hooks';

// ---------------------------------------------------------------------------
// Constants + validator surface
describe('U023-w96 — bubble is a declarable (not an engine) permission mode', () => {
  it('BUBBLE_PERMISSION_MODE is the "bubble" sentinel', () => {
    assert.equal(BUBBLE_PERMISSION_MODE, 'bubble');
  });

  it('AGENT_PERMISSION_MODES (engine-lockstep set) does NOT include bubble', () => {
    assert.deepEqual([...AGENT_PERMISSION_MODES], ['auto-allow', 'ask', 'deny-all', 'plan']);
    assert.ok(!(AGENT_PERMISSION_MODES as readonly string[]).includes('bubble'));
  });

  it('AGENT_DECLARABLE_PERMISSION_MODES adds the bubble sentinel on top of the engine modes', () => {
    assert.deepEqual([...AGENT_DECLARABLE_PERMISSION_MODES], ['auto-allow', 'ask', 'deny-all', 'plan', 'bubble']);
  });

  it('validateAgentDefinition accepts permissionMode "bubble" with no warning', () => {
    const r = validateAgentDefinition({
      name: 'bg-worker',
      systemPrompt: 'a background worker that bubbles permission prompts to the parent',
      permissionMode: 'bubble',
    });
    assert.equal(r.valid, true);
    assert.equal(r.errors.length, 0);
    assert.ok(
      !r.warnings.some((w) => w.includes('permission mode')),
      `no permission-mode warning expected, got: ${r.warnings.join(' | ')}`,
    );
  });

  it('still warns on an unrecognized mode and lists bubble among the valid options', () => {
    const r = validateAgentDefinition({
      name: 'bad',
      systemPrompt: 'a sufficiently long system prompt body',
      // @ts-expect-error — exercise an invalid frontmatter value at runtime
      permissionMode: 'yolo',
    });
    assert.ok(r.warnings.some((w) => w.includes('Unknown permission mode "yolo"')));
    assert.ok(r.warnings.some((w) => w.includes('bubble')));
  });
});

// ---------------------------------------------------------------------------
// End-to-end: a named agent DEFINITION declaring permissionMode:'bubble' resolves
// to the parent's live stance at spawn time (not an error, not a fixed mode).
type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  cfg: EngineConfig | null;
}

async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
): Promise<Harness> {
  let cfg: EngineConfig | null = null;
  async function* fakeSubmit(_p: string) {
    for (const e of [{ type: 'text_delta', text: 'ok' }] as Ev[]) yield e as never;
  }
  const hookManager = {
    executeSubagentStart: async (_c: SubagentContext) => {},
    executeSubagentStop: async () => {},
    executeTeammateIdle: async () => {},
  };
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: () => {},
    _hookManager: hookManager as unknown as ToolContext['_hookManager'],
    _engineFactory: (c) => { cfg = c; void (c.tools as ToolDefinition[]); return { submit: fakeSubmit, shutdown: () => {} }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg };
}

function userAgentSkillManager(agent: AgentDefinition): SkillManagerLike {
  return {
    getAgent: (name: string) => (name === agent.name ? agent : null),
    listAgents: () => [agent],
  } as unknown as SkillManagerLike;
}

beforeEach(() => { resetAgentRegistry(); });
afterEach(() => { resetAgentRegistry(); });

describe('U023-w96 — a bubble agent definition adopts the parent live stance', () => {
  function bubbleAgent(): AgentDefinition {
    return {
      name: 'bubbler', description: 'd', systemPrompt: 'be a background bubbler',
      filePath: '/x/bubbler.md', source: 'project', permissionMode: 'bubble',
    };
  }

  it('resolves to the parent live permission mode (not an error, not literal bubble)', async () => {
    const h = await runTask(
      { prompt: 'do x', subagent_type: 'bubbler' },
      { _parentPermissionMode: 'acceptEdits', _skillManager: userAgentSkillManager(bubbleAgent()) },
    );
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.permissionMode, 'acceptEdits');
  });

  it('falls back to ask when the parent mode is not threaded', async () => {
    const h = await runTask(
      { prompt: 'do x', subagent_type: 'bubbler' },
      { _skillManager: userAgentSkillManager(bubbleAgent()) },
    );
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.permissionMode, 'ask');
  });

  it('a non-bubble agent-def mode still clamps and is unaffected', async () => {
    const agent: AgentDefinition = {
      name: 'strict', description: 'd', systemPrompt: 'be strict',
      filePath: '/x/strict.md', source: 'project', permissionMode: 'deny-all',
    };
    const h = await runTask(
      { prompt: 'do x', subagent_type: 'strict' },
      { _parentPermissionMode: 'ask', _skillManager: userAgentSkillManager(agent) },
    );
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.permissionMode, 'deny-all');
  });
});
