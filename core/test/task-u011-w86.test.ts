import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import { resetTeam } from '../src/tools/team';
import { getRuntimeMainLoopModel } from '../src/model-registry';
import type { ToolContext, EngineConfig } from '../src/types';

// ---------------------------------------------------------------------------
// Gap U011/w86: 'inherit' routes through permission-mode-aware runtime model
// resolution (claudeRef utils/model/agent.ts:L80-88 — getRuntimeMainLoopModel).
// When a subagent inherits the parent model, it must re-resolve it the way the
// main loop would at spawn time (e.g. the deferred `opusplan` alias runs Opus in
// plan mode and Sonnet during execution) instead of copying the raw string.
// ---------------------------------------------------------------------------

type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  cfg: EngineConfig | null;
}

/** Run the task tool against a fake engine, capturing the sub-engine config. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<Harness> {
  let cfg: EngineConfig | null = null;
  async function* fakeSubmit(_p: string) {
    for (const e of events) yield e as never;
  }
  const hookManager = {
    executeSubagentStart: async () => {},
    executeSubagentStop: async () => {},
    executeTeammateIdle: async () => {},
  };
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'opusplan' },
    _emitChild: () => {},
    _hookManager: hookManager as unknown as ToolContext['_hookManager'],
    _engineFactory: (c) => { cfg = c; return { submit: fakeSubmit, shutdown: () => {} }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg };
}

beforeEach(() => { resetAgentRegistry(); resetTeam(); delete process.env.SUPERINFERENCE_SUBAGENT_MODEL; });
afterEach(() => { resetAgentRegistry(); resetTeam(); delete process.env.SUPERINFERENCE_SUBAGENT_MODEL; });

describe('getRuntimeMainLoopModel — permission-mode-aware runtime resolution', () => {
  it('resolves the deferred opusplan alias to Opus in plan mode', () => {
    assert.equal(
      getRuntimeMainLoopModel({ permissionMode: 'plan', mainLoopModel: 'opusplan' }),
      'claude-opus-4',
    );
  });

  it('resolves the deferred opusplan alias to Sonnet outside plan mode', () => {
    assert.equal(
      getRuntimeMainLoopModel({ permissionMode: 'acceptEdits', mainLoopModel: 'opusplan' }),
      'claude-sonnet-4',
    );
    assert.equal(
      getRuntimeMainLoopModel({ mainLoopModel: 'opusplan' }),
      'claude-sonnet-4',
    );
  });

  it('is case/whitespace tolerant for the opusplan alias', () => {
    assert.equal(
      getRuntimeMainLoopModel({ permissionMode: 'plan', mainLoopModel: '  OpusPlan ' }),
      'claude-opus-4',
    );
  });

  it('passes a concrete model id through unchanged', () => {
    assert.equal(
      getRuntimeMainLoopModel({ permissionMode: 'plan', mainLoopModel: 'claude-opus-4-8' }),
      'claude-opus-4-8',
    );
  });

  it('returns undefined when no model is configured', () => {
    assert.equal(getRuntimeMainLoopModel({ mainLoopModel: undefined }), undefined);
  });
});

describe('task inherit path — re-resolves the inherited model by permission mode', () => {
  it('a plan-mode subagent inheriting an opusplan parent runs Opus', async () => {
    const h = await runTask({ prompt: 'do x', model: 'inherit', permission_mode: 'plan' });
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.provider?.model, 'claude-opus-4');
  });

  it('an executing subagent inheriting an opusplan parent runs Sonnet', async () => {
    const h = await runTask({ prompt: 'do x', model: 'inherit', permission_mode: 'acceptEdits' });
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.provider?.model, 'claude-sonnet-4');
  });

  it('an implicit inherit (no model specified) also routes through runtime resolution', async () => {
    const h = await runTask({ prompt: 'do x', permission_mode: 'plan' });
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.provider?.model, 'claude-opus-4');
  });

  it('an explicitly-pinned model is NOT rewritten by runtime resolution', async () => {
    const h = await runTask({ prompt: 'do x', model: 'claude-opus-4-8', permission_mode: 'acceptEdits' });
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.provider?.model, 'claude-opus-4-8');
  });

  it('a concrete inherited parent model passes through unchanged', async () => {
    const h = await runTask(
      { prompt: 'do x', model: 'inherit', permission_mode: 'plan' },
      { _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-sonnet-4-5' } },
    );
    assert.equal(h.isError, undefined);
    assert.equal(h.cfg?.provider?.model, 'claude-sonnet-4-5');
  });
});
