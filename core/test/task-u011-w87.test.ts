import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  resolveSubagentDefaultModel,
} from '../src/tools/task';
import { resetTeam } from '../src/tools/team';
import { resolveModelAlias } from '../src/model-registry';
import type { ToolContext } from '../src/types';

type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  model: string | undefined;
}

/**
 * Run the task tool against a fake engine and capture the model the sub-engine was
 * created with (subConfig.provider.model). Mirrors the harness used by the sibling
 * task-u011 wave tests.
 */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<Harness> {
  let model: string | undefined;
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
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: () => {},
    _hookManager: hookManager as unknown as ToolContext['_hookManager'],
    _engineFactory: (c) => {
      model = c.provider?.model;
      return { submit: fakeSubmit, shutdown: () => {} };
    },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, model };
}

beforeEach(() => { resetAgentRegistry(); resetTeam(); });
afterEach(() => { resetAgentRegistry(); resetTeam(); });

// ---------------------------------------------------------------------------
// Gap U011/w87: persistent subagentDefaultModel config knob with tri-state
// "Default → follow leader" semantics (claudeRef
// tools/shared/spawnMultiAgent.ts:L72-82 getDefaultTeammateModel +
// utils/swarm/teammateModel.ts:L4-10 persistent teammateDefaultModel).
// ---------------------------------------------------------------------------
describe('resolveSubagentDefaultModel — tri-state knob resolution', () => {
  it('returns undefined when the knob is unset (undefined)', () => {
    assert.equal(resolveSubagentDefaultModel(undefined, 'claude-opus-4-8'), undefined);
  });

  it('follows the leader (parent model) when the knob is null ("Default")', () => {
    assert.equal(resolveSubagentDefaultModel(null, 'claude-opus-4-8'), 'claude-opus-4-8');
    // Follow-leader with no parent model yields undefined (nothing to inherit).
    assert.equal(resolveSubagentDefaultModel(null, undefined), undefined);
  });

  it('resolves a configured family alias through the canonical alias table', () => {
    assert.equal(
      resolveSubagentDefaultModel('haiku', 'claude-opus-4-8'),
      resolveModelAlias('haiku'),
    );
  });

  it('resolves the inherit sentinel to the parent model', () => {
    assert.equal(resolveSubagentDefaultModel('inherit', 'claude-opus-4-8'), 'claude-opus-4-8');
    assert.equal(resolveSubagentDefaultModel('  INHERIT  ', 'claude-sonnet-4'), 'claude-sonnet-4');
  });

  it('passes a full model id through verbatim', () => {
    assert.equal(
      resolveSubagentDefaultModel('my-provider/custom-model', 'claude-opus-4-8'),
      'my-provider/custom-model',
    );
  });

  it('treats a blank/whitespace-only knob as unset', () => {
    assert.equal(resolveSubagentDefaultModel('   ', 'claude-opus-4-8'), undefined);
    assert.equal(resolveSubagentDefaultModel('', 'claude-opus-4-8'), undefined);
  });
});

describe('subagentDefaultModel knob — end-to-end through the task tool', () => {
  it('applies a configured default model to a spawn that names none', async () => {
    const h = await runTask(
      { prompt: 'do a thing', mode: 'general' },
      { _subagentDefaultModel: 'haiku' },
    );
    assert.equal(h.isError, undefined);
    assert.equal(h.model, resolveModelAlias('haiku'));
  });

  it('null knob ("Default") keeps the parent model (follow the leader)', async () => {
    const h = await runTask(
      { prompt: 'do a thing', mode: 'general' },
      { _subagentDefaultModel: null },
    );
    assert.equal(h.model, 'claude-opus-4-8');
  });

  it('unset knob keeps the inherited parent model (provider-aware fallback)', async () => {
    const h = await runTask({ prompt: 'do a thing', mode: 'general' });
    assert.equal(h.model, 'claude-opus-4-8');
  });

  it('an explicit per-call model overrides the configured default knob', async () => {
    const h = await runTask(
      { prompt: 'do a thing', mode: 'general', model: 'sonnet' },
      { _subagentDefaultModel: 'haiku' },
    );
    // The per-call model wins; the knob does not get a say.
    assert.equal(h.model, resolveModelAlias('sonnet'));
    assert.notEqual(h.model, resolveModelAlias('haiku'));
  });
});
